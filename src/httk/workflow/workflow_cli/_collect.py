"""The collect command."""

import argparse
import contextlib
import json
import re
from collections.abc import Sequence
from dataclasses import fields, is_dataclass, replace
from pathlib import Path
from typing import Any, cast

from httk.core import Run, RunEdge
from httk.core.storage import content_id, resolve_storage_record

from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, CollectedJob, collect, job_records
from ..errors import SealError
from ..id_keys import UnstableIdentityError, ledger_key
from ..seals import default_workspace_keys
from ._common import *
from ._common import _LOGGER, _leaf, _local_root

_CONTENT_ID_RE = re.compile(r"[0-9a-f]{64}\Z")


def _edge_counts(run: Run) -> dict[str, int]:
    return {side: len(getattr(run, side)) for side in ("inputs", "artifacts", "outputs")}


def _stored_entry_id(store: Any, entry_type: str, entry_id: str) -> str | None:
    """Resolve a stored public id or content-id through the destination store."""

    family = next(
        (candidate for candidate in store.entry_records if getattr(candidate, "type", None) == entry_type),
        None,
    )
    if family is None:
        return None

    # ``fetch_entry`` is content-id based.  Public ids are queried through the
    # backing records instead, so an already stored entry id is preserved as-is
    # rather than being mistaken for a content id.
    for record_type in store.entry_records[family]:
        searcher = store.searcher()
        variable = searcher.variable(record_type)
        searcher.add(variable.id == entry_id)
        searcher.output(variable, "entry")
        try:
            fetched = next(iter(searcher)).values[0]
        except StopIteration:
            continue
        if isinstance(getattr(fetched, "id", None), str):
            return entry_id

    fetched = store.fetch_entry(family, entry_id, eager=True)
    fetched_id = getattr(fetched, "id", None)
    return fetched_id if isinstance(fetched_id, str) else None


def _resolve_entry_id(store: Any, remap: dict[str, str], entry_type: str, entry_id: str) -> str:
    replacement = remap.get(entry_id)
    if replacement is not None:
        return replacement
    resolved = _stored_entry_id(store, entry_type, entry_id)
    if resolved is not None:
        return resolved
    if _CONTENT_ID_RE.fullmatch(entry_id) is not None:
        raise ValueError(f"unresolved provenance reference {entry_type}/{entry_id}")
    # Provenance edges may intentionally point outside this store.  Preserve
    # those loose served-entry references instead of requiring a local row.
    return entry_id


def _rewrite_run_edges(run: Run, store: Any, remap: dict[str, str]) -> Run:
    """Rewrite content-id references to ids minted by the destination store."""

    def _rewrite_edges(source_edges: tuple[RunEdge, ...]) -> tuple[RunEdge, ...]:
        rewritten: list[RunEdge] = []
        for edge in source_edges:
            rewritten.append(
                RunEdge(edge.label, edge.entry_type, _resolve_entry_id(store, remap, edge.entry_type, edge.entry_id))
            )
        return tuple(rewritten)

    return replace(
        run,
        inputs=_rewrite_edges(run.inputs),
        artifacts=_rewrite_edges(run.artifacts),
        outputs=_rewrite_edges(run.outputs),
    )


def _collected_mapping(item: CollectedJob) -> dict[str, object]:
    outputs = item.outputs

    def _output_id(value: object) -> str:
        entry_id = getattr(value, "id", None)
        if isinstance(entry_id, str):
            return entry_id
        try:
            return content_id(value)
        except (TypeError, ValueError):
            return ""

    mapping: dict[str, object] = {
        "format": "httk-workflow-collected",
        "format_version": 2,
        "job_id": item.record.job_id,
        "job_key": item.record.job_key,
        "workflow": item.workflow_id,
        "outputs": {
            role: {"type": getattr(value, "type", ""), "id": _output_id(value)} for role, value in outputs.items()
        },
        "unfulfilled": list(item.unfulfilled),
        "missing_collector": item.missing_collector,
        "run": {
            "workflow_declaration_uri": item.run.workflow_declaration_uri,
            "edges": _edge_counts(item.run),
        },
        "products": [
            {
                "source_type": product.source_type,
                "source_id": product.source_id,
                "target_type": product.target_type,
                "target_id": product.target_id,
                "label": product.label,
                "workflow_declaration_uri": product.workflow_declaration_uri,
            }
            for product in item.products
        ],
    }
    if item.products_unlinked:
        mapping["products_unlinked"] = list(item.products_unlinked)
    if item.collector_exit_status is not None:
        mapping["collector_exit_status"] = item.collector_exit_status
    if item.identity_stable is not None:
        mapping["identity_stable"] = item.identity_stable
    return mapping


def _emit_collect_summary(
    *, collected: int, degraded: int, unfulfilled_roles: int, storage_errors: int, skipped_unreadable: int
) -> int:
    """Print the trailing collect-summary line and return the sweep exit code.

    The exit code is ``0`` only when nothing was degraded, no store failed, and
    nothing was skipped for an unreadable ``job.json``; unfulfilled roles alone
    do not fail the sweep, they are reported for triage.

    :param collected: Count the jobs collected without degradation.
    :param degraded: Count the degraded jobs.
    :param unfulfilled_roles: Count the declared output roles left unfulfilled.
    :param storage_errors: Count the jobs a ``--into`` store could not persist.
    :param skipped_unreadable: Count the jobs dropped for an unreadable payload.
    :return: ``0`` on a fully clean sweep, ``1`` otherwise.
    """

    print(
        json.dumps(
            {
                "format": "httk-workflow-collect-summary",
                "format_version": 2,
                "collected": collected,
                "degraded": degraded,
                "unfulfilled_roles": unfulfilled_roles,
                "storage_errors": storage_errors,
                "skipped_unreadable": skipped_unreadable,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if degraded == 0 and storage_errors == 0 and skipped_unreadable == 0 else 1


def _storage_layout(items: list[CollectedJob]) -> tuple[dict[type, tuple[type, ...]], dict[int, str]]:
    """Resolve the lazy core entry registry for the values this sweep stores."""

    from httk.core.register import known_entry_families, known_entry_records, resolve_entry_family, resolve_entry_record

    required_types = {"records", "runs"}
    failures: dict[int, str] = {}
    for index, item in enumerate(items):
        for value in item.outputs.values():
            entry_type = getattr(value, "type", None)
            if not isinstance(entry_type, str):
                failures[index] = "cannot store an output without a string entry type"
                continue
            required_types.add(entry_type)
        for edge in (*item.run.inputs, *item.run.artifacts, *item.run.outputs):
            required_types.add(edge.entry_type)
        for product in item.products:
            required_types.add(product.source_type)
            required_types.add(product.target_type)

    configurations: dict[str, tuple[type, tuple[type, ...]]] = {}
    for entry_type in required_types:
        family_name: str | None = None
        import_failures: list[BaseException] = []
        for candidate in known_entry_families():
            try:
                family = resolve_entry_family(candidate)
            except (ImportError, ModuleNotFoundError) as exc:
                import_failures.append(exc)
                continue
            if getattr(family, "type", None) == entry_type:
                family_name = candidate
                break
        if family_name is None:
            detail = f": {import_failures[-1]}" if import_failures else ""
            message = f"cannot store entry type {entry_type!r}: no registered entry family{detail}"
            for index, item in enumerate(items):
                if any(getattr(value, "type", None) == entry_type for value in item.outputs.values()):
                    failures[index] = message
            continue
        try:
            records = tuple(resolve_entry_record(name) for name in known_entry_records(family_name))
        except (ImportError, ModuleNotFoundError, TypeError, ValueError) as exc:
            message = f"cannot store entry type {entry_type!r}: {exc}"
            for index, item in enumerate(items):
                if any(getattr(value, "type", None) == entry_type for value in item.outputs.values()):
                    failures[index] = message
            continue
        configurations[entry_type] = (resolve_entry_family(family_name), records)

    layout: dict[type, tuple[type, ...]] = {}
    for family, records in configurations.values():
        layout[family] = records
    return layout, failures


def _id_settable(value: object) -> bool:
    """Report whether an output value can be handed an explicit entry id.

    Only a dataclass carrying its own ``id`` field — a record like ``DataRecord``
    or ``Run`` that is its own storage record — can be given a ledger id through
    :func:`dataclasses.replace`. A view over a record whose backing dataclass has
    no ``id`` field (a structure view) cannot, so the store mints its id instead.

    :param value: The collected output value.
    :return: Whether an explicit id can be threaded onto it.
    """

    return is_dataclass(value) and any(field.name == "id" for field in fields(value))


def _open_id_ledger(
    ledger_path: str, keys: Sequence[tuple[str, bytes]], id_base: str, id_series: str, items: list[CollectedJob]
) -> Any:
    """Create or open the sweep's id ledger, deriving its per-family bases.

    Each family gets a distinct base ``<id_base>.<family>`` (the store's
    ``type_in_base`` convention), so ids are ``<id_base>.<family>-<series>-<n>``
    and never collide across families — the ledger enforces id uniqueness
    globally, not per family, so one shared base would brick a second sweep.

    :param ledger_path: Where the ledger seal document lives.
    :param keys: The signing keys used to seal the ledger.
    :param id_base: The entry-id namespace base minted ids carry.
    :param id_series: The entry-id series minted ids carry.
    :param items: The collected sweep, read for the families to configure.
    :return: The open, locked ledger, or ``None`` when nothing in the sweep can
        be allocated through it (no id-settable output).
    """

    from httk.store import IdLedger

    values = [value for item in items if item.missing_collector is None for value in (*item.outputs.values(), item.run)]
    families = sorted(
        {str(getattr(value, "type", "")) for value in values if getattr(value, "type", None) and _id_settable(value)}
    )
    if not families:
        _LOGGER.info("no id-settable entries in this sweep; not creating an id ledger at %s", ledger_path)
        return None
    location = Path(ledger_path).expanduser()
    location.parent.mkdir(parents=True, exist_ok=True)
    if location.exists():
        return IdLedger.open(location, keys=keys)
    _LOGGER.warning(
        "creating id ledger %s: entry ids for this store are now allocated through it and stay stable across "
        "rebuilds. Keep this file with the store (commit it alongside it) — deleting it re-mints every id.",
        location,
    )
    bases = {family: f"{id_base}.{family}" for family in families}
    return IdLedger.create(location, bases=bases, series=id_series, keys=keys)


def _stored_ledger_id(store: Any, type_to_family: dict[str, type], entry_type: str, identity: str) -> str | None:
    """Return the public id an entry-type row already stored under one content id.

    This is how a *later* sweep aliases onto content an *earlier* sweep stored in
    the same store (the ledger maps keys, not content, so a brand-new key for
    already-stored content is invisible to it otherwise).

    :param store: The destination store.
    :param type_to_family: Entry type to its registered family class.
    :param entry_type: The value's entry type.
    :param identity: The content id to look up.
    :return: The stored public id, or ``None`` when the content is not present.
    """

    family = type_to_family.get(entry_type)
    if family is None:
        return None
    try:
        existing = store.fetch_entry(family, identity, eager=False)
    except Exception:
        # The content is not stored (the normal miss), or the family is not
        # content-addressable; either way there is no id to alias onto.
        return None
    stored = getattr(existing, "id", None)
    return stored if isinstance(stored, str) else None


def _ledger_entry_id(
    ledger: Any,
    store: Any,
    type_to_family: dict[str, type],
    item: CollectedJob,
    role: str | None,
    value: object,
    original_id: object,
    content_to_ledger: dict[str, str],
    warned_unstable: set[str],
) -> str | None:
    """Return the ledger id to save an output under, or ``None`` to let the store mint.

    An output already carrying a real (non-content) public id is never
    overwritten; a value that cannot carry an explicit id is minted by the store;
    an unstable-identity job warns once and is minted; content already allocated
    a ledger id this sweep is aliased onto that id, and anything else mints a
    fresh id keyed by the job coordinate.

    :param ledger: The open id ledger.
    :param store: The destination store, consulted for already-stored content.
    :param type_to_family: Entry type to its registered family class.
    :param item: The collected job the output belongs to.
    :param role: The declared output role, or ``None`` for the job's run.
    :param value: The output value.
    :param original_id: The value's own ``id`` before storing, if any.
    :param content_to_ledger: The sweep's content-id to ledger-id map, updated in place.
    :param warned_unstable: Job coordinates already warned about, updated in place.
    :return: The ledger id to inject, or ``None`` to fall back to store minting.
    """

    from httk.store import IdLedgerError

    if isinstance(original_id, str) and _CONTENT_ID_RE.fullmatch(original_id) is None:
        # The value already carries a user-assigned id (conforming or not); never
        # overwrite it.  Only a content-id-shaped placeholder (64 lowercase hex)
        # is treated as "no id yet" and gets allocated one here.
        # ponytail: a user id that is itself exactly 64 lowercase hex is misread as a content id and overwritten; carry an explicit "has real id" flag if that collision must be ruled out.
        return None
    if not _id_settable(value):
        return None
    try:
        key = ledger_key(item, role=role)
    except UnstableIdentityError:
        coordinate = f"{item.record.workspace_id}:{item.record.job_id}"
        if coordinate not in warned_unstable:
            warned_unstable.add(coordinate)
            _LOGGER.warning(
                "job %s has an unstable identity (a v1 tree with no manifest); its entry ids are store-minted "
                "and will NOT be stable across rebuilds.",
                coordinate,
            )
        return None
    identity = content_id(value)
    prior = ledger.lookup(key)
    if prior is not None:
        # A later job with identical content must alias onto this id, not mint.
        content_to_ledger.setdefault(identity, prior)
        return prior
    entry_type = str(getattr(value, "type", ""))
    # Alias onto an id this content already holds: allocated earlier this sweep,
    # or stored by an earlier sweep into this same store.  This is the residual
    # dedup case the plan calls out; resolving it here (before the save) aliases
    # cleanly and never writes a bogus assignment the append-only ledger cannot
    # take back.
    allocated = content_to_ledger.get(identity) or _stored_ledger_id(store, type_to_family, entry_type, identity)
    try:
        if allocated is not None:
            ledger.alias(key, allocated)
            content_to_ledger[identity] = allocated
            return allocated
        entry_id = ledger.assign(key, entry_type)
    except IdLedgerError as exc:
        if allocated is not None:
            # The content is stored under an id outside the ledger (mixed
            # ledger/no-ledger use of one store); reuse it so the store stays
            # consistent, without a ledger record.
            _LOGGER.warning("reusing store id %s for %s (not ledger-managed): %s", allocated, key, exc)
            return allocated
        _LOGGER.warning("id ledger could not allocate for %s: %s; falling back to store minting.", key, exc)
        return None
    content_to_ledger[identity] = entry_id
    return entry_id


def _store_collected(
    items: list[CollectedJob],
    path: str,
    *,
    id_base: str,
    id_series: str,
    ledger_path: str | None = None,
    ledger_keys: Sequence[tuple[str, bytes]] = (),
) -> list[dict[str, object]]:
    """Save one bounded collected sweep into a file-backed SQLite store.

    :param items: The collected jobs to store.
    :param path: The SQLite store file path.
    :param id_base: The entry-id namespace base.
    :param id_series: The entry-id campaign series.
    :param ledger_path: An id-ledger seal document to allocate stable ids
        through, or ``None`` to let the store mint ids directly.
    :param ledger_keys: The signing keys the ledger reseals with.
    :return: One report mapping per collected job.
    """

    try:
        from httk.store import Backend, EntryIdScheme, SqlStore
        from httk.store.backend.schema import SchemaError
        from httk.store.backend.sql import StorageLayoutUpgradeRequiredError
    except ImportError as exc:
        raise ValueError("--into requires httk-store with its database dependencies") from exc

    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    layout, failures = _storage_layout(items)
    requested = sorted(
        {
            str(getattr(value, "type", ""))
            for item in items
            for value in item.outputs.values()
            if getattr(value, "type", None)
        }
    )
    reports = [_collected_mapping(item) for item in items]
    with contextlib.ExitStack() as stack:
        ledger = (
            _open_id_ledger(ledger_path, ledger_keys, id_base, id_series, items) if ledger_path is not None else None
        )
        if ledger is not None:
            stack.enter_context(ledger)
        database = stack.enter_context(Backend.sqlite(target))
        content_to_ledger: dict[str, str] = {}
        warned_unstable: set[str] = set()
        type_to_family: dict[str, type] = {
            str(getattr(family, "type", "")): family for family in layout if getattr(family, "type", None)
        }
        try:
            store = SqlStore(database, entry_records=layout, entry_ids=EntryIdScheme(id_base, id_series))
        except StorageLayoutUpgradeRequiredError as exc:
            needs = ", ".join(requested) or "no entry types"
            raise ValueError(
                f"{target} was created for a different set of entry types than this sweep needs ({needs}); "
                f"its stored layout differs in {json.dumps(exc.diff, sort_keys=True, default=str)}. "
                "Collect into a new store file."
            ) from exc
        stored_output_ids: dict[int, list[str]] = {}
        remap: dict[str, str] = {}

        # Pass one stores every output and builds a sweep-wide map.  The map is
        # published only after each job transaction commits, so rolled-back
        # outputs cannot be referenced by a later job.
        for index, item in enumerate(items):
            report = reports[index]
            if item.missing_collector is not None:
                # A degraded job produced no outputs: store nothing, and never a
                # bare Run, so the store cannot fill with empty provenance.
                report["stored"] = None
                report["skipped"] = "degraded"
                continue
            error = failures.get(index)
            if error is not None:
                report["storage_error"] = error
                continue
            try:
                with store.transaction():
                    pending_remap: dict[str, str] = {}
                    entry_ids: list[str] = []
                    for role, value in item.outputs.items():
                        original_id = getattr(value, "id", None)
                        chosen = (
                            _ledger_entry_id(
                                ledger,
                                store,
                                type_to_family,
                                item,
                                role,
                                value,
                                original_id,
                                content_to_ledger,
                                warned_unstable,
                            )
                            if ledger is not None
                            else None
                        )
                        # Only an id-settable dataclass reaches a non-None id, so
                        # this replace never lands on the view-fallback branch.
                        saved = replace(cast(Any, value), id=chosen) if chosen is not None else value
                        sid = store.save(saved)
                        try:
                            fetched = store.fetch(type(saved), sid)
                        except (SchemaError, TypeError) as exc:
                            # Structure-family collectors may return a view over
                            # a storable record; fetch the record backing that
                            # view when the view class itself is not a dataclass.
                            try:
                                record_type = resolve_storage_record(saved)
                            except (SchemaError, TypeError):
                                raise exc
                            fetched = store.fetch(record_type, sid)
                        fetched_id = getattr(fetched, "id", None)
                        if not isinstance(fetched_id, str):
                            raise ValueError(f"stored output {type(value).__name__} has no string entry id")
                        # A ledger-chosen id always matches what the store stores
                        # under it: the pre-check in _ledger_entry_id aliases onto
                        # already-stored content rather than minting a colliding id,
                        # so the store never deduplicates onto a different id here.
                        entry_ids.append(fetched_id)
                        pending_remap[content_id(value)] = fetched_id
                        if isinstance(original_id, str) and original_id != fetched_id:
                            pending_remap[original_id] = fetched_id
                remap.update(pending_remap)
                stored_output_ids[index] = entry_ids
            except Exception as exc:
                report["storage_error"] = f"could not store job {item.record.job_id}: {exc}"

        # Pass two resolves all run and product references after every output
        # in the sweep has contributed to the shared remap.
        for index, item in enumerate(items):
            report = reports[index]
            if index not in stored_output_ids:
                continue
            entry_ids = stored_output_ids[index]
            try:
                with store.transaction():
                    rewritten_run = _rewrite_run_edges(item.run, store, remap)
                    rewritten_products = tuple(
                        replace(
                            product,
                            source_id=_resolve_entry_id(store, remap, product.source_type, product.source_id),
                            target_id=_resolve_entry_id(store, remap, product.target_type, product.target_id),
                        )
                        for product in item.products
                    )
                    # The run is the provenance hub every relationship points
                    # at, so it takes a ledger id too, keyed by the bare job
                    # coordinate.  The edges were already rewritten above; the
                    # run's own id is orthogonal to them.
                    run_chosen = (
                        _ledger_entry_id(
                            ledger,
                            store,
                            type_to_family,
                            item,
                            None,
                            rewritten_run,
                            getattr(rewritten_run, "id", None),
                            content_to_ledger,
                            warned_unstable,
                        )
                        if ledger is not None
                        else None
                    )
                    if run_chosen is not None:
                        rewritten_run = replace(rewritten_run, id=run_chosen)
                    run_sid = store.save(rewritten_run)
                    fetched_run = store.fetch(type(rewritten_run), run_sid)
                    run_id = getattr(fetched_run, "id", None)
                    if not isinstance(run_id, str):
                        raise ValueError("stored run has no string entry id")
                    for product in rewritten_products:
                        store.save(product)
                report["stored"] = {"entries": entry_ids, "run": run_id}
            except Exception as exc:
                report["storage_error"] = f"could not store job {item.record.job_id}: {exc}"
    return reports


def _ledger_settings(
    arguments: argparse.Namespace, workspace: Workspace
) -> tuple[str | None, Sequence[tuple[str, bytes]]]:
    """Resolve the id-ledger path and signing keys for a ``--into`` sweep.

    A ledger is on by default at ``<into>.ids.json`` so entry ids stay stable
    across rebuilds; ``--no-id-ledger`` opts out and ``--id-ledger PATH``
    relocates it. A sweep with no resolvable workspace signing key falls back to
    no ledger with a loud warning rather than failing the collect.

    :param arguments: The parsed collect arguments.
    :param workspace: The workspace the sweep collects from.
    :return: The ledger path (or ``None`` when disabled) and the signing keys.
    """

    if arguments.no_id_ledger:
        _LOGGER.warning(
            "--no-id-ledger: entry ids for %s are store-minted and will NOT be stable across rebuilds.",
            arguments.into,
        )
        return None, ()
    try:
        resolved = default_workspace_keys(workspace)
    except SealError as exc:
        _LOGGER.warning(
            "no signing key is available to seal an id ledger (%s); entry ids for %s are store-minted and will "
            "NOT be stable across rebuilds. Configure seal.keys, or pass --no-id-ledger to silence this.",
            exc,
            arguments.into,
        )
        return None, ()
    return arguments.id_ledger or f"{arguments.into}.ids.json", resolved.keys


def handle_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream collected workflow summaries, or raw job records."""

    workspace = Workspace(_local_root(arguments, context, action="collect from"), mutable=False)
    if arguments.into is not None and arguments.raw:
        raise ValueError("--into cannot be combined with --raw")
    if arguments.into is not None and arguments.id_base is None:
        raise ValueError("--id-base is required with --into")
    if arguments.degraded and arguments.raw:
        raise ValueError("--degraded filters collected summaries and cannot be combined with --raw")
    skipped = 0

    def _skip(_job_key: str) -> None:
        nonlocal skipped
        skipped += 1

    if arguments.raw:
        records = job_records(
            workspace,
            states=arguments.state or DEFAULT_COLLECT_STATES,
            placement=arguments.placement,
            on_skipped=_skip,
        )
        collected = 0
        for record in records:
            print(json.dumps(record.as_mapping(), sort_keys=True, separators=(",", ":")))
            collected += 1
        return _emit_collect_summary(
            collected=collected, degraded=0, unfulfilled_roles=0, storage_errors=0, skipped_unreadable=skipped
        )
    items = list(
        collect(
            workspace,
            states=arguments.state or DEFAULT_COLLECT_STATES,
            placement=arguments.placement,
            allow_job_collector=arguments.allow_job_collector,
            on_skipped=_skip,
        )
    )
    if arguments.into is not None:
        ledger_path, ledger_keys = _ledger_settings(arguments, workspace)
        reports = _store_collected(
            items,
            arguments.into,
            id_base=arguments.id_base,
            id_series=arguments.id_series,
            ledger_path=ledger_path,
            ledger_keys=ledger_keys,
        )
    else:
        reports = [_collected_mapping(item) for item in items]
    for item, report in zip(items, reports):
        # --degraded prints only the degraded lines; the summary below still
        # reflects the whole sweep, so the counts do not silently shrink.
        if arguments.degraded and item.missing_collector is None:
            continue
        print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    degraded = sum(1 for item in items if item.missing_collector is not None)
    return _emit_collect_summary(
        collected=len(items) - degraded,
        degraded=degraded,
        unfulfilled_roles=sum(len(item.unfulfilled) for item in items),
        storage_errors=sum(1 for report in reports if report.get("storage_error") is not None),
        skipped_unreadable=skipped,
    )


def build_collect_parser(subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    parser = _leaf(
        subparsers,
        "collect",
        summary="stream collected workflow summaries",
        description="Collect finished jobs from one execution workspace",
        handler=handle_collect,
    )
    parser.add_argument(
        "--workspace",
        metavar="WORKSPACE",
        help="the workspace to collect from (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )
    parser.add_argument(
        "--state",
        action="append",
        metavar="STATE",
        choices=COLLECTABLE_KINDS,
        help=f"state kind to collect (repeatable, default: {', '.join(DEFAULT_COLLECT_STATES)})",
    )
    parser.add_argument("--placement", metavar="PLACEMENT", help="collect only jobs at or below this placement")
    parser.add_argument(
        "--degraded",
        action="store_true",
        help="print only the degraded per-job lines; the summary still counts the whole sweep",
    )
    parser.add_argument("--raw", action="store_true", help="print raw collect records instead of summaries")
    parser.add_argument(
        "--allow-job-collector",
        action="store_true",
        help="allow collectors loaded and verified from a pinned workspace workflow tree",
    )
    parser.add_argument(
        "--into",
        metavar="PATH",
        help="save collected entries, runs, and products into a file-backed SQLite store",
    )
    parser.add_argument(
        "--id-base",
        metavar="BASE",
        help="entry-id namespace base (required with --into)",
    )
    parser.add_argument(
        "--id-series",
        metavar="SERIES",
        default="1",
        help="entry-id campaign series (default: 1)",
    )
    parser.add_argument(
        "--no-id-ledger",
        action="store_true",
        help="do not allocate ids through a stable id ledger (ids become unstable across rebuilds)",
    )
    parser.add_argument(
        "--id-ledger",
        metavar="PATH",
        help="id-ledger seal document location (default: <into>.ids.json; on by default with --into)",
    )
