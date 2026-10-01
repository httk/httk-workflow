"""Store collected jobs into a file-backed SQLite store.

:func:`store_collected` is the storage counterpart of
:func:`~httk.workflow.collecting.collect` and
:func:`~httk.workflow.calculations.collect_tree`: it saves each collected job's
entries, its run and its product links, rewriting the content ids the
collectors produced into the ids the store mints (or an id ledger allocates).
"""

import contextlib
import json
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import fields, is_dataclass, replace
from pathlib import Path
from typing import Any, cast

from httk.core import DataRecord, Run, RunEdge
from httk.core.storage import content_id, resolve_storage_record

from .collecting import CollectedJob
from .id_keys import UnstableIdentityError, ledger_key

__all__ = ["store_collected"]

_LOGGER = logging.getLogger(__name__)
_CONTENT_ID_RE = re.compile(r"[0-9a-f]{64}\Z")
#: The pinned order of the core records of the ``records`` family, as registry
#: names: the generic ``DataRecord`` first, then the typed core records. A record's
#: position in its family decides the numbers of the ids the store mints, so this
#: order is fixed when a store is created, and every core record is declared then.
#: Other registered records of the family follow in name order. A later additive
#: upgrade may only append to this order, never insert or reorder.
_CORE_RECORDS_ORDER = ("core-data-record", "core-total-energy", "core-average-total-energy")
_REBUILD_TYPED = (
    "{target} predates typed records: its values are stored as generic DataRecord rows. Rebuild it: delete "
    "{target} and collect again, keeping the id ledger beside it, so record and run ids are preserved."
)


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
        row = searcher.results(entry=variable).first()
        if row is None:
            continue
        fetched = row.entry
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


def _rewrite_product_of(value: Any, store: Any, remap: dict[str, str]) -> Any:
    """Rewrite a data record's ``product_of`` edges to ids minted by the destination store."""
    edges = tuple(
        RunEdge(edge.label, edge.entry_type, _resolve_entry_id(store, remap, edge.entry_type, edge.entry_id))
        for edge in value.product_of
    )
    return value if edges == tuple(value.product_of) else replace(value, product_of=edges)


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


def _children_first(items: Sequence[CollectedJob]) -> list[int]:
    """Order a sweep so every job comes after the children it spawned in it."""

    by_source = {item.run.source_id: index for index, item in enumerate(items) if item.run.source_id is not None}
    ordered: list[int] = []
    placed: set[int] = set()

    def place(index: int, visiting: set[int]) -> None:
        if index in placed or index in visiting:
            return
        visiting.add(index)
        for _label, source in items[index].child_runs:
            child = by_source.get(source)
            if child is not None:
                place(child, visiting)
        placed.add(index)
        ordered.append(index)

    for index in range(len(items)):
        place(index, set())
    return ordered


def _latest_stored_run(store: Any, source_id: str) -> Run | None:
    """Return the latest stored revision of the run of one job, by its source id."""

    family = next((candidate for candidate in store.entry_records if getattr(candidate, "type", None) == "runs"), None)
    if family is None:
        return None
    for record_type in store.entry_records[family]:
        searcher = store.searcher(only_latest=True)
        variable = searcher.variable(record_type)
        searcher.add(variable.source_id == source_id)
        row = searcher.results(entry=variable).first()
        if row is not None and isinstance(getattr(row.entry, "id", None), str):
            return cast(Run, row.entry)
    return None


def _with_child_runs(
    run: Run,
    item: CollectedJob,
    store: Any,
    run_ids_by_source: Mapping[str, str],
    in_sweep: set[str],
    stored_cache: dict[str, str | None],
) -> Run:
    """Add a ``has_artifact`` edge from *run* to the run of each child *item* spawned.

    A child counts when its run was stored earlier in this sweep, or, when it is
    not part of this sweep at all, by an earlier one; a child of this sweep that
    was not stored now (degraded, skipped, or failed) is left out rather than
    linked to a stale run, as is a child never collected. The spawn label names
    the edge, or the child's run source id when the label would repeat one
    already on the artifact side.
    """

    if not item.child_runs:
        return run
    labels = {edge.label for edge in run.artifacts}
    edges = list(run.artifacts)
    for label, source in item.child_runs:
        child_run = run_ids_by_source.get(source)
        if child_run is None and source not in in_sweep:
            if source not in stored_cache:
                stored = _latest_stored_run(store, source)
                stored_cache[source] = None if stored is None else stored.id
            child_run = stored_cache[source]
        if child_run is None:
            continue
        name = label if label not in labels else source
        if name in labels:
            continue
        labels.add(name)
        edges.append(RunEdge(name, "runs", child_run))
    return replace(run, artifacts=tuple(edges))


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
            "workflow_definition_uri": item.run.workflow_definition_uri,
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
    if item.run_only:
        mapping["run_only"] = True
    if item.child_runs:
        mapping["children"] = [{"label": label, "run_source_id": source} for label, source in item.child_runs]
    # A recognized calculation's stand-in record is coordinated by its collector
    # (``workspace_id`` is the collector's name, which is also its workflow); name
    # its directory, relative to the swept root, since its id is only a digest.
    if (
        item.identity_stable is True
        and item.record.workspace_id == item.workflow_id
        and item.record.workdir_path is not None
    ):
        mapping["directory"] = item.record.workdir_path.as_posix()
    return mapping


def _storage_layout(
    items: list[CollectedJob], persisted: Mapping[str, Sequence[str]] | None = None
) -> tuple[dict[type, tuple[type, ...]], dict[int, str]]:
    """Resolve the lazy core entry registry for the values this sweep stores.

    A family an existing store already declares keeps its *persisted* record
    order, with newly registered records appended after it, because a record's
    position is part of the store's id numbering and a reorder is never an
    additive upgrade. A new family (and a new store) uses the pinned core order
    for ``records`` and name order otherwise.
    """

    from httk.core.register import known_entry_families, known_entry_records, resolve_entry_family, resolve_entry_record

    required_types = {"records", "runs"}
    failures: dict[int, str] = {}
    for index, item in enumerate(items):
        for value in (*item.outputs.values(), *item.inputs.values()):
            entry_type = getattr(value, "type", None)
            if not isinstance(entry_type, str):
                failures[index] = "cannot store an output without a string entry type"
                continue
            required_types.add(entry_type)
        for edge in (*item.run.inputs, *item.run.artifacts, *item.run.outputs):
            required_types.add(edge.entry_type)
        for value in item.outputs.values():
            for edge in getattr(value, "product_of", ()):
                required_types.add(edge.entry_type)
        for product in item.products:
            required_types.add(product.source_type)
            required_types.add(product.target_type)

    required_families: set[str] = set()
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
                if any(
                    getattr(value, "type", None) == entry_type
                    for value in (*item.outputs.values(), *item.inputs.values())
                ):
                    failures[index] = message
            continue

        required_families.add(family_name)

    configurations: dict[str, tuple[type, tuple[type, ...]]] = {}
    for family_name in required_families | set(persisted or ()):
        try:
            family = resolve_entry_family(family_name)
        except (ImportError, ModuleNotFoundError, TypeError, ValueError):
            if family_name in (persisted or {}):
                raise
            continue
        names = known_entry_records(family_name)
        if family_name == "records":
            names = [name for name in _CORE_RECORDS_ORDER if name in names] + [
                name for name in names if name not in _CORE_RECORDS_ORDER
            ]
        stored = [] if persisted is None else list(persisted.get(family_name, ()))
        names = [*stored, *(name for name in names if name not in stored)]
        try:
            records = tuple(resolve_entry_record(name) for name in names)
        except (ImportError, ModuleNotFoundError, TypeError, ValueError) as exc:
            if family_name in (persisted or {}):
                raise
            entry_type = str(getattr(family, "type", ""))
            message = f"cannot store entry type {entry_type!r}: {exc}"
            for index, item in enumerate(items):
                if any(
                    getattr(value, "type", None) == entry_type
                    for value in (*item.outputs.values(), *item.inputs.values())
                ):
                    failures[index] = message
            continue
        configurations[family_name] = (family, records)

    layout: dict[type, tuple[type, ...]] = {}
    for family, records in configurations.values():
        layout[family] = records
    return layout, failures


def _typed_record_classes(records: Sequence[type]) -> dict[str, type]:
    """Map each property IRI a typed record of the ``records`` family carries to that class.

    A typed record declares exactly one property in ``__httk_property_definitions__``
    and builds itself from a generic record through ``from_data_record``.
    """

    typed: dict[str, type] = {}
    for record in records:
        definitions = getattr(record, "__httk_property_definitions__", None)
        if record is DataRecord or not definitions:
            continue
        if not callable(getattr(record, "from_data_record", None)) or len(definitions) != 1:
            # Its values would silently stay generic and unserved; the class owner must fix it.
            _LOGGER.warning(
                "typed record %s is not used for collected values: it needs a from_data_record classmethod "
                "and exactly one property definition",
                record.__qualname__,
                extra={"context": "workflow"},
            )
            continue
        (definition,) = definitions.values()
        typed.setdefault(definition.definition_id, record)
    return typed


def _typed(value: object, typed: Mapping[str, type]) -> object:
    """Return a generic data record as the typed record class carrying its definition, if any."""

    if isinstance(value, DataRecord) and value.definition_id in typed:
        return cast(Any, typed[value.definition_id]).from_data_record(value)
    return value


def _persisted_record_order(path: Path) -> dict[str, tuple[str, ...]] | None:
    """Return each family's persisted record order of an existing store, or ``None``.

    It reads the stored ``entry_declaration`` through httk-store's
    ``read_store_metadata`` without opening the store, so it also works on a store
    whose declaration upgrade was interrupted. A missing store, or one without a
    readable declaration, yields ``None``.
    """

    if not path.is_file():
        return None
    from httk.store.backend.sql import Backend  # pyright: ignore[reportMissingImports]
    from httk.store.backend.sql.layout import read_store_metadata  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database, database.engine.connect() as connection:
        metadata = read_store_metadata(connection)
    try:
        families = json.loads(cast(Any, metadata)["entry_declaration"])["families"]
        return {family["family"]: tuple(record["record"] for record in family["records"]) for family in families}
    except (KeyError, TypeError, ValueError):
        return None


def _predates_typed_records(diff: object) -> bool:
    """Report whether a layout refusal is a ``records`` family declared before typed records existed.

    That is a stored ``records`` family without ``core-total-energy``, the first typed
    record: its generic total energies cannot move to the typed backing under their
    ids. A store that has it and only lacks records appended later is not this case.
    """

    try:
        stored = json.loads(cast(Any, diff)["declaration"]["entry_declaration"]["expected"])
        families = {family["family"]: family for family in stored["families"]}
        names = {record["record"] for record in families["records"]["records"]}
    except (KeyError, TypeError, ValueError):
        return False
    return "core-total-energy" not in names


def _layout_additions(diff: object) -> str | None:
    """Name the record kinds an additive declaration change appends, from the refusal diff."""

    try:
        entry = cast(Any, diff)["declaration"]["entry_declaration"]
        stored = {
            family["family"]: [record["record"] for record in family["records"]]
            for family in json.loads(entry["expected"])["families"]
        }
        target = {
            family["family"]: [record["record"] for record in family["records"]]
            for family in json.loads(entry["actual"])["families"]
        }
    except (KeyError, TypeError, ValueError):
        return None
    added = [f"{name}/{record}" for name, records in target.items() for record in records[len(stored.get(name, ())) :]]
    return "new record kinds: " + ", ".join(added) if added else None


def _holds_generic_typed(path: Path, typed: Mapping[str, type]) -> bool:
    """Report whether an existing store, opened in its persisted layout, holds now-typed generic rows.

    A store that cannot be opened in its persisted layout (an interrupted upgrade)
    is not inspected here; the check after the open covers it.
    """

    from httk.store import SqliteStore  # pyright: ignore[reportMissingImports]

    try:
        current = SqliteStore(path)
    except Exception:
        return False
    with current:
        return _stored_generic_typed(current, typed)


def _stored_generic_typed(store: Any, typed: Mapping[str, type]) -> bool:
    """Report whether the store holds generic data records for a definition that is now typed."""

    if not any(DataRecord in records for records in store.entry_records.values()):
        return False
    for definition_id in typed:
        searcher = store.searcher()
        variable = searcher.variable(DataRecord)
        searcher.add(variable.definition_id == definition_id)
        if searcher.results(entry=variable).first() is not None:
            return True
    return False


def _latest_stored_entry(store: Any, value: object, entry_id: str) -> Any:
    """Return the latest stored revision of the entry *value* is saved as under *entry_id*, if any."""

    searcher = store.searcher(only_latest=True)
    variable = searcher.variable(resolve_storage_record(value))
    searcher.add(variable.id == entry_id)
    row = searcher.results(entry=variable).first()
    return None if row is None else row.entry


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

    :param ledger_path: Where the ledger database lives.
    :param keys: The signing keys used to seal the ledger.
    :param id_base: The entry-id namespace base minted ids carry.
    :param id_series: The entry-id series minted ids carry.
    :param items: The collected sweep, read for the families to configure.
    :return: The open, locked ledger, or ``None`` when nothing in the sweep can
        be allocated through it (no id-settable output).
    """

    from httk.store import IdLedger  # pyright: ignore[reportMissingImports]

    values = [
        value
        for item in items
        if item.missing_collector is None
        for value in (*item.outputs.values(), *item.inputs.values(), item.run)
    ]
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
    shared_ids: set[str],
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
    :param shared_ids: Ids more than one key resolves to (alias targets), updated in
        place when this call aliases a key onto an id.
    :return: The ledger id to inject, or ``None`` to fall back to store minting.
    """

    from httk.store import IdLedgerError  # pyright: ignore[reportMissingImports]

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
            shared_ids.add(allocated)
            return allocated
        entry_id = ledger.assign(key, entry_type)
    except IdLedgerError as exc:
        if allocated is not None:
            # The content is stored under an id outside the ledger (mixed
            # ledger/no-ledger use of one store); reuse it so the store stays
            # consistent, without a ledger record.
            _LOGGER.warning("reusing store id %s for %s (not ledger-managed): %s", allocated, key, exc)
            shared_ids.add(allocated)
            return allocated
        _LOGGER.warning("id ledger could not allocate for %s: %s; falling back to store minting.", key, exc)
        return None
    content_to_ledger[identity] = entry_id
    return entry_id


def store_collected(
    items: list[CollectedJob],
    path: str,
    *,
    id_base: str,
    id_series: str = "1",
    ledger_path: str | None = None,
    ledger_keys: Sequence[tuple[str, bytes]] = (),
    bare_runs: bool = True,
    upgrade: bool = False,
) -> list[dict[str, object]]:
    """Save one bounded collected sweep into a file-backed SQLite store.

    Every job's run is stored, including the run of a job whose workflow has
    nothing to collect (a *bare* run) unless *bare_runs* is off, so parent and
    child jobs alike leave provenance naming their workflow declarations. A
    parent's run gains one ``has_artifact`` edge to the run of each child it
    spawned that is stored in this sweep or already in the store; children are
    stored first so their run ids exist when the parent's run is written.

    The entries a recognized calculation read (the ``inputs`` of
    :class:`~httk.workflow.collecting.CollectedJob`) are stored like its
    outputs, and the run's input edges name their stored ids.
    Collector outputs arrive carrying content ids; every edge, product link and
    ``product_of`` reference is rewritten to the id the store stored it under.

    With an id ledger, each record and run is keyed by its job coordinate and
    role (:func:`~httk.workflow.ledger_key`), so a store rebuilt from the
    same jobs keeps its ids. A job collected again with changed content is a
    revision: the record or run keeps its id and the store appends a new
    revision to its lineage. Views that cannot carry an explicit id, such as
    structures, are store-minted and deduplicated by content.

    A generic ``DataRecord`` whose property definition a typed record of the
    ``records`` family carries (``TotalEnergyRecord`` for the core total energy)
    is stored as that typed record, so its value is served and filterable; any
    other definition stays a generic record, stored but not served as a value.
    A dataclass entry that is not a registered record of its family is a
    storage error rather than a row in an unserved table.

    Each report is the job's collected summary plus ``"stored"`` (the stored
    entry ids and run id, or ``None`` when nothing was stored, with
    ``"skipped"`` saying why), ``"storage_error"`` when the job could not be
    stored, and ``"revised"``, which is true when this call replaced an
    existing entry of the job (its run, or one of its records) with a new
    revision.

    :param items: The collected jobs to store.
    :param path: The SQLite store file path.
    :param id_base: The entry-id namespace base.
    :param id_series: The entry-id campaign series.
    :param ledger_path: An id-ledger database to allocate stable ids
        through, or ``None`` to let the store mint ids directly.
    :param ledger_keys: The signing keys each appended segment is signed with.
    :param bare_runs: Store the runs of jobs whose workflows have nothing to collect.
    :param upgrade: Apply an additive layout upgrade the store needs (new record
        kinds or families, for example a typed record a newer *httk* ships)
        instead of refusing. Keep a backup of the store first.
    :return: One report mapping per collected job.
    :raises ValueError: If *httk-store* is unavailable, or the store at *path*
        was created for a different set of entry types, predates typed records, or
        needs an additive upgrade and *upgrade* is false.
    """

    try:
        from httk.store import EntryIdScheme, SqliteStore  # pyright: ignore[reportMissingImports]
        from httk.store.backend.schema import SchemaError  # pyright: ignore[reportMissingImports]
        from httk.store.backend.sql import StorageLayoutUpgradeRequiredError  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise ValueError("--into requires httk-store with its database dependencies") from exc

    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        layout, failures = _storage_layout(items, _persisted_record_order(target))
    except (ImportError, ModuleNotFoundError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{target} cannot resolve its stored entry layout: {exc}. Restore the missing entry registration and "
            "package, or collect into a new store file."
        ) from exc
    requested = sorted(
        {
            str(getattr(value, "type", ""))
            for item in items
            for value in (*item.outputs.values(), *item.inputs.values())
            if getattr(value, "type", None)
        }
    )
    reports = [{**_collected_mapping(item), "revised": False} for item in items]
    with contextlib.ExitStack() as stack:
        ledger = (
            _open_id_ledger(ledger_path, ledger_keys, id_base, id_series, items) if ledger_path is not None else None
        )
        if ledger is not None:
            stack.enter_context(ledger)
        content_to_ledger: dict[str, str] = {}
        warned_unstable: set[str] = set()
        # An id several keys resolve to belongs to no single job, so no job may revise it.
        shared_ids: set[str] = (
            set() if ledger is None else {binding.id for binding in ledger.bindings().values() if binding.is_alias}
        )
        type_to_family: dict[str, type] = {
            str(getattr(family, "type", "")): family for family in layout if getattr(family, "type", None)
        }
        records_family = next(
            (records for family, records in layout.items() if family is type_to_family.get("records")), ()
        )
        typed = _typed_record_classes(records_family)

        def refusal(exc: Any) -> ValueError:
            """Turn a layout refusal into the collect error its remedy calls for."""
            if exc.remedy == "upgrade":
                additions = _layout_additions(exc.diff) or exc.hint or "an additive layout change"
                return ValueError(
                    f"{target} needs an additive layout upgrade ({additions}); rerun with --upgrade "
                    "(upgrade=True), keeping a backup of the store first"
                )
            if exc.remedy in ("reopen", "retry"):
                return ValueError(f"{target}: {exc}")
            if _predates_typed_records(exc.diff):
                return ValueError(_REBUILD_TYPED.format(target=target))
            needs = ", ".join(requested) or "no entry types"
            return ValueError(
                f"{target} was created for a different set of entry types than this sweep needs ({needs}); "
                f"its stored layout differs in {json.dumps(exc.diff, sort_keys=True, default=str)}. "
                "Collect into a new store file."
            )

        try:
            store = stack.enter_context(
                SqliteStore(target, entry_records=layout, entry_ids=EntryIdScheme(id_base, id_series))
            )
        except StorageLayoutUpgradeRequiredError as exc:
            if exc.remedy != "upgrade" or not upgrade:
                if exc.remedy == "upgrade" and _holds_generic_typed(target, typed):
                    raise ValueError(_REBUILD_TYPED.format(target=target)) from exc
                raise refusal(exc) from exc
            # Checked before anything is upgraded: generic rows of a definition that is
            # now typed cannot move to the typed backing under their ids, so appending
            # the typed record would leave them stranded. Only a rebuild fixes that.
            if _holds_generic_typed(target, typed):
                raise ValueError(_REBUILD_TYPED.format(target=target)) from exc
            try:
                store = stack.enter_context(
                    SqliteStore(target, entry_records=layout, entry_ids=EntryIdScheme(id_base, id_series), upgrade=True)
                )
            except StorageLayoutUpgradeRequiredError as again:
                raise refusal(again) from again
        if _stored_generic_typed(store, typed):
            raise ValueError(_REBUILD_TYPED.format(target=target))
        # A dataclass entry is stored only as a record of its family; anything else
        # would land in a private table no provider serves.
        members = {record for records in layout.values() for record in records}
        for index, item in enumerate(items):
            for role, value in (*item.outputs.items(), *item.inputs.items()):
                if index in failures:
                    break
                try:
                    stored_as = _typed(value, typed)
                except (TypeError, ValueError) as exc:
                    # A value the typed record refuses (a list-valued total energy) fails this job only.
                    failures[index] = f"cannot store role {role!r}: {exc}"
                    break
                if is_dataclass(stored_as) and type(stored_as) not in members:
                    failures[index] = (
                        f"cannot store role {role!r}: {type(stored_as).__name__} is not a registered record of the "
                        f"{getattr(stored_as, 'type', None)!r} entry family; register it with "
                        "httk.core.register.register_entry_record, or return a registered record"
                    )
        stored_output_ids: dict[int, list[str]] = {}
        deferred_outputs: dict[int, list[tuple[str, Any]]] = {}
        remap: dict[str, str] = {}

        def _save_output(
            item: CollectedJob,
            role: str,
            value: Any,
            *,
            original_key: str,
            pending_remap: dict[str, str],
            entry_ids: list[str],
        ) -> bool:
            """Store one output, recording its minted id under ``original_key`` and any prior id.

            Return whether the save replaced an earlier revision stored under the same ledger id.
            A generic data record whose definition a typed record carries is stored as that
            typed record; ``original_key`` stays the generic record's content id, which is
            what run and ``product_of`` edges name, while ledger and alias lookups see the
            typed content.
            """
            value = _typed(value, typed)
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
                    shared_ids,
                )
                if ledger is not None
                else None
            )
            # Only an id-settable dataclass reaches a non-None id, so
            # this replace never lands on the view-fallback branch.
            saved = replace(cast(Any, value), id=chosen) if chosen is not None else value
            # A ledger id this job's key owns outright, already in the store, names the
            # entry an earlier sweep collected: changed content is its next revision, as
            # for runs. An id other keys share is never revised; the plain save then
            # refuses the changed content under it, as before revisions existed.
            owned = chosen is not None and chosen not in shared_ids
            predecessor = _latest_stored_entry(store, saved, chosen) if owned and chosen is not None else None
            revised = False
            if predecessor is not None and content_id(predecessor) != content_id(saved):
                before = store.sid_of(predecessor)
                sid = store.replace(predecessor, saved)
                # Content equal to an earlier revision of the lineage is a no-op
                # that returns that revision's sid instead of writing a row.
                revised = before is not None and sid > before
            else:
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
            pending_remap[original_key] = fetched_id
            if isinstance(original_id, str) and original_id != fetched_id:
                pending_remap[original_id] = fetched_id
            return revised

        # Pass one stores every output and builds a sweep-wide map.  The map is
        # published only after each job transaction commits, so rolled-back
        # outputs cannot be referenced by a later job.  Data records carrying
        # ``product_of`` edges wait for pass two: the edge is record content and
        # must hold the store-minted id of the entry it describes.
        for index, item in enumerate(items):
            report = reports[index]
            if item.run_only and not bare_runs:
                report["stored"] = None
                report["skipped"] = "run-only"
                continue
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
                    revised = False
                    for role, value in (*item.outputs.items(), *item.inputs.items()):
                        if getattr(value, "product_of", ()):
                            deferred_outputs.setdefault(index, []).append((role, value))
                            continue
                        revised |= _save_output(
                            item,
                            role,
                            value,
                            original_key=content_id(value),
                            pending_remap=pending_remap,
                            entry_ids=entry_ids,
                        )
                remap.update(pending_remap)
                stored_output_ids[index] = entry_ids
                report["revised"] = revised
            except Exception as exc:
                report["storage_error"] = f"could not store job {item.record.job_id}: {exc}"
        # Pass two resolves all run and product references after every output
        # in the sweep has contributed to the shared remap. Children go first, so
        # a parent's run can name the runs of the children it spawned.
        run_ids_by_source: dict[str, str] = {}
        in_sweep = {item.run.source_id for item in items if item.run.source_id is not None}
        stored_cache: dict[str, str | None] = {}
        for index in _children_first(items):
            item = items[index]
            report = reports[index]
            if index not in stored_output_ids:
                continue
            entry_ids = stored_output_ids[index]
            try:
                with store.transaction():
                    pending_remap = {}
                    revised = False
                    for role, value in deferred_outputs.get(index, ()):
                        # The run's output edge names the record by its pre-rewrite
                        # content id, so that is the key the remap must carry.
                        rewritten = _rewrite_product_of(value, store, remap)
                        revised |= _save_output(
                            item,
                            role,
                            rewritten,
                            original_key=content_id(value),
                            pending_remap=pending_remap,
                            entry_ids=entry_ids,
                        )
                    # Resolve against the committed map plus this job's pending ids, but
                    # publish the pending ids only after the transaction commits (as in
                    # pass one), so a rolled-back record is never referenced by a later job.
                    job_remap = {**remap, **pending_remap}
                    rewritten_run = _with_child_runs(
                        _rewrite_run_edges(item.run, store, job_remap),
                        item,
                        store,
                        run_ids_by_source,
                        in_sweep,
                        stored_cache,
                    )
                    rewritten_products = tuple(
                        replace(
                            product,
                            source_id=_resolve_entry_id(store, job_remap, product.source_type, product.source_id),
                            target_id=_resolve_entry_id(store, job_remap, product.target_type, product.target_id),
                        )
                        for product in item.products
                    )
                    # The run is the provenance hub every relationship points
                    # at, so it takes a ledger id too, keyed by the bare job
                    # coordinate.  The edges were already rewritten above; the
                    # run's own id is orthogonal to them.
                    # A job collected before is revised, never duplicated: its run
                    # may have changed since (a parent whose children have since
                    # been collected gains their edges), and the new revision
                    # extends the same lineage under the same entry id.
                    predecessor = (
                        None if rewritten_run.source_id is None else _latest_stored_run(store, rewritten_run.source_id)
                    )
                    if predecessor is not None:
                        rewritten_run = replace(rewritten_run, id=predecessor.id)
                        if content_id(predecessor) == content_id(rewritten_run):
                            run_sid = store.save(rewritten_run)
                        else:
                            before = store.sid_of(predecessor)
                            run_sid = store.replace(predecessor, rewritten_run)
                            revised = revised or (before is not None and run_sid > before)
                    else:
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
                                shared_ids,
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
                remap.update(pending_remap)
                if item.run.source_id is not None:
                    run_ids_by_source[item.run.source_id] = run_id
                report["stored"] = {"entries": entry_ids, "run": run_id}
                report["revised"] = bool(report["revised"]) or revised
            except Exception as exc:
                report["storage_error"] = f"could not store job {item.record.job_id}: {exc}"
    return reports
