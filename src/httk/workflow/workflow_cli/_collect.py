"""The collect command."""

import argparse
import json
from pathlib import Path
from typing import Any, cast

from httk.core import Run

from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, CollectedJob, collect, job_records
from ._common import *
from ._common import _leaf, _local_root


def _edge_counts(run: Run) -> dict[str, int]:
    return {side: len(getattr(run, side)) for side in ("inputs", "artifacts", "outputs")}


def _collected_mapping(item: CollectedJob) -> dict[str, object]:
    outputs = item.outputs
    mapping: dict[str, object] = {
        "format": "httk-workflow-collected",
        "format_version": 1,
        "job_id": item.record.job_id,
        "job_key": item.record.job_key,
        "workflow": item.workflow_id,
        "outputs": {
            role: {"type": getattr(value, "type", ""), "id": getattr(value, "id", "")}
            for role, value in outputs.items()
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
                "format_version": 1,
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

    required_types = {"_httk_records", "_httk_runs"}
    failures: dict[int, str] = {}
    for index, item in enumerate(items):
        for value in item.outputs.values():
            entry_type = getattr(value, "type", None)
            if not isinstance(entry_type, str):
                failures[index] = "cannot store an output without a string entry type"
                continue
            required_types.add(entry_type)

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


def _store_collected(items: list[CollectedJob], path: str) -> list[dict[str, object]]:
    """Save one bounded collected sweep into a file-backed SQLite store."""

    try:
        from httk.store.db import Database, SqlStore, StorageLayoutUpgradeRequiredError
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
    reports: list[dict[str, object]] = []
    with Database.sqlite(target) as database:
        try:
            store = SqlStore(database, entry_records=layout)
        except StorageLayoutUpgradeRequiredError as exc:
            needs = ", ".join(requested) or "no entry types"
            raise ValueError(
                f"{target} was created for a different set of entry types than this sweep needs ({needs}); "
                f"its stored layout differs in {json.dumps(exc.diff, sort_keys=True, default=str)}. "
                "Collect into a new store file."
            ) from exc
        for index, item in enumerate(items):
            report = _collected_mapping(item)
            if item.missing_collector is not None:
                # A degraded job produced no outputs: store nothing, and never a
                # bare Run, so the store cannot fill with empty provenance.
                report["stored"] = None
                report["skipped"] = "degraded"
                reports.append(report)
                continue
            error = failures.get(index)
            if error is not None:
                report["storage_error"] = error
                reports.append(report)
                continue
            try:
                with store.transaction():
                    entry_ids = []
                    for value in item.outputs.values():
                        store.save(value)
                        entry_ids.append(str(cast(Any, value).id))
                    store.save(item.run)
                    for product in item.products:
                        store.save(product)
                report["stored"] = {"entries": entry_ids, "run": item.run.id}
            except Exception as exc:
                report["storage_error"] = f"could not store job {item.record.job_id}: {exc}"
            reports.append(report)
    return reports


def handle_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream collected workflow summaries, or raw job records."""

    workspace = Workspace(_local_root(arguments, context, action="collect from"), mutable=False)
    if arguments.into is not None and (arguments.raw or arguments.json):
        raise ValueError("--into cannot be combined with --raw")
    if arguments.degraded and (arguments.raw or arguments.json):
        raise ValueError("--degraded filters collected summaries and cannot be combined with --raw")
    skipped = 0

    def _skip(_job_key: str) -> None:
        nonlocal skipped
        skipped += 1

    if arguments.raw or arguments.json:
        records = job_records(
            workspace,
            states=arguments.state or DEFAULT_COLLECT_STATES,
            placement=arguments.placement,
            on_skipped=_skip,
        )
        if arguments.json:
            # The hidden pure-array compatibility form stays a single JSON value.
            print(json.dumps([record.as_mapping() for record in records], indent=2, sort_keys=True))
            return 0
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
    reports = (
        _store_collected(items, arguments.into)
        if arguments.into is not None
        else [_collected_mapping(item) for item in items]
    )
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
        description="Collect finished jobs from one workflow workspace",
        handler=handle_collect,
    )
    add_workspace_argument(parser, help_text="the workspace to collect from")
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
    parser.add_argument("--jsonl", action="store_true", dest="raw", help=argparse.SUPPRESS)
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
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
