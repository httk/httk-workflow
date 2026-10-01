"""The top-level ``httk collect`` command: workspaces and recognized calculations."""

import argparse
import json
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from httk.core.cli import CLIContext

from ..calculations import DirectoryClaim, claims, collect_tree
from ..collecting import COLLECTABLE_KINDS, DEFAULT_COLLECT_STATES, CollectedJob, collect, job_records
from ..errors import ResolutionMiss, SealError
from ..seals import default_workspace_keys, tree_ledger_keys
from ..storing import _collected_mapping, store_collected
from ..workspace import Workspace
from ._common import _LOGGER, _leaf, _local_root

CLAIM_FORMAT = "httk-collect-claim"
CLAIM_FORMAT_VERSION = 1
#: Options that only apply to one kind of collect target, with their flags.
_TREE_ONLY = (("dry_run", "--dry-run"), ("prefer", "--prefer"), ("exclude", "--exclude"), ("collector", "--collector"))
_WORKSPACE_ONLY = (
    ("state", "--state"),
    ("placement", "--placement"),
    ("raw", "--raw"),
    ("allow_job_collector", "--allow-job-collector"),
)


def _positive_int(value: str) -> int:
    """Parse one positive CLI integer, rejecting bool-like API values elsewhere."""

    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _emit_collect_summary(
    *,
    collected: int,
    degraded: int,
    unfulfilled_roles: int,
    storage_errors: int,
    skipped_unreadable: int,
    unclaimed: int | None = None,
    revised: int | None = None,
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
    :param unclaimed: Count the directories a collector declined, for a tree sweep.
    :param revised: Count the jobs whose stored entries gained a revision, with ``--into``.
    :return: ``0`` on a fully clean sweep, ``1`` otherwise.
    """

    summary: dict[str, object] = {
        "format": "httk-workflow-collect-summary",
        "format_version": 2,
        "collected": collected,
        "degraded": degraded,
        "unfulfilled_roles": unfulfilled_roles,
        "storage_errors": storage_errors,
        "skipped_unreadable": skipped_unreadable,
    }
    if unclaimed is not None:
        summary["unclaimed"] = unclaimed
    if revised is not None:
        summary["revised"] = revised
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")))
    return 0 if degraded == 0 and storage_errors == 0 and skipped_unreadable == 0 else 1


def _ledger_settings(
    arguments: argparse.Namespace, resolve_keys: Callable[[], Sequence[tuple[str, bytes]]]
) -> tuple[str | None, Sequence[tuple[str, bytes]]]:
    """Resolve the id-ledger path and signing keys for a ``--into`` sweep.

    A ledger is on by default at ``<into>.ids.sqlite`` so entry ids stay stable
    across rebuilds; ``--no-id-ledger`` opts out and ``--id-ledger PATH``
    relocates it. A sweep with no resolvable workspace signing key falls back to
    no ledger with a loud warning rather than failing the collect.

    :param arguments: The parsed collect arguments.
    :param resolve_keys: Resolve the signing keys; it raises
        :class:`~httk.workflow.errors.SealError` or returns none when no key is available.
    :return: The ledger path (or ``None`` when disabled) and the signing keys.
    """

    if arguments.no_id_ledger:
        _LOGGER.warning(
            "--no-id-ledger: entry ids for %s are store-minted and will NOT be stable across rebuilds.",
            arguments.into,
        )
        return None, ()
    try:
        keys = resolve_keys()
    except SealError as exc:
        _LOGGER.warning(
            "no signing key is available to seal an id ledger (%s); entry ids for %s are store-minted and will "
            "NOT be stable across rebuilds. Configure seal.keys, or pass --no-id-ledger to silence this.",
            exc,
            arguments.into,
        )
        return None, ()
    if not keys:
        return None, ()
    if not arguments.id_ledger:
        default_path = Path(f"{arguments.into}.ids.sqlite")
        legacy_path = Path(f"{arguments.into}.ids.json")
        if not default_path.exists() and legacy_path.exists():
            raise ValueError(
                f"the id ledger format changed to sqlite: {default_path} does not exist, but a legacy JSON "
                f"ledger {legacy_path} does. Creating a fresh ledger here would re-mint every id from 1. "
                "Migrate the old ledger to sqlite, or pass --id-ledger PATH / --no-id-ledger explicitly."
            )
    return arguments.id_ledger or f"{arguments.into}.ids.sqlite", keys


def _collect_target(arguments: argparse.Namespace, context: CLIContext) -> tuple[bool, Path]:
    """Resolve what ``httk collect`` collects: ``(is_tree, path)``.

    No PATH resolves a workspace as every workspace command does. A PATH that is
    not a directory is a registered workspace name. A directory is collected as
    the workspace it is the root of, refused when it lies inside a workspace,
    and otherwise walked as a calculation tree.
    """

    if arguments.path is None:
        return False, _local_root(arguments, context, action="collect from")
    if arguments.workspace is not None:
        raise ValueError("give either PATH or --workspace, not both")
    candidate = Path(arguments.path).expanduser()
    if not candidate.is_absolute():
        candidate = Path(context.cwd) / candidate
    if not candidate.is_dir():
        arguments.workspace = arguments.path
        try:
            return False, _local_root(arguments, context, action="collect from")
        except ResolutionMiss:
            raise ValueError(f"PATH is neither a directory nor a registered workspace name: {arguments.path}") from None
    candidate = candidate.resolve()
    enclosing = Workspace.discover(candidate)
    if enclosing is None:
        return True, candidate
    if enclosing == candidate:
        return False, candidate
    raise ValueError(
        f"{arguments.path} is inside workspace {enclosing}; collect {enclosing} (use --placement to narrow)"
    )


def _claim_mapping(claim: DirectoryClaim) -> dict[str, object]:
    return {
        "format": CLAIM_FORMAT,
        "format_version": CLAIM_FORMAT_VERSION,
        "directory": claim.directory,
        "kind": claim.kind,
        "collector": claim.collector,
        "priority": claim.priority,
        "identity": claim.identity,
        "reason": claim.reason,
        "also_matched": list(claim.also_matched),
        "duplicate_of": claim.duplicate_of,
    }


def handle_collect(arguments: argparse.Namespace, context: CLIContext) -> int:
    """Stream collected summaries of a workspace or a calculation tree, or raw job records."""

    tree, root = _collect_target(arguments, context)
    refused = _WORKSPACE_ONLY if tree else _TREE_ONLY
    for name, flag in refused:
        if getattr(arguments, name):
            raise ValueError(f"{flag} only applies to {'a workspace' if tree else 'a calculation tree'}")
    if arguments.into is not None and arguments.raw:
        raise ValueError("--into cannot be combined with --raw")
    if arguments.dry_run:
        dry_run_refused = (
            ("into", "--into"),
            ("fail_fast", "--fail-fast"),
            ("degraded", "--degraded"),
            ("batch_size", "--batch-size"),
            ("no_bare_runs", "--no-bare-runs"),
        )
        for name, flag in dry_run_refused:
            if getattr(arguments, name) not in (None, False):
                raise ValueError(f"--dry-run cannot be combined with {flag}")
    if arguments.into is not None and arguments.id_base is None:
        raise ValueError("--id-base is required with --into")
    if arguments.into is None and arguments.no_bare_runs:
        raise ValueError("--no-bare-runs only applies with --into")
    if arguments.into is None and arguments.upgrade:
        raise ValueError("--upgrade only applies with --into")
    if arguments.degraded and arguments.raw:
        raise ValueError("--degraded filters collected summaries and cannot be combined with --raw")
    skipped = 0
    unclaimed = 0

    def _skip(_job_key: str) -> None:
        nonlocal skipped
        skipped += 1

    def _unclaimed(_claim: DirectoryClaim) -> None:
        nonlocal unclaimed
        unclaimed += 1

    if tree:
        options = {
            "collectors": arguments.collector or (),
            "prefer": arguments.prefer or (),
            "exclude": arguments.exclude or (),
        }
        if arguments.dry_run:
            for claim in claims(root, **options):
                print(json.dumps(_claim_mapping(claim), sort_keys=True, separators=(",", ":")))
            return 0
        collected_items: Iterable[CollectedJob] = collect_tree(
            root, fail_fast=arguments.fail_fast, on_unclaimed=_unclaimed, **options
        )

        def resolve_keys() -> Sequence[tuple[str, bytes]]:
            return tree_ledger_keys(root)

    else:
        workspace = Workspace(root, mutable=False)
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
        collected_items = collect(
            workspace,
            states=arguments.state or DEFAULT_COLLECT_STATES,
            placement=arguments.placement,
            allow_job_collector=arguments.allow_job_collector,
            on_skipped=_skip,
            fail_fast=arguments.fail_fast,
            batch_size=64 if arguments.batch_size is None else arguments.batch_size,
        )

        def resolve_keys() -> Sequence[tuple[str, bytes]]:
            return default_workspace_keys(workspace).keys

    collected = degraded = unfulfilled_roles = storage_errors = revised = 0
    if arguments.into is not None:
        # --into resolves cross-job provenance in a second storage pass, so it
        # deliberately retains the sweep; ordinary reporting does not.
        items = list(collected_items)
        ledger_path, ledger_keys = _ledger_settings(arguments, resolve_keys)
        reports = store_collected(
            items,
            arguments.into,
            id_base=arguments.id_base,
            id_series=arguments.id_series,
            ledger_path=ledger_path,
            ledger_keys=ledger_keys,
            bare_runs=not arguments.no_bare_runs,
            upgrade=arguments.upgrade,
        )
        for item, report in zip(items, reports):
            degraded += item.missing_collector is not None
            collected += item.missing_collector is None
            unfulfilled_roles += len(item.unfulfilled)
            storage_errors += report.get("storage_error") is not None
            revised += bool(report.get("revised"))
            if not arguments.degraded or item.missing_collector is not None:
                print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    else:
        for item in collected_items:
            degraded += item.missing_collector is not None
            collected += item.missing_collector is None
            unfulfilled_roles += len(item.unfulfilled)
            if not arguments.degraded or item.missing_collector is not None:
                print(json.dumps(_collected_mapping(item), sort_keys=True, separators=(",", ":")))
    return _emit_collect_summary(
        collected=collected,
        degraded=degraded,
        unfulfilled_roles=unfulfilled_roles,
        storage_errors=storage_errors,
        skipped_unreadable=skipped,
        unclaimed=unclaimed if tree else None,
        revised=revised if arguments.into is not None else None,
    )


def build_collect_parser(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]", *, program: str | None = None
) -> None:
    """Declare the top-level ``collect`` command.

    :param subparsers: The subparser action the command is added to.
    :param program: The program name its usage shows, when it is mounted standalone.
    """

    parser = _leaf(
        subparsers,
        "collect",
        summary="collect workspaces and recognized calculations into records",
        description=(
            "Collect finished jobs from an execution workspace, or walk a directory tree and collect the "
            "finished calculations that registered or given collectors recognize"
        ),
        handler=handle_collect,
    )
    if program is not None:
        parser.prog = program
    parser.add_argument(
        "path",
        nargs="?",
        metavar="PATH",
        help=(
            "a workspace root, a registered workspace name, or a directory tree of calculations "
            "(default: the enclosing workspace, this project's workspace, or the per-user default)"
        ),
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
        "--fail-fast",
        action="store_true",
        help="stop at the first degraded job; disables executable batching (one collector process per job)",
    )
    parser.add_argument(
        "--batch-size",
        type=_positive_int,
        metavar="N",
        help="records retained and executable requests grouped at once (default: 64; ignored with --fail-fast)",
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
        "--no-bare-runs",
        action="store_true",
        help="with --into, store no run for a job whose workflow has nothing to collect (default: store one)",
    )
    parser.add_argument(
        "--upgrade",
        action="store_true",
        help=(
            "with --into, apply an additive layout upgrade the store needs (new record kinds or families); "
            "keep a backup of the store first"
        ),
    )
    parser.add_argument(
        "--no-id-ledger",
        action="store_true",
        help="do not allocate ids through a stable id ledger (ids become unstable across rebuilds)",
    )
    parser.add_argument(
        "--id-ledger",
        metavar="PATH",
        help="id-ledger database location (default: <into>.ids.sqlite; on by default with --into)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="for a calculation tree, print how each directory would be claimed instead of collecting",
    )
    parser.add_argument(
        "--prefer",
        action="append",
        metavar="NAME",
        help="the collector that wins a tie at the highest priority (repeatable, first named first)",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        metavar="PATTERN",
        help="skip tree directories whose root-relative POSIX path matches this glob (repeatable)",
    )
    parser.add_argument(
        "--collector",
        action="append",
        metavar="DIR",
        help="also use the collector package in this directory; it replaces a registered one of its name (repeatable)",
    )
