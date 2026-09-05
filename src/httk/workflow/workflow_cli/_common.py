"""The canonical :command:`httk workflow` command tree.

Every command this package offers is one leaf of a single nested
:mod:`argparse` tree, so the help of a group is argparse's own help, an unknown
action under a known group is argparse's own error naming that group, with no
second executable or parser implementation.

The module is laid out group by group. Each group has a ``build_*_parser``
function that declares its subcommands, and each subcommand has a ``handle_*``
function that receives the parsed :class:`argparse.Namespace` and the
:class:`~httk.core.cli.CLIContext` and does nothing but call the library.

Some spellings are protocol rather than user interface: the argument vectors a
transfer runs on the far side of a remote adapter are listed once, near the
top, because the machine that runs them may have an older or newer *httk*
installed than the machine that composed them.
"""

import argparse
import fnmatch
import json
import logging
import sys
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path

import httk.core
from httk.core.cli import CLIContext

# The packaged domains register their workflows as an import side effect, so the
# CLI resolves `job new --workflow NAME` against a populated registry. The generic
# execution layer never imports a domain; the CLI does, exactly here.
# Importing the domain registers its built-in workflows in the shared registry.
from .. import vasp as _vasp  # noqa: F401
from .._manager_runners import RUNNER_TREE_ENTRY
from ..adapters import resolve_remote, run_adapter
from ..errors import WorkflowError
from ..registry import LOCAL_REMOTE, WorkspaceBinding, default_workspace, resolve_workspace
from ..scaffold import STRUCTURE_PATTERNS, JobItem, _sanitize_tag, structure_tag
from ..workspace import Workspace

_LOGGER = logging.getLogger(__name__)

#: Everything a handler may raise that is an operator's problem rather than a
#: defect. Anything here is reported as ``PROGRAM: message`` and exits ``2``.
# RuntimeError membership is load-bearing: httk-core's SealError / SealedError are
# RuntimeError subclasses, so a sealed-tree refusal renders as a clean CLI error here.
_ERRORS = (WorkflowError, OSError, ValueError, RuntimeError, TimeoutError)

#: The hidden protocol subcommands the ``transfer`` verb dispatches by name: the
#: far-side halves one machine invokes on another. A workspace name can never be
#: one of these, so the verb never mistakes ``transfer offer`` for a move.
_TRANSFER_PROTOCOL = ("receive", "offer", "retire")

Handler = Callable[[argparse.Namespace, CLIContext], int]


# Parser construction helpers
# ---------------------------------------------------------------------------


class HelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Leave room for the longest command name, and print epilogs as written.

    argparse measures a subcommand listing without the extra indentation it then
    prints it with, so a command whose name is only just too long has its one
    line of help pushed onto a second one. Measuring it the way it is printed is
    what keeps a group's listing a readable two-column table.
    """

    def __init__(self, prog: str) -> None:
        super().__init__(prog, max_help_position=32)

    def add_argument(self, action: argparse.Action) -> None:
        """Reserve the column width the indented subcommand names really need."""

        super().add_argument(action)
        subactions = getattr(action, "_get_subactions", None)
        if subactions is None:
            return
        longest = max(
            (len(self._format_action_invocation(item)) for item in subactions()),
            default=0,
        )
        self._action_max_length = max(self._action_max_length, longest + self._current_indent + 2)


def _group(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    name: str,
    *,
    description: str,
    summary: str,
    prog: str | None = None,
) -> tuple[argparse.ArgumentParser, "argparse._SubParsersAction[argparse.ArgumentParser]"]:
    """Add one command group, and return it with its subcommand action.

    A group invoked with no subcommand prints its own help and exits zero, which
    is what an operator exploring the tree means by typing it.
    """

    if prog is None:
        parser = subparsers.add_parser(
            name,
            help=summary,
            description=description,
            formatter_class=HelpFormatter,
        )
    else:
        parser = subparsers.add_parser(
            name,
            prog=prog,
            help=summary,
            description=description,
            formatter_class=HelpFormatter,
        )
    parser.set_defaults(handler=None, help_parser=parser)
    return parser, parser.add_subparsers(metavar="COMMAND")


def _leaf(
    subparsers: "argparse._SubParsersAction[argparse.ArgumentParser]",
    name: str,
    *,
    description: str,
    summary: str,
    handler: Handler,
    hidden: bool = False,
) -> argparse.ArgumentParser:
    """Add one command, and bind the function that runs it."""

    if hidden:
        parser = subparsers.add_parser(name, description=description, formatter_class=HelpFormatter)
    else:
        parser = subparsers.add_parser(
            name,
            help=summary,
            description=description,
            formatter_class=HelpFormatter,
        )
    parser.set_defaults(handler=handler)
    return parser


def add_durability_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the durability switches to one command that publishes protocol state.

    Both default to ``argparse.SUPPRESS`` so that the value an alias
    executable parsed before its subcommand survives into the handler.
    """

    parser.add_argument(
        "--durable",
        action="store_true",
        default=argparse.SUPPRESS,
        help="fsync protocol publications (the default; accepted for compatibility)",
    )
    parser.add_argument(
        "--no-durable",
        action="store_true",
        default=argparse.SUPPRESS,
        help="do not fsync protocol publications; a crashed node may then strand markers",
    )


def add_workspace_argument(parser: argparse.ArgumentParser, *, help_text: str) -> None:
    """Add the optional workspace name shared by workspace-selecting leaves."""

    parser.add_argument(
        "workspace",
        metavar="WORKSPACE",
        nargs="?",
        default=None,
        help=f"{help_text} (default: the enclosing workspace, this project's workspace, or the per-user default)",
    )


def _durable(arguments: argparse.Namespace) -> bool:
    """Report whether this invocation asked for durable publication.

    Durability is the default: a manager that survives a node crash must not be
    left holding markers that reference journal frames the page cache lost.
    """

    return not getattr(arguments, "no_durable", False)


def _required(
    value: str | None,
    label: str,
    *,
    non_interactive: bool,
    default: str | None = None,
) -> str:
    """Return *value*, asking for it on a terminal and refusing without one."""

    if value:
        return value
    if non_interactive or not sys.stdin.isatty():
        raise ValueError(f"missing required value {label!r} in non-interactive operation")
    suffix = f" [{default}]" if default else ""
    entered = input(f"{label}{suffix}: ").strip()
    result = entered or default
    if not result:
        raise ValueError(f"{label} cannot be empty")
    return result


def confirm(prompt: str, *, force: bool) -> bool:
    """Ask for confirmation on a terminal, or refuse without one.

    This is the one destructive-action gate every ``remove``/``unseal`` leaf
    shares. ``--force`` answers yes without asking; without a terminal and
    without ``--force`` the action is refused with a hint rather than blocking on
    an unanswerable prompt. It calls the builtin :func:`input` so a test that
    patches it still drives the prompt.

    :param prompt: The question to ask, without the ``[y/N]`` suffix.
    :param force: Whether ``--force`` was given, which answers yes unconditionally.
    :return: Whether the action was confirmed.
    """

    if force:
        return True
    if not sys.stdin.isatty():
        print("this operation without a terminal requires --force", file=sys.stderr)
        return False
    if input(f"{prompt} [y/N] ").strip().lower() in {"y", "yes"}:
        return True
    print("not removed")
    return False


def _json_value(text: str, label: str) -> object:
    """Return the JSON value one command-line argument denotes.

    The spelling is the one the Bash SDK's ``--input`` uses: ``@path`` is the JSON
    content of a file, which is how a value too large or too quoted for a command
    line is passed, and anything else is a JSON value when it parses as one and the
    literal string when it does not, so ``k=42`` is a number and ``k=Si`` a string
    without the author quoting either.
    """

    if text.startswith("@"):
        path = Path(text[1:]).expanduser()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"{label} cannot be read as JSON from {path}: {exc}") from exc
    try:
        return json.loads(text)
    except ValueError:
        return text


def _pairs(values: Sequence[str], label: str) -> list[tuple[str, str]]:
    """Split ``NAME=VALUE`` arguments, keeping the order they were given in."""

    result: list[tuple[str, str]] = []
    for item in values:
        name, separator, text = item.partition("=")
        if not separator or not name:
            raise ValueError(f"{label} must be spelled NAME=VALUE, not {item!r}")
        result.append((name, text))
    return result


def _has_input_reader(name: str) -> bool:
    """Report whether a directory file is one this scanner will load.

    A file is loadable when a reader is registered for its name, or — matching
    the SDK's own :func:`~httk.workflow.scaffold.structure_files` — when its name
    is one of the structure conventions, which are then read as POSCAR even where
    ``POSCAR.Si2O`` registers no reader of its own.
    """

    return httk.core.has_reader_for(name) or any(fnmatch.fnmatch(name, pattern) for pattern in STRUCTURE_PATTERNS)


def _load_input_file(path: Path) -> object:
    """Load one input file, forcing the POSCAR reader for a bare structure name.

    :param path: Locate the input file to read.
    :return: The loaded input value.
    :raises ValueError: If the file cannot be read; the message names the file.
    """

    force_poscar = not httk.core.has_reader_for(path.name) and any(
        fnmatch.fnmatch(path.name, pattern) for pattern in STRUCTURE_PATTERNS
    )
    try:
        if force_poscar:
            return httk.core.load_source(str(path), "POSCAR")
        return httk.core.load(str(path))
    except Exception as exc:
        raise ValueError(f"cannot read input source {path}: {exc}") from exc


def _scan_input_directory(path: Path) -> tuple[list[Path], list[str]]:
    """Return the loadable files of a directory and the names it skips."""

    files = sorted(
        (child for child in path.iterdir() if child.is_file() and not child.is_symlink()),
        key=lambda child: child.name,
    )
    loadable = [child for child in files if _has_input_reader(child.name)]
    skipped = [child.name for child in files if not _has_input_reader(child.name)]
    if skipped:
        shown = ", ".join(skipped[:5])
        suffix = f", … (+{len(skipped) - 5} more)" if len(skipped) > 5 else ""
        print(
            f"httk workflow: skipped {len(skipped)} of {len(files)} files in {path} "
            f"(no registered reader): {shown}{suffix}",
            file=sys.stderr,
        )
    return loadable, skipped


def _load_inputs(
    values: Sequence[str], occurrences: Sequence[Sequence[str]]
) -> tuple[dict[str, object], list[JobItem], str | None]:
    """Load staged input sources and split shared values from a batch source.

    A directory expands to its loadable files; unreadable files are skipped with
    one stderr note, and a structure-named file with no reader of its own is read
    as POSCAR. The batch — a directory or a multi-file occurrence — is returned
    as a list so both the single-shot ``job new`` submitter and the campaign
    submitter, which re-reads and re-tags it, see one stable result.
    """

    shared: dict[str, object] = {name: text for name, text in _pairs(values, "a staged workflow input")}
    batch: list[JobItem] = []
    batch_present = False
    single_files: list[Path] = []
    single_tags: list[str | None] = []
    for occurrence in occurrences:
        if len(occurrence) < 2 or not occurrence[0]:
            raise ValueError("--input-from requires NAME followed by at least one SOURCE")
        name = occurrence[0]
        found: list[Path] = []
        for source in occurrence[1:]:
            path = Path(source).expanduser()
            if path.is_dir():
                loadable, _skipped = _scan_input_directory(path)
                if not loadable:
                    raise ValueError(f"no readable input files in {path}")
                found.extend(loadable)
            else:
                if not path.is_file():
                    raise ValueError(f"input source does not exist: {path}")
                found.append(path)
        if len(found) > 1:
            if batch_present:
                raise ValueError("only one --input-from occurrence may contain multiple files")
            batch_present = True
            batch = [
                {"inputs": {name: _load_input_file(path)}, "tag": structure_tag(path) or _sanitize_tag(path.stem)}
                for path in found
            ]
        else:
            path = found[0]
            shared[name] = _load_input_file(path)
            single_files.append(path)
            single_tags.append(structure_tag(path) or _sanitize_tag(path.stem))
    tag = single_tags[0] if len(single_files) == 1 else None
    return shared, batch, tag


def _settings(values: Sequence[str]) -> dict[str, str]:
    """Return the ``KEY=VALUE`` settings one command line carried."""

    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key:
            raise ValueError(f"setting must use KEY=VALUE: {value!r}")
        result[key] = item
    return result


def _field(name: str, value: object) -> str:
    """Render one name/value line of a human-readable description."""

    return f"{name:<22}{value}"


# ---------------------------------------------------------------------------
# Workspace name resolution
#
# Every explicitly named workspace is a registered name, never a bare path. The
# helpers below turn that name into either a local path or a remote binding.
# An omitted name may resolve to an enclosing workspace path. The other
# unregistered path spelling is ``--by-path``: a hidden switch the transfer
# choreography and remote-workspace creation set when one machine invokes a
# command on another, where there is no registry and the workspace is addressed
# by its path.
# ---------------------------------------------------------------------------


def _by_path(arguments: argparse.Namespace) -> bool:
    """Report whether this invocation addresses its workspace by literal path."""

    return bool(getattr(arguments, "by_path", False))


def _resolve_binding(arguments: argparse.Namespace, context: CLIContext) -> tuple[WorkspaceBinding | None, Path | None]:
    """Resolve the workspace argument to a binding and, when local, its path.

    ``--by-path`` yields ``(None, path)``: a literal filesystem path with no
    registry lookup, for the protocol spellings one machine runs on another. A
    missing workspace argument first checks for an enclosing workspace and
    yields ``(None, path)`` in the same way. A registered name yields the
    binding, and its path too when the binding is local; a remote binding yields
    ``(binding, None)`` so the caller either dispatches over the adapter or
    refuses.
    """

    if _by_path(arguments):
        if arguments.workspace is None:
            raise ValueError("--by-path requires an explicit path")
        return None, Path(arguments.workspace)
    if arguments.workspace is None:
        discovered = Workspace.discover(context.cwd)
        if discovered is not None:
            return None, discovered
    binding = (
        default_workspace(project=context.cwd)
        if arguments.workspace is None
        else resolve_workspace(arguments.workspace, project=context.cwd)
    )
    if binding.remote == LOCAL_REMOTE:
        assert binding.path is not None
        return binding, Path(binding.path)
    return binding, None


def _local_root(arguments: argparse.Namespace, context: CLIContext, *, action: str) -> Path:
    """Return the local path of the named workspace, refusing a remote binding."""

    binding, root = _resolve_binding(arguments, context)
    if root is None:
        assert binding is not None
        raise ValueError(
            f"workspace {binding.name!r} is bound to the remote {binding.remote!r}, so it cannot "
            f"{action} locally; run this on {binding.remote}, or reach it with "
            f"`httk workflow transfer` and `httk workspace status {binding.name}`"
        )
    return root


def _run_remote_workspace(
    binding: WorkspaceBinding,
    context: CLIContext,
    argv_tail: Sequence[str],
    *,
    timeout: float | None = None,
    unwrap_json_array: bool = False,
) -> int:
    """Run one command against a remote binding's workspace and echo its output.

    The workspace on the far side resolves its plain name in its own registry.
    Its standard output and error are relayed verbatim and its exit status is
    returned, so a read command reads exactly as it would locally.

    :param binding: Remote-qualified workspace binding.
    :param context: Current CLI invocation context.
    :param argv_tail: Complete far-side command argument vector.
    :param timeout: Adapter timeout override.
    :param unwrap_json_array: Unwrap the far side's one-target batch response
        before the local batch wrapper incorporates it.
    :return: The far-side process exit status.
    """

    returncode, stdout, stderr = remote_workspace_output(
        binding,
        context,
        argv_tail,
        timeout=timeout,
    )
    if stdout and unwrap_json_array:
        values = json.loads(stdout)
        if not isinstance(values, list) or len(values) > 1:
            raise ValueError("remote workspace command returned an invalid batch response")
        stdout = json.dumps(values[0] if values else None, indent=2, sort_keys=True)
    if stdout:
        sys.stdout.write(stdout if stdout.endswith("\n") else stdout + "\n")
    if stderr:
        sys.stderr.write(stderr)
    return returncode


def remote_workspace_output(
    binding: WorkspaceBinding,
    context: CLIContext,
    argv_tail: Sequence[str],
    *,
    timeout: float | None = None,
) -> tuple[int, str, str]:
    """Invoke a remote workspace command and return status, stdout, stderr.

    Unlike :func:`_run_remote_workspace`, this variant never prints. It is the
    returning transport used by consumers such as the monitor, whose worker
    threads must not write into a curses terminal.

    :param binding: Remote-qualified workspace binding.
    :param context: CLI context used to resolve the adapter.
    :param argv_tail: Complete command vector for the remote side.
    :param timeout: Adapter timeout override.
    :return: Remote return code, standard output, and standard error.
    """

    if ":" not in binding.name:
        raise ValueError(f"remote workspace binding has no remote-qualified name: {binding.name}")
    target = resolve_remote(binding.remote, project=context.cwd)
    result = run_adapter(
        target.bundle,
        "invoke",
        {"argv": list(argv_tail)},
        timeout=timeout,
    )
    return (
        int(result.get("returncode", 0) or 0),
        str(result.get("stdout", "")),
        str(result.get("stderr", "")),
    )


def _remote_workspace_read(
    binding: WorkspaceBinding,
    context: CLIContext,
    command: Sequence[str],
    arguments: argparse.Namespace,
    *,
    flags: Sequence[str] = (),
    tail: Sequence[str] = (),
    unwrap_json_array: bool | None = None,
) -> int:
    """Dispatch one read-style workspace command to a remote binding.

    *command* is a pinned far-side vector, e.g. ``REMOTE_STATUS_COMMAND``; the
    enabled *flags* and *tail* options precede the remote plain name. This is the single path every remote-capable read command runs
    through, so ``status``, ``gc``, ``fsck``, ``job_records``, and the ``job`` reads
    all reach a remote the same way.
    """

    enabled_flags: list[str] = []
    for flag in flags:
        if getattr(arguments, flag.lstrip("-").replace("-", "_"), False):
            enabled_flags.append(flag)
    argv = [*command, *enabled_flags, *tail, binding.name.split(":", 1)[1]]
    returncode, stdout, stderr = remote_workspace_output(
        binding,
        context,
        argv,
        timeout=getattr(arguments, "adapter_timeout", None),
    )
    if stdout and (getattr(arguments, "json", False) if unwrap_json_array is None else unwrap_json_array):
        values = json.loads(stdout)
        if not isinstance(values, list) or len(values) > 1:
            raise ValueError("remote workspace command returned an invalid batch response")
        stdout = json.dumps(values[0] if values else None, indent=2, sort_keys=True)
    if stdout:
        sys.stdout.write(stdout if stdout.endswith("\n") else stdout + "\n")
    if stderr:
        sys.stderr.write(stderr)
    return returncode


def remote_workspace_read_output(
    binding: WorkspaceBinding,
    context: CLIContext,
    command: Sequence[str],
    arguments: argparse.Namespace,
    *,
    flags: Sequence[str] = (),
    tail: Sequence[str] = (),
    unwrap_json_array: bool | None = None,
) -> tuple[int, str, str]:
    """Return one remote read's output without writing to process streams.

    :param binding: Remote-qualified workspace binding.
    :param context: CLI context used to resolve the adapter.
    :param command: Pinned remote command vector.
    :param arguments: Parsed arguments supplying enabled flags and timeout.
    :param flags: Optional switches copied when true on *arguments*.
    :param tail: Command arguments before the remote workspace name.
    :param unwrap_json_array: Retained for API symmetry; output is not decoded.
    :return: Remote return code, standard output, and standard error.
    """

    if ":" not in binding.name:
        raise ValueError(f"remote workspace binding has no remote-qualified name: {binding.name}")
    enabled_flags = [flag for flag in flags if getattr(arguments, flag.lstrip("-").replace("-", "_"), False)]
    argv = [*command, *enabled_flags, *tail, binding.name.split(":", 1)[1]]
    return remote_workspace_output(
        binding,
        context,
        argv,
        timeout=getattr(arguments, "adapter_timeout", None),
    )


def _add_by_path_argument(parser: argparse.ArgumentParser) -> None:
    """Add the hidden switch that makes the workspace argument a literal path.

    It is protocol, not user interface: the transfer choreography and remote
    workspace creation set it when one machine runs a command on another, where
    the workspace is addressed by path because the far side keeps no registry.
    """

    parser.add_argument("--by-path", action="store_true", help=argparse.SUPPRESS)


def _add_adapter_timeout(parser: argparse.ArgumentParser) -> None:
    """Add the bound on every adapter call one command makes."""

    parser.add_argument(
        "--adapter-timeout",
        type=float,
        metavar="SECONDS",
        help="bound every adapter operation this command runs (default: the remote's timeout_seconds)",
    )


def _published_runner_entries(directory: Path) -> Iterator[Path]:
    """Yield each runner a workspace store publishes: a file, or one tree.

    A subdirectory that holds the tree entry point is one directory runner; any
    other subdirectory is a namespace descended into. This is the single walk
    both ``runner describe`` and ``workspace workflows`` list the store by.

    :param directory: The store root, or a namespace within it, to walk.
    :yield: Each published file runner and directory tree, name-sorted.
    """

    for path in sorted(directory.iterdir()):
        if path.is_file():
            yield path
        elif path.is_dir():
            if (path / RUNNER_TREE_ENTRY).is_file():
                yield path
            else:
                yield from _published_runner_entries(path)
