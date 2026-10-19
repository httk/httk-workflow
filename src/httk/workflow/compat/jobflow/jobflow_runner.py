#!/usr/bin/env python3
"""Run a jobflow Flow as one persistent httk scheduler and many child jobs."""

import fnmatch
import importlib
import importlib.util
import json
import os
import shlex
import shutil
import signal
import sys
import traceback
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import chdir, contextmanager, suppress
from pathlib import Path, PurePosixPath
from typing import Any, cast

from httk.workflow import Attempt, ChildSpec, Runner, Workspace, _fs, _kernel
from httk.workflow._durations import TIME_RESOURCES, format_duration
from httk.workflow.codes import launch_command
from httk.workflow.compat.jobflow import OUTPUTS_FILE, STAGED_DIRECTORY, _validate_overrides
from httk.workflow.models import canonical_uuid, normalize_resources

WORKFLOW = "jobflow.workflow"
STATE_FILE = "state.json"
STORE_FILE = "store.json"
SPOOL_DIRECTORY = "spool"
CHILD_OUTPUTS_FILE = "child-outputs.json"
#: Stands for a child job's ``run/exec`` directory in shipped documents: ``<token><job_id>[/suffix]``.
EXEC_TOKEN = "httk-jobflow-exec://"
#: Stands for a staged directory input: ``<token><port>/<basename>[/suffix]``, resolved in each child
#: to its own copy under ``run/inputs/``.
INPUT_TOKEN = "httk-jobflow-input://"

run = Runner(WORKFLOW)

#: The httk settings naming VASP commands, and the atomate2 variables they set.
_VASP_COMMANDS = (
    ("vasp.command", "ATOMATE2_VASP_CMD"),
    ("vasp.gamma_command", "ATOMATE2_VASP_GAMMA_CMD"),
    ("vasp.ncl_command", "ATOMATE2_VASP_NCL_CMD"),
)
#: httk-workflow-vasp's VASP launch wrapper, run as a module; this runner never imports it.
_DISPATCHER = "httk.codes.vasp.launch"
#: Workspace setting prefixes passed through to the variables a library reads.
_LIBRARY_PREFIXES = {"atomate2.": "ATOMATE2_", "pymatgen.": "PMG_"}
#: Job parameters a parent forwards to every child, so the job layer of these settings reaches ``enter``.
_FORWARDED_PARAMETERS = frozenset(name for name, _variable in _VASP_COMMANDS) | {
    "pymatgen.psp_dir",
    "vasp.parallelization",
}
# ponytail: fixed, below the manager's default 10 s cancel grace so the SIGKILL escalation of
# custodian's groups runs before the manager kills this runner; derive it if that grace is tuned.
_STOP_GRACE_SECONDS = 5.0
# Each lost race needs the parent re-claimed between one lookup and the next open: a
# few tries make losing them all practically impossible.
_PARENT_ATTEMPTS = 5
# A bound for read_bounded only: the spool files are this workflow's own, written by the parent.
_SPOOL_LIMIT = 1 << 31


def _atomate2_environment(a: Attempt, *, vasp: bool = True) -> dict[str, str]:
    """Return the atomate2 and pymatgen variables this attempt's settings imply.

    Workspace settings and job parameters ``atomate2.<key>`` and ``pymatgen.<key>``
    become ``ATOMATE2_<KEY>`` and ``PMG_<KEY>``; ``pymatgen.psp_dir`` instead becomes
    ``PMG_VASP_PSP_DIR``. With *vasp*, when ``vasp.command`` resolves, it and
    ``vasp.gamma_command``/``vasp.ncl_command`` set ``ATOMATE2_VASP_CMD``,
    ``ATOMATE2_VASP_GAMMA_CMD`` and ``ATOMATE2_VASP_NCL_CMD``. When
    *httk-workflow-vasp* is installed, all three name its launch wrapper,
    ``python -m httk.codes.vasp.launch`` with the commands as ``--standard``,
    ``--ncl`` (``vasp.command`` when unset) and ``--gamma`` (when set), in front of
    the launch prefix: run once per launch in the job, it picks the program from
    the INCAR and KPOINTS, writes planned ``KPAR``/``NCORE`` into the INCAR (unless
    ``vasp.parallelization`` is ``off``) and applies the attempt's launch prefix
    (``HTTK_WORKFLOW_LAUNCH``) itself. Without it each command maps directly with
    the launch prefix prepended, an unset gamma/ncl command reusing
    ``vasp.command``, and a set one appends a run-log warning. These variables
    override the libraries' own configuration files, which a confined attempt
    cannot see.

    :param a: The attempt whose settings are mapped.
    :param vasp: Whether to map the VASP commands.
    :return: The variables to set.
    :raises ValueError: If a VASP command names a parallel launcher while a launch prefix applies.
    """

    result: dict[str, str] = {}
    for key, value in {**a.context.settings, **a.parameters}.items():
        if key == "pymatgen.psp_dir" or isinstance(value, bool) or not isinstance(value, (str, int, float)):
            continue
        for prefix, variable in _LIBRARY_PREFIXES.items():
            if key.startswith(prefix):
                result[variable + key.removeprefix(prefix).upper().replace(".", "_")] = str(value)
    psp_dir = a.setting("pymatgen.psp_dir", None)
    if isinstance(psp_dir, str) and psp_dir:
        result["PMG_VASP_PSP_DIR"] = psp_dir
    command = a.setting("vasp.command", None)
    if not vasp or not isinstance(command, str) or not command.strip():
        return result
    commands = {
        name: value
        for name, _variable in _VASP_COMMANDS
        if isinstance(value := a.setting(name, None), str) and value.strip()
    }
    if _dispatcher_installed():
        for name, value in commands.items():
            try:
                launch_command(shlex.split(value))  # refuses a launcher while a prefix applies
            except ValueError as exc:
                raise ValueError(f"the {name} setting: {exc}") from exc
        # -P: the job's writable working directory stays off the wrapper's sys.path. An unset
        # ncl command reuses vasp.command, as run_vasp does, so a run keeps the command it was given.
        argv = [sys.executable, "-P", "-m", _DISPATCHER, "--standard", commands["vasp.command"]]
        argv += ["--ncl", commands.get("vasp.ncl_command", commands["vasp.command"])]
        if "vasp.gamma_command" in commands:
            argv += ["--gamma", commands["vasp.gamma_command"]]
        # The INCAR is written inside the job, so the variant and parallelization are chosen there,
        # once, before the wrapper applies the launch prefix; whichever command custodian's
        # auto_gamma picks, it runs this same wrapper.
        wrapper = shlex.join(argv)
        return {**result, **{variable: wrapper for _name, variable in _VASP_COMMANDS}}
    if commands.keys() - {"vasp.command"}:
        a.log.append(
            "warning",
            "vasp.ncl_command and vasp.gamma_command need httk-workflow-vasp where the job runs; "
            "mapping them directly instead",
        )
    for name, variable in _VASP_COMMANDS:
        value = commands.get(name)
        if value is None:
            # An unset gamma/ncl command reuses vasp.command, so custodian's
            # auto_gamma never runs a bare vasp_gam outside the launch prefix.
            result[variable] = result["ATOMATE2_VASP_CMD"]
            continue
        try:
            result[variable] = shlex.join(launch_command(shlex.split(value)))
        except ValueError as exc:
            raise ValueError(f"the {name} setting: {exc}") from exc
    return result


def _dispatcher_installed() -> bool:
    """Report whether *httk-workflow-vasp*'s VASP variant dispatcher is installed, without importing it.

    Finding ``httk.codes.vasp`` imports only the ``httk.codes`` namespace, never the VASP package.
    """
    try:
        spec = importlib.util.find_spec(_DISPATCHER.rpartition(".")[0])
    except Exception:
        return False
    locations = (spec.submodule_search_locations or []) if spec is not None else []
    return any(Path(location, "launch.py").is_file() for location in locations)


def _apply_atomate2_environment(a: Attempt, spooled: Mapping[str, object]) -> bool:
    """Export the settings-derived atomate2 variables for one spooled job, or fail the attempt.

    A launch conflict fails only an atomate2 job (its function's module is under
    ``atomate2.``); any other job runs without the VASP variables, with a run-log warning.

    :param a: The attempt to configure.
    :param spooled: The spooled job mapping, not yet decoded.
    :return: Whether the step may continue.
    """

    try:
        environment = _atomate2_environment(a)
    except ValueError as exc:
        function = spooled.get("function")
        module = function.get("@module") if isinstance(function, Mapping) else None
        if isinstance(module, str) and module.startswith("atomate2."):
            a.fail("jobflow.launch_conflict", str(exc))
            return False
        environment = _atomate2_environment(a, vasp=False)
        a.log.append("warning", f"VASP commands are not exported to this non-atomate2 job: {exc}")
    os.environ.update(environment)
    return True


@contextmanager
def _forwarded_stop_signals() -> Iterator[None]:
    """Forward ``SIGTERM``/``SIGINT`` to every process group of a descendant other than this one's.

    custodian starts VASP in a new session, out of reach of the manager's signal to
    this attempt's group. On a signal, each such group gets it, every one of those
    groups gets ``SIGKILL`` after a grace period (members can outlive their leader),
    and the signal is re-raised with its default action. Without ``psutil`` nothing
    is installed.
    """
    try:
        import psutil
    except ImportError:
        yield
        return

    def forward(signum: int, _frame: object) -> None:
        own = os.getpgrp()
        members: list[psutil.Process] = []
        groups: set[int] = set()
        for child in psutil.Process().children(recursive=True):
            with suppress(OSError):
                group = os.getpgid(child.pid)
                if group != own:
                    members.append(child)
                    groups.add(group)
        for group in groups:
            with suppress(OSError):
                os.killpg(group, signum)
        psutil.wait_procs(members, timeout=_STOP_GRACE_SECONDS)
        for group in groups:
            with suppress(OSError):
                os.killpg(group, signal.SIGKILL)
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    previous = {number: signal.signal(number, forward) for number in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def _require_dependencies(maker_module: str | None, *, need_pymatgen: bool = False) -> None:
    """Require the packages needed by a jobflow runner process."""
    missing = [name for name in ("jobflow", "maggma", "monty") if importlib.util.find_spec(name) is None]
    if need_pymatgen and importlib.util.find_spec("pymatgen") is None:
        missing.append("pymatgen")
    if (
        maker_module is not None
        and maker_module.startswith("atomate2.")
        and importlib.util.find_spec("atomate2") is None
    ):
        raise ValueError("missing dependency atomate2; install it with pip install httk-workflow[atomate2]")
    if missing:
        raise ValueError(
            f"missing dependency {missing[0]}; install jobflow support with pip install httk-workflow[jobflow]"
        )


def _write_json(path: Path, value: object) -> None:
    """Write one JSON document through a same-directory atomic replace."""
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _map_strings(value: object, rewrite: Callable[[str], str]) -> Any:
    """Return *value* with every string nested in dicts and lists passed through *rewrite*."""
    if isinstance(value, str):
        return rewrite(value)
    if isinstance(value, dict):
        return {key: _map_strings(item, rewrite) for key, item in value.items()}
    if isinstance(value, list):
        return [_map_strings(item, rewrite) for item in value]
    return value


def _tokenize_exec(value: str, directories: Mapping[str, str]) -> str:
    """Replace a reference into a known directory, optionally ``host:``-prefixed, by its token.

    :param value: The string to rewrite.
    :param directories: The token each directory stands for (an exec directory's
        ``EXEC_TOKEN + job_id``).
    :return: The token form, or *value* unchanged.
    """
    host, separator, rest = value.partition(":")
    for candidate in (value, rest) if separator and "/" not in host else (value,):
        for directory, token in directories.items():
            if candidate == directory or candidate.startswith(directory + "/"):
                suffix = candidate[len(directory) :]
                if suffix or not token.endswith(":/"):  # the inputs root itself has no token
                    return f"{token}{suffix}"
    return value


def _exec_token_ids(value: object) -> set[str]:
    """Return the job ids of the exec-directory tokens nested in *value*."""
    ids: set[str] = set()

    def note(text: str) -> str:
        if text.startswith(EXEC_TOKEN):
            ids.add(text.removeprefix(EXEC_TOKEN).partition("/")[0])
        return text

    _map_strings(value, note)
    return ids


def _locate_exec_directories(a: Attempt, placement: PurePosixPath, job_ids: set[str]) -> dict[str, str]:
    """Return the current ``run/exec`` directory of each job found, from one settled listing pass.

    :param a: The attempt whose workspace is searched.
    :param placement: The placement the jobs share (their parent's).
    :param job_ids: The job ids to find; malformed ones are never found.
    :return: The exec directory of each job found, by job id.
    """
    wanted = set()
    for job_id in job_ids:
        with suppress(ValueError):
            wanted.add(canonical_uuid(job_id, "job_id"))
    if not wanted:
        return {}
    refs = _kernel.locate_many(
        Workspace(a.workspace, durable=a.context.durable), sorted(wanted), placements=[placement], settle=True
    )
    return {job_id: str(ref.path / "run" / "exec") for job_id, ref in refs.items()}


def _resolve_exec(
    value: str,
    directories: Mapping[str, str],
    inputs_root: str | None = None,
    copied: frozenset[str] | set[str] = frozenset(),
) -> str:
    """Replace an exec-directory or directory-input token by the current local path, when known.

    :param value: The string to rewrite.
    :param directories: The exec directory of each job id.
    :param inputs_root: This child's copy of the staged directory inputs.
    :param copied: The ``<port>/<basename>`` directory inputs copied under *inputs_root*.
    :return: The plain absolute path, or *value* unchanged.
    :raises ValueError: If the token's path climbs out of its directory.
    """
    for token in (INPUT_TOKEN, EXEC_TOKEN):
        if value.startswith(token):
            rest = value.removeprefix(token)
            break
    else:
        return value
    if ".." in PurePosixPath(rest).parts:
        raise ValueError(f"the directory reference {value!r} climbs out of its directory")
    if token == INPUT_TOKEN:
        member = "/".join(PurePosixPath(rest).parts[:2])
        return f"{inputs_root}/{rest}" if inputs_root is not None and member in copied else value
    job_id, slash, suffix = rest.partition("/")
    directory = directories.get(job_id)
    return value if directory is None else directory + slash + suffix


def _input_members(values: object) -> set[str]:
    """Return the ``<port>/<basename>`` directory inputs the input tokens in *values* name.

    :param values: The documents to scan.
    :return: The named directory inputs.
    :raises ValueError: If a token is malformed.
    """
    members: set[str] = set()

    def note(text: str) -> str:
        if text.startswith(INPUT_TOKEN):
            parts = PurePosixPath(text.removeprefix(INPUT_TOKEN)).parts[:2]
            if len(parts) != 2 or ".." in parts or "/" in parts:
                raise ValueError(f"malformed directory input reference {text!r}")
            members.add("/".join(parts))
        return text

    _map_strings(values, note)
    return members


def _stage_directory_inputs(anchor: int, members: Iterable[str], root: Path) -> None:
    """Copy staged directory inputs from the parent's anchored ``files/inputs`` into this child's *root*.

    The copy reads through descriptors, so the parent's job directory may be renamed (re-claimed
    or moved) meanwhile without disturbing it.

    :param anchor: A descriptor of the parent's job directory.
    :param members: The ``<port>/<basename>`` directory inputs to copy.
    :param root: This child's ``inputs`` directory.
    :raises ValueError: If a directory input is not staged in the parent.
    """
    # ponytail: every child naming a directory input copies the whole tree (a WAVECAR too);
    # copy only the files a consumer reads if that cost shows up.
    for member in sorted(members):
        port, basename = member.split("/")
        target = root / port / basename
        shutil.rmtree(target, ignore_errors=True)
        try:
            port_directory = _fs.open_dir_under(anchor, f"{STAGED_DIRECTORY}/{port}")
            try:
                _fs.copy_tree(_fs.anchored(port_directory, basename), target, durable=False)
            finally:
                os.close(port_directory)
        except (OSError, _fs.UnsafePath) as exc:
            raise ValueError(f"the staged directory input {member!r} is not in the parent job: {exc}") from exc


def _payload_path(a: Attempt, value: object) -> Path:
    """Resolve one safe payload-relative path."""
    relative = PurePosixPath(str(value))
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"payload path {value!r} escapes the payload")
    return a.payload.joinpath(*relative.parts)


def _maker_kwargs(a: Attempt) -> dict[str, object]:
    """Return present declared Maker configuration values."""
    names = a.parameter("jobflow_maker_parameters", ())
    if isinstance(names, (str, bytes)) or not isinstance(names, Sequence):
        raise ValueError("jobflow_maker_parameters must be an array")
    defaults = a.parameter("jobflow_maker_defaults", {})
    if not isinstance(defaults, Mapping):
        raise ValueError("jobflow_maker_defaults must be an object")
    missing = object()
    result: dict[str, object] = {}
    for name in names:
        if not isinstance(name, str):
            raise ValueError("jobflow_maker_parameters must contain strings")
        value = a.parameter(name, missing)
        if value is missing and name in defaults:
            value = defaults[name]
        if value is not missing:
            result[name] = value
    return result


def _load_maker(a: Attempt, kwargs: Mapping[str, object]) -> object:
    """Load and configure the root Maker from a spec or staged document."""
    maker_spec = a.parameter("jobflow_maker", None)
    if isinstance(maker_spec, str):
        module_name, separator, class_name = maker_spec.partition(":")
        if not separator:
            raise ValueError(f"jobflow_maker is not a module:Class spec: {maker_spec!r}")
        maker_class = getattr(importlib.import_module(module_name), class_name)
        return maker_class(**kwargs)

    pointer = a.parameter("jobflow_document", None)
    if not isinstance(pointer, str):
        raise ValueError("jobflow needs either jobflow_maker or jobflow_document")
    from monty.json import MontyDecoder

    maker = MontyDecoder().process_decoded(json.loads(_payload_path(a, pointer).read_text(encoding="utf-8")))
    if kwargs:
        from dataclasses import replace

        maker = replace(maker, **kwargs)
    return maker


def _apply_maker_override(maker: object, update: Mapping[str, object]) -> object:
    """Return ``maker`` with each dotted path of a ``maker`` override set, without mutating it.

    Each path walks dataclass fields, or keys of a mapping; a mapping leaf may be a new key.
    Values are monty-decoded, so an ``@module``/``@class`` object becomes the instance it names.
    Rebuilt mappings are plain dicts.

    :param maker: The root Maker, before ``make()``.
    :param update: The override's ``update``: dotted path to JSON value, applied in order.
    :return: The rebuilt root Maker.
    :raises ValueError: If a path names no field or key, passes through ``None``, or a value cannot be decoded.
    """
    from dataclasses import fields, is_dataclass, replace

    from monty.json import MontyDecoder

    def put(obj: object, segments: list[str], value: object, path: str) -> object:
        segment, rest = segments[0], segments[1:]
        if isinstance(obj, Mapping):
            if rest and segment not in obj:
                raise ValueError(f"path {path!r}: no key {segment!r}")
            mapping = cast(Mapping[str, object], obj)
            return {**mapping, segment: put(mapping[segment], rest, value, path) if rest else value}
        if obj is None:
            raise ValueError(f"path {path!r}: {segment!r} is reached through None")
        if not is_dataclass(obj) or isinstance(obj, type) or segment not in {f.name for f in fields(obj)}:
            raise ValueError(f"path {path!r}: {segment!r} is not a field of {type(obj).__name__}")
        child = put(getattr(obj, segment), rest, value, path) if rest else value
        return replace(obj, **{segment: child})

    def undecoded(value: object) -> bool:
        if isinstance(value, Mapping):
            return ("@module" in value and "@class" in value) or any(undecoded(v) for v in value.values())
        return isinstance(value, list) and any(undecoded(v) for v in cast(list[object], value))

    decoder = MontyDecoder()
    for path, value in update.items():
        decoded = decoder.process_decoded(value)
        # monty keeps an object it cannot import as its dict; refuse it before any job runs.
        if undecoded(decoded):
            raise ValueError(f"path {path!r}: the value cannot be decoded (an @module/@class object is not importable)")
        maker = put(maker, path.split("."), decoded, path)
    return maker


def _import_spec(spec: str) -> Any:
    """Import the object a ``module:name`` spec names."""
    module_name, _, name = spec.partition(":")
    return getattr(importlib.import_module(module_name), name)


def _apply_override(flow: Any, override: Mapping[str, Any]) -> Any:
    """Apply one validated ``jobflow_overrides`` operation and return the resulting flow or job."""
    from jobflow.core.flow import Flow
    from jobflow.core.job import Job

    kind = override["kind"]
    if kind == "maker_kwargs":
        class_filter = override.get("class_filter")
        flow.update_maker_kwargs(
            dict(override["update"]),
            name_filter=override.get("name_filter"),
            class_filter=None if class_filter is None else _import_spec(class_filter),
            nested=override.get("nested", True),
            dict_mod=override.get("dict_mod", False),
        )
        return flow
    if kind == "config":
        flow.update_config(
            dict(override["config"]),
            name_filter=override.get("name_filter"),
            attributes=override.get("attributes"),
            dynamic=override.get("dynamic", True),
            dict_mod=override.get("dict_mod", False),
        )
        return flow
    kwargs = {
        name: _import_spec(value) if name == "class_filter" and isinstance(value, str) else value
        for name, value in override.get("kwargs", {}).items()
    }
    result = _import_spec(override["function"])(flow, **kwargs)
    return result if isinstance(result, (Flow, Job)) else flow


def _load_inputs(a: Attempt) -> dict[str, object]:
    """Decode literal, JSON, and structure inputs staged by the language hook."""
    raw_inputs = a.parameter("jobflow_inputs", {})
    if not isinstance(raw_inputs, Mapping):
        raise ValueError("jobflow_inputs must be an object")
    from monty.json import MontyDecoder

    decoder = MontyDecoder()
    result: dict[str, object] = {}
    for label, raw in raw_inputs.items():
        try:
            if not isinstance(raw, Mapping) or raw.get("kind") not in {"value", "path", "directory"}:
                raise ValueError("invalid kind")
            if raw["kind"] == "value":
                result[str(label)] = decoder.process_decoded(raw.get("value"))
                continue
            if raw["kind"] == "directory":
                # Each child copies the staged directory and resolves this token to its copy.
                staged = PurePosixPath(str(raw.get("value"))).relative_to(STAGED_DIRECTORY)
                result[str(label)] = f"{INPUT_TOKEN}{staged.as_posix()}"
                continue
            path = _payload_path(a, raw.get("value"))
            if path.suffix == ".json":
                result[str(label)] = decoder.process_decoded(json.loads(path.read_text(encoding="utf-8")))
                continue
            from pymatgen.core import Structure

            result[str(label)] = Structure.from_file(path)
        except Exception as exc:
            raise ValueError(f"input {label!r}: {exc}") from exc
    return result


def _new_documents(
    store: Any,
    baseline: set[tuple[str, int]],
    baseline_blobs: set[tuple[str, str]],
) -> list[dict[str, object]]:
    """Return newly created main and additional-store documents."""
    documents: list[dict[str, object]] = []
    for document in store.query():
        key = (str(document.get("uuid")), int(document.get("index", 0)))
        if key not in baseline:
            documents.append({key: value for key, value in document.items() if key != "_id"})
    for name, blob_store in store.additional_stores.items():
        blob_store.connect()
        for document in blob_store.query():
            blob_key = (name, str(document.get("blob_uuid")))
            if blob_key in baseline_blobs:
                continue
            documents.append({"store": name, **{key: value for key, value in document.items() if key != "_id"}})
    return documents


def _open_store(snapshot: Path | None = None) -> Any:
    """Open an in-memory JobStore, optionally loading an atomic snapshot."""
    from jobflow.core.store import JobStore
    from maggma.stores import MemoryStore

    def connected() -> Any:
        # jobflow writes a data=True output to an additional store it creates on first use.
        blob_store = MemoryStore()
        blob_store.connect()
        return blob_store

    store = JobStore(MemoryStore(), additional_stores=defaultdict(connected))
    store.connect()
    if snapshot is not None:
        from httk.workflow.compat.jobflow._driver import merge_documents

        documents = json.loads(snapshot.read_text(encoding="utf-8"))
        if not isinstance(documents, list):
            raise ValueError("jobflow store snapshot is not an array")
        merge_documents(store, [item for item in documents if isinstance(item, Mapping)])
    return store


def _persist_store(store: Any, snapshot: Path) -> None:
    """Persist a complete in-memory store through an atomic snapshot replace."""
    from httk.workflow.compat.jobflow._driver import snapshot_documents

    _write_json(snapshot, snapshot_documents(store))


def _recover_unmaterialized(
    state: Any,
    children: dict[str, str],
    observed_labels: set[str],
    processed: set[str],
) -> None:
    """Re-pend running jobs whose spawn outcome was never materialized."""
    materialized = {children[label] for label in observed_labels if label in children}
    for running_key in list(state.running):
        if running_key in materialized:
            continue
        state.mark_pending(running_key)
        for label, key in list(children.items()):
            if key == running_key and label not in observed_labels:
                children.pop(label)
                processed.discard(label)


def _decode_response(raw: object) -> Any:
    """Decode the child response mapping into jobflow's Response class."""
    from jobflow.core.job import Response
    from monty.json import MontyDecoder

    if not isinstance(raw, Mapping):
        raise ValueError("child response is not an object")
    decoded = MontyDecoder().process_decoded(dict(raw))
    if not isinstance(decoded, Mapping):
        raise ValueError("child response did not decode to an object")
    fields = {
        name: decoded.get(name)
        for name in (
            "output",
            "detour",
            "addition",
            "replace",
            "stored_data",
            "stop_children",
            "stop_jobflow",
            "job_dir",
        )
    }
    return Response(**cast(Any, fields))


def _child_resources(a: Attempt, job: Any) -> dict[str, int | str] | None:
    """Return one ready job's resources: the parent's overlaid by its table, or ``None`` to inherit.

    The table is the first ``jobflow_job_resources`` row whose glob matches the job name,
    else the job's ``manager_config["httk"]["resources"]``.

    :param a: The parent attempt.
    :param job: The ready jobflow job.
    :return: The child's resources with Slurm duration strings, or ``None``.
    :raises ValueError: If the job's ``manager_config`` resource table is malformed.
    """
    table: Mapping[str, int] | None = None
    for glob, resources in cast(list[list[Any]], a.parameter("jobflow_job_resources", [])):
        if fnmatch.fnmatchcase(str(job.name), str(glob)):
            table = cast(Mapping[str, int], resources)
            break
    if table is None:
        namespace = (job.config.manager_config or {}).get("httk", {})
        if not isinstance(namespace, Mapping):
            raise ValueError(f"jobflow job {job.name!r}: manager_config['httk'] must be a mapping")
        if namespace.get("resources") is None:
            return None
        table = normalize_resources(namespace["resources"], f"jobflow job {job.name!r} manager_config httk.resources")
    cap = a.context.resources.get("maxtime")
    if cap is not None and table.get("maxtime", 0) > cap:
        a.log.append(
            "warning",
            f"jobflow job {job.name!r} asks for maxtime {format_duration(table['maxtime'])}, but this flow's "
            f"attempt has {format_duration(cap)}; the job gets {format_duration(cap)}",
        )
    inherited = cast(Mapping[str, int], a.job.resources)
    merged = {**{name: value for name, value in inherited.items() if name != "mintime"}, **table}
    return {name: format_duration(value) if name in TIME_RESOURCES else value for name, value in merged.items()}


def _failure_message(child: Any) -> str:
    """Return the most useful message available from one failed child."""
    failure = getattr(child, "failure", None)
    message = getattr(failure, "message", None)
    return str(message or getattr(child, "kind", "child failed"))


@run.step
def start(a: Attempt) -> None:
    """Build the root flow and initialize its persistent scheduler state."""
    # No VASP runs here, so only the library passthrough applies and nothing can conflict.
    os.environ.update(_atomate2_environment(a, vasp=False))
    maker_module = None
    maker_spec = a.parameter("jobflow_maker", None)
    if isinstance(maker_spec, str):
        maker_module = maker_spec.partition(":")[0]
    else:
        pointer = a.parameter("jobflow_document", None)
        if isinstance(pointer, str):
            try:
                maker_module = str(json.loads(_payload_path(a, pointer).read_text(encoding="utf-8"))["@module"])
            except (KeyError, OSError, TypeError, ValueError) as exc:
                a.fail("jobflow.document_invalid", f"cannot inspect Maker document: {exc}")
                return
    try:
        inputs_parameter = a.parameter("jobflow_inputs", {})
        needs_structure = isinstance(inputs_parameter, Mapping) and any(
            raw.get("kind") == "path" and Path(str(raw.get("value"))).suffix != ".json"
            for raw in inputs_parameter.values()
            if isinstance(raw, Mapping)
        )
        _require_dependencies(maker_module, need_pymatgen=needs_structure)
    except ValueError as exc:
        a.fail("jobflow.missing_dependency", str(exc))
        return
    try:
        maker = _load_maker(a, _maker_kwargs(a))
    except Exception as exc:
        a.fail("jobflow.maker_config_failed", f"cannot configure the jobflow Maker: {exc}")
        return
    overrides = a.parameter("jobflow_overrides", [])
    try:
        # Checked at job creation too; a job that arrived another way is checked here.
        _validate_overrides(overrides)
    except ValueError as exc:
        a.fail("jobflow.override_failed", str(exc))
        return
    # Declared-parameter constructor kwargs, then ``maker`` overrides (before ``make()`` reads the
    # Maker), then ``make()``, then the flow kinds; each group keeps list order.
    operations = list(enumerate(cast(list[Mapping[str, Any]], overrides)))
    for index, override in operations:
        if override["kind"] != "maker":
            continue
        try:
            maker = _apply_maker_override(maker, override["update"])
        except Exception as exc:
            a.fail(
                "jobflow.override_failed",
                f"jobflow_overrides[{index}] (maker) failed: {type(exc).__name__}: {exc}",
            )
            return
    make = getattr(maker, "make", None)
    if not callable(make):
        a.fail("jobflow.document_invalid", "the configured Maker has no callable make method")
        return
    try:
        inputs = _load_inputs(a)
    except Exception as exc:
        a.fail("jobflow.input_invalid", f"cannot load a jobflow input: {exc}")
        return
    try:
        flow = make(**inputs)
    except Exception as exc:
        a.fail("jobflow.make_failed", f"Maker.make failed: {type(exc).__name__}: {exc}")
        return
    from jobflow.core.flow import Flow
    from jobflow.core.job import Job

    if not isinstance(flow, (Flow, Job)):
        a.fail("jobflow.make_failed", f"Maker.make returned {type(flow).__name__}, not a jobflow Flow or Job")
        return
    for index, override in operations:
        if override["kind"] == "maker":
            continue
        try:
            flow = _apply_override(flow, override)
        except Exception as exc:
            a.fail(
                "jobflow.override_failed",
                f"jobflow_overrides[{index}] ({override.get('kind')}) failed: {type(exc).__name__}: {exc}",
            )
            return
    try:
        from httk.workflow.compat.jobflow._driver import DriverState

        root = a.workdir / "jobflow"
        shutil.rmtree(root, ignore_errors=True)
        (root / SPOOL_DIRECTORY).mkdir(parents=True, exist_ok=True)
        store = _open_store()
        _persist_store(store, root / STORE_FILE)
        state = DriverState.from_flow(flow)
        _write_json(root / STATE_FILE, state.to_mapping())
        a.state.merge({"jobflow_children": {}, "jobflow_processed": [], "jobflow_ordinal": 0})
    except Exception as exc:
        a.fail("jobflow.start_failed", f"cannot initialize jobflow state: {exc}")
        return
    a.advance("advance")


@run.step
def advance(a: Attempt) -> None:
    """Process completed children, dispatch ready jobs, or finish the flow."""
    from httk.workflow.compat.jobflow._driver import (
        DriverState,
        merge_documents,
        resolve_final_output,
        snapshot_documents,
        spool_job,
    )

    root = a.workdir / "jobflow"
    try:
        state = DriverState.from_mapping(json.loads((root / STATE_FILE).read_text(encoding="utf-8")))
        store = _open_store(root / STORE_FILE)
    except Exception as exc:
        a.fail("jobflow.state_invalid", f"cannot load scheduler state: {exc}")
        return
    children_raw = a.state.get("jobflow_children", {})
    processed_raw = a.state.get("jobflow_processed", [])
    children: dict[str, str] = (
        {str(label): str(key) for label, key in children_raw.items()} if isinstance(children_raw, Mapping) else {}
    )
    processed = {str(label) for label in processed_raw} if isinstance(processed_raw, list) else set()
    ordinal_raw = a.state.get("jobflow_ordinal", 0)
    ordinal = int(ordinal_raw) if isinstance(ordinal_raw, (int, str)) else 0
    observed_labels = {str(child.label) for child in a.children if child.label is not None}
    _recover_unmaterialized(state, children, observed_labels, processed)
    for child in a.children:
        label = child.label
        if label is None or label in processed or child.kind not in {"succeeded", "failed", "cancelled"}:
            continue
        child_key = children.get(label)
        if not isinstance(child_key, str) or state.is_settled(child_key):
            processed.add(label)
            continue
        if not child.succeeded:
            state.apply_error(child_key, _failure_message(child))
            processed.add(label)
            continue
        try:
            workdir = child.workdir
            if workdir is None:
                raise ValueError("child has no workdir")
            output_path = workdir / CHILD_OUTPUTS_FILE
            payload = json.loads(output_path.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping) or not isinstance(payload.get("docs", []), list):
                raise ValueError("child outputs document is malformed")
            merge_documents(store, [item for item in payload["docs"] if isinstance(item, Mapping)])
            _persist_store(store, root / STORE_FILE)
            response = _decode_response(payload.get("response"))
            state.apply_success(child_key, response)
        except Exception as exc:
            state.apply_error(child_key, f"child {label} succeeded but its outputs were unreadable: {exc}")
        processed.add(label)
    prior_live_labels: list[str] = [
        str(child.label)
        for child in a.children
        if child.label is not None and child.kind not in {"succeeded", "failed", "cancelled"}
    ]
    live_labels = list(prior_live_labels)
    spool_root = root / SPOOL_DIRECTORY
    forwarded = {
        name: value
        for name, value in a.parameters.items()
        if name in _FORWARDED_PARAMETERS or name.startswith(tuple(_LIBRARY_PREFIXES))
    }
    try:
        ready = [(job, _child_resources(a, job)) for job in state.ready()]
    except ValueError as exc:
        a.fail("jobflow.resources_invalid", str(exc))
        return
    for job, resources in ready:
        key = f"{job.uuid}:{job.index}"
        state.mark_running(key)
        spool_path = spool_root / f"{key}.json"
        snapshot_path = spool_root / f"{key}-store.json"
        _write_json(spool_path, spool_job(job))
        # Only what the job's references resolve through travels, unless the job reads the store itself.
        _write_json(
            snapshot_path,
            snapshot_documents(store, None if getattr(job.config, "expose_store", False) else job.input_uuids),
        )
        label = f"j{ordinal:05d}"
        a.spawn(
            ChildSpec(
                step="enter",
                parameters={
                    **forwarded,
                    # Relative to this workdir: the parent's directory moves while it waits, and the
                    # child reads them through its parent accessor.
                    "jobflow_child_job": spool_path.relative_to(a.workdir).as_posix(),
                    "jobflow_child_snapshot": snapshot_path.relative_to(a.workdir).as_posix(),
                },
                resources=resources,
            ),
            label=label,
        )
        children[label] = key
        ordinal += 1
        live_labels.append(label)
    _write_json(root / STATE_FILE, state.to_mapping())
    bookkeeping = {"jobflow_children": children, "jobflow_processed": sorted(processed), "jobflow_ordinal": ordinal}
    # Crash order: durable driver state, then atomic a.state, then exactly one
    # published outcome. Reprocessing is guarded by processed labels and settled
    # keys; spool writes are deterministic overwrites and spawns publish together.
    a.state.merge(bookkeeping)
    if live_labels:
        a.gather("advance", when="any_terminal", rejoin=tuple(prior_live_labels))
        return
    if state.is_complete:
        if state.succeeded:
            try:
                output = resolve_final_output(state, store)
                outputs: dict[str, object] = {"output": output}
                if state.stored_data:
                    from monty.json import jsanitize

                    outputs["stored_data"] = jsanitize(state.stored_data, strict=True, enum_values=True)
                output_path = a.workdir / OUTPUTS_FILE
                _write_json(output_path, outputs)
                a.put(output_path, f"{a.parameter('jobflow_data_prefix', 'jobflow')}/{OUTPUTS_FILE}")
                a.log.append("headline", "jobflow completed successfully")
                a.succeed()
            except Exception as exc:
                a.fail("jobflow.output_failed", f"cannot resolve final jobflow output: {exc}")
        else:
            summary = state.failure_summary()
            a.fail("jobflow.flow_failed", json.dumps(summary, sort_keys=True), details=summary)
        return
    a.fail("jobflow.flow_stalled", "jobflow scheduler has no live children and no ready jobs")


@run.step
def enter(a: Attempt) -> None:
    """Execute exactly one spooled jobflow Job in a private store."""
    try:
        _require_dependencies(None)
    except ValueError as exc:
        a.fail("jobflow.missing_dependency", str(exc))
        return
    try:
        # The parent is renamed whenever it is re-claimed: everything is read through one
        # descriptor of its job directory, which stays put, and a rename before it is open
        # (or a lookup that missed the parent in transit) is answered by locating it afresh.
        parent = a.parent
        anchor: int | None = None
        for _ in range(_PARENT_ATTEMPTS):
            if parent is None:
                parent = a._locate_parent(attempts=_PARENT_ATTEMPTS)
            if parent is None:
                a.fail("jobflow.job_unloadable", "the parent job holding the spooled job is not in this workspace")
                return
            try:
                anchor = _fs.open_dir(parent.payload)
                break
            except FileNotFoundError:
                parent = None
            except (OSError, _fs.UnsafePath) as exc:
                a.fail("jobflow.job_unloadable", f"cannot open the parent job directory: {exc}")
                return
        if anchor is None:
            a.fail(
                "jobflow.job_unloadable",
                f"the parent job directory moved away each of {_PARENT_ATTEMPTS} times it was located",
            )
            return
        try:
            _enter(a, parent, anchor)
        finally:
            os.close(anchor)
    except Exception as exc:
        a.fail(
            "jobflow.job_failed",
            f"{type(exc).__name__}: {exc}",
            details={"traceback": "\n".join(traceback.format_exc().splitlines()[-20:])},
        )


def _read_json_under(anchor: int, relative: PurePosixPath) -> object:
    """Read one JSON file below a directory descriptor, never following a symlink below it.

    :param anchor: The directory descriptor.
    :param relative: The file below it, at least one directory deep.
    :return: The decoded JSON value.
    :raises FileNotFoundError: If the file or a directory on its way is missing.
    :raises ValueError: If *relative* leaves the directory or the content is not JSON.
    :raises httk.workflow._fs.UnsafePath: If a component is a symlink or not the expected kind.
    """
    directory = _fs.open_dir_under(anchor, relative.parent)
    try:
        data = _fs.read_bounded(_fs.anchored(directory, relative.name), _SPOOL_LIMIT)
    finally:
        os.close(directory)
    if data is None:
        raise FileNotFoundError(f"{relative} is not in the parent job")
    return json.loads(data)


def _enter(a: Attempt, parent: Any, anchor: int) -> None:
    """Run ``enter`` once the parent is located and its job directory is anchored at *anchor*."""
    # The spool paths are relative to the parent's workdir, read here below its anchored directory.
    workdir = PurePosixPath(parent.workdir.relative_to(parent.payload).as_posix())
    try:
        spooled = _read_json_under(anchor, workdir / str(a.parameter("jobflow_child_job")))
        if not isinstance(spooled, dict):
            raise ValueError("the spooled job is not an object")
    except (OSError, ValueError, _fs.UnsafePath) as exc:
        a.fail("jobflow.job_unloadable", str(exc))
        return
    try:
        snapshot = _read_json_under(anchor, workdir / str(a.parameter("jobflow_child_snapshot")))
        if not isinstance(snapshot, list):
            raise ValueError("jobflow store snapshot is not an array")
    except (OSError, ValueError, _fs.UnsafePath) as exc:
        a.fail("jobflow.snapshot_invalid", f"cannot load the jobflow store snapshot: {exc}")
        return
    # Before any job module (atomate2, pymatgen) is imported, which read these at import.
    if not _apply_atomate2_environment(a, spooled):
        return
    from httk.workflow.compat.jobflow._driver import load_spooled_job, merge_documents

    # Sibling jobs share the parent's placement; their exec tokens become current paths
    # for this run only, in memory (the parent's files are never rewritten).
    found = _locate_exec_directories(a, parent.placement, _exec_token_ids([spooled, snapshot]))
    # Directory inputs this job's own arguments name are copied into its workdir; a
    # placeholder elsewhere in the store stays a placeholder.
    inputs_root = a.workdir / "inputs"
    try:
        copied = _input_members(spooled)
        _stage_directory_inputs(anchor, copied, inputs_root)
    except ValueError as exc:
        a.fail("jobflow.reference_unresolved", str(exc))
        return
    unresolved = sorted(_exec_token_ids(spooled) - found.keys())
    if unresolved:
        a.fail(
            "jobflow.reference_unresolved",
            f"this job's inputs name the directory of jobflow job {', '.join(unresolved)}, "
            "which is not in this workspace",
        )
        return
    try:
        job = load_spooled_job(
            _map_strings(spooled, lambda value: _resolve_exec(value, found, str(inputs_root), copied))
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        a.fail("jobflow.job_unloadable", str(exc))
        return
    # ponytail: a whole referenced output document counts, not just the referenced field.
    inputs = set(job.input_uuids)
    referenced = [item for item in snapshot if isinstance(item, Mapping) and item.get("uuid") in inputs]
    unresolved = sorted(_exec_token_ids(referenced) - found.keys())
    if unresolved:
        a.fail(
            "jobflow.reference_unresolved",
            f"this job's input references name the directory of jobflow job {', '.join(unresolved)}, "
            "which is not in this workspace",
        )
        return
    try:
        # The documents the job's references resolve to are its arguments too.
        named = _input_members(referenced) - copied
        _stage_directory_inputs(anchor, named, inputs_root)
        copied |= named
    except ValueError as exc:
        a.fail("jobflow.reference_unresolved", str(exc))
        return
    missing = sorted(_exec_token_ids(snapshot) - found.keys())
    if missing:
        a.log.append("warning", f"the jobflow store names directories of absent jobs {', '.join(missing)}")
    store = _open_store()
    try:
        snapshot = _map_strings(snapshot, lambda value: _resolve_exec(value, found, str(inputs_root), copied))
        merge_documents(store, [item for item in snapshot if isinstance(item, Mapping)])
    except ValueError as exc:
        a.fail("jobflow.snapshot_invalid", f"cannot load the jobflow store snapshot: {exc}")
        return
    baseline = {(str(item.get("uuid")), int(item.get("index", 0))) for item in store.query()}
    baseline_blobs: set[tuple[str, str]] = set()
    for name, blob_store in store.additional_stores.items():
        blob_store.connect()
        baseline_blobs.update((name, str(item.get("blob_uuid"))) for item in blob_store.query())
    exec_dir = a.workdir / "exec"
    shutil.rmtree(exec_dir, ignore_errors=True)
    exec_dir.mkdir(parents=True, exist_ok=True)
    with chdir(exec_dir), _forwarded_stop_signals():
        response = job.run(store=store)
    response.output = None
    from jobflow.core.flow import get_flow

    for name in ("replace", "detour", "addition"):
        value = getattr(response, name)
        if value is not None:
            setattr(response, name, get_flow(value, allow_external_references=True))
    from monty.json import jsanitize

    shipped = {
        "response": jsanitize(vars(response), strict=True, enum_values=True),
        "docs": jsanitize(_new_documents(store, baseline, baseline_blobs), strict=True, enum_values=True),
    }
    # This payload moves once it finishes, so references into exec/ (atomate2's
    # dir_name) ship as tokens that a consuming sibling resolves to the current path.
    # Sibling directories resolved above ship back as their tokens too.
    directories = {
        path: f"{EXEC_TOKEN}{job_id}"
        for job_id, directory in [*found.items(), (a.context.job_id, str(exec_dir))]
        for path in (directory, os.path.realpath(directory))
    }
    # The copied directory inputs ship back as input tokens ("<root>/port/x" -> "<token>port/x").
    for path in (str(inputs_root), os.path.realpath(inputs_root)):
        directories[path] = INPUT_TOKEN.removesuffix("/")
    shipped = _map_strings(shipped, lambda value: _tokenize_exec(value, directories))
    _write_json(a.workdir / CHILD_OUTPUTS_FILE, shipped)
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
