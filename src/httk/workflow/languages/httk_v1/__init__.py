"""Realize legacy httk v1 task packages through the ordinary runner path."""

from __future__ import annotations

import os
import shlex
import stat
import sys
from collections.abc import Mapping
from dataclasses import replace
from io import StringIO
from pathlib import Path, PurePosixPath
from string import Template
from typing import TYPE_CHECKING, cast

from httk.workflow.languages import LanguagePorts, LanguageRequest, LanguageScaffold, WorkflowLanguage, runner_reference
from httk.workflow.protocol import validate_label

if TYPE_CHECKING:
    from httk.workflow.collecting import JobRecord
    from httk.workflow.runtime_builders import JobSpec
    from httk.workflow.scaffold import InstantiateContext

PACKAGE = __name__
RUNNER = "v1_runner.py"
PROGRAMS = ("ht_steps", "ht_run")
V1_PRIORITY_MAP = {1: 100, 2: 300, 3: 500, 4: 700, 5: 900}
V2_TO_V1_PRIORITY = {value: key for key, value in V1_PRIORITY_MAP.items()}


def _program(root: Path) -> str | None:
    for name in PROGRAMS:
        if (root / name).is_file() or (root / f"{name}.template").is_file():
            return name
    return None


def matches(path: Path) -> bool:
    """Return false: bare v1 directories require an explicit format."""

    del path
    return False


def ports(path: Path) -> LanguagePorts:
    """Return the port shape of a v1 task, which has no document ports."""

    del path
    return LanguagePorts((), ())


def validate_runner(options: Mapping[str, object], root: Path) -> None:
    """Validate v1 taskset and retry options."""

    del root
    for key in options:
        if key not in {"taskset", "attempts"}:
            raise ValueError(f"unknown runner option {key!r} for httk-v1")
    taskset = options.get("taskset", "default")
    if not isinstance(taskset, str):
        raise ValueError("runner option 'taskset' must be a label")
    validate_label(taskset, "taskset")
    attempts = options.get("attempts", 10)
    if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
        raise ValueError("runner option 'attempts' must be an integer of zero or greater")


def _snapshot_package(root: Path, excluded: tuple[str, ...]) -> tuple[dict[str, int], dict[str, tuple[bytes, int]]]:
    directories: dict[str, int] = {}
    snapshot: dict[str, tuple[bytes, int]] = {}
    excluded_set = set(excluded)

    def visit(directory: Path, relative_root: PurePosixPath | None = None) -> None:
        if relative_root is None:
            relative_root = PurePosixPath()
        with os.scandir(directory) as entries:
            for entry in sorted(entries, key=lambda item: item.name):
                relative = relative_root / entry.name
                member = relative.as_posix()
                if entry.is_symlink():
                    raise ValueError(f"httk-v1 package member must not be a symlink: {member}")
                if entry.is_dir(follow_symlinks=False):
                    directories[member] = stat.S_IMODE(entry.stat(follow_symlinks=False).st_mode)
                    visit(Path(entry.path), relative)
                elif entry.is_file(follow_symlinks=False):
                    if member not in excluded_set:
                        snapshot[member] = (Path(entry.path).read_bytes(), stat.S_IMODE(entry.stat().st_mode))
                else:
                    raise ValueError(f"httk-v1 package member must be a regular file: {member}")

    visit(root)
    return directories, snapshot


def _write_snapshot(payload: Path, snapshot: tuple[Mapping[str, int], Mapping[str, tuple[bytes, int]]]) -> None:
    directories, files = snapshot
    for member in sorted(directories, key=lambda value: (len(PurePosixPath(value).parts), value)):
        payload.joinpath(*PurePosixPath(member).parts).mkdir(parents=True, exist_ok=True)
    for member, (content, mode) in files.items():
        destination = payload.joinpath(*PurePosixPath(member).parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
        destination.chmod(mode)
    for member in sorted(directories, key=lambda value: (-len(PurePosixPath(value).parts), value)):
        payload.joinpath(*PurePosixPath(member).parts).chmod(directories[member])


def _render_template(source: Path, destination: Path, values: Mapping[str, object]) -> None:
    """Render one trusted v1 template exactly as the legacy renderer did."""

    locals_: dict[str, object] = {}
    globals_ = dict(values)
    rendered = Template(Template(source.read_text(encoding="utf-8")).safe_substitute(locals_)).safe_substitute(globals_)
    shebang, separator, body = rendered.partition("\n")
    if shebang.startswith("#!") and separator:
        rendered, output = body, shebang + separator
    else:
        output = ""
    lexer = shlex.shlex(rendered)
    lexer.whitespace = ""
    eval_nesting = exec_nesting = 0
    command = ""
    for token in lexer:
        if eval_nesting == 0 and exec_nesting == 0:
            if token == "\\":
                token += lexer.get_token() or ""
            if token == "$":
                token += lexer.get_token() or ""
            if token == "$(":
                eval_nesting, command = 1, ""
                continue
            if token == "${":
                exec_nesting, command = 1, ""
                continue
            output += "$" if token == "\\$" else token
        elif exec_nesting:
            if token == "{":
                exec_nesting += 1
            if token == "}":
                exec_nesting -= 1
            if exec_nesting == 0:
                old_stdout = sys.stdout
                sys.stdout = StringIO()
                try:
                    exec(command, globals_, locals_)  # noqa: S102 - v1 templates intentionally execute trusted code
                    output += sys.stdout.getvalue()
                finally:
                    sys.stdout = old_stdout
                output = output.removesuffix("\n")
                continue
            command += token
        else:
            if token == "(":
                eval_nesting += 1
            if token == ")":
                eval_nesting -= 1
            if eval_nesting == 0:
                output += str(eval(command, globals_, locals_))
                continue
            command += token
    destination.write_text(output, encoding="utf-8")
    destination.chmod(stat.S_IMODE(source.stat().st_mode))


def _apply_templates(payload: Path, values: Mapping[str, object]) -> None:
    for source in sorted(payload.rglob("*.template")):
        if source.is_file():
            target = source.with_name(source.name.removesuffix(".template"))
            _render_template(source, target, values)
            source.unlink()


def _execute_instantiator(payload: Path, globals_: Mapping[str, object]) -> None:
    script = payload / "ht.instantiate.py"
    if not script.is_file():
        raise ValueError("instantiate_globals were supplied but ht.instantiate.py is missing")
    namespace = dict(globals_)
    namespace.setdefault("__file__", str(script))
    namespace.setdefault("__name__", "__httk_v1_instantiate__")
    old_cwd, old_argv = Path.cwd(), sys.argv
    try:
        os.chdir(payload)
        sys.argv = [str(script)]
        exec(compile(script.read_bytes(), str(script), "exec"), namespace, namespace)  # noqa: S102
    finally:
        sys.argv = old_argv
        os.chdir(old_cwd)
    script.unlink()


def prepare(request: LanguageRequest) -> LanguageScaffold:
    """Prepare one v1 package realization."""

    if request.directory is None:
        raise ValueError("httk-v1 requires a task directory")
    root = request.directory.resolve()
    program = _program(root)
    if program is None:
        raise ValueError(f"{root}: no ht_steps or ht_run program found")
    snapshot = _snapshot_package(root, request.excluded_members)
    taskset = str(request.runner_options.get("taskset", "default"))
    attempts = cast(int, request.runner_options.get("attempts", 10))

    def instantiate(ctx: InstantiateContext) -> None:
        _write_snapshot(ctx.payload, snapshot)
        supplied: dict[str, object] = {}
        for name, value in ctx.inputs.items():
            metadata = request.inputs.get(name, {})
            if "entry_type" in metadata and isinstance(value, (str, os.PathLike)) and Path(value).is_file():
                import httk.core

                supplied[name] = httk.core.load(os.fspath(value))
            else:
                supplied[name] = value
        values = {key: value for key, value in {**ctx.parameters, **supplied}.items() if key != "workflow_language"}
        _apply_templates(ctx.payload, values)
        if (ctx.payload / "ht.instantiate.py").is_file():
            _execute_instantiator(ctx.payload, values)
        runner = ctx.payload / program
        if not runner.is_file() or not os.access(runner, os.X_OK):
            raise ValueError(f"legacy runner is missing or not executable: {runner}")

    def finalize(spec: JobSpec) -> JobSpec:
        return replace(
            spec,
            compatibility={
                "profile": "httk-v1-task-v1",
                "program": program,
                "legacy_priority": V2_TO_V1_PRIORITY.get(spec.priority, 3),
                "attempts": attempts,
            },
            claim_pool=taskset,
            retry_on=("lease_lost", "process_failure"),
            maximum_attempts_per_activation=attempts + 1,
            workdir_mode="persistent",
            workdir_path="ht.run.current",
            data_mode="none",
        )

    return LanguageScaffold(
        documents={},
        files={},
        parameters={"workflow_language": "httk-v1"},
        runner=runner_reference(PACKAGE, RUNNER),
        workdir_path="ht.run.current",
        required_capabilities=(),
        instantiate=instantiate,
        finalize=finalize,
    )


def collect(record: JobRecord) -> Mapping[str, object]:
    """Reject use as a default collector for v1 packages."""

    del record
    raise ValueError("an httk-v1 workflow package declares [workflow.collect]")


LANGUAGE = WorkflowLanguage(
    name="httk-v1",
    steps=("start",),
    initial_step="start",
    document_policy="forbidden",
    has_default_collector=False,
    allows_modes=False,
    matches=matches,
    ports=ports,
    validate_runner=validate_runner,
    prepare=prepare,
    collect=collect,
    environment={
        "httk_v1.timeout": {"type": "integer", "default": 21600},
        "httk_v1.wrapper": {"type": "string", "default": ""},
        "httk_v1.log_compression": {"type": "string", "default": "bzip2"},
        "httk_v1.root": {"type": "string", "default": ""},
    },
)
