"""Shared helpers for tests that run real jobs on the filesystem kernel.

A test installs a workflow package whose runner (:data:`RUNNER`) does what the
job's ``parameters`` say, submits v3 jobs from a CLI owner's scratch and drives
them with a real :class:`~httk.workflow.TaskManager`.
"""

import json
import uuid
from collections.abc import Mapping
from pathlib import Path

from httk.workflow import TaskManager, Workspace, _kernel, _store
from httk.workflow._job import JobDefinition
from httk.workflow._state import StateDoc, read_state_unowned

#: The scriptable runner. ``parameters.script[<step>]`` is the step's action:
#:
#: - ``succeed``; ``fail`` or ``fail:<code>``; ``advance:<step>``; ``retry_once``;
#: - ``exit`` (no outcome: a process failure, exit status 3); ``pause``; ``sleep``; ``crash_once`` (sleeps on the
#:   first attempt only);
#: - ``spawn``: publish ``parameters.spawn.children`` (``{label, script, placement?, parameters?, workflow?, declarations?}``) and wait
#:   on them with ``parameters.spawn.condition`` (default ``all_succeeded``), then ``next_step`` (``gather``);
#:   ``on_impossible`` names a rescue step.
#:
#: Before the action, the step writes ``parameters.files[<step>]`` (``{relative path: text}``) into its
#: workdir, ``parameters.declare[<step>]`` (``{name: document}``) as observed declarations,
#: ``parameters.put[<step>]`` (``{payload path: text}``) through one committed transaction,
#: ``parameters.headline[<step>]`` as a runner run-log headline and ``parameters.breadcrumb[<step>]`` as
#: ``error.json``. ``parameters.runner_steps`` is published with every outcome, and
#: ``parameters.resources[<step>]`` with that step's. Every step prints ``runner is working on <step>`` to
#: stdout and ``diagnostic from <step>`` to stderr.
RUNNER = """#!/usr/bin/env python3
import json, os, pathlib, sys, time, uuid

context = json.loads(os.environ["HTTK_WORKFLOW_CONTEXT"])
control = pathlib.Path(os.environ["HTTK_WORKFLOW_CONTROL_DIR"])
job_dir = pathlib.Path(os.environ["HTTK_WORKFLOW_JOB_DIR"])
job = json.loads((job_dir / "job.json").read_text())
parameters = job["parameters"]
step = context["step"]
behavior = parameters["script"][step]
print("runner is working on " + step, flush=True)
sys.stderr.write("diagnostic from " + step + "\\n")
for name, text in parameters.get("files", {}).get(step, {}).items():
    pathlib.Path(name).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(name).write_text(text)
for name, document in parameters.get("declare", {}).get(step, {}).items():
    (job_dir / ".httk-job" / "declarations").mkdir(parents=True, exist_ok=True)
    (job_dir / ".httk-job" / "declarations" / f"{name}.json").write_text(json.dumps(document))
puts = parameters.get("put", {}).get(step, {})
if puts:
    staging = control / "txn" / "000001.tmp"
    for name, text in puts.items():
        (staging / name).parent.mkdir(parents=True, exist_ok=True)
        (staging / name).write_text(text)
    staging.rename(control / "txn" / "000001")
if step in parameters.get("headline", {}):
    record = {"format": "httk-workflow-runlog-event", "format_version": 2, "timestamp": "2026-10-09T00:00:00+00:00",
              "kind": "headline", "message": parameters["headline"][step], "files": []}
    (job_dir / ".httk-job").mkdir(exist_ok=True)
    with open(job_dir / ".httk-job" / "runlog.jsonl", "a") as stream:
        stream.write(json.dumps(record) + "\\n")
if step in parameters.get("breadcrumb", {}):
    (control / "error.json").write_text(json.dumps(parameters["breadcrumb"][step]))
outcome = {key: context[key] for key in ("job_id", "activation_id", "attempt_id")}
outcome.update(format="httk-workflow-outcome", format_version=2)
if "runner_steps" in parameters:
    outcome["runner_steps"] = parameters["runner_steps"]
if step in parameters.get("resources", {}):
    outcome["resources"] = parameters["resources"][step]
draft = control / "outcome.tmp.x"
draft.mkdir()
if behavior == "spawn":
    spec = parameters["spawn"]
    entries, references = [], []
    for index, child_spec in enumerate(spec["children"]):
        child_id, spawn_id, label = str(uuid.uuid4()), str(uuid.uuid4()), child_spec["label"]
        key = f"{label}--{child_id}"
        placement = child_spec.get("placement", f"{job['placement']}/c{index}")
        child = dict(job, id=child_id, tag=label, name=label, placement=placement, initial_step="start")
        child["workflow"] = child_spec.get("workflow", job["workflow"])
        child["declarations"] = child_spec.get("declarations", {})
        child["parameters"] = dict(child_spec.get("parameters", {}), script=child_spec["script"])
        child["parent"] = {
            "workspace_id": context["workspace_id"], "job_id": job["id"], "job_key": context["job_key"],
            "placement": job["placement"], "activation_id": context["activation_id"], "spawn_id": spawn_id,
        }
        (draft / "children" / "jobs" / key).mkdir(parents=True)
        (draft / "children" / "jobs" / key / "job.json").write_text(json.dumps(child))
        entries.append({"job_key": key, "label": label, "placement": placement, "spawn_id": spawn_id})
        references.append({"workspace_id": context["workspace_id"], "job_id": child_id, "job_key": key,
                           "placement_hint": placement, "label": label})
    spawn = {"format": "httk-workflow-spawn", "format_version": 2, "children": entries}
    (draft / "children" / "spawn.json").write_text(json.dumps(spawn))
    join = {"children": references, "condition": spec.get("condition", "all_succeeded")}
    if spec.get("on_impossible"):
        join["on_impossible"] = {"action": "advance", "next_step": spec["on_impossible"]}
    outcome.update(action="wait", next_step=spec.get("next_step", "gather"), join=join)
elif behavior.startswith("advance:"):
    outcome.update(action="advance", next_step=behavior.split(":", 1)[1])
elif behavior == "retry_once" and context["attempt_ordinal"] == 1:
    outcome.update(action="retry", retry={"reason": "try_again"})
elif behavior.split(":")[0] == "fail":
    code = behavior.split(":", 1)[1] if ":" in behavior else "converge"
    outcome.update(action="fail", failure={"code": code, "message": "did not converge", "details": {"cycles": 3}})
elif behavior == "pause":
    outcome.update(action="pause", pause={"reason": "operator inspection"})
elif behavior == "exit":
    raise SystemExit(3)
elif behavior == "sleep" or (behavior == "crash_once" and not context["is_restart"]):
    pathlib.Path("sleeping").write_text(str(os.getpid()))
    time.sleep(120)
else:
    outcome.update(action="succeed")
(draft / "outcome.json").write_text(json.dumps(outcome))
draft.rename(control / "outcome.ready")
"""

STEPS = ("start", "finish", "gather", "rescue")


def workspace(root: Path) -> Workspace:
    """Initialize a fast, non-durable workspace at *root*."""

    return Workspace.initialize(root, durable=False, policy={"visibility_deadline_seconds": 0.05})


def cli_owner(ws: Workspace) -> _kernel.Owner:
    """Register a CLI owner (use it as a context manager)."""

    return _kernel.register_owner(ws, kind="cli", label="test", allocation=None, advertised={})


def install(
    ws: Workspace,
    package: Path,
    *,
    name: str = "demo",
    workflow: str = "",
    manifest: str = "",
    files: Mapping[str, str] | None = None,
    executables: Mapping[str, str] | None = None,
    build: bool = True,
) -> _store.Installed:
    """Write a workflow package running :data:`RUNNER` at *package* and install it into *ws*.

    :param workflow: Further ``[workflow]`` members, as TOML lines.
    :param manifest: Further TOML appended to the manifest (tables after ``[workflow.runner]``).
    :param files: Further package members.
    :param executables: Further package members made executable.
    :param build: Build the package when it declares ``[workflow.build]``.
    """

    package.mkdir(parents=True)
    head = (
        f'[workflow]\nname = "{name}"\n{workflow}\n'
        f'[workflow.runner]\nsteps = {json.dumps(list(STEPS))}\ninitial_step = "start"\n'
    )
    (package / "httk_workflow.toml").write_text(head + manifest, encoding="utf-8")
    for member, text in {"run": RUNNER, **(executables or {})}.items():
        (package / member).write_text(text, encoding="utf-8")
        (package / member).chmod(0o755)
    for member, text in (files or {}).items():
        (package / member).parent.mkdir(parents=True, exist_ok=True)
        (package / member).write_text(text, encoding="utf-8")
    with cli_owner(ws) as owner:
        return _store.install(ws, owner, package, build=build)


def job_mapping(
    workflow: _store.Installed | tuple[str, str],
    script: Mapping[str, str],
    *,
    tag: str | None = "demo",
    placement: str = "project/0",
    parameters: Mapping[str, object] | None = None,
    declarations: Mapping[str, object] | None = None,
    declared: Mapping[str, object] | None = None,
    environment: Mapping[str, object] | None = None,
    retry_policy: Mapping[str, object] | None = None,
    priority: int = 500,
    pool: str = "default",
    capabilities: tuple[str, ...] = (),
    resources: Mapping[str, object] | None = None,
    step_resources: Mapping[str, object] | None = None,
    initial_step: str = "start",
    name: str = "demo job",
) -> dict[str, object]:
    """Return a v3 ``job.json`` mapping for :data:`RUNNER`."""

    workflow_id, workflow_name = (workflow.id, workflow.name) if isinstance(workflow, _store.Installed) else workflow
    return {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": str(uuid.uuid4()),
        "tag": tag,
        "name": name,
        "placement": placement,
        "workflow": {"id": workflow_id, "name": workflow_name},
        "initial_step": initial_step,
        "priority": priority,
        "claim": {"pool": pool, "required_capabilities": list(capabilities)},
        "retry_policy": dict(retry_policy or {"maximum_attempts_per_activation": 1}),
        "resources": dict(resources or {}),
        "step_resources": dict(step_resources or {}),
        "parameters": {**(parameters or {}), "script": dict(script)},
        "declarations": dict(declarations or {}),
        "declared": dict(declared or {}),
        "environment": dict(environment or {}),
        "parent": None,
        "seal_succeeded": None,
    }


def submit_mapping(
    ws: Workspace, mapping: Mapping[str, object], *, members: Mapping[str, str] | None = None
) -> _kernel.JobRef:
    """Submit one ``job.json`` mapping (plus payload *members*) from a CLI owner's scratch."""

    job = JobDefinition.from_mapping(mapping)
    with cli_owner(ws) as owner:
        staging = owner.scratch("submit") / "job"
        staging.mkdir()
        (staging / "job.json").write_bytes(job.encode())
        for member, text in (members or {}).items():
            (staging / member).parent.mkdir(parents=True, exist_ok=True)
            (staging / member).write_text(text, encoding="utf-8")
        return _kernel.submit(ws, owner, staging)


def submit(
    ws: Workspace,
    workflow: _store.Installed | tuple[str, str],
    script: Mapping[str, str],
    *,
    members: Mapping[str, str] | None = None,
    **options: object,
) -> _kernel.JobRef:
    """Submit one job of :data:`RUNNER`; *options* are :func:`job_mapping` keywords."""

    return submit_mapping(ws, job_mapping(workflow, script, **options), members=members)  # type: ignore[arg-type]


def run(ws: Workspace, **manager: object) -> None:
    """Run a manager until the workspace is idle."""

    with TaskManager(ws, heartbeat_interval=0.01, **manager) as task_manager:  # type: ignore[arg-type]
        task_manager.run_until_idle(timeout=120)


def only(ws: Workspace, state: str) -> _kernel.JobRef:
    """Return the one job in *state*."""

    refs = list(_kernel.list_jobs(ws, state))
    assert len(refs) == 1, f"{state}: {refs}"
    return refs[0]


def find(ws: Workspace, job_id: str) -> _kernel.JobRef:
    """Locate a job anywhere in the workspace."""

    ref = _kernel.locate(ws, job_id, placement_hint=None, exhaustive=True)
    assert ref is not None, job_id
    return ref


def state_of(ref: _kernel.JobRef) -> StateDoc:
    """Return a job's readable ``state.json``."""

    doc, damaged = read_state_unowned(ref.path / "state.json")
    assert doc is not None and not damaged
    return doc


def events(ref: _kernel.JobRef) -> list[str]:
    """Return the event names of a job's owner run log."""

    lines = (ref.path / "logs" / "runlog.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line)["event"] for line in lines]
