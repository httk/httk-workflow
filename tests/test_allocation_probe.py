"""Allocation probes: Slurm, host and site envelopes, their CLI selection and the launchers that pick them."""

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Self, cast

import pytest
from httk.core.cli import CLIContext

from httk.workflow import TaskManager, Workspace, launch_runtime, launchers
from httk.workflow._allocation import (
    Allocation,
    Node,
    allocation_from_envelope,
    argv_allocation,
    exec_allocation,
    host_allocation,
    parse_allocation_spec,
    probe_allocation,
)
from httk.workflow._slurm import slurm_allocation, slurm_counts
from httk.workflow.errors import FormatError
from httk.workflow.workflow_cli import _manager
from v3_helpers import workspace as initialize_workspace

_SLURM_FIXTURES = [
    {"SLURM_NTASKS": "8"},
    {
        "SLURM_JOB_ID": "123",
        "SLURM_NTASKS": "8",
        "SLURM_CPUS_PER_TASK": "2",
        "SLURM_MEM_PER_CPU": "2000",
        "SLURM_GPUS": "2",
        "SLURM_JOB_NUM_NODES": "1",
    },
    {"SLURM_JOB_ID": "123", "SLURM_JOB_NUM_NODES": "2", "SLURM_MEM_PER_NODE": "4G"},
    {"SLURM_JOB_ID": "123", "SLURM_NTASKS": "2", "SLURM_MEM_PER_CPU": "4G"},
    {"SLURM_JOB_ID": "123", "SLURM_NTASKS": "garbage", "SLURM_MEM_PER_CPU": "2000"},
]

_TWO_NODES = {
    "SLURM_JOB_ID": "9",
    "SLURM_JOB_END_TIME": "5000",
    "SLURM_JOB_NODELIST": "n[01-02]",
    "SLURM_JOB_NUM_NODES": "2",
    "SLURM_NTASKS": "32",
    "SLURM_TASKS_PER_NODE": "16(x2)",
    "SLURM_MEM_PER_NODE": "64000",
    "SLURM_GPUS": "a100:4",
    "SLURM_GPUS_PER_NODE": "2",
    "SLURMD_NODENAME": "n01",
    "CUDA_VISIBLE_DEVICES": "GPU-a, GPU-b",
}


def _scontrol(stdout: str, returncode: int = 0):
    calls: list[list[str]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        assert kwargs["timeout"] == 30 and kwargs["check"] is False
        return subprocess.CompletedProcess(argv, returncode, stdout, "")

    return run, calls


def _no_scontrol(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
    raise FileNotFoundError("scontrol")


@pytest.mark.parametrize("environ", _SLURM_FIXTURES)
def test_slurm_counts_are_the_cli_capacity_and_the_allocation_capacity(environ: dict[str, str]) -> None:
    assert _manager._scheduler_resources(environ) == slurm_counts(environ)
    allocation = slurm_allocation(environ, run=_no_scontrol)
    assert (allocation.capacity() if allocation is not None else {}) == slurm_counts(environ)


def test_slurm_counts_accept_typed_gpus() -> None:
    assert slurm_counts({"SLURM_JOB_ID": "1", "SLURM_GPUS": "a100:4"}) == {"gpus": 4}
    assert slurm_counts({"SLURM_JOB_ID": "1", "SLURM_GPUS": "a100:2,v100:3"}) == {"gpus": 5}
    assert slurm_counts({"SLURM_JOB_ID": "1", "SLURM_GPUS": "a100:x"}) == {}


def test_slurm_allocation_lists_nodes_with_batch_host_gpu_ids() -> None:
    run, calls = _scontrol("n01\nn02\n")
    allocation = slurm_allocation(_TWO_NODES, run=run)
    assert calls == [["scontrol", "show", "hostnames", "n[01-02]"]]
    assert allocation == Allocation(
        "slurm",
        5000.0,
        (
            Node("n01", 16, 64000, 2, gpu_ids=("GPU-a", "GPU-b"), gpu_variable="CUDA_VISIBLE_DEVICES", local=True),
            Node("n02", 16, 64000, 2),
        ),
        {},
        identity={"job_id": "9"},
    )
    assert allocation is not None
    assert allocation.capacity() == slurm_counts(_TWO_NODES) == {"procs": 32, "gpus": 4, "nodes": 2, "mem": 128000}


def test_slurm_allocation_memory_per_cpu_follows_the_tasks() -> None:
    environ = {**_TWO_NODES, "SLURM_TASKS_PER_NODE": "20,12", "SLURM_MEM_PER_CPU": "1G"}
    del environ["SLURM_MEM_PER_NODE"]
    allocation = slurm_allocation(environ, run=_scontrol("n01 n02")[0])
    assert allocation is not None
    assert [(node.procs, node.mem) for node in allocation.nodes] == [(20, 20480), (12, 12288)]
    assert allocation.capacity() == slurm_counts(environ)


@pytest.mark.parametrize(
    ("change", "run"),
    [
        ({}, _no_scontrol),
        ({}, _scontrol("", 1)[0]),
        ({"SLURM_TASKS_PER_NODE": "16,8"}, _scontrol("n01 n02")[0]),
        ({"SLURM_TASKS_PER_NODE": "garbage"}, _scontrol("n01 n02")[0]),
        ({"SLURM_GPUS": "3"}, _scontrol("n01 n02")[0]),
        ({"SLURM_JOB_NUM_NODES": "3"}, _scontrol("n01 n02")[0]),
    ],
)
def test_slurm_allocation_falls_back_to_aggregate_counts(
    change: dict[str, str], run: object, caplog: pytest.LogCaptureFixture
) -> None:
    environ = {**_TWO_NODES, **change}
    allocation = slurm_allocation(environ, run=run)  # type: ignore[arg-type]
    assert allocation is not None and allocation.nodes == ()
    assert allocation.resources == slurm_counts(environ) == allocation.capacity()
    assert allocation.end_time == 5000.0
    assert "aggregate counts only" in caplog.text or "using the counts only" in caplog.text


def test_slurm_does_not_infer_equal_gpu_distribution_without_typed_counts() -> None:
    environ = {key: value for key, value in _TWO_NODES.items() if key != "SLURM_GPUS_PER_NODE"}
    allocation = slurm_allocation(environ, run=_scontrol("n01 n02")[0])
    assert allocation is not None
    assert allocation.nodes == ()
    assert allocation.resources == {"procs": 32, "gpus": 4, "nodes": 2, "mem": 128000}


def test_slurm_visible_gpu_ids_contradicting_typed_distribution_fall_back() -> None:
    environ = {**_TWO_NODES, "CUDA_VISIBLE_DEVICES": "GPU-a,GPU-b,GPU-c"}
    allocation = slurm_allocation(environ, run=_scontrol("n01 n02")[0])
    assert allocation is not None and allocation.nodes == ()
    assert allocation.resources["gpus"] == 4


def test_slurm_allocation_without_ntasks_lists_nodes_without_procs() -> None:
    # sbatch -N2 --exclusive exports no SLURM_NTASKS.
    environ = {key: value for key, value in _TWO_NODES.items() if key not in ("SLURM_NTASKS", "SLURM_TASKS_PER_NODE")}
    allocation = slurm_allocation(environ, run=_scontrol("n01 n02")[0])
    assert allocation is not None and [node.procs for node in allocation.nodes] == [0, 0]
    assert allocation.capacity() == slurm_counts(environ) == {"gpus": 4, "nodes": 2, "mem": 128000}


@pytest.mark.parametrize("batch", ["login1", None])
def test_batch_gpu_ids_belong_to_no_node_when_the_batch_host_is_not_listed(batch: str | None) -> None:
    environ = {key: value for key, value in _TWO_NODES.items() if key != "SLURMD_NODENAME"}
    if batch is not None:
        environ["SLURMD_NODENAME"] = batch
    allocation = slurm_allocation(environ, run=_scontrol("n01 n02")[0])
    assert allocation is not None and len(allocation.nodes) == 2
    assert all(node.gpu_ids is None and node.gpus == 2 for node in allocation.nodes)


def test_single_node_slurm_job_needs_no_scontrol() -> None:
    environ = {"SLURM_JOB_ID": "1", "SLURM_JOB_NUM_NODES": "1", "SLURM_NTASKS": "4", "SLURMD_NODENAME": "n07"}
    allocation = slurm_allocation(environ, run=_no_scontrol)
    assert allocation is not None and allocation.nodes == (Node("n07", 4, local=True),)
    assert slurm_allocation({}, run=_no_scontrol) is None


def test_host_allocation_reads_announced_gpus() -> None:
    plain = host_allocation({})
    (node,) = plain.nodes
    assert plain.kind == "host" and plain.end_time is None and node.procs >= 1
    assert node.gpus == 0 and node.gpu_ids is None
    (gpu,) = host_allocation({"ROCR_VISIBLE_DEVICES": "0,1,,2"}).nodes
    assert (gpu.gpus, gpu.gpu_ids, gpu.gpu_variable) == (3, ("0", "1", "2"), "ROCR_VISIBLE_DEVICES")
    assert host_allocation({"CUDA_VISIBLE_DEVICES": ""}).nodes[0].gpus == 0


_ENVELOPE = {
    "format": "httk-workflow-allocation",
    "format_version": 1,
    "kind": "pbs",
    "end_time": 1790000000,
    "nodes": [
        {
            "host": "n001",
            "procs": 4,
            "mem": 128000,
            "gpus": 2,
            "cpus": ["0-7", "8-15", "16,18", "24-31"],
            "gpu_ids": ["0", "1"],
            "gpu_variable": "CUDA_VISIBLE_DEVICES",
        },
        {"host": "n002", "procs": 2},
    ],
    "resources": {"license": 2},
}


def test_envelope_round_trips() -> None:
    allocation = allocation_from_envelope(_ENVELOPE)
    assert allocation.kind == "pbs" and allocation.end_time == 1790000000.0
    assert allocation.nodes[0].cpu_slots == ("0-7", "8-15", "16,18", "24-31")
    assert allocation.nodes[0].gpu_ids == ("0", "1") and allocation.nodes[1] == Node("n002", 2)
    # n002 does not know its memory, so the allocation has no mem capacity.
    assert allocation.capacity() == {"procs": 6, "gpus": 2, "nodes": 2, "license": 2}


def test_envelope_rejects_duplicate_gpu_ids_and_overlapping_cpu_slots() -> None:
    with pytest.raises(FormatError, match="duplicate device ids"):
        allocation_from_envelope(_node(gpu_ids=["0", "0"]))
    with pytest.raises(FormatError, match="cpus slots overlap"):
        allocation_from_envelope(_node(cpus=["0-2", "2-3", "4-5", "6-7"]))


def test_envelope_allows_repeated_cpu_ids_on_different_nodes() -> None:
    envelope = {
        **_ENVELOPE,
        "nodes": [
            {"host": "n001", "procs": 1, "cpus": ["0-3"]},
            {"host": "n002", "procs": 1, "cpus": ["0-3"]},
        ],
    }
    assert allocation_from_envelope(envelope).nodes[1].cpu_slots == ("0-3",)


def test_envelope_allows_at_most_one_local_node() -> None:
    envelope = {
        **_ENVELOPE,
        "nodes": [
            {**_ENVELOPE["nodes"][0], "local": True},  # type: ignore[index]
            {**_ENVELOPE["nodes"][1], "local": True},  # type: ignore[index]
        ],
    }
    with pytest.raises(FormatError, match="at most one node local"):
        allocation_from_envelope(envelope)


def _node(**change: object) -> dict[str, object]:
    nodes = cast(list[dict[str, object]], _ENVELOPE["nodes"])
    return {**_ENVELOPE, "nodes": [{**nodes[0], **change}]}


@pytest.mark.parametrize(
    "envelope",
    [
        [],
        {**_ENVELOPE, "extra": 1},
        {**_ENVELOPE, "format": "other"},
        {**_ENVELOPE, "format_version": 2},
        {**_ENVELOPE, "kind": "Bad Kind"},
        {**_ENVELOPE, "end_time": 0},
        {**_ENVELOPE, "end_time": "soon"},
        {**_ENVELOPE, "cpus_per_proc": True},
        {**_ENVELOPE, "cpus_per_proc": 0},
        {**_ENVELOPE, "nodes": []},
        {**_ENVELOPE, "nodes": [_ENVELOPE["nodes"][1], _ENVELOPE["nodes"][1]]},  # type: ignore[index]
        {**_ENVELOPE, "resources": {"procs": 1}},
        {**_ENVELOPE, "resources": {"maxtime": 1}},
        {**_ENVELOPE, "resources": {"license": -1}},
        _node(host=""),
        _node(procs=-1),
        _node(procs=True),
        _node(mem="lots"),
        _node(gpus=1.5),
        _node(cpus=["0-7"]),
        _node(cpus=["0-7", "8-15", "x", "24-31"]),
        _node(gpu_ids=["0"]),
        _node(gpu_ids=["0", ""]),
        _node(gpu_variable="1BAD"),
        _node(gpu_variable=None),
        _node(local="yes"),
        _node(socket=0),
    ],
)
def test_envelope_rules_are_strict(envelope: object) -> None:
    with pytest.raises(FormatError):
        allocation_from_envelope(envelope)


def test_exec_allocation_runs_a_site_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = tmp_path / "allocation"
    probe.write_text(f"#!/bin/sh\ncat <<'EOF'\n{json.dumps(_ENVELOPE)}\nEOF\n", encoding="utf-8")
    probe.chmod(0o755)
    assert exec_allocation(str(probe), {}) == allocation_from_envelope(_ENVELOPE)
    # The probe is recorded absolute, so a manager in another working directory runs the same one.
    assert probe_allocation(f"exec:{probe}", {}) == replace(
        allocation_from_envelope(_ENVELOPE), probe=f"exec:{os.path.realpath(probe)}"
    )
    with monkeypatch.context() as moved:
        moved.chdir(tmp_path)
        relative = probe_allocation("exec:./allocation", {})
    assert relative is not None and relative.probe == f"exec:{os.path.realpath(probe)}"
    failing = tmp_path / "failing"
    failing.write_text("#!/bin/sh\necho no PBS_NODEFILE >&2\nexit 3\n", encoding="utf-8")
    failing.chmod(0o755)
    with pytest.raises(ValueError, match=r"failing exited with status 3: no PBS_NODEFILE"):
        exec_allocation(str(failing), {})
    garbage = tmp_path / "garbage"
    garbage.write_text("#!/bin/sh\necho '{'\n", encoding="utf-8")
    garbage.chmod(0o755)
    with pytest.raises(ValueError, match="invalid envelope"):
        exec_allocation(str(garbage), {})
    invalid = tmp_path / "invalid"
    invalid.write_text(f"#!/bin/sh\necho '{json.dumps({**_ENVELOPE, 'kind': 'Bad Kind'})}'\n", encoding="utf-8")
    invalid.chmod(0o755)
    with pytest.raises(ValueError, match=r"allocation.kind.*stdout: '\{\"format\""):
        exec_allocation(str(invalid), {})
    with pytest.raises(ValueError, match="cannot run"):
        exec_allocation(str(tmp_path / "missing"), {})


def test_probe_specs() -> None:
    assert probe_allocation("auto", {}) is None
    assert probe_allocation("none", {"SLURM_JOB_ID": "1"}) is None
    assert probe_allocation("host", {}).kind == "host"  # type: ignore[union-attr]
    slurm = probe_allocation("auto", {"SLURM_JOB_ID": "1", "SLURM_NTASKS": "2"})
    assert slurm is not None and slurm.kind == "slurm" and slurm.capacity() == {"procs": 2}
    for spec in ("auto", "none", "slurm", "host", "exec:/x"):
        assert parse_allocation_spec(spec) == spec
    for spec in ("", "exec:", "pbs", "Slurm"):
        with pytest.raises(ValueError, match="--allocation"):
            parse_allocation_spec(spec)


def _arguments(**options: object) -> argparse.Namespace:
    return argparse.Namespace(**{**_manager.manager_option_defaults(), **options})


def test_allocation_option_parses_and_forwards(tmp_path: Path) -> None:
    from httk.workflow import workflow_cli

    parser = workflow_cli.build_parser("httk workflow", CLIContext("httk", tmp_path))
    for leaf in (["manager", "run"], ["run"]):
        assert parser.parse_args(leaf).allocation == "auto"
        parsed = parser.parse_args([*leaf, "--allocation", "exec:/site/allocation"])
        assert _manager.manager_argv_tail(parsed) == ["--allocation", "exec:/site/allocation"]
    assert _manager.manager_argv_tail(_arguments()) == []
    with pytest.raises(ValueError, match="--allocation 'exec:'"):
        _manager.manager_argv_tail(_arguments(allocation="exec:"))


def test_manager_capacity_prefers_cli_resources() -> None:
    allocation = Allocation("pbs", None, (Node("a", 8, 100), Node("b", 8, 100)), {"license": 1})
    assert _manager._manager_capacity(_arguments(), allocation) == {"procs": 16, "mem": 200, "nodes": 2, "license": 1}
    assert _manager._manager_capacity(_arguments(worker_resource=[["procs", "4"]]), allocation)["procs"] == 4
    assert _manager._manager_capacity(_arguments(), None) == {}


def _fake_manager(seen: dict[str, object]) -> type:
    class FakeManager:
        manager_id = "manager"
        pools = frozenset({"default"})
        capabilities: frozenset[str] = frozenset()
        allowed_executors = frozenset({"path"})
        drained: str | None = None

        def __init__(self, _workspace: Workspace, **kwargs: object) -> None:
            seen.update(kwargs)
            self.resources = kwargs["resources"]

        def __enter__(self) -> Self:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def run_until_idle(self, **_kwargs: object) -> object:
            return type("Census", (), {"summary_line": lambda _self: "idle", "time_advice": lambda _self: None})()

    return FakeManager


def test_a_manager_without_a_probed_end_keeps_the_slurm_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from httk.workflow.workflow_cli import command

    workspace = Workspace.initialize(tmp_path / "workspace")
    end = int(time.time()) + 7200
    seen: dict[str, object] = {}
    monkeypatch.setattr(_manager, "TaskManager", _fake_manager(seen))
    monkeypatch.setenv("SLURM_JOB_ID", "7")
    monkeypatch.setenv("SLURM_JOB_END_TIME", str(end))
    for spec in ("none", "host"):
        arguments = [
            "manager",
            "run",
            "--by-path",
            "--workspace",
            str(workspace.root),
            "--inline",
            "--allocation",
            spec,
        ]
        assert command(arguments, CLIContext("httk", tmp_path)) == 0
        assert seen["end_time"] == float(end)


def test_run_uses_the_probed_allocation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    from httk.workflow.workflow_cli import command

    workspace = Workspace.initialize(tmp_path / "workspace")
    probe = tmp_path / "allocation"
    end = int(time.time()) + 7200
    probe.write_text(f"#!/bin/sh\ncat <<'EOF'\n{json.dumps({**_ENVELOPE, 'end_time': end})}\nEOF\n", encoding="utf-8")
    probe.chmod(0o755)
    seen: dict[str, object] = {}
    monkeypatch.setattr(_manager, "TaskManager", _fake_manager(seen))
    arguments = [
        *("manager", "run", "--by-path", "--workspace", str(workspace.root), "--inline"),
        *("--allocation", f"exec:{probe}", "--worker-resource", "procs", "3", "--time-limit", "1:00:00"),
    ]
    assert command(arguments, CLIContext("httk", tmp_path)) == 0
    assert seen["resources"] == {"procs": 3, "gpus": 2, "nodes": 2, "license": 2}
    assert isinstance(seen["allocation"], Allocation) and seen["allocation"].kind == "pbs"
    # --time-limit ends before the allocation does.
    assert isinstance(seen["end_time"], float) and seen["end_time"] < end - 3000
    assert "allocation=pbs nodes=2" in capsys.readouterr().err


def test_owner_json_records_the_allocation(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    allocation = Allocation("slurm", None, (Node("n01", 4), Node("n02", 4)), {})
    with TaskManager(workspace, allocation=allocation) as manager:
        record = json.loads((manager.manager_directory / "owner.json").read_text())
        assert manager.allocation is allocation
        assert (record["allocation"]["kind"], record["allocation"]["end_time"]) == ("slurm", None)
    with TaskManager(workspace) as manager:
        record = json.loads((manager.manager_directory / "owner.json").read_text())
        assert record["allocation"] is None


def test_slurm_dispatcher_appends_the_configured_probe() -> None:
    argv = ["httk", "workflow", "manager", "run"]
    assert launch_runtime._manager_argv(argv, {})[-2:] == ["--allocation", "slurm"]
    configured = launch_runtime._manager_argv(argv, {"manager.allocation": "exec:/site/allocation"})
    assert configured[-2:] == ["--allocation", "exec:/site/allocation"]
    for explicit in ([*argv, "--allocation", "none"], [*argv, "--allocation=none"]):
        assert launch_runtime._manager_argv(explicit, {"manager.allocation": "host"}) == explicit
    assert argv_allocation(["--allocation=host", "--allocation", "none"]) == "none"
    assert argv_allocation(["--allocation"]) is None and argv_allocation(argv) is None
    with pytest.raises(ValueError, match="--allocation"):
        launch_runtime._manager_argv(argv, {"manager.allocation": "pbs"})


def test_process_launcher_probes_the_host_for_one_manager_only(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    calls: list[list[str]] = []

    class Child:
        pid = 1

    def fake_popen(argv: list[str], **_kwargs: object) -> Child:
        calls.append(argv)
        return Child()

    monkeypatch.setattr(launchers.subprocess, "Popen", fake_popen)
    argv = [sys.executable, "-m", "httk.core.cli", "workflow", "manager", "run"]
    for count, spec in ((1, "host"), (2, "none")):
        calls.clear()
        launchers.launch_processes(workspace_root=tmp_path, argv=argv, count=count, settings={}, capacity={})
        assert all(launched[-2:] == ["--allocation", spec] for launched in calls) and len(calls) == count
    calls.clear()
    explicit = [*argv, "--allocation=exec:/site/allocation"]
    launchers.launch_processes(workspace_root=tmp_path, argv=explicit, count=1, settings={}, capacity={})
    assert calls == [explicit]
    calls.clear()
    launchers.launch_processes(workspace_root=tmp_path, argv=explicit, count=2, settings={}, capacity={})
    assert all(launched[-2:] == ["--allocation", "none"] for launched in calls)
    assert "children of --count 2 schedule by counts only" in caplog.text


def test_foreground_count_children_only_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    launched: list[list[str]] = []

    class Child:
        def __init__(self, argv: list[str], **_kwargs: object) -> None:
            launched.append(argv)

        def poll(self) -> int:
            return 0

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(_manager.subprocess, "Popen", Child)
    context = CLIContext("httk", tmp_path)
    assert _manager._run_local_manager_children(_arguments(), tmp_path, context, 2) == 0
    assert all("--allocation none" in " ".join(argv) for argv in launched) and len(launched) == 2
    launched.clear()
    assert not caplog.records
    _manager._run_local_manager_children(_arguments(allocation="host"), tmp_path, context, 2)
    assert all(argv[-4:] == ["--allocation", "host", "--allocation", "none"] for argv in launched)
    assert "children of --count 2 schedule by counts only" in caplog.text
