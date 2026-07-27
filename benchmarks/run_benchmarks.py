#!/usr/bin/env python3
"""Run the opt-in filesystem scale benchmarks.

This is intentionally a script rather than a pytest module.  It creates all
workspaces below a temporary directory, uses no network, and reports the
measurements it actually made as both a compact table and optional JSON.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import tracemalloc
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from httk.workflow import TaskManager, Workspace, harvest
from httk.workflow.models import Marker
from httk.workflow.protocol import ChildReference, JobSpec, join_mapping, prepare_job_payload

NOOP_RUNNER = "#!/bin/sh\nexit 0\n"
SIZES = {"quick": (500,), "snapshot": (2500, 10000)}
BENCHMARK_NAMES = (
    "streamed_submission",
    "cold_scheduling_tick",
    "warm_scheduling_tick",
    "time_to_first_claim",
    "memory_per_active_marker",
    "heartbeat_under_scan",
    "join_evaluation",
    "harvest_throughput",
    "fsck_and_gc",
)

type Scalar = int | float | str | bool
type Parameters = dict[str, Scalar]
type Result = dict[str, object]


def _progress(job_id: str, *, step: str = "only", **extra: object) -> dict[str, object]:
    return {
        "step": step,
        "activation_id": str(uuid.uuid5(uuid.UUID(job_id), "benchmark-activation")),
        "activation_ordinal": 1,
        "attempt_ordinal": 0,
        "total_attempts": 0,
        "data_generation": None,
        **extra,
    }


def _result(name: str, parameters: Parameters, duration: float, rate: float, unit: str, **extra: object) -> Result:
    return {
        "name": name,
        "size_parameters": parameters,
        "duration_seconds": duration,
        "rate": rate,
        "rate_unit": unit,
        **extra,
    }


def _workspace(root: Path, name: str, *, policy: Mapping[str, object] | None = None) -> Workspace:
    return Workspace.initialize(root / name, durable=False, policy=policy)


def _prepare_and_submit(
    workspace: Workspace,
    source_root: Path,
    count: int,
    placement: Callable[[int], str],
) -> tuple[float, list[Marker]]:
    source_root.mkdir(parents=True, exist_ok=True)
    markers: list[Marker] = []
    started = time.perf_counter()
    for index in range(count):
        payload = source_root / f"job-{index:05d}"
        runner = payload / "files" / "runner"
        runner.parent.mkdir(parents=True)
        runner.write_text(NOOP_RUNNER, encoding="utf-8")
        runner.chmod(0o755)
        prepare_job_payload(
            payload,
            JobSpec(
                name="Benchmark job",
                workflow="benchmarks.noop",
                runner_path="files/runner",
                initial_step="only",
                tag=f"j{index}",
            ),
        )
        markers.append(workspace.submit(payload, placement(index), move=True))
    return time.perf_counter() - started, markers


def _move_to_ready(workspace: Workspace, markers: list[Marker]) -> None:
    with workspace.open_journal_writer() as writer:
        for marker in markers:
            workspace.transition(writer, marker, "ready", _progress(marker.job_id, reason="benchmark_ready"))


def _move_ready_to_terminal(workspace: Workspace) -> None:
    markers = list(workspace.scan_markers(("ready",)))
    with workspace.open_journal_writer() as writer:
        for marker in markers:
            workspace.transition(writer, marker, "succeeded", _progress(marker.job_id, reason="benchmark_terminal"))


def _manager(workspace: Workspace, count: int) -> TaskManager:
    return TaskManager(
        workspace,
        maximum_workers=1,
        maximum_pass_markers=max(count + 1, 1),
        discovery_budget=max(count * 4, 4096),
        heartbeat_interval=3600.0,
    )


def _eligibility_only(manager: TaskManager) -> None:
    """Prevent process launch while retaining the real tick discovery path."""

    def no_launch(_marker: Any) -> bool:
        return False

    manager._claim_and_launch = no_launch  # type: ignore[method-assign]


def _ready_workspace(root: Path, name: str, count: int, placement: Callable[[int], str]) -> tuple[Workspace, float]:
    workspace = _workspace(root, name)
    duration, markers = _prepare_and_submit(workspace, root / f"{name}-payloads", count, placement)
    _move_to_ready(workspace, markers)
    return workspace, duration


def _run_ready_benchmarks(root: Path, count: int, submission_duration: float) -> list[Result]:
    workspace, _ = _ready_workspace(
        root,
        "bounded",
        count,
        lambda index: f"bench/{index % 64:02d}/{index // 64:04d}",
    )
    results: list[Result] = [
        _result(
            "streamed_submission",
            {"N": count, "placement": "bench/<i%64>/<batch>"},
            submission_duration,
            count / submission_duration,
            "jobs/s",
        )
    ]

    with _manager(workspace, count) as manager:
        _eligibility_only(manager)
        eligible_counts: list[int] = []
        real_eligible_ready = manager._eligible_ready

        def observed_eligible_ready() -> list[Marker]:
            eligible = real_eligible_ready()
            eligible_counts.append(len(eligible))
            return eligible

        manager._eligible_ready = observed_eligible_ready  # type: ignore[method-assign]
        started = time.perf_counter()
        manager.tick()
        cold = time.perf_counter() - started
        if not eligible_counts or eligible_counts[-1] != count:
            observed = eligible_counts[-1] if eligible_counts else 0
            raise RuntimeError(f"cold_scheduling_tick: expected {count} eligible jobs, observed {observed}")
        results.append(
            _result("cold_scheduling_tick", {"N": count, "workers": 1, "launch": "disabled"}, cold, 1 / cold, "ticks/s")
        )
        started = time.perf_counter()
        manager.tick()
        warm = time.perf_counter() - started
        if len(eligible_counts) < 2 or eligible_counts[-1] != count:
            observed = eligible_counts[-1] if eligible_counts else 0
            raise RuntimeError(f"warm_scheduling_tick: expected {count} eligible jobs, observed {observed}")
        results.append(
            _result("warm_scheduling_tick", {"N": count, "workers": 1, "launch": "disabled"}, warm, 1 / warm, "ticks/s")
        )

    first_claim: list[float] = []
    with _manager(workspace, count) as manager:
        real_transition = workspace.transition

        def observed_transition(
            writer: Any,
            marker: Marker,
            kind: str,
            updates: Mapping[str, object],
            **kwargs: Any,
        ) -> Marker:
            if kind == "claimed" and not first_claim:
                first_claim.append(time.perf_counter())
            return real_transition(writer, marker, kind, updates, **kwargs)

        workspace.transition = observed_transition  # type: ignore[method-assign]
        started = time.perf_counter()
        manager.tick()
        if not first_claim:
            raise RuntimeError("time_to_first_claim: no claimed transition was observed")
        elapsed = first_claim[0] - started
        manager._draining = True
        for _ in range(3):
            time.sleep(0.005)
            manager.tick()
        results.append(_result("time_to_first_claim", {"N": count, "workers": 1}, elapsed, 1 / elapsed, "claims/s"))

    memory_workspace = Workspace(workspace.root, durable=False, marker_index_capacity=max(count, 1))
    with _manager(memory_workspace, count) as manager:
        tracemalloc.start()
        tracemalloc.reset_peak()
        started = time.perf_counter()
        memory_workspace.find_marker_by_id(str(uuid.uuid5(uuid.NAMESPACE_DNS, "benchmark-missing")))
        _eligibility_only(manager)
        manager._eligible_ready()
        duration = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    results.append(
        _result(
            "memory_per_active_marker",
            {"N": count, "index_capacity": count},
            duration,
            peak / count,
            "bytes/marker",
            peak_traced_bytes=peak,
            approximation="tracemalloc index+bookkeeping, not RSS",
        )
    )
    return results


def _heartbeat_benchmark(root: Path, count: int) -> tuple[Workspace, Result]:
    workspace, _ = _ready_workspace(root, "flat", count, lambda _index: "flat")
    gaps: list[float] = []
    observed: list[float] = []
    with _manager(workspace, count) as manager:
        _eligibility_only(manager)
        real_heartbeat = manager.heartbeat

        def observed_heartbeat(*, force: bool = False) -> None:
            now = time.perf_counter()
            if observed:
                gaps.append(now - observed[-1])
            observed.append(now)
            real_heartbeat(force=True)

        manager.heartbeat = observed_heartbeat  # type: ignore[method-assign]
        started = time.perf_counter()
        manager.tick()
        duration = time.perf_counter() - started
    maximum_gap = max(gaps, default=0.0)
    return workspace, _result(
        "heartbeat_under_scan",
        {"N": count, "placement": "flat", "discovery_budget": max(count * 4, 4096)},
        duration,
        (1 / maximum_gap) if maximum_gap else 0.0,
        "heartbeats/s",
        maximum_inter_heartbeat_seconds=maximum_gap,
        heartbeat_calls=len(observed),
        approximation="instrumented hook opportunities with forced writes",
    )


def _join_benchmark(root: Path, children: int) -> Result:
    workspace = _workspace(root, "join")
    child_refs: list[ChildReference] = []
    source_root = root / "join-payloads"
    source_root.mkdir()
    with workspace.open_journal_writer() as writer:
        for index in range(children):
            payload = source_root / f"child-{index:04d}"
            runner = payload / "files" / "runner"
            runner.parent.mkdir(parents=True)
            runner.write_text(NOOP_RUNNER, encoding="utf-8")
            runner.chmod(0o755)
            job = prepare_job_payload(
                payload,
                JobSpec(name="Join child", workflow="benchmarks.noop", runner_path="files/runner", tag=f"c{index}"),
            )
            marker = workspace.submit(payload, "children", move=True)
            marker = workspace.transition(writer, marker, "ready", _progress(marker.job_id, reason="benchmark_ready"))
            workspace.transition(writer, marker, "succeeded", _progress(marker.job_id, reason="benchmark_terminal"))
            child_refs.append(ChildReference(workspace.workspace_id, job.id, job.job_key, "children"))

    parent_source = root / "join-parent-payload"
    parent_runner = parent_source / "files" / "runner"
    parent_runner.parent.mkdir(parents=True)
    parent_runner.write_text(NOOP_RUNNER, encoding="utf-8")
    parent_runner.chmod(0o755)
    parent_job = prepare_job_payload(
        parent_source,
        JobSpec(name="Join parent", workflow="benchmarks.noop", runner_path="files/runner", tag="parent"),
    )
    parent = workspace.submit(parent_source, "parent", move=True)
    with workspace.open_journal_writer() as writer:
        parent = workspace.transition(writer, parent, "ready", _progress(parent.job_id, reason="benchmark_ready"))
        workspace.transition(
            writer,
            parent,
            "waiting",
            _progress(
                parent.job_id,
                reason="benchmark_waiting",
                next_step="after",
                join=join_mapping(child_refs),
            ),
        )
    with _manager(workspace, children) as manager:
        started = time.perf_counter()
        manager._evaluate_joins()
        duration = time.perf_counter() - started
    completed_parent = workspace.find_marker_by_id(parent_job.id)
    if completed_parent is None or completed_parent.kind != "ready":
        observed_kind = None if completed_parent is None else completed_parent.kind
        raise RuntimeError(f"join_evaluation: parent did not advance to ready (observed {observed_kind!r})")
    completed_state = workspace.read_state(completed_parent)
    summary = completed_state.get("join_summary")
    if not isinstance(summary, list) or len(summary) != children:
        observed_children = len(summary) if isinstance(summary, list) else 0
        raise RuntimeError(
            f"join_evaluation: expected observations for {children} children, observed {observed_children}"
        )
    return _result(
        "join_evaluation", {"C": children, "parent_state": "waiting"}, duration, children / duration, "children/s"
    )


def _terminal_benchmarks(root: Path, count: int) -> list[Result]:
    workspace, heartbeat_result = _heartbeat_benchmark(root, count)
    _move_ready_to_terminal(workspace)
    harvest_start = time.perf_counter()
    harvested = sum(1 for _ in harvest(workspace, states=("succeeded",)))
    harvest_duration = time.perf_counter() - harvest_start
    fsck_start = time.perf_counter()
    fsck_report = workspace.check()
    fsck_duration = time.perf_counter() - fsck_start
    workspace.set_policy({"retention": {"journal_days": 0.0}})
    gc_start = time.perf_counter()
    gc_report = workspace.collect_garbage()
    gc_duration = time.perf_counter() - gc_start
    combined = fsck_duration + gc_duration
    return [
        heartbeat_result,
        _result(
            "harvest_throughput",
            {"N": count, "states": "succeeded"},
            harvest_duration,
            harvested / harvest_duration,
            "records/s",
        ),
        _result(
            "fsck_and_gc",
            {"N": count, "retention": "journal_days=0"},
            combined,
            count / combined,
            "jobs/s",
            fsck_seconds=fsck_duration,
            gc_seconds=gc_duration,
            fsck_markers_checked=fsck_report.markers_checked,
            gc_candidates=gc_report.candidates,
        ),
    ]


def _run_size(root: Path, count: int) -> list[Result]:
    workspace = _workspace(root, "submission")
    submission_duration, _ = _prepare_and_submit(
        workspace,
        root / "submission-payloads",
        count,
        lambda index: f"bench/{index % 64:02d}/{index // 64:04d}",
    )
    results = _run_ready_benchmarks(root, count, submission_duration)
    results.extend(_terminal_benchmarks(root, count))
    results.append(_join_benchmark(root, 500))
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=int, help="run one requested N (snapshot still adds N//4)")
    parser.add_argument("--json", dest="json_path", type=Path, help="write machine-readable results here")
    parser.add_argument("--profile", choices=tuple(SIZES), default="quick")
    args = parser.parse_args()
    if args.scale is not None and args.scale < 1:
        parser.error("--scale must be positive")
    return args


def _sizes(args: argparse.Namespace) -> tuple[int, ...]:
    if args.scale is None:
        return SIZES[args.profile]
    if args.profile == "snapshot":
        return (max(args.scale // 4, 1), args.scale)
    return (args.scale,)


def _print_results(results: list[Result]) -> None:
    def as_float(value: object) -> float:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        raise TypeError(f"expected a numeric result field, got {value!r}")

    print("name                         size       wall_s       rate")
    print("---------------------------  ---------  -----------  ----------------")
    for item in results:
        parameters = item["size_parameters"]
        size = parameters if isinstance(parameters, dict) else str(parameters)
        print(
            f"{str(item['name']):27}  {str(size):9}  {as_float(item['duration_seconds']):11.6f}  "
            f"{as_float(item['rate']):10.2f} {item['rate_unit']}"
        )


def main() -> int:
    args = _parse_args()
    sizes = _sizes(args)
    all_results: list[Result] = []
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="httk-workflow-bench-") as temporary:
        root = Path(temporary)
        for count in sizes:
            size_root = root / f"N-{count}"
            size_root.mkdir()
            all_results.extend(_run_size(size_root, count))
    ratios: dict[str, float] = {}
    if len(sizes) == 2:
        low, high = sizes
        for name in BENCHMARK_NAMES:
            low_rows = [
                row
                for row in all_results
                if row["name"] == name
                and isinstance(row["size_parameters"], dict)
                and row["size_parameters"].get("N") == low
            ]
            high_rows = [
                row
                for row in all_results
                if row["name"] == name
                and isinstance(row["size_parameters"], dict)
                and row["size_parameters"].get("N") == high
            ]
            if low_rows and high_rows:
                high_duration = high_rows[0]["duration_seconds"]
                low_duration = low_rows[0]["duration_seconds"]
                if not isinstance(high_duration, (int, float)) or not isinstance(low_duration, (int, float)):
                    raise TypeError("benchmark duration is not numeric")
                ratios[name] = float(high_duration) / float(low_duration)
    payload = {
        "profile": args.profile,
        "sizes": list(sizes),
        "duration_seconds": time.perf_counter() - started,
        "benchmarks": all_results,
        "linearity_ratios": ratios,
    }
    _print_results(all_results)
    print(f"total wall time: {payload['duration_seconds']:.3f}s")
    if ratios:
        print("linearity ratios (high/low): " + ", ".join(f"{name}={ratio:.2f}x" for name, ratio in ratios.items()))
    if args.json_path is not None:
        args.json_path.parent.mkdir(parents=True, exist_ok=True)
        args.json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + os.linesep, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
