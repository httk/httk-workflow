"""The parts of the local many-job benchmark (``benchmarks/local_scale.py``, plan WORKFLOW_LOCAL_COMMANDS L0b) that
need no workspace: its arguments, the critical-path sync time, the output and state checks that make a run valid,
the guards, the acceptance verdicts and the summary.
"""

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "local_scale", Path(__file__).resolve().parents[1] / "benchmarks" / "local_scale.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench = _load()


# -- arguments ------------------------------------------------------------------------------------------------------


def test_the_defaults_measure_every_command_at_100_jobs_by_prefix_under_auto(tmp_path: Path) -> None:
    arguments = bench.parser().parse_args(["--out", str(tmp_path)])
    assert (arguments.jobs, arguments.selector, arguments.io_sync, arguments.backlog) == (100, "prefix", "auto", False)
    assert (arguments.repeat, arguments.traced) == (3, 1)
    assert arguments.commands == ["new", "cancel", "delete", "pause", "continue"]
    assert arguments.workflow.startswith("git+file://") and arguments.workflow.endswith("#vasp-static")
    bench.check_arguments(arguments)


def test_commands_are_a_subset_kept_in_chain_order(tmp_path: Path) -> None:
    arguments = bench.parser().parse_args(["--out", str(tmp_path), "--commands", "continue, delete"])
    assert arguments.commands == ["delete", "continue"]


@pytest.mark.parametrize(
    "extra",
    [["--commands", "recover"], ["--commands", ""], ["--selector", "glob"], ["--io-sync", "never"], []],
    ids=["unknown command", "no command", "selector", "io-sync", "no --out"],
)
def test_arguments_outside_the_choices_are_refused(tmp_path: Path, extra: list[str]) -> None:
    out = [] if not extra else ["--out", str(tmp_path)]
    with pytest.raises(SystemExit):
        bench.parser().parse_args([*out, *extra])


def test_a_traced_only_run_is_accepted(tmp_path: Path) -> None:
    bench.check_arguments(bench.parser().parse_args(["--out", str(tmp_path), "--repeat", "0"]))


@pytest.mark.parametrize(
    "extra",
    [["--jobs", "0"], ["--repeat", "-1"], ["--traced", "-1"], ["--timeout", "0"], ["--repeat", "0", "--traced", "0"]],
    ids=["jobs", "repeat", "traced", "timeout", "no chain"],
)
def test_values_no_run_can_use_are_refused(tmp_path: Path, extra: list[str]) -> None:
    arguments = bench.parser().parse_args(["--out", str(tmp_path), *extra])
    with pytest.raises(SystemExit):
        bench.check_arguments(arguments)


def test_a_run_refuses_tmp_and_a_remote_workflow_before_touching_anything(tmp_path: Path) -> None:
    arguments = bench.parser().parse_args(["--out", "/tmp/local-scale-never"])
    with pytest.raises(SystemExit, match="/tmp"):
        bench.run(arguments)
    remote = bench.parser().parse_args(["--out", str(tmp_path), "--workflow", "git+https://example.invalid/x#y"])
    with pytest.raises(SystemExit, match="git\\+file"):
        bench.run(remote)
    assert not Path("/tmp/local-scale-never").exists() and not any(tmp_path.iterdir())


# -- the probe ------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("intervals", "expected"),
    [
        ([], 0.0),
        ([(1.0, 2.0)], 1.0),
        ([(0.0, 1.0), (2.0, 3.0)], 2.0),
        ([(0.0, 2.0), (1.0, 3.0)], 3.0),  # overlapping pooled calls count once
        ([(0.0, 4.0), (1.0, 2.0), (1.5, 3.0)], 4.0),  # nested
        ([(2.0, 3.0), (0.0, 1.0), (1.0, 2.0)], 3.0),  # touching, out of order
        ([(0.0, 1.0), (0.0, 1.0)], 1.0),  # the same interval twice
    ],
)
def test_the_critical_path_is_the_length_of_the_union_of_the_sync_intervals(
    intervals: list[tuple[float, float]], expected: float
) -> None:
    assert bench.critical_path(intervals) == pytest.approx(expected)


def test_the_probe_s_sync_wrapper_counts_sums_and_unions_concurrent_calls() -> None:
    probe = bench._Probe()
    sleep = probe.synced("fsync", lambda seconds: time.sleep(seconds))
    threads = [threading.Thread(target=sleep, args=(0.2,)) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    document = probe.document()
    assert document["sync"]["fsync"]["count"] == 4 and document["sync_calls"] == 4
    assert document["sync_seconds"] >= 0.8
    assert 0.2 <= document["critical_path_sync_seconds"] < 0.6  # concurrent: counted about once, not four times


def test_the_probe_counts_the_entries_a_listing_yields_even_when_it_is_abandoned(tmp_path: Path) -> None:
    for index in range(5):
        (tmp_path / f"f{index}").write_text("x", encoding="utf-8")
    probe = bench._Probe()
    with bench._Listing(probe, os.scandir(tmp_path)) as listing:
        next(listing)
        next(listing)
    assert list(bench._Listing(probe, os.scandir(tmp_path))) and probe.document()["entries_examined"] == 7
    counted = probe.listed("listdir", os.listdir)
    assert len(counted(tmp_path)) == 5 and probe.document()["entries_examined"] == 12


# -- the validity checks --------------------------------------------------------------------------------------------


def test_selectors_name_jobs_by_prefix_full_id_or_state_and_delete_falls_back_to_ids() -> None:
    jobs = [("si-001", "a" * 8), ("si-002", "b" * 8)]
    assert bench.selector_arguments("prefix", "cancel", jobs) == (["si-001--", "si-002--"], "prefix")
    assert bench.selector_arguments("full", "delete", jobs) == (["a" * 8, "b" * 8], "full")
    assert bench.selector_arguments("state", "cancel", jobs) == (["--state", "ready"], "state")
    assert bench.selector_arguments("state", "pause", jobs) == (["--state", "ready"], "state")
    assert bench.selector_arguments("state", "continue", jobs) == (["--state", "paused"], "state")
    assert bench.selector_arguments("state", "delete", jobs) == (["a" * 8, "b" * 8], "full (job delete has no --state)")
    assert bench.selector_arguments("state", "new", jobs) == ([], "none")


def test_the_measured_command_lines() -> None:
    root, structures = Path("/w/ws"), Path("/w/structures")
    new = bench.command_arguments("new", root, "git+file:///r#vasp-static", structures, [])
    assert new == [
        "job", "new", "--workspace", "/w/ws", "git+file:///r#vasp-static", "--install",
        "--input-from", "structure", "/w/structures", "--duplicate-tag", "skip",
    ]  # fmt: skip
    assert bench.command_arguments("delete", root, "", structures, ["x--"]) == [
        "job", "delete", "--workspace", "/w/ws", "--yes", "x--",
    ]  # fmt: skip
    assert bench.command_arguments("cancel", root, "", structures, ["--state", "ready"]) == [
        "job", "cancel", "--workspace", "/w/ws", "--reason", "bench", "--state", "ready",
    ]  # fmt: skip
    assert bench.command_arguments("continue", root, "", structures, ["x--"])[:2] == ["job", "resume"]


def test_tags_are_distinct_and_widen_at_1000_jobs() -> None:
    assert bench.structure_names(3) == ["si-001.vasp", "si-002.vasp", "si-003.vasp"]
    names = bench.structure_names(1000)
    assert names[0] == "si-0001.vasp" and names[-1] == "si-1000.vasp" and len(set(names)) == 1000
    # No tag prefix names another job: si-0001-- is not a prefix of si-00010--.
    assert not any(other.startswith(f"{name.removesuffix('.vasp')}--") for name in names for other in names)


def test_prepared_structures_cycle_the_sources_under_the_new_names(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    for index in range(3):
        (source / f"s{index}.vasp").write_text(f"structure {index}\n", encoding="utf-8")
    tags = bench._prepare_structures(tmp_path / "copies", source, 1000)
    assert tags[0] == "si-0001" and len(set(tags)) == 1000
    assert (tmp_path / "copies" / "si-0004.vasp").read_text(encoding="utf-8") == "structure 0\n"
    assert len(os.listdir(tmp_path / "copies")) == 1000


def _request_lines(action: str, statuses: list[str]) -> str:
    return "".join(
        f"{index:08x}\t{index:032x}\t{status}\t{action} {status}; state=x\n" for index, status in enumerate(statuses)
    )


def test_output_checks_accept_exactly_n_outcomes_of_the_expected_kind() -> None:
    assert bench.output_problems("new", 3, "", "warning\nsubmitted 3 jobs\n") == []
    assert bench.output_problems("cancel", 2, _request_lines("cancel", ["applied"] * 2), "") == []
    removed = "k1\tcancelled\tremoved\nk2\tcancelled\tremoved\nremoved 2 of 2 job(s)\n"
    assert bench.output_problems("delete", 2, removed, "") == []


@pytest.mark.parametrize(
    ("command", "stdout", "stderr"),
    [
        ("new", "", "submitted 2 jobs\n"),
        ("new", "", "submitted 2 jobs (1 skipped)\n"),
        ("new", "", ""),
        ("cancel", _request_lines("cancel", ["applied", "applied"]), ""),
        ("cancel", _request_lines("cancel", ["applied", "applied", "refused"]), ""),
        ("pause", _request_lines("pause", ["applied", "applied", "queued"]), ""),
        ("pause", _request_lines("cancel", ["applied"] * 3), ""),  # another action's outcomes
        ("delete", "k1\tcancelled\tremoved\nk2\tcancelled\tremoved\nk3\tcancelled\tremoved\n", ""),
        ("delete", "k1\tc\tremoved\nk2\tc\tremoved\nk3\tc\trefused\tno\nremoved 2 of 3 job(s)\n", ""),
    ],
    ids=["new short", "new skipped", "new silent", "short", "refused", "queued", "other action", "no total", "refused"],
)
def test_output_checks_refuse_any_other_report(command: str, stdout: str, stderr: str) -> None:
    assert bench.output_problems(command, 3, stdout, stderr)


def test_the_census_must_hold_exactly_the_jobs_in_the_expected_state_and_no_request() -> None:
    ids = {"a", "b"}
    empty: dict[str, set[str]] = {"ready": set(), "cancelled": set(), "owned": set()}
    assert bench.census_problems({**empty, "cancelled": ids}, "cancelled", ids, 0) == []
    assert bench.census_problems(empty, None, ids, 0) == []
    assert bench.census_problems({**empty, "cancelled": {"a"}, "ready": {"b"}}, "cancelled", ids, 0)
    assert bench.census_problems({**empty, "cancelled": ids | {"c"}}, "cancelled", ids, 0)
    assert bench.census_problems({**empty, "owned": {"a"}}, None, ids, 0)
    assert bench.census_problems({**empty, "cancelled": ids}, "cancelled", ids, 1) == ["1 request file(s) left"]
    assert bench.census_problems(empty, "paused", ids, 0) == ["paused: not listed"]


# -- the guards -----------------------------------------------------------------------------------------------------


def test_the_real_registry_ignores_an_httk_config_home_override(tmp_path: Path) -> None:
    real = bench.real_registry({"HTTK_CONFIG_HOME": str(tmp_path), "XDG_CONFIG_HOME": "/x/config"})
    assert real == Path("/x/config/httk/workspaces.json")
    assert bench.real_registry({}) == (Path.home() / ".config" / "httk").resolve() / "workspaces.json"


def test_the_run_refuses_user_directories_that_are_not_below_its_output(tmp_path: Path) -> None:
    out, real = tmp_path / "out", tmp_path / "home" / ".config" / "httk" / "workspaces.json"
    config, data = out / "work" / "r" / "config", out / "work" / "r" / "data"
    assert bench.redirection_problems(out, config, data, config / "workspaces.json", real) == []
    outside = tmp_path / "elsewhere"
    assert len(bench.redirection_problems(out, outside, data, outside / "workspaces.json", real)) == 2
    assert bench.redirection_problems(out, config, outside, config / "workspaces.json", real)
    assert bench.redirection_problems(out, real.parent, data, real, real)


def test_the_source_key_names_the_commit_and_the_src_changes() -> None:
    assert bench.source_key("90a13ee" + "0" * 33, None) == "90a13ee00000"
    assert bench.source_key("90a13ee" + "0" * 33, "f" * 64) == "90a13ee00000+dirty.ffffffffffff"


# -- acceptance and the summary -------------------------------------------------------------------------------------


def _result(
    run: str,
    jobs: int,
    walls: dict[str, float],
    *,
    key: str = "90a13ee00000",
    critical: float = 0.1,
    valid: bool = True,
    selector: str = "prefix",
    format_version: int = 2,
) -> dict[str, Any]:
    commands = {
        command: {
            "selector": [selector],
            "wall_median": wall,
            "ms_per_job": 1000 * wall / jobs,
            "plain_walls": [wall],
            "traced_runs": 1,
            "traced": {
                "wall_median": wall,
                "sync_calls": 10,
                "sync_seconds": 2 * critical,
                "critical_path_sync_seconds": critical,
                "critical_path_sync_share": critical / wall,
                "entries_examined": 100,
                "strategies": [],
            },
        }
        for command, wall in walls.items()
    }
    return {
        "benchmark": "local_scale",
        "format_version": format_version,
        "run": run,
        "valid": valid,
        "problems": [] if valid else ["exit status 1"],
        "parameters": {"jobs": jobs, "selector": selector, "io_sync": "auto", "backlog": False},
        "source": {"key": key, "src_dirty": "+dirty" in key},
        "policy": {"resolved": ["fsync: fsync (zfs is not vetted for auto)"]},
        "commands": commands,
    }


def test_acceptance_checks_the_wall_target_only_at_100_jobs_for_new_cancel_and_delete() -> None:
    result = _result("r100", 100, {"new": 2.4, "cancel": 16.0, "pause": 1.0})
    verdicts = bench.acceptance(result, [])
    assert verdicts["new"]["wall"]["verdict"] == "pass" and verdicts["cancel"]["wall"]["verdict"] == "fail"
    assert verdicts["pause"]["wall"]["verdict"] == "not measured"
    assert bench.acceptance(_result("r1000", 1000, {"new": 2.0}), [])["new"]["wall"]["verdict"] == "not measured"
    assert verdicts["new"]["flatness"]["verdict"] == "not measured"


@pytest.mark.parametrize(
    ("wall", "critical", "verdict"),
    [
        (2.0, 0.24, "pass"),  # 12% of the wall, within the 250 ms floor
        (2.0, 0.25, "pass"),  # exactly the floor
        (2.0, 0.250001, "fail"),  # unrounded: a microsecond over the floor fails
        (16.0, 1.6, "pass"),  # exactly 10% of the wall, above the floor
        (16.0, 1.6000001, "fail"),
        (0.5, 0.2, "pass"),  # 40% of a short wall, within the floor
    ],
)
def test_acceptance_allows_critical_path_sync_up_to_max_of_250_ms_and_10_percent_of_the_wall(
    wall: float, critical: float, verdict: str
) -> None:
    sync = bench.acceptance(_result("a", 100, {"new": wall}, critical=critical), [])["new"]["sync"]
    assert sync["verdict"] == verdict
    assert sync["allowance"] == max(0.25, 0.1 * wall) and sync["critical_path_sync_seconds"] == critical


def test_acceptance_of_sync_needs_a_traced_run() -> None:
    untraced = _result("c", 100, {"new": 2.0})
    del untraced["commands"]["new"]["traced"]
    assert bench.acceptance(untraced, [])["new"]["sync"]["verdict"] == "not measured"


def test_flatness_compares_100_and_1000_jobs_of_the_same_source_and_case_only() -> None:
    small = _result("small", 100, {"cancel": 10.0, "delete": 10.0})
    flat = _result("large", 1000, {"cancel": 120.0, "delete": 130.0})
    other_source = _result("other", 1000, {"cancel": 100.0, "delete": 100.0}, key="90a13ee00000+dirty.abc")
    other_case = _result("case", 1000, {"cancel": 100.0, "delete": 100.0}, selector="full")
    invalid = _result("invalid", 1000, {"cancel": 100.0, "delete": 100.0}, valid=False)
    verdicts = bench.acceptance(flat, [small, other_source, other_case, invalid])
    assert verdicts["cancel"]["flatness"] == {
        "verdict": "pass",
        "ratio": 1.2,
        "ms_per_job": {"100": 100.0, "1000": 120.0},
        "partner": "small",
        "target": 1.25,
    }
    assert verdicts["delete"]["flatness"]["verdict"] == "fail" and verdicts["delete"]["flatness"]["ratio"] == 1.3
    assert bench.acceptance(small, [flat])["cancel"]["flatness"]["partner"] == "large"
    alone = bench.acceptance(small, [other_source, other_case, invalid])
    assert alone["cancel"]["flatness"]["verdict"] == "not measured"


def test_the_summary_groups_by_source_and_skips_other_files(tmp_path: Path) -> None:
    results = tmp_path / "results"
    results.mkdir()
    documents = [
        _result("clean-100", 100, {"new": 2.0, "cancel": 10.0}),
        _result("clean-1000", 1000, {"new": 15.0, "cancel": 100.0}),
        _result("dirty-100", 100, {"new": 1.0}, key="90a13ee00000+dirty.abcdef"),
        _result("broken", 100, {"new": 1.0}, valid=False),
        _result("older", 100, {"new": 3.0}, format_version=1),
    ]
    for document in documents:
        (results / f"{document['run']}.json").write_text(json.dumps(document), encoding="utf-8")
    (results / "torn.json").write_text("{", encoding="utf-8")
    (results / "transfer.json").write_text(json.dumps({"run": "t", "parameters": {}}), encoding="utf-8")
    current, skipped = bench.load_results(results)
    assert sorted(document["run"] for document in current) == [
        "broken",
        "clean-100",
        "clean-1000",
        "dirty-100",
        "older",
    ]
    assert skipped == 2
    summary = bench.summarize(tmp_path).read_text(encoding="utf-8")
    assert "Skipped 2 file(s)" in summary
    clean, dirty = summary.index("## Source 90a13ee00000 (src clean)"), summary.index("## Source 90a13ee00000+dirty")
    assert clean < summary.index("| clean-1000 |") < dirty < summary.index("| dirty-100 |")
    row = next(line for line in summary.splitlines() if line.startswith("| clean-1000 |") and "| cancel |" in line)
    assert row.endswith("| n/m | pass | pass |")  # flatness 1.0, once both sizes are in
    assert "| older | yes* |" in summary and "| clean-100 | yes |" in summary
    assert "max(0.250 s, 10% of the traced wall)" in summary
    assert "## Invalid runs" in summary and "- broken: exit status 1" in summary
    assert "- recover: job recover --all needs away copies" in summary


def test_a_parsed_namespace_round_trips_the_parameters_the_run_records(tmp_path: Path) -> None:
    arguments = bench.parser().parse_args(
        ["--out", str(tmp_path), "--jobs", "1000", "--selector", "state", "--io-sync", "fsync", "--backlog"]
    )
    assert isinstance(arguments, argparse.Namespace)
    assert (arguments.jobs, arguments.selector, arguments.io_sync, arguments.backlog) == (1000, "state", "fsync", True)


# -- the correction round: protected paths, the filesystem census, the backlog precheck, listings, the src guard ----


def test_an_out_inside_the_real_configuration_or_data_directories_is_refused(tmp_path: Path) -> None:
    home = tmp_path / "home"
    environment = {"XDG_CONFIG_HOME": str(home / ".config"), "XDG_DATA_HOME": str(home / ".local" / "share")}
    for inside in (home / ".config" / "httk", home / ".config" / "httk" / "bench", home / ".local" / "share" / "httk"):
        assert bench.protected_problems([inside], environment), inside
    assert bench.protected_problems([home / ".config"], environment)
    assert bench.protected_problems([tmp_path / "cache" / "bench"], environment) == []
    override = {**environment, "HTTK_CONFIG_HOME": str(tmp_path / "custom")}
    assert bench.protected_problems([tmp_path / "custom" / "out"], override)


def test_a_run_refuses_an_out_in_the_real_configuration_before_writing_anything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    out = config / "httk"
    with pytest.raises(SystemExit, match="refusing --out"):
        bench.run(bench.parser().parse_args(["--out", str(out)]))
    assert not config.exists()


def _workspace(root: Path) -> tuple[Any, list[str]]:
    """A real workspace with three ready jobs (no placement): it and their ids."""

    from test_moving import WORKFLOW
    from v3_helpers import job_mapping, submit_mapping, workspace

    ws = workspace(root)
    refs = [
        submit_mapping(ws, job_mapping(WORKFLOW, {"start": "succeed"}, tag=f"si-00{index}", placement=""))
        for index in range(1, 4)
    ]
    return ws, [ref.job_id for ref in refs]


def test_the_filesystem_census_finds_a_clean_workspace_clean(tmp_path: Path) -> None:
    ws, ids = _workspace(tmp_path / "ws")
    census, leftovers = bench.filesystem_census(ws.root)
    assert census["ready"] == set(ids) and leftovers == []
    assert bench.census_problems(census, "ready", set(ids), 0) == []
    assert set(census) >= {"ready", "cancelled", "paused"} and "owned" not in census


def test_the_filesystem_census_rejects_a_delete_that_left_its_jobs_in_the_trash(tmp_path: Path) -> None:
    ws, ids = _workspace(tmp_path / "ws")
    trash = ws.control / "tmp" / f"{'0' * 32}.trash.{'a' * 16}"
    trash.mkdir()
    for path in list((ws.jobs / "ready").iterdir()):
        path.rename(trash / path.name)
    census, leftovers = bench.filesystem_census(ws.root)
    assert bench.census_problems(census, None, set(ids), 0) == []  # every state is empty...
    assert leftovers == [f".httk-workspace/tmp/{trash.name}"]  # ...but the trash is still there
    assert bench.leftover_problems(leftovers) == [f"1 leftover(s): .httk-workspace/tmp/{trash.name}"]


def test_the_filesystem_census_rejects_owner_records_and_owned_jobs(tmp_path: Path) -> None:
    from httk.workflow import _kernel
    from v3_helpers import cli_owner

    ws, ids = _workspace(tmp_path / "ws")
    owner = cli_owner(ws)  # left open: its record and its claimed job stay
    owned = _kernel.claim(ws, owner, next(_kernel.list_jobs(ws, "ready")))
    assert owned is not None
    census, leftovers = bench.filesystem_census(ws.root)
    assert census["ready"] == set(ids) - {owned.job_id}
    assert f"jobs/owned/{owner.owner_id}" in leftovers
    assert f".httk-workspace/owners/{owner.owner_id}" in leftovers


@pytest.mark.parametrize(
    ("make", "expected"),
    [
        (lambda ws: (ws.jobs / "cancelled" / "si-001--truncated").mkdir(), "jobs/cancelled/si-001--truncated"),
        (lambda ws: (ws.jobs / "ready" / "stray.json").write_text("{}"), "jobs/ready/stray.json"),
        (lambda ws: (ws.jobs / "elsewhere").write_text("x"), "jobs/elsewhere: not a state directory"),
        (lambda ws: (ws.control / "requests" / "x.json.tmp").write_text("x"), ".httk-workspace/requests/x.json.tmp"),
        (lambda ws: (ws.control / "quarantine" / "bad").mkdir(), ".httk-workspace/quarantine/bad"),
        (lambda ws: (ws.control / "exchange-jobs" / "entry").mkdir(), ".httk-workspace/exchange-jobs/entry"),
    ],
    ids=["malformed job name", "file among jobs", "unknown state", "request temporary", "quarantine", "exchange"],
)
def test_the_filesystem_census_rejects_any_other_leftover(tmp_path: Path, make: Any, expected: str) -> None:
    ws, _ids = _workspace(tmp_path / "ws")
    make(ws)
    _census, leftovers = bench.filesystem_census(ws.root)
    assert len(leftovers) == 1 and leftovers[0].startswith(expected)


def test_the_filesystem_census_rejects_a_job_in_two_states(tmp_path: Path) -> None:
    import shutil

    ws, _ids = _workspace(tmp_path / "ws")
    job = next((ws.jobs / "ready").iterdir())
    shutil.copytree(job, ws.jobs / "cancelled" / job.name)
    _census, leftovers = bench.filesystem_census(ws.root)
    assert len(leftovers) == 1 and "is also in" in leftovers[0]


def test_the_filesystem_census_allows_pending_requests_only_when_asked(tmp_path: Path) -> None:
    ws, _ids = _workspace(tmp_path / "ws")
    (ws.control / "requests" / "pending.json").write_text("{}", encoding="utf-8")
    assert bench.filesystem_census(ws.root)[1] == [".httk-workspace/requests/pending.json"]
    assert bench.filesystem_census(ws.root, requests=True)[1] == []


JOBS = [f"{index:08x}-0000-4000-8000-000000000000" for index in range(3)]
OPERATOR = "Bench Operator <bench@example.invalid>"


def _pending(job: str, *, request: str | None = None, **changes: object) -> tuple[str, tuple[dict[str, Any], str]]:
    request = request or job.replace("0000-4000", "1111-4000")
    document = {
        "job_id": job,
        "request_id": request,
        "action": "set_priority",
        "priority": 321,
        "operator": OPERATOR,
        **changes,
    }
    return f"{job}.{request}.json", (document, "ed25519:key")


def test_the_backlog_precheck_accepts_exactly_one_signed_set_priority_per_job() -> None:
    entries = dict(_pending(job) for job in JOBS)
    assert bench.backlog_problems(entries, set(JOBS), OPERATOR, "ed25519:key") == []


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda e: e.pop(next(iter(e))), "1 job(s) without a pending backlog request"),
        (lambda e: e.update([_pending(JOBS[0], request="f" * 8 + "-0000-4000-8000-000000000000")]), "more than one"),
        (lambda e: e.update([_pending("a" * 8 + "-0000-4000-8000-000000000000")]), "jobs not in the run"),
        (lambda e: e.update([_pending(JOBS[1], action="cancel")]), "unexpected action"),
        (lambda e: e.update([_pending(JOBS[1], priority=500)]), "unexpected priority"),
        (lambda e: e.update([_pending(JOBS[1], operator="Someone <x@y>")]), "unexpected operator"),
        (lambda e: e.update([_pending(JOBS[1], job_id=JOBS[2])]), "unexpected job_id"),
        (lambda e: e.update({_pending(JOBS[1])[0]: (_pending(JOBS[1])[1][0], None)}), "signed by None"),
        (lambda e: e.update({_pending(JOBS[1])[0]: (None, None)}), "unreadable"),
        (lambda e: e.update({"x.json.tmp": (None, None)}), "not a request file"),
    ],
    ids=["missing", "two", "foreign", "action", "priority", "operator", "job", "unsigned", "unreadable", "temporary"],
)
def test_the_backlog_precheck_refuses_anything_else(mutate: Any, expected: str) -> None:
    entries = dict(_pending(job) for job in JOBS)
    mutate(entries)
    problems = bench.backlog_problems(entries, set(JOBS), OPERATOR, "ed25519:key")
    assert any(expected in problem for problem in problems), problems


LISTING_PROBE = """
import glob, importlib.util, json, os, pathlib, sys
spec = importlib.util.spec_from_file_location("local_scale", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
probe = module._Probe()
probe.install_listings()
directory = pathlib.Path(sys.argv[2])
counts = {}
for name, call in (
    ("path_glob", lambda: list(directory.glob("*.txt"))),
    ("path_rglob", lambda: list(directory.rglob("*.txt"))),
    ("glob", lambda: glob.glob(str(directory / "*.txt"))),
    ("listdir", lambda: os.listdir(directory)),
):
    before = probe.document()["entries_examined"]
    found = call()
    counts[name] = [len(found), probe.document()["entries_examined"] - before]
print(json.dumps({"version": list(sys.version_info[:2]), "counts": counts, "patched": probe.patched}))
"""


def _interpreters() -> list[Any]:
    root = Path(__file__).resolve().parents[4]
    found = [pytest.param(sys.executable, id="current")]
    for name in ("py312", "py313", "py314"):
        python = root / ".venvs" / name / "bin" / "python"
        mark = pytest.mark.skipif(not python.exists(), reason=f"no {python}")
        found.append(pytest.param(str(python), id=name, marks=mark))
    return found


@pytest.mark.parametrize("python", _interpreters())
def test_installed_listings_count_every_entry_path_glob_examines(python: str, tmp_path: Path) -> None:
    # Python 3.13's glob._StringGlobber captured os.scandir at import; Path.glob/rglob list through it.
    directory = tmp_path / "listed"
    directory.mkdir()
    for name in ("a.txt", "b.txt", "c.dat", "d.dat", "e.dat"):
        (directory / name).write_text("x", encoding="utf-8")
    script = tmp_path / "probe.py"
    script.write_text(LISTING_PROBE, encoding="utf-8")
    benchmark = Path(__file__).resolve().parents[1] / "benchmarks" / "local_scale.py"
    completed = subprocess.run(
        [python, "-I", str(script), str(benchmark), str(directory)], capture_output=True, text=True, check=True
    )
    report = json.loads(completed.stdout)
    counts = report["counts"]
    assert counts["path_glob"][0] == 2 and counts["path_glob"][1] >= 5, report
    assert counts["path_rglob"][0] == 2 and counts["path_rglob"][1] >= 5, report
    assert counts["glob"][0] == 2 and counts["glob"][1] >= 5, report
    assert counts["listdir"] == [5, 5], report
    if report["version"] == [3, 13]:
        assert "_StringGlobber.scandir" in report["patched"], report


def test_a_measured_command_during_which_src_changed_is_invalid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    arguments = bench.parser().parse_args(["--out", str(tmp_path), "--jobs", "2"])
    run = bench._Run(arguments, tmp_path, "r")
    run.source_key = "90a13ee00000"
    ids = {"si-001": JOBS[0], "si-002": JOBS[1]}
    root = tmp_path / "chain" / "a" / "workspace"
    (run.logs / "chain").mkdir(parents=True)
    module = str(bench.SOURCE / "httk" / "workflow" / "__init__.py")
    applied = "".join(f"{job}\t{'1' * 36}\tapplied\tpause applied; state=paused\n" for job in ids.values())

    def httk(self: Any, argv: list[str], cwd: Path, log: Path, *, mode: str = "plain") -> dict[str, Any]:
        del self, argv, cwd, log, mode
        return {
            "exit_status": 0,
            "wall_seconds": 1.0,
            "stdout": applied,
            "stderr": "",
            "probe": {"workflow_module": module},
        }

    states: dict[str, set[str]] = {"ready": set(), "paused": set(ids.values())}
    monkeypatch.setattr(bench._Run, "httk", httk)
    monkeypatch.setattr(bench, "_census", lambda root: ({**states, "owned": set()}, {}, 0))
    monkeypatch.setattr(bench, "filesystem_census", lambda root, requests=False: (states, []))
    keys = iter(["90a13ee00000", "90a13ee00000"])
    monkeypatch.setattr(bench, "_source_state", lambda: {"key": next(keys)})
    assert bench._Run.step(run, tmp_path / "chain", root, "pause", "plain", ids, measured=True)["problems"] == []
    keys = iter(["90a13ee00000", "90a13ee00000+dirty.abcdef012345"])
    problems = bench._Run.step(run, tmp_path / "chain", root, "pause", "plain", ids, measured=True)["problems"]
    assert len(problems) == 1 and problems[0].startswith("src changed: 90a13ee00000 before and 90a13ee00000+dirty")
    keys = iter(["90a13ee00000+dirty.abcdef012345", "90a13ee00000+dirty.abcdef012345"])
    problems = bench._Run.step(run, tmp_path / "chain", root, "pause", "plain", ids, measured=True)["problems"]
    assert len(problems) == 1 and "not the run's 90a13ee00000" in problems[0]
