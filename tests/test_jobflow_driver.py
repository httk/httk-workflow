"""Differential tests for the resumable jobflow scheduler."""

import json
from collections import defaultdict
from contextlib import chdir
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest

jobflow = pytest.importorskip("jobflow")

from jobflow.core.flow import Flow, JobOrder
from jobflow.core.job import JobConfig, Response, job
from jobflow.core.reference import OnMissing
from jobflow.core.store import JobStore
from jobflow.managers.local import run_locally
from maggma.stores import MemoryStore
from monty.json import MontyDecoder, jsanitize

from httk.workflow.languages.jobflow._driver import (
    DriverState,
    load_spooled_job,
    merge_documents,
    resolve_final_output,
    snapshot_documents,
    spool_job,
)


@job
def number(value: int | None) -> int | None:
    """Return one value."""
    return value


@job
def plus(left: int | None, right: int | None) -> int:
    """Add two values, treating a missing reference as zero."""
    return (left or 0) + (right or 0)


@job
def replace_response(value: int) -> Response:
    """Replace this job with a one-job flow."""
    replacement = number(value + 1)
    return Response(replace=Flow([replacement], output=replacement.output))


@job
def replace_automatic(value: int):
    """Return a job, which jobflow converts into a replacement response."""
    return number(value + 1)


@job
def detour_response(value: int) -> Response:
    """Run an independent detour."""
    return Response(output=value, detour=number(value + 1))


@job
def addition_response(value: int) -> Response:
    """Run an independent addition."""
    return Response(output=value, addition=number(value + 1))


@job
def replace_twice(value: int):
    """Replace twice before producing a final value."""
    return replace_twice(value + 1) if value < 3 else value


@job
def nested_detour(value: int) -> Response:
    """Create a detour whose first job creates another detour."""
    detour = nested_detour(2) if value == 1 else number(value + 1)
    return Response(output=value, detour=detour)


@job
def replacement_with_addition(value: int):
    """Replace once, then create addition work from inside that replacement."""
    return replacement_with_addition(2) if value == 1 else Response(output=value, addition=number(value + 1))


@job
def failing_detour(value: int) -> Response:
    """Return output while an independent detour fails."""
    return Response(output=value, detour=fail())


@job
def failing_replace(value: int) -> Response:
    """Return output while a replacement flow fails."""
    replacement = fail()
    return Response(output=value, replace=Flow([replacement], output=replacement.output))


@job
def stop_flow() -> Response:
    """Stop the remaining flow."""
    return Response(stop_jobflow=True)


@job
def stop_descendants() -> Response:
    """Stop this job's children."""
    return Response(output=1, stop_children=True)


@job
def fail() -> None:
    """Fail deliberately."""
    raise RuntimeError("expected test failure")


def _store() -> JobStore:
    return JobStore(MemoryStore(), additional_stores=defaultdict(MemoryStore))


def _clone(flow: Flow) -> Flow:
    return MontyDecoder().process_decoded(deepcopy(jsanitize(flow, strict=True, enum_values=True, allow_bson=True)))


def _run_driver(flow: Flow, tmp_path: Path) -> tuple[DriverState, JobStore]:
    state = DriverState.from_flow(flow)
    store = _store()
    store.connect()
    tmp_path.mkdir(exist_ok=True)
    with chdir(tmp_path):
        while not state.is_complete:
            ready = state.ready()
            assert ready, state.failure_summary()
            for job_to_run in ready:
                state.mark_running(f"{job_to_run.uuid}:{job_to_run.index}")
                try:
                    response = job_to_run.run(store)
                except Exception as error:
                    state.apply_error(f"{job_to_run.uuid}:{job_to_run.index}", str(error))
                else:
                    state.apply_success(f"{job_to_run.uuid}:{job_to_run.index}", response)
            state = DriverState.from_mapping(json.loads(json.dumps(state.to_mapping())))
    return state, store


def _documents(store: JobStore) -> list[tuple[str, int, str]]:
    def stable(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: stable(item)
                for key, item in value.items()
                if not (value.get("@class") == "OutputReference" and key == "uuid")
            }
        if isinstance(value, list):
            return [stable(item) for item in value]
        return value

    return sorted(
        (document["name"], document["index"], json.dumps(stable(document["output"]), sort_keys=True))
        for document in store.query()
    )


def _linear() -> tuple[Flow, bool]:
    first = number(1)
    second = number(first.output)
    return Flow([first, second], output=second.output), True


def _diamond() -> tuple[Flow, bool]:
    root = number(1)
    left = number(root.output)
    right = number(root.output)
    joined = plus(left.output, right.output)
    return Flow([root, left, right, joined], output=joined.output), True


def _linear_order() -> tuple[Flow, bool]:
    return Flow([number(1), number(2)], order=JobOrder.LINEAR), True


def _nested() -> tuple[Flow, bool]:
    first = number(1)
    second = number(first.output)
    inner = Flow([first, second], output=second.output)
    third = number(inner.output)
    return Flow([inner, third], output=third.output), True


def _replace() -> tuple[Flow, bool]:
    root = replace_response(1)
    return Flow([root], output=root.output), True


def _automatic_replace() -> tuple[Flow, bool]:
    root = replace_automatic(1)
    return Flow([root], output=root.output), True


def _detour() -> tuple[Flow, bool]:
    root = detour_response(1)
    return Flow([root], output=root.output), True


def _addition() -> tuple[Flow, bool]:
    root = addition_response(1)
    return Flow([root], output=root.output), True


def _failing_detour() -> tuple[Flow, bool]:
    root = failing_detour(5)
    child = number(root.output)
    return Flow([root, child], output=child.output), False


def _failing_replace() -> tuple[Flow, bool]:
    root = failing_replace(5)
    child = number(root.output)
    return Flow([root, child], output=child.output), False


def _stop_jobflow() -> tuple[Flow, bool]:
    return Flow([stop_flow(), number(2)], order=JobOrder.LINEAR), False


def _stop_children() -> tuple[Flow, bool]:
    root = stop_descendants()
    child = number(root.output)
    return Flow([root, child]), False


def _error_default() -> tuple[Flow, bool]:
    root = fail()
    child = number(root.output)
    return Flow([root, child]), False


def _error_none() -> tuple[Flow, bool]:
    root = fail()
    child = number(root.output)
    config = cast(JobConfig, child.config)
    config.on_missing_references = OnMissing.NONE
    return Flow([root, child]), False


@pytest.mark.parametrize(
    ("factory", "success"),
    [
        (_linear, True),
        (_diamond, True),
        (_linear_order, True),
        (_nested, True),
        (_replace, True),
        (_automatic_replace, True),
        (_detour, True),
        (_addition, True),
        (_stop_jobflow, False),
        (_stop_children, False),
        (_error_default, False),
        (_error_none, False),
    ],
)
def test_driver_matches_run_locally(factory: Any, success: bool, tmp_path: Path) -> None:
    """The resumable loop writes the same named outputs as jobflow's local manager."""
    flow, expected_success = factory()
    assert expected_success is success
    driver_state, driver_store = _run_driver(_clone(flow), tmp_path / "driver")
    local_store = _store()
    (tmp_path / "local").mkdir()
    with chdir(tmp_path / "local"):
        run_locally(_clone(flow), store=local_store, log=False)
    assert _documents(driver_store) == _documents(local_store)
    assert driver_state.succeeded is success


@pytest.mark.parametrize(("factory", "child_runs"), [(_failing_detour, True), (_failing_replace, False)])
def test_failed_diversion_matches_local_child_outcome(factory: Any, child_runs: bool, tmp_path: Path) -> None:
    """Failed diversions preserve jobflow's distinct detour and replacement child behavior."""
    flow, _ = factory()
    driver_state, driver_store = _run_driver(_clone(flow), tmp_path / "driver")
    local_store = _store()
    (tmp_path / "local").mkdir()
    with chdir(tmp_path / "local"):
        run_locally(_clone(flow), store=local_store, log=False)
    assert _documents(driver_store) == _documents(local_store)
    assert (("number", 1, "5") in _documents(driver_store)) is child_runs
    assert not driver_state.succeeded


def test_replace_blocks_join_until_replacement_finishes(tmp_path: Path) -> None:
    """A replacement branch keeps a join out of the ready queue."""
    root = number(1)
    replacing = replace_response(root.output)
    other = number(root.output)
    joined = plus(replacing.output, other.output)
    state = DriverState.from_flow(Flow([root, replacing, other, joined]))
    store = _store()
    store.connect()
    with chdir(tmp_path):
        root_job = state.ready()[0]
        state.mark_running(root_job.uuid)
        state.apply_success(root_job.uuid, root_job.run(store))
        first_wave = state.ready()
        assert {item.uuid for item in first_wave} == {replacing.uuid, other.uuid}
        replace_job = next(item for item in first_wave if item.uuid == replacing.uuid)
        other_job = next(item for item in first_wave if item.uuid == other.uuid)
        state.mark_running(replace_job.uuid)
        state.apply_success(replace_job.uuid, replace_job.run(store))
        state.mark_running(other_job.uuid)
        state.apply_success(other_job.uuid, other_job.run(store))
        assert joined.uuid not in {item.uuid for item in state.ready()}
        while state.ready() and joined.uuid not in {item.uuid for item in state.ready()}:
            replacement_job = state.ready()[0]
            state.mark_running(f"{replacement_job.uuid}:{replacement_job.index}")
            state.apply_success(f"{replacement_job.uuid}:{replacement_job.index}", replacement_job.run(store))
        assert joined.uuid in {item.uuid for item in state.ready()}


def test_diamond_exposes_parallel_width(tmp_path: Path) -> None:
    """Both fan-out branches are ready once their common parent completes."""
    flow, _ = _diamond()
    state = DriverState.from_flow(flow)
    store = _store()
    store.connect()
    with chdir(tmp_path):
        root = state.ready()[0]
        state.mark_running(root.uuid)
        state.apply_success(root.uuid, root.run(store))
    assert len(state.ready()) == 2


def test_dynamic_external_reference_waits_for_replacement(tmp_path: Path) -> None:
    """A dynamic job waits for the latest unsettled attempt of its referenced UUID."""
    replacing = replace_automatic(1)
    trigger = number(0)
    state = DriverState.from_flow(Flow([replacing, trigger]))
    store = _store()
    store.connect()
    with chdir(tmp_path):
        replacing_job = next(item for item in state.ready() if item.uuid == replacing.uuid)
        state.mark_running(replacing_job.uuid)
        state.apply_success(replacing_job.uuid, replacing_job.run(store))
        state.apply_success(trigger.uuid, Response(addition=number(replacing.output)))
        dynamic_key = next(
            key for key, (uuid, _) in state.identities.items() if uuid not in {replacing.uuid, trigger.uuid}
        )
        assert dynamic_key not in {f"{item.uuid}:{item.index}" for item in state.ready()}
        replacement = state.ready()[0]
        state.mark_running(f"{replacement.uuid}:{replacement.index}")
        state.apply_success(f"{replacement.uuid}:{replacement.index}", replacement.run(store))
        dynamic = next(item for item in state.ready() if f"{item.uuid}:{item.index}" == dynamic_key)
        state.mark_running(dynamic.uuid)
        state.apply_success(dynamic.uuid, dynamic.run(store))
    document = store.query_one({"uuid": dynamic.uuid, "index": dynamic.index})
    assert document is not None and document["output"] == 2


def test_dynamic_external_reference_to_completed_job_is_ready(tmp_path: Path) -> None:
    """An external reference with a stored, settled source does not add a dependency."""
    completed = number(3)
    trigger = number(0)
    state = DriverState.from_flow(Flow([completed, trigger]))
    store = _store()
    store.connect()
    with chdir(tmp_path):
        completed_job = next(item for item in state.ready() if item.uuid == completed.uuid)
        state.mark_running(completed_job.uuid)
        state.apply_success(completed_job.uuid, completed_job.run(store))
        state.apply_success(trigger.uuid, Response(addition=number(completed.output)))
    assert any(item.uuid not in {completed.uuid, trigger.uuid} for item in state.ready())


def test_recursive_replacement_extends_downstream_barrier(tmp_path: Path) -> None:
    """A downstream consumer waits for every replacement attempt and gets the final value."""
    root = replace_twice(1)
    downstream = number(root.output)
    flow = Flow([root, downstream], output=downstream.output)
    state = DriverState.from_flow(flow)
    store = _store()
    store.connect()
    (tmp_path / "driver").mkdir()
    with chdir(tmp_path / "driver"):
        first = state.ready()[0]
        state.mark_running(first.uuid)
        state.apply_success(first.uuid, first.run(store))
        second = state.ready()[0]
        state.mark_running(f"{second.uuid}:{second.index}")
        state.apply_success(f"{second.uuid}:{second.index}", second.run(store))
        assert downstream.uuid not in {item.uuid for item in state.ready()}
        state = DriverState.from_mapping(json.loads(json.dumps(state.to_mapping())))
        third = state.ready()[0]
        state.mark_running(f"{third.uuid}:{third.index}")
        state.apply_success(f"{third.uuid}:{third.index}", third.run(store))
        final = next(item for item in state.ready() if item.uuid == downstream.uuid)
        state.mark_running(final.uuid)
        state.apply_success(final.uuid, final.run(store))
    document = store.query_one({"uuid": downstream.uuid, "index": downstream.index})
    assert document is not None and document["output"] == 3
    local_store = _store()
    (tmp_path / "local").mkdir()
    with chdir(tmp_path / "local"):
        run_locally(_clone(flow), store=local_store, log=False)
    assert _documents(store) == _documents(local_store)


def test_nested_detour_extends_downstream_barrier(tmp_path: Path) -> None:
    """A downstream consumer waits for detours created by an existing detour barrier."""
    root = nested_detour(1)
    downstream = number(root.output)
    flow = Flow([root, downstream])
    state = DriverState.from_flow(flow)
    store = _store()
    store.connect()
    (tmp_path / "driver").mkdir()
    with chdir(tmp_path / "driver"):
        root_job = state.ready()[0]
        state.mark_running(root_job.uuid)
        state.apply_success(root_job.uuid, root_job.run(store))
        first_detour = state.ready()[0]
        state.mark_running(first_detour.uuid)
        state.apply_success(first_detour.uuid, first_detour.run(store))
        assert downstream.uuid not in {item.uuid for item in state.ready()}
        second_detour = state.ready()[0]
        state.mark_running(second_detour.uuid)
        state.apply_success(second_detour.uuid, second_detour.run(store))
        assert downstream.uuid in {item.uuid for item in state.ready()}
        final = next(item for item in state.ready() if item.uuid == downstream.uuid)
        state.mark_running(final.uuid)
        state.apply_success(final.uuid, final.run(store))
    local_store = _store()
    (tmp_path / "local").mkdir()
    with chdir(tmp_path / "local"):
        run_locally(_clone(flow), store=local_store, log=False)
    assert _documents(store) == _documents(local_store)


def test_addition_inside_replacement_extends_downstream_barrier(tmp_path: Path) -> None:
    """An addition created inside a replacement completes before the replacement's child runs."""
    root = replacement_with_addition(1)
    downstream = number(root.output)
    state = DriverState.from_flow(Flow([root, downstream]))
    store = _store()
    store.connect()
    with chdir(tmp_path):
        first = state.ready()[0]
        state.mark_running(first.uuid)
        state.apply_success(first.uuid, first.run(store))
        replacement = state.ready()[0]
        state.mark_running(f"{replacement.uuid}:{replacement.index}")
        state.apply_success(f"{replacement.uuid}:{replacement.index}", replacement.run(store))
        assert downstream.uuid not in {item.uuid for item in state.ready()}
        addition = state.ready()[0]
        state.mark_running(addition.uuid)
        state.apply_success(addition.uuid, addition.run(store))
    assert downstream.uuid in {item.uuid for item in state.ready()}


def test_resolve_final_output(tmp_path: Path) -> None:
    """The root flow output is resolved against the worker store."""
    flow, _ = _linear()
    state, store = _run_driver(flow, tmp_path)
    assert resolve_final_output(state, store) == 1


def test_spooled_unknown_function_is_rejected() -> None:
    """Workers receive a useful error when their job function is not importable."""
    mapping = spool_job(number(1))
    mapping["function"] = {"not": "a callable"}
    with pytest.raises(RuntimeError, match="function could not be deserialized"):
        load_spooled_job(mapping)


def test_merge_documents_is_idempotent() -> None:
    """Raw worker documents use the same UUID/index upsert key as JobStore."""
    store = _store()
    store.connect()
    document = {"uuid": "worker", "index": 1, "output": 7, "name": "number"}
    merge_documents(store, [document])
    merge_documents(store, [document])
    assert [{key: value for key, value in item.items() if key != "_id"} for item in store.query()] == [document]


def test_additional_store_documents_survive_a_snapshot_round_trip() -> None:
    """Blob documents are carried by the tagged JSON-safe snapshot envelope."""
    store = _store()
    store.connect()
    merge_documents(store, [{"store": "blob", "blob_uuid": "b", "data": "x"}])
    restored = _store()
    restored.connect()
    merge_documents(restored, snapshot_documents(store))
    restored_store: Any = restored
    blob_store: Any = restored_store.additional_stores["blob"]
    assert [{key: value for key, value in item.items() if key != "_id"} for item in blob_store.query()] == [
        {"blob_uuid": "b", "data": "x"}
    ]
