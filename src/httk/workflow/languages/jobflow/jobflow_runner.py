#!/usr/bin/env python3
"""Run a jobflow Flow as one persistent httk scheduler and many child jobs."""

import importlib
import importlib.util
import json
import os
import shutil
import sys
import traceback
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import chdir
from pathlib import Path, PurePosixPath
from typing import Any, cast

try:
    from httk.workflow import Attempt, ChildSpec, Runner, RunnerRef
except ModuleNotFoundError:  # pragma: no cover - interpreter bootstrap
    # The manager launches this file directly, so the interpreter is whatever the
    # shebang found on PATH, which on a cluster is not necessarily the one httk is
    # installed in. HTTK_WORKFLOW_PYTHON is the interpreter the manager itself runs,
    # so re-exec under it once and let a second failure be reported honestly.
    _python = os.environ.get("HTTK_WORKFLOW_PYTHON")
    if _python is None or os.environ.get("HTTK_WORKFLOW_RUNNER_BOOTSTRAP") == "1":
        raise
    os.environ["HTTK_WORKFLOW_RUNNER_BOOTSTRAP"] = "1"
    os.execv(_python, [_python, os.path.abspath(__file__), *sys.argv[1:]])

from httk.workflow.languages.jobflow import OUTPUTS_FILE

WORKFLOW = "jobflow.workflow"
STATE_FILE = "state.json"
STORE_FILE = "store.json"
SPOOL_DIRECTORY = "spool"
CHILD_OUTPUTS_FILE = "child-outputs.json"

run = Runner(WORKFLOW)


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
            if not isinstance(raw, Mapping) or raw.get("kind") not in {"value", "path"}:
                raise ValueError("invalid kind")
            if raw["kind"] == "value":
                result[str(label)] = decoder.process_decoded(raw.get("value"))
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

    store = JobStore(MemoryStore(), additional_stores=defaultdict(MemoryStore))
    store.connect()
    if snapshot is not None:
        from httk.workflow.languages.jobflow._driver import merge_documents

        documents = json.loads(snapshot.read_text(encoding="utf-8"))
        if not isinstance(documents, list):
            raise ValueError("jobflow store snapshot is not an array")
        merge_documents(store, [item for item in documents if isinstance(item, Mapping)])
    return store


def _persist_store(store: Any, snapshot: Path) -> None:
    """Persist a complete in-memory store through an atomic snapshot replace."""
    from httk.workflow.languages.jobflow._driver import snapshot_documents

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


def _failure_message(child: Any) -> str:
    """Return the most useful message available from one failed child."""
    failure = getattr(child, "failure", None)
    message = getattr(failure, "message", None)
    return str(message or getattr(child, "kind", "child failed"))


@run.step
def start(a: Attempt) -> None:
    """Build the root flow and initialize its persistent scheduler state."""
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
    try:
        from httk.workflow.languages.jobflow._driver import DriverState

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
    from httk.workflow.languages.jobflow._driver import (
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
    for job in state.ready():
        key = f"{job.uuid}:{job.index}"
        state.mark_running(key)
        spool_path = spool_root / f"{key}.json"
        snapshot_path = spool_root / f"{key}-store.json"
        _write_json(spool_path, spool_job(job))
        _write_json(snapshot_path, snapshot_documents(store))
        label = f"j{ordinal:05d}"
        a.spawn(
            ChildSpec(
                step="enter",
                parameters={
                    "jobflow_child_job": str(spool_path),
                    "jobflow_child_snapshot": str(snapshot_path),
                },
                runner=RunnerRef.inherit(),
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
                    outputs["stored_data"] = state.stored_data
                output_path = a.workdir / OUTPUTS_FILE
                _write_json(output_path, outputs)
                if a.context.data_generation is not None:
                    prefix = a.parameter("jobflow_data_prefix", "jobflow")
                    a.put(output_path, f"{prefix}/{OUTPUTS_FILE}")
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
        from httk.workflow.languages.jobflow._driver import load_spooled_job, merge_documents

        job_path = Path(str(a.parameter("jobflow_child_job")))
        snapshot_path = Path(str(a.parameter("jobflow_child_snapshot")))
        try:
            job = load_spooled_job(json.loads(job_path.read_text(encoding="utf-8")))
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            a.fail("jobflow.job_unloadable", str(exc))
            return
        store = _open_store()
        try:
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            if not isinstance(snapshot, list):
                raise ValueError("jobflow store snapshot is not an array")
            merge_documents(store, [item for item in snapshot if isinstance(item, Mapping)])
        except (OSError, UnicodeError, ValueError) as exc:
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
        with chdir(exec_dir):
            response = job.run(store=store)
        response.output = None
        from jobflow.core.flow import get_flow

        for name in ("replace", "detour", "addition"):
            value = getattr(response, name)
            if value is not None:
                setattr(response, name, get_flow(value, allow_external_references=True))
        from monty.json import jsanitize

        shipped = {
            "response": jsanitize(vars(response), strict=True, enum_values=True, allow_bson=True),
            "docs": jsanitize(
                _new_documents(store, baseline, baseline_blobs), strict=True, enum_values=True, allow_bson=True
            ),
        }
        _write_json(a.workdir / CHILD_OUTPUTS_FILE, shipped)
        a.succeed()
    except Exception as exc:
        a.fail(
            "jobflow.job_failed",
            f"{type(exc).__name__}: {exc}",
            details={"traceback": "\n".join(traceback.format_exc().splitlines()[-20:])},
        )


if __name__ == "__main__":
    raise SystemExit(run.main())
