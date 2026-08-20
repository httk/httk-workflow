"""Small standard-library helpers for executable workflow hooks."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

COLLECT_STREAM_FORMAT = "httk-workflow-collect-stream"
COLLECT_STREAM_VERSION = 2
INSTANTIATE_FORMAT = "httk-workflow-instantiate"
INSTANTIATE_VERSION = 2


@dataclass(frozen=True)
class InstantiateRequest:
    """The v1 request passed to an executable instantiate hook.

    :param workflow: The declared workflow id.
    :param tag: The caller's tag, or ``None``.
    :param parameters: The parameters already supplied for the job.
    :param inputs: Hook-consumed input descriptors. A descriptor is either
        ``{"kind": "value", "value": ...}`` or
        ``{"kind": "file", "path": "payload-relative/posix/path"}``.

    The process current working directory is the staging payload. File
    descriptors therefore name files relative to that directory.
    """

    workflow: str
    tag: str | None
    parameters: Mapping[str, Any]
    inputs: Mapping[str, Mapping[str, Any]]


def instantiate_main(fn: Callable[[InstantiateRequest], Mapping[str, Any]]) -> None:
    """Run *fn* as a v1 executable instantiate hook.

    The hook receives one JSON document on standard input:
    ``{"format": "httk-workflow-instantiate", "format_version": 2,
    "workflow": <workflow-id>, "tag": <string-or-null>, "parameters":
    {...}, "inputs": {<name>: <descriptor>}}``. The current working
    directory is the staging payload; file descriptors contain payload-relative
    POSIX paths. The returned mapping must contain ``parameters`` and may
    contain ``tag``; it is emitted as one JSON document on standard output.
    Exceptions, malformed requests, and invalid responses are reported on
    standard error and exit with status 1.

    :param fn: Convert the request into parameter mutations and an optional tag.
    :return: Nothing; the response is written as one JSON document.
    """

    try:
        request = json.loads(sys.stdin.read())
        if not isinstance(request, Mapping):
            raise ValueError("instantiate request must be a JSON object")
        if request.get("format") != INSTANTIATE_FORMAT or request.get("format_version") != INSTANTIATE_VERSION:
            raise ValueError("invalid instantiate request envelope")
        workflow = request.get("workflow")
        tag = request.get("tag")
        parameters = request.get("parameters")
        inputs = request.get("inputs")
        if not isinstance(workflow, str) or (tag is not None and not isinstance(tag, str)):
            raise ValueError("instantiate request has invalid workflow or tag")
        if not isinstance(parameters, Mapping) or not isinstance(inputs, Mapping):
            raise ValueError("instantiate request has invalid parameters or inputs mapping")
        typed_inputs: dict[str, Mapping[str, Any]] = {}
        for name, descriptor in inputs.items():
            if not isinstance(name, str) or not isinstance(descriptor, Mapping):
                raise ValueError("instantiate input descriptors must be named mappings")
            typed_inputs[name] = descriptor
        response = fn(InstantiateRequest(workflow, tag, parameters, typed_inputs))
        if not isinstance(response, Mapping):
            raise TypeError("instantiate hook must return a mapping")
        response_parameters = response.get("parameters")
        response_tag = response.get("tag")
        if not isinstance(response_parameters, Mapping) or (
            response_tag is not None and not isinstance(response_tag, str)
        ):
            raise ValueError("instantiate response has invalid parameters or tag")
        output: dict[str, Any] = {"parameters": dict(response_parameters)}
        if response_tag is not None:
            output["tag"] = response_tag
        print(json.dumps(output, separators=(",", ":"), allow_nan=False), flush=True)
    except Exception as exc:
        raise SystemExit(str(exc) or type(exc).__name__) from exc


def collect_main(fn: Callable[[Mapping[str, Any]], Mapping[str, Any]]) -> None:
    """Run *fn* as a streaming executable collector.

    :param fn: Convert one serialized job record into role-to-value wrappers.
    :return: Nothing; responses are written as JSONL to standard output.
    """

    first = sys.stdin.readline()
    try:
        handshake = json.loads(first)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid collect stream handshake: {exc}") from exc
    if handshake != {"format": COLLECT_STREAM_FORMAT, "format_version": COLLECT_STREAM_VERSION}:
        raise SystemExit("invalid collect stream handshake")
    for line in sys.stdin:
        job_id = ""
        try:
            request = json.loads(line)
            record = request["record"]
            job_id = record["job_id"]
            outputs = fn(record)
            if not isinstance(outputs, Mapping):
                raise TypeError("collector must return a mapping of output roles")
            response: dict[str, Any] = {"job_id": job_id, "outputs": dict(outputs)}
            print(json.dumps(response, separators=(",", ":"), allow_nan=False), flush=True)
        except Exception as exc:
            response = {"job_id": job_id, "error": str(exc) or type(exc).__name__}
            print(json.dumps(response, separators=(",", ":"), allow_nan=False), flush=True)
