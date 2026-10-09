"""Unit tests of :mod:`httk.workflow._job`: the strict ``job.json`` version 3 and its digest."""

import hashlib
import json
import os
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow._job import MAX_JOB_BYTES, JobDefinition
from httk.workflow.errors import FormatError

JOB_ID = "0b6f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e01"
PARENT_ID = "1c7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e02"


def _mapping(**changes: object) -> dict[str, object]:
    mapping: dict[str, object] = {
        "format": "httk-workflow-job",
        "format_version": 3,
        "id": JOB_ID,
        "tag": "silicon-relax",
        "name": "Silicon relaxation",
        "placement": "project-17/0/03a",
        "workflow": {"id": "git+https://example.org/w.git@abc#vasp-relax", "name": "vasp.relax"},
        "initial_step": "prepare",
        "priority": 500,
        "claim": {"pool": "default", "required_capabilities": ["gpu", "avx"]},
        "retry_policy": {"maximum_attempts_per_activation": 3, "retry_on": ["owner_lost"]},
        "resources": {"cores": 4, "maxtime": 3600},
        "step_resources": {"relax": {"cores": 8}},
        "parameters": {"encut": 520, "kpoints": [4, 4, 4]},
        "declarations": {},
        "declared": {},
        "environment": {},
        "parent": {
            "workspace_id": "2d7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e03",
            "job_id": PARENT_ID,
            "job_key": f"parent--{PARENT_ID}",
            "placement": "project-17",
            "activation_id": "3e7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e04",
            "spawn_id": "4f7f3f6e-6f7a-4a4b-9a43-4c1f0d6c8e05",
        },
        "seal_succeeded": True,
    }
    mapping.update(changes)
    return mapping


def test_round_trip_and_canonical_encoding() -> None:
    job = JobDefinition.from_mapping(_mapping())
    assert job.job_key == f"silicon-relax--{JOB_ID}"
    assert job.placement == PurePosixPath("project-17/0/03a")
    assert (job.workflow_id, job.workflow_name) == ("git+https://example.org/w.git@abc#vasp-relax", "vasp.relax")
    assert job.required_capabilities == frozenset({"gpu", "avx"})
    assert job.retry_policy.maximum_attempts_per_activation == 3
    assert JobDefinition.from_mapping(job.as_mapping()) == job
    data = job.encode()
    # As legacy job.json: compact, sorted keys, trailing newline.
    assert data.endswith(b"}\n") and b": " not in data
    assert json.loads(data) == job.as_mapping()
    assert JobDefinition.from_bytes(data) == job


def test_digest_is_sha256_of_the_stored_bytes(tmp_path: Path) -> None:
    job = JobDefinition.from_mapping(_mapping())
    assert job.digest == hashlib.sha256(job.encode()).hexdigest()
    # Stored bytes in another (valid) layout pin their own digest.
    stored = json.dumps(_mapping(), indent=2).encode()
    path = tmp_path / "job.json"
    path.write_bytes(stored)
    read = JobDefinition.from_path(path)
    assert read.digest == hashlib.sha256(stored).hexdigest() != job.digest
    assert read == job


def test_no_tag_and_no_parent() -> None:
    job = JobDefinition.from_mapping(_mapping(tag=None, parent=None, seal_succeeded=None, placement=""))
    assert job.job_key == JOB_ID and job.parent is None and job.placement == PurePosixPath()
    assert job.as_mapping()["placement"] == ""


def test_definition_is_immutable_all_the_way_down() -> None:
    job = JobDefinition.from_mapping(_mapping())
    with pytest.raises(TypeError):
        job.parameters["encut"] = 1  # type: ignore[index]
    assert isinstance(job.parameters["kpoints"], tuple)
    exported = job.as_mapping()
    exported["parameters"]["encut"] = 1  # type: ignore[index]
    assert job.parameters["encut"] == 520


@pytest.mark.parametrize(
    ("member", "value"),
    [
        ("format_version", 2),
        ("format", "httk-workflow-state"),
        ("id", JOB_ID.upper()),
        ("tag", "a~b"),
        ("tag", "A"),
        ("tag", "a--b"),
        ("name", ""),
        ("placement", "/abs"),
        ("placement", "a/../b"),
        ("placement", "a//b"),
        ("placement", "a/b~c"),
        ("placement", f"a/{JOB_ID}"),
        ("placement", 3),
        ("workflow", {"id": "x"}),
        ("workflow", {"id": "x", "name": "y", "runner": "z"}),
        ("initial_step", "a/b"),
        ("priority", 1000),
        ("priority", True),
        ("claim", {"pool": "default"}),
        ("claim", {"pool": "default", "required_capabilities": "gpu"}),
        ("retry_policy", {"retry_on": [], "maximum_attempts": 3}),
        ("retry_policy", {"maximum_total_attempts": 0}),
        ("resources", {"cores": -1}),
        ("step_resources", {"relax": {"cores": "x"}}),
        ("parameters", None),
        ("parameters", {"": 1}),
        ("parent", {"job_id": PARENT_ID}),
        ("seal_succeeded", "yes"),
    ],
)
def test_malformed_members_are_refused(member: str, value: object) -> None:
    with pytest.raises(FormatError):
        JobDefinition.from_mapping(_mapping(**{member: value}))


def test_parent_must_be_consistent() -> None:
    parent = json.loads(json.dumps(_mapping()["parent"]))
    parent["job_key"] = f"parent--{JOB_ID}"
    with pytest.raises(FormatError, match="parent.job_id"):
        JobDefinition.from_mapping(_mapping(parent=parent))


@pytest.mark.parametrize("member", ["runner", "workdir", "data", "requires", "calls"])
def test_removed_and_unknown_members_are_refused(member: str) -> None:
    with pytest.raises(FormatError, match=member):
        JobDefinition.from_mapping(_mapping(**{member: {}}))


def test_missing_members_are_refused() -> None:
    mapping = _mapping()
    del mapping["seal_succeeded"]
    with pytest.raises(FormatError, match="seal_succeeded"):
        JobDefinition.from_mapping(mapping)


def test_from_path_refusals(tmp_path: Path) -> None:
    with pytest.raises(FormatError):
        JobDefinition.from_path(tmp_path / "missing.json")
    target = tmp_path / "real.json"
    target.write_bytes(JobDefinition.from_mapping(_mapping()).encode())
    link = tmp_path / "job.json"
    os.symlink(target, link)
    with pytest.raises(FormatError):
        JobDefinition.from_path(link)
    big = tmp_path / "big.json"
    big.write_bytes(b" " * (MAX_JOB_BYTES + 1))
    with pytest.raises(FormatError):
        JobDefinition.from_path(big)
    os.mkfifo(tmp_path / "fifo")
    with pytest.raises(FormatError):
        JobDefinition.from_path(tmp_path / "fifo")
    for data in (b"[]", b"\xff", b"{"):
        with pytest.raises(FormatError):
            JobDefinition.from_bytes(data)
