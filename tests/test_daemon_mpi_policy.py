"""Test the protected daemon MPI policy schema and invariants."""

import base64
import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from httk.workflow._daemon_policy import MPIProfile, MPISettings, Policy, Profile, load_policy

AUTHORIZED_KEY = "ed25519:" + base64.b64encode(bytes(range(32))).decode("ascii")


def _document(tmp_path: Path) -> dict[str, Any]:
    roots = {
        name: tmp_path / name
        for name in ("workspace", "requests", "responses", "state", "runtime", "broker", "control", "pmix", "shm")
    }
    return {
        "format": "httk-workspace-daemon-policy",
        "format_version": 1,
        "workspace": str(roots["workspace"]),
        "workspace_id": str(uuid.uuid4()),
        "enrollment_id": "1" * 32,
        "requests": str(roots["requests"]),
        "responses": str(roots["responses"]),
        "state": str(roots["state"]),
        "bwrap": str(roots["broker"] / "bwrap"),
        "python": str(roots["runtime"] / "python"),
        "sbatch": str(roots["broker"] / "sbatch"),
        "squeue": str(roots["broker"] / "squeue"),
        "scancel": str(roots["broker"] / "scancel"),
        "cluster": "cluster",
        "readonly_paths": [str(roots["runtime"])],
        "broker_paths": [str(roots["broker"])],
        "authorized_keys": [AUTHORIZED_KEY],
        "profiles": {
            "serial": {"cpus": 2, "memory_mb": 1024, "time_minutes": 10},
            "mpi": {
                "cpus": 4,
                "memory_mb": 8192,
                "time_minutes": 60,
                "mpi": {"nodes": 2, "ranks": 8},
            },
        },
        "mpi": {
            "srun": str(roots["broker"] / "srun"),
            "control_root": str(roots["control"]),
            "pmix_roots": [str(roots["pmix"])],
            "shm_root": str(roots["shm"]),
            "devices": ["/dev/null"],
            "environment": {"OMPI_MCA_btl": "self,vader,tcp", "SITE_SETTING": "value"},
            "max_steps": 32,
            "termination_grace": 3.5,
        },
    }


def _write(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    path.chmod(0o600)
    return path


def test_mpi_policy_decodes_to_frozen_values(tmp_path: Path) -> None:
    policy = load_policy(_write(tmp_path, _document(tmp_path)))
    assert policy.profile("serial").mpi is None
    assert policy.profile("mpi").mpi == MPIProfile(nodes=2, ranks=8)
    assert policy.mpi == MPISettings(
        srun=tmp_path / "broker/srun",
        control_root=tmp_path / "control",
        pmix_roots=(tmp_path / "pmix",),
        shm_root=tmp_path / "shm",
        devices=(Path("/dev/null"),),
        environment=(("OMPI_MCA_btl", "self,vader,tcp"), ("SITE_SETTING", "value")),
        max_steps=32,
        termination_grace=3.5,
    )
    with pytest.raises(AttributeError):
        policy.mpi.max_steps = 1  # type: ignore[misc]


def test_serial_policy_keeps_mpi_optional(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document.pop("mpi")
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    profiles.pop("mpi")
    policy = load_policy(_write(tmp_path, document))
    assert policy.mpi is None
    assert policy.profiles == (Profile("serial", 2, 1024, 10),)


@pytest.mark.parametrize(
    ("nodes", "ranks"),
    [(0, 1), (1, 0), (2, 1), (4097, 4097), (1, 65_537), (True, 1)],
)
def test_mpi_geometry_is_bounded(nodes: object, ranks: object) -> None:
    with pytest.raises(ValueError):
        MPIProfile(nodes, ranks)  # type: ignore[arg-type]


def test_mpi_profile_requires_settings(tmp_path: Path) -> None:
    document = _document(tmp_path)
    document.pop("mpi")
    with pytest.raises(ValueError, match="require policy MPI settings"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("location", ["policy", "profile"])
def test_present_mpi_field_cannot_be_null(tmp_path: Path, location: str) -> None:
    document = _document(tmp_path)
    if location == "policy":
        document["mpi"] = None
    else:
        profiles = document["profiles"]
        assert isinstance(profiles, dict)
        profiles["mpi"]["mpi"] = None
    with pytest.raises(ValueError, match="mpi|MPI"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("field", ["unknown", "pmix"])
def test_mpi_schema_rejects_unknown_fields(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi[field] = []
    with pytest.raises(ValueError, match="fields are missing or unknown"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize(
    "name",
    [
        "PMI_RANK",
        "PMIX_SERVER_URI2",
        "SLURM_PROCID",
        "SLURMD_NODENAME",
        "HTTK_DAEMON_MPI_HANDLE",
        "OMPI_COMM_WORLD_RANK",
        "OPAL_PREFIX",
    ],
)
def test_mpi_environment_refuses_identity_overrides(tmp_path: Path, name: str) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi["environment"] = {name: "forged"}
    with pytest.raises(ValueError, match="identity"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("name", ["OMPI_MCA_btl", "OPAL_MCA_memory_linux_disable"])
def test_mpi_environment_allows_protected_tuning(tmp_path: Path, name: str) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi["environment"] = {name: "1"}
    assert load_policy(_write(tmp_path, document)).mpi.environment == ((name, "1"),)  # type: ignore[union-attr]


@pytest.mark.parametrize("device", ["/dev", "/dev/shm/card", "/dev/fd/4", "/tmp/device"])
def test_mpi_devices_cannot_replace_private_mounts(tmp_path: Path, device: str) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi["devices"] = [device]
    with pytest.raises(ValueError, match="devices"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("field", ["control_root", "shm_root"])
def test_mpi_roots_are_disjoint_from_mutable_and_runtime_roots(tmp_path: Path, field: str) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi[field] = document["workspace"] if field == "control_root" else document["readonly_paths"][0]
    with pytest.raises(ValueError, match="disjoint"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize(("field", "value"), [("max_steps", 0), ("max_steps", 65_537), ("termination_grace", 0)])
def test_mpi_limits_are_bounded(tmp_path: Path, field: str, value: object) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi[field] = value
    with pytest.raises(ValueError):
        load_policy(_write(tmp_path, document))


def test_direct_mpi_dataclasses_require_immutable_collections(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="tuples"):
        MPISettings(
            tmp_path / "runtime/srun",
            tmp_path / "control",
            pmix_roots=[tmp_path / "pmix"],  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="tuple"):
        MPISettings(
            tmp_path / "runtime/srun",
            tmp_path / "control",
            environment=[("A", "B")],  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="too many"):
        MPISettings(
            tmp_path / "runtime/srun",
            tmp_path / "control",
            environment=tuple((f"SITE_{index}", "") for index in range(129)),
        )


@pytest.mark.parametrize("role", ["readonly_paths", "broker_paths"])
def test_run_mpi_destination_is_reserved(tmp_path: Path, role: str) -> None:
    document = _document(tmp_path)
    document[role] = ["/run"]
    if role == "readonly_paths":
        document["python"] = "/run/python"
    else:
        document["bwrap"] = "/run/bwrap"
        document["sbatch"] = "/run/sbatch"
        document["squeue"] = "/run/squeue"
        document["scancel"] = "/run/scancel"
        mpi = document["mpi"]
        assert isinstance(mpi, dict)
        mpi["srun"] = "/run/srun"
    with pytest.raises(ValueError, match="reserved sandbox destinations"):
        load_policy(_write(tmp_path, document))


@pytest.mark.parametrize("role", ["readonly_paths", "broker_paths"])
def test_serial_policy_preserves_run_runtime_roots(tmp_path: Path, role: str) -> None:
    document = _document(tmp_path)
    document.pop("mpi")
    profiles = document["profiles"]
    assert isinstance(profiles, dict)
    profiles.pop("mpi")
    document[role] = ["/run"]
    if role == "readonly_paths":
        document["python"] = "/run/python"
    else:
        document["bwrap"] = "/run/bwrap"
        document["sbatch"] = "/run/sbatch"
        document["squeue"] = "/run/squeue"
        document["scancel"] = "/run/scancel"
    policy = load_policy(_write(tmp_path, document))
    assert getattr(policy, role) == (Path("/run"),)


def test_mpi_srun_must_be_in_an_approved_runtime(tmp_path: Path) -> None:
    document = _document(tmp_path)
    mpi = document["mpi"]
    assert isinstance(mpi, dict)
    mpi["srun"] = str(tmp_path / "other/srun")
    with pytest.raises(ValueError, match="within an approved runtime"):
        load_policy(_write(tmp_path, document))


def test_policy_can_be_constructed_directly_with_mpi(tmp_path: Path) -> None:
    mpi = MPISettings(tmp_path / "broker/srun", tmp_path / "control", shm_root=tmp_path / "shm")
    policy = Policy(
        workspace=tmp_path / "workspace",
        workspace_id="12345678-1234-1234-1234-123456789abc",
        enrollment_id="0" * 32,
        requests=tmp_path / "requests",
        responses=tmp_path / "responses",
        state=tmp_path / "state",
        bwrap=tmp_path / "broker/bwrap",
        python=tmp_path / "runtime/python",
        sbatch=tmp_path / "broker/sbatch",
        squeue=tmp_path / "broker/squeue",
        scancel=tmp_path / "broker/scancel",
        cluster="cluster",
        readonly_paths=(tmp_path / "runtime",),
        broker_paths=(tmp_path / "broker",),
        profiles=(Profile("mpi", 2, 4096, 10, mpi=MPIProfile(1, 2)),),
        mpi=mpi,
    )
    assert policy.mpi is mpi
