"""Strict codecs, bounds and file names of the confined launch protocol."""

import json
import os
from pathlib import Path

import pytest

from httk.workflow._launch_protocol import (
    MAX_ARGUMENT_BYTES,
    MAX_ARGUMENTS,
    MAX_CWD_BYTES,
    MAX_ENVIRONMENT_ENTRIES,
    MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_ERROR_BYTES,
    MAX_REQUEST_BYTES,
    RESERVED_ENVIRONMENT_PREFIXES,
    LaunchConfinement,
    LaunchRequest,
    LaunchStatus,
    TrustedLaunch,
    decode_request,
    decode_status,
    decode_trusted,
    encode_request,
    encode_status,
    encode_trusted,
    is_environment_name,
    lock_name,
    new_request_id,
    read_bounded,
    request_name,
    request_relative_path,
    request_temporary_name,
    status_name,
    stderr_name,
    stdout_name,
    stop_name,
    trusted_name,
)

REQUEST_ID = "0123456789abcdef0123456789abcdef"
ATTEMPT_ID = "6f1c1f0e-5b7a-4c1e-9a43-0c2f4b1d7e55"
TOKEN = "fedcba9876543210fedcba9876543210"
WORKSPACE_ID = "0f6b6c2a-1d55-4c69-8d43-7c0a1b2c3d4e"


def _request(**changes: object) -> LaunchRequest:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "attempt_id": ATTEMPT_ID,
        "argv": ("vasp_std", "--flag", ""),
        "cwd": "run/1",
        "environment": (("PATH", "/usr/bin"), ("OMP_NUM_THREADS", "4")),
    }
    values.update(changes)
    return LaunchRequest(**values)  # type: ignore[arg-type]


def _confinement(**changes: object) -> LaunchConfinement:
    values: dict[str, object] = {
        "bwrap": Path("/usr/bin/bwrap"),
        "block_userns": True,
        "readonly_paths": (Path("/usr"), Path("/opt/site")),
        "devices": (Path("/dev/infiniband/uverbs0"),),
        "pmix_roots": (Path("/var/spool/slurmd"),),
        "shm_root": Path("/dev/shm"),
        "environment": (("OMPI_MCA_btl_vader_single_copy_mechanism", "none"),),
    }
    values.update(changes)
    return LaunchConfinement(**values)  # type: ignore[arg-type]


def _trusted(**changes: object) -> TrustedLaunch:
    values: dict[str, object] = {
        "request_id": REQUEST_ID,
        "attempt_id": ATTEMPT_ID,
        "workspace_id": WORKSPACE_ID,
        "workspace_root": Path("/scratch/ws"),
        "placement": "project/a",
        "job_key": "relax--" + ATTEMPT_ID,
        "request": request_relative_path(ATTEMPT_ID, REQUEST_ID),
        "confine": _confinement(),
        "python": Path("/usr/bin/python3"),
        "token": TOKEN,
    }
    values.update(changes)
    return TrustedLaunch(**values)  # type: ignore[arg-type]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def test_request_round_trip_is_canonical_and_sorted() -> None:
    request = _request()
    assert request.environment == (("OMP_NUM_THREADS", "4"), ("PATH", "/usr/bin"))
    data = encode_request(request)
    assert decode_request(data) == request
    assert data == _canonical(json.loads(data))
    assert json.loads(data) == {
        "format": "httk-workflow-launch-request",
        "format_version": 1,
        "request_id": REQUEST_ID,
        "attempt_id": ATTEMPT_ID,
        "argv": ["vasp_std", "--flag", ""],
        "cwd": "run/1",
        "environment": [["OMP_NUM_THREADS", "4"], ["PATH", "/usr/bin"]],
    }


def test_request_non_ascii_round_trip() -> None:
    request = _request(argv=("echö", "☃"), environment=(("LANGUAGE", "sv_SE:å"),))
    data = encode_request(request)
    assert data.isascii()
    assert decode_request(data) == request


def _document(**changes: object) -> dict[str, object]:
    value: dict[str, object] = json.loads(encode_request(_request()))
    value.update(changes)
    return value


@pytest.mark.parametrize(
    "data",
    [
        b"\xef\xbb\xbf" + encode_request(_request()),
        encode_request(_request()) + b"\n",
        json.dumps(json.loads(encode_request(_request())), indent=1).encode(),
        _canonical(_document())[:-1] + b',"cwd":"x"}',
        _canonical(_document(extra=1)),
        _canonical({key: value for key, value in _document().items() if key != "cwd"}),
        _canonical(_document(format="other")),
        _canonical(_document(format_version=2)),
        _canonical(_document(format_version=True)),
        _canonical(_document(format_version=1.0)),
        _canonical(_document(environment=[["PATH", "/usr/bin"], ["OMP_NUM_THREADS", "4"]])),
        _canonical(_document(environment={"PATH": "/usr/bin"})),
        _canonical(_document(environment=[["PATH"]])),
        _canonical(_document(argv="vasp_std")),
        _canonical(_document(argv=[])),
        _canonical(_document(argv=[""])),
        _canonical(_document(argv=["a", 1])),
        _canonical(_document(request_id=REQUEST_ID.upper())),
        _canonical(_document(attempt_id=ATTEMPT_ID.upper())),
        _canonical(_document(attempt_id="not-a-uuid")),
        b'{"format":NaN}',
        b"[]",
        b"\xff",
        json.dumps(_document(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        .replace("vasp_std", "vasp_sté")
        .encode("utf-8"),
    ],
)
def test_request_decode_refusals(data: bytes) -> None:
    with pytest.raises(ValueError):
        decode_request(data)


def test_request_decode_refuses_oversized_document() -> None:
    with pytest.raises(ValueError):
        decode_request(b" " * (MAX_REQUEST_BYTES + 1))
    big = _request(argv=tuple("x" * (MAX_ARGUMENT_BYTES - 1) for _ in range(20)))
    with pytest.raises(ValueError, match="exceeds"):
        encode_request(big)


@pytest.mark.parametrize(
    "changes",
    [
        {"argv": ()},
        {"argv": ["a"]},
        {"argv": ("",)},
        {"argv": ("a", "b\0c")},
        {"argv": ("a",) * (MAX_ARGUMENTS + 1)},
        {"argv": ("a", "x" * (MAX_ARGUMENT_BYTES + 1))},
        {"request_id": "0123"},
        {"attempt_id": "0123"},
        {"cwd": ""},
        {"cwd": "/abs"},
        {"cwd": "./a"},
        {"cwd": "a/"},
        {"cwd": "a//b"},
        {"cwd": "a/../b"},
        {"cwd": ".."},
        {"cwd": "a/./b"},
        {"cwd": "a\0b"},
        {"cwd": "x" * (MAX_CWD_BYTES + 1)},
        {"environment": [("A", "b")]},
        {"environment": (("A",),)},
        {"environment": (("A", "1"), ("A", "2"))},
        {"environment": (("1A", "x"),)},
        {"environment": (("BASH_FUNC_x%%", "x"),)},
        {"environment": (("A" * 257, "x"),)},
        {"environment": (("A", "x" * (MAX_ENVIRONMENT_VALUE_BYTES + 1)),)},
        {"environment": (("A", "x\0"),)},
        {"environment": tuple((f"V{index}", "") for index in range(MAX_ENVIRONMENT_ENTRIES + 1))},
    ],
)
def test_request_bounds(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _request(**changes)


def test_request_bounds_accept_limits() -> None:
    request = _request(
        argv=("a",) * MAX_ARGUMENTS,
        cwd="c" * MAX_CWD_BYTES,
        environment=tuple((f"V{index}", "") for index in range(MAX_ENVIRONMENT_ENTRIES)),
    )
    assert decode_request(encode_request(request)) == request
    assert _request(environment=(("A" * 256, "x" * MAX_ENVIRONMENT_VALUE_BYTES),)).environment[0][0] == "A" * 256


@pytest.mark.parametrize("prefix", RESERVED_ENVIRONMENT_PREFIXES)
def test_request_refuses_reserved_environment(prefix: str) -> None:
    with pytest.raises(ValueError, match="reserved"):
        _request(environment=((prefix + "X", "1"),))
    document = _document(environment=[[prefix + "X", "1"]])
    with pytest.raises(ValueError, match="reserved"):
        decode_request(_canonical(document))


def test_reserved_prefixes_are_exactly_the_protocol_list() -> None:
    assert RESERVED_ENVIRONMENT_PREFIXES == (
        "PMI_",
        "PMIX_",
        "OMPI_",
        "OPAL_",
        "SLURM_",
        "SLURMD_",
        "SRUN_",
        "SBATCH_",
        "SALLOC_",
        "HTTK_",
    )
    assert is_environment_name("_A1")
    assert not is_environment_name("A-B")
    assert not is_environment_name("")


@pytest.mark.parametrize("cwd", [".", "a", "a/b", "a.b/.c", "...", "a/.../b"])
def test_request_cwd_canonical_forms(cwd: str) -> None:
    assert decode_request(encode_request(_request(cwd=cwd))).cwd == cwd


@pytest.mark.parametrize(
    ("state", "exit_code", "error"),
    [
        ("exited", 0, None),
        ("exited", 255, "x"),
        ("stopped", None, None),
        ("stopped", 143, "stopped by the manager"),
        ("refused", None, "one launch at a time"),
        ("uncertain", None, "not reaped"),
        ("uncertain", None, "e" * MAX_ERROR_BYTES),
    ],
)
def test_status_round_trip(state: str, exit_code: int | None, error: str | None) -> None:
    status = LaunchStatus(REQUEST_ID, state, exit_code, error)  # type: ignore[arg-type]
    data = encode_status(status)
    assert decode_status(data) == status
    assert json.loads(data) == {
        "format": "httk-workflow-launch-status",
        "format_version": 1,
        "request_id": REQUEST_ID,
        "state": state,
        "exit_code": exit_code,
        "error": error,
    }


@pytest.mark.parametrize(
    ("state", "exit_code", "error"),
    [
        ("done", 0, None),
        ("exited", None, None),
        ("exited", 256, None),
        ("exited", -1, None),
        ("exited", True, None),
        ("exited", 1.0, None),
        ("refused", 2, None),
        ("uncertain", 1, None),
        ("exited", 0, "e" * (MAX_ERROR_BYTES + 1)),
        ("exited", 0, "a\0b"),
        ("exited", 0, 5),
    ],
)
def test_status_refusals(state: object, exit_code: object, error: object) -> None:
    with pytest.raises(ValueError):
        LaunchStatus(REQUEST_ID, state, exit_code, error)  # type: ignore[arg-type]
    document = {
        "format": "httk-workflow-launch-status",
        "format_version": 1,
        "request_id": REQUEST_ID,
        "state": state,
        "exit_code": exit_code,
        "error": error,
    }
    with pytest.raises(ValueError):
        decode_status(_canonical(document))


def test_status_decode_requires_exact_fields() -> None:
    document = json.loads(encode_status(LaunchStatus(REQUEST_ID, "exited", 0)))
    del document["error"]
    with pytest.raises(ValueError, match="fields"):
        decode_status(_canonical(document))
    with pytest.raises(ValueError, match="canonical"):
        decode_status(json.dumps(json.loads(encode_status(LaunchStatus(REQUEST_ID, "exited", 0)))).encode())


def test_trusted_round_trip() -> None:
    launch = _trusted()
    data = encode_trusted(launch)
    assert decode_trusted(data) == launch
    value = json.loads(data)
    assert value["format"] == "httk-workflow-launch"
    assert value["request"] == f"attempts/{ATTEMPT_ID}/launch/{REQUEST_ID}.request.json"
    assert value["token"] == TOKEN
    assert value["confine"] == {
        "bwrap": "/usr/bin/bwrap",
        "block_userns": True,
        "readonly_paths": ["/usr", "/opt/site"],
        "devices": ["/dev/infiniband/uverbs0"],
        "pmix_roots": ["/var/spool/slurmd"],
        "shm_root": "/dev/shm",
        "environment": [["OMPI_MCA_btl_vader_single_copy_mechanism", "none"]],
        "block_mpi_spawn": "on",
    }
    for mode in ("on", "off", "auto"):
        spawn = _trusted(confine=_confinement(block_mpi_spawn=mode))
        assert decode_trusted(encode_trusted(spawn)).confine.block_mpi_spawn == mode
    no_placement = _trusted(placement="")
    assert decode_trusted(encode_trusted(no_placement)).placement == ""


@pytest.mark.parametrize(
    "changes",
    [
        {"workspace_id": "x"},
        {"workspace_root": Path("relative")},
        {"workspace_root": "/a/../b"},
        {"workspace_root": "//net/ws"},
        {"placement": "."},
        {"placement": "/abs"},
        {"placement": "a/../b"},
        {"placement": "a/"},
        {"job_key": ""},
        {"job_key": "a/b"},
        {"job_key": ".."},
        {"request": "attempts/x/launch/y.request.json"},
        {"python": Path("python3")},
        {"token": ""},
        {"token": TOKEN.upper()},
        {"token": "../" + TOKEN[3:]},
        {"token": REQUEST_ID[:-1]},
        {"confine": {"bwrap": "/usr/bin/bwrap"}},
    ],
)
def test_trusted_refusals(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _trusted(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"bwrap": Path("bwrap")},
        {"block_userns": 1},
        {"readonly_paths": [Path("/usr")]},
        {"readonly_paths": (Path("usr"),)},
        {"devices": (Path("/dev/../etc/shadow"),)},
        {"pmix_roots": (Path("/a"),) * 1025},
        {"shm_root": Path("shm")},
        {"environment": (("A-B", "x"),)},
        {"environment": (("A", "1"), ("A", "2"))},
        {"block_mpi_spawn": "ON"},
        {"block_mpi_spawn": True},
    ],
)
def test_confinement_refusals(changes: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _confinement(**changes)


def test_trusted_decode_refusals() -> None:
    value = json.loads(encode_trusted(_trusted()))
    value["confine"]["extra"] = 1
    with pytest.raises(ValueError, match="confine fields"):
        decode_trusted(_canonical(value))
    value = json.loads(encode_trusted(_trusted()))
    del value["confine"]["block_mpi_spawn"]
    with pytest.raises(ValueError, match="confine fields"):
        decode_trusted(_canonical(value))
    value = json.loads(encode_trusted(_trusted()))
    value["confine"]["block_mpi_spawn"] = "maybe"
    with pytest.raises(ValueError, match="block_mpi_spawn"):
        decode_trusted(_canonical(value))
    value = json.loads(encode_trusted(_trusted()))
    value["confine"]["environment"] = [["B", "1"], ["A", "1"]]
    with pytest.raises(ValueError, match="canonical"):
        decode_trusted(_canonical(value))
    value = json.loads(encode_trusted(_trusted()))
    value["confine"]["readonly_paths"] = "/usr"
    with pytest.raises(ValueError):
        decode_trusted(_canonical(value))
    with pytest.raises(ValueError):
        decode_trusted(b"x" * (1024 * 1024 + 1))
    value = json.loads(encode_trusted(_trusted()))
    del value["token"]
    with pytest.raises(ValueError, match="fields"):
        decode_trusted(_canonical(value))


def test_trusted_names_carry_the_attempt() -> None:
    assert trusted_name(ATTEMPT_ID, REQUEST_ID) == f"{ATTEMPT_ID}.{REQUEST_ID}"
    with pytest.raises(ValueError):
        trusted_name("x", REQUEST_ID)
    with pytest.raises(ValueError):
        trusted_name(ATTEMPT_ID, "../x")


def test_file_names() -> None:
    assert lock_name(REQUEST_ID) == f"{REQUEST_ID}.lock"
    assert request_name(REQUEST_ID) == f"{REQUEST_ID}.request.json"
    assert request_temporary_name(REQUEST_ID) == f".{REQUEST_ID}.request.tmp"
    assert stop_name(REQUEST_ID) == f"{REQUEST_ID}.stop"
    assert stdout_name(REQUEST_ID) == f"{REQUEST_ID}.stdout"
    assert stderr_name(REQUEST_ID) == f"{REQUEST_ID}.stderr"
    assert status_name(REQUEST_ID) == f"{REQUEST_ID}.status.json"
    with pytest.raises(ValueError):
        lock_name("../x")
    first, second = new_request_id(), new_request_id()
    assert first != second
    assert lock_name(first) == f"{first}.lock"


def test_read_bounded(tmp_path: Path) -> None:
    (tmp_path / "doc").write_bytes(b"abc")
    (tmp_path / "big").write_bytes(b"x" * 11)
    (tmp_path / "link").symlink_to(tmp_path / "doc")
    (tmp_path / "dir").mkdir()
    os.mkfifo(tmp_path / "fifo")
    descriptor = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert read_bounded(descriptor, "doc", 3) == b"abc"
        with pytest.raises(ValueError, match="exceeds"):
            read_bounded(descriptor, "big", 10)
        with pytest.raises(ValueError, match="symlink"):
            read_bounded(descriptor, "link", 10)
        with pytest.raises(ValueError, match="regular"):
            read_bounded(descriptor, "dir", 10)
        with pytest.raises(ValueError, match="regular"):
            read_bounded(descriptor, "fifo", 10)
        with pytest.raises(FileNotFoundError):
            read_bounded(descriptor, "missing", 10)
        with pytest.raises(ValueError, match="entry name"):
            read_bounded(descriptor, "a/b", 10)
    finally:
        os.close(descriptor)
