"""JobRecord.parameter and JobRecord.result_file locate results for collect hooks."""

from pathlib import Path, PurePosixPath

import pytest

from httk.workflow.collecting import JobRecord

JOB_ID = "12345678-1234-4234-8234-123456789abc"


def _record(
    root: Path,
    *,
    data: bool = True,
    generation: int | None = 1,
    workdir: bool = True,
    job: dict[str, object] | None = None,
) -> JobRecord:
    return JobRecord(
        workspace_root=root,
        workspace_id="ws",
        job_id=JOB_ID,
        job_key=f"job--{JOB_ID}",
        job={} if job is None else job,
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=PurePosixPath("jobs"),
        payload_path=PurePosixPath(f"jobs/job--{JOB_ID}"),
        workdir_path=PurePosixPath("run") if workdir else None,
        data_path=PurePosixPath("data") if data else None,
        data_generation=generation,
        provenance={},
        runner_steps=None,
        children={},
        declarations={},
    )


def _touch(root: Path, *parts: str) -> Path:
    path = root.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    return path


def test_published_data_is_read_below_the_prefix(tmp_path: Path) -> None:
    expected = _touch(tmp_path, "data", "vasp", "OUTCAR")
    assert _record(tmp_path).result_file("OUTCAR", data_prefix="vasp") == expected


@pytest.mark.parametrize("prefix", ["", "custom/results"])
def test_published_name_replaces_the_workdir_name_only_in_data(tmp_path: Path, prefix: str) -> None:
    published = _touch(tmp_path, "data", *prefix.split("/"), "static", "OUTCAR")
    workdir = _touch(tmp_path, "run", "OUTCAR")
    assert _record(tmp_path).result_file("OUTCAR", data_prefix=prefix, published="static/OUTCAR") == published
    assert _record(tmp_path, data=False).result_file("OUTCAR", data_prefix=prefix, published="static/OUTCAR") == workdir


@pytest.mark.parametrize("data", [True, False])
def test_missing_file_names_the_job_and_file(tmp_path: Path, data: bool) -> None:
    with pytest.raises(ValueError, match=rf"ws:{JOB_ID}.*OUTCAR"):
        _record(tmp_path, data=data).result_file("OUTCAR")


@pytest.mark.parametrize("generation", [None, 1])
def test_transactional_job_never_falls_back_to_the_workdir(tmp_path: Path, generation: int | None) -> None:
    _touch(tmp_path, "run", "OUTCAR")
    with pytest.raises(ValueError, match="expected published data file"):
        _record(tmp_path, generation=generation).result_file("OUTCAR")


def test_job_without_data_or_workdir_fails(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="OUTCAR.*no workdir"):
        _record(tmp_path, data=False, workdir=False, generation=None).result_file("OUTCAR")


@pytest.mark.parametrize("value", ["", "x", 3, None, ["a"]])
def test_parameter_returns_present_values_as_is(tmp_path: Path, value: object) -> None:
    assert _record(tmp_path, job={"parameters": {"k": value}}).parameter("k", "default") == value


def test_parameter_default_and_missing(tmp_path: Path) -> None:
    record = _record(tmp_path, job={"parameters": {"b": 1, "a": 2}})
    assert record.parameter("c", "fallback") == "fallback"
    with pytest.raises(KeyError, match="defined parameters: a, b"):
        record.parameter("c")
    bare = _record(tmp_path)
    assert bare.parameter("c", "fallback") == "fallback"
    with pytest.raises(KeyError, match="defined parameters: none"):
        bare.parameter("c")
