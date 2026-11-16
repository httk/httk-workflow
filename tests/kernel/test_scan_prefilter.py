"""The lexical UUID pre-filter of :func:`httk.workflow._kernel._scan`: only names that could match are parsed."""

import uuid
from pathlib import Path, PurePosixPath

import pytest

from httk.workflow import Workspace, _fs, _kernel
from httk.workflow._kernel import OWNED, JobRef, format_job_name

PLACEMENT = PurePosixPath("p/q")


def _job(directory: Path, key: str, from_state: str | None = None) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    name = format_job_name(key, 500, _fs.fresh_token(), from_state)
    (directory / name).mkdir()
    return name


def test_only_wanted_names_are_parsed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ws = Workspace.initialize(tmp_path / "ws", durable=False, policy={"visibility_deadline_seconds": 0.05})
    ids = [str(uuid.uuid4()) for _ in range(200)]
    ready = ws.jobs / "ready" / PLACEMENT
    owned = ws.jobs / OWNED / uuid.uuid4().hex
    names = {
        job_id: _job(ready, f"t{index % 3}--{job_id}" if index % 2 else job_id) for index, job_id in enumerate(ids)
    }
    owned_ids = [str(uuid.uuid4()) for _ in range(20)]
    names |= {job_id: _job(owned, job_id, "ready") for job_id in owned_ids}
    wanted = [ids[0], ids[101], owned_ids[7]]
    # Names of the wrong grammar are skipped, as a FormatError skips them; one carrying a wanted id is still parsed.
    broken = [f"{ids[0]}~p500~notatoken", ids[101], f"{ids[101]}~p500~{_fs.fresh_token()}~ready~x"]
    for name in [*broken, "sub", "~", "x~y", f"t--{ids[5]}~p5~bad"]:
        (ready / name).mkdir()
    exhaustive = _kernel._settled(ws, set(wanted), (), include_owned=True, settle=False, exhaustive=True)

    parsed: list[str] = []
    real = JobRef.from_path

    def from_path(cls: type[JobRef], path: Path, **where: object) -> JobRef:
        parsed.append(path.name)
        return real(path, **where)  # type: ignore[arg-type]

    monkeypatch.setattr(JobRef, "from_path", classmethod(from_path))
    found = _kernel.locate_many(ws, wanted, placements=[PLACEMENT], settle=False)
    assert found == exhaustive
    assert found.keys() == set(wanted)
    assert sorted(parsed) == sorted([names[job_id] for job_id in wanted] + broken)
