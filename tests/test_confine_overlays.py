"""The attempt sandbox keeps a job's trusted entries read-only over its writable directory (note §3.3)."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from httk.workflow._confine import (
    ConfinementUnavailableError,
    confine_settings,
    filtered_attempt_environment,
    probe_bwrap,
)
from test_confine_sandbox import (
    _Layout,
    _open_descriptors,
    _option_index,
    _prepare,
    _settings,
    _shm_roots_are_tmpfs,  # noqa: F401  (the autouse fixture)
    _unsupported_namespace_failure,
    layout,  # noqa: F401  (the fixture)
)

_TRUSTED = ("job.json", "state.json", ".httk-job/seal.json", "logs")


def _plant_trusted(created: _Layout) -> None:
    for name in ("job.json", "state.json"):
        (created.job / name).write_text("{}\n", encoding="utf-8")
    (created.job / ".httk-job").mkdir()
    (created.job / ".httk-job" / "seal.json").write_text("{}\n", encoding="utf-8")
    (created.job / "logs").mkdir()


def test_trusted_entries_are_overlaid_read_only_after_the_job_bind(layout: _Layout) -> None:  # noqa: F811
    _plant_trusted(layout)
    prepared = _prepare(layout, _settings())
    try:
        argv = prepared.argv
        job_bind = _option_index(argv, "--bind-fd", argv[argv.index(str(layout.job)) - 1], str(layout.job))
        overlays = [
            _option_index(argv, "--ro-bind-fd", argv[argv.index(str(layout.job / name)) - 1], str(layout.job / name))
            for name in _TRUSTED
        ]
        assert job_bind < min(overlays) and max(overlays) < _option_index(argv, "--proc", "/proc")
        for index, name in zip(overlays, _TRUSTED, strict=True):
            descriptor = int(argv[index + 1])
            assert descriptor in prepared.descriptors and os.get_inheritable(descriptor)
            assert os.fstat(descriptor).st_ino == os.lstat(layout.job / name).st_ino
    finally:
        prepared.close()


def test_a_symlinked_job_state_dir_gets_no_seal_overlay(layout: _Layout, tmp_path: Path) -> None:  # noqa: F811
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "seal.json").write_text("{}\n", encoding="utf-8")
    (layout.job / ".httk-job").symlink_to(tmp_path / "elsewhere", target_is_directory=True)
    prepared = _prepare(layout, _settings())
    try:
        assert str(layout.job / ".httk-job" / "seal.json") not in prepared.argv
    finally:
        prepared.close()


@pytest.mark.parametrize("name", ["state.json", "logs"])
def test_a_planted_trusted_entry_is_refused_and_descriptors_are_closed(
    layout: _Layout,  # noqa: F811
    tmp_path: Path,
    name: str,
) -> None:
    (tmp_path / "target").mkdir()
    (layout.job / name).symlink_to(tmp_path / "target", target_is_directory=True)
    before = _open_descriptors()
    with pytest.raises(ValueError, match="trusted job entry"):
        _prepare(layout, _settings())
    assert _open_descriptors() == before


def test_real_sandbox_refuses_writes_to_the_trusted_entries(layout: _Layout) -> None:  # noqa: F811
    if shutil.which("bwrap") is None:
        pytest.skip("Bubblewrap executable is unavailable")
    settings = confine_settings({"manager.confine": "bwrap"})
    try:
        block_userns = probe_bwrap(settings)
    except ConfinementUnavailableError as exc:
        _unsupported_namespace_failure(str(exc))
        raise
    _plant_trusted(layout)
    environment = filtered_attempt_environment({"PATH": "/usr/bin:/bin"})
    prepared = _prepare(layout, settings, block_userns=block_userns, workdir=layout.job, environment=environment)
    targets = ["own", "job.json", "state.json", ".httk-job/seal.json", "logs/x"]
    script = (
        'for target in "$@"; do\n'
        '  if (echo data > "$target") 2>/dev/null; then echo "writable $target"; else echo "refused $target"; fi\n'
        "done\n"
        'if mv state.json moved 2>/dev/null; then echo "moved"; else echo "pinned"; fi\n'
    )
    try:
        result = subprocess.run(
            [*prepared.argv, "/bin/sh", "-c", script, "sh", *targets],
            pass_fds=prepared.descriptors,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
            check=False,
        )
    finally:
        prepared.close()
    if result.returncode != 0:
        _unsupported_namespace_failure(result.stderr or result.stdout)
    assert result.stdout.splitlines() == [
        "writable own",
        "refused job.json",
        "refused state.json",
        "refused .httk-job/seal.json",
        "refused logs/x",
        "pinned",
    ]
    assert (layout.job / "state.json").read_text(encoding="utf-8") == "{}\n"
    assert not (layout.job / "logs" / "x").exists()
