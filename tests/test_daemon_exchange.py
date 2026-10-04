"""Broker exchange movers against real directories and hostile staging entries."""

import json
import logging
import os
import signal
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from httk.workflow import _daemon_exchange as exchange_module
from httk.workflow._daemon_exchange import ExchangeMover

ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"
MANAGER = {"handle": "a" * 32, "profile": "cpu", "request_id": "1" * 32, "state": "submitted"}


def _site(tmp_path: Path) -> tuple[ExchangeMover, Path, Path]:
    site = tmp_path / "site"
    exchange = site / "exchange"
    staging = site / "workspace/.httk-workspace/exchange"
    for directory in (
        exchange / "requests",
        exchange / "responses",
        exchange / "inbox",
        exchange / "outbox/rejected",
        staging / "inbox",
        staging / "outbox/rejected",
        staging / "records",
    ):
        directory.mkdir(parents=True)
    return ExchangeMover(site, "exchange", "workspace", ENROLLMENT_ID), exchange, staging


def _bundle(path: Path) -> Path:
    path.mkdir()
    (path / "payload").write_text("content", encoding="utf-8")
    return path


def _poll(mover: ExchangeMover, managers: list[dict[str, str]] | None = None) -> None:
    def expired(_signum: int, _frame: object) -> None:
        raise TimeoutError("exchange poll blocked")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.alarm(5)
    try:
        mover.poll([] if managers is None else managers)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def test_bundles_move_in_all_three_directions(tmp_path: Path) -> None:
    mover, exchange, staging = _site(tmp_path)
    _bundle(exchange / "inbox/job-1")
    _bundle(staging / "outbox/job_2.done")
    _bundle(staging / "outbox/rejected/bad-3")
    _poll(mover)
    assert (staging / "inbox/job-1/payload").read_text(encoding="utf-8") == "content"
    assert (exchange / "outbox/job_2.done/payload").is_file()
    assert (exchange / "outbox/rejected/bad-3/payload").is_file()
    assert not (exchange / "inbox/job-1").exists()
    assert sorted(os.listdir(staging / "outbox")) == ["rejected"]
    assert os.listdir(staging / "outbox/rejected") == []


def test_existing_target_is_not_replaced_until_it_disappears(tmp_path: Path) -> None:
    mover, exchange, staging = _site(tmp_path)
    _bundle(exchange / "inbox/job")
    existing = _bundle(staging / "inbox/job")
    (existing / "payload").write_text("previous", encoding="utf-8")
    _poll(mover)
    assert (exchange / "inbox/job/payload").read_text(encoding="utf-8") == "content"
    assert (staging / "inbox/job/payload").read_text(encoding="utf-8") == "previous"
    (existing / "payload").unlink()
    existing.rmdir()
    _poll(mover)
    assert not (exchange / "inbox/job").exists()
    assert (staging / "inbox/job/payload").read_text(encoding="utf-8") == "content"


def test_ineligible_entries_are_skipped_and_logged_once(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    mover, exchange, staging = _site(tmp_path)
    outbox = staging / "outbox"
    target = _bundle(tmp_path / "outside")
    (outbox / "linked").symlink_to(target, target_is_directory=True)
    (outbox / "plain").write_text("file", encoding="utf-8")
    os.mkfifo(outbox / "pipe")
    for name in (".hidden", "records", "inbox", "-dash", "bad name", "x" * 129):
        _bundle(outbox / name)
    with caplog.at_level(logging.WARNING):
        _poll(mover)
        _poll(mover)
    assert sorted(os.listdir(exchange / "outbox")) == ["managers.json", "rejected"]
    assert set(os.listdir(outbox)) == {
        "linked",
        "plain",
        "pipe",
        ".hidden",
        "records",
        "inbox",
        "-dash",
        "bad name",
        "x" * 129,
        "rejected",
    }
    for name in ("linked", "plain", "pipe", "-dash"):
        assert sum(name in record.getMessage() for record in caplog.records) == 1
    assert not any(".hidden" in record.getMessage() for record in caplog.records)


def test_vanished_source_is_skipped_silently(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    mover, exchange, staging = _site(tmp_path)
    bundle = _bundle(exchange / "inbox/job")
    original = mover._renameat2

    def vanish(source: int, name: bytes, target: int, target_name: bytes, flags: int) -> int:
        (bundle / "payload").unlink()
        bundle.rmdir()
        return original(source, name, target, target_name, flags)

    mover._renameat2 = vanish
    with caplog.at_level(logging.WARNING):
        _poll(mover)
    assert os.listdir(staging / "inbox") == []
    assert "daemon_exchange" not in caplog.text


@pytest.mark.parametrize("direction", ["inbox", "outbox"])
def test_source_swapped_for_symlink_after_check_is_quarantined(
    tmp_path: Path, direction: str, caplog: pytest.LogCaptureFixture
) -> None:
    mover, exchange, staging = _site(tmp_path)
    source, target = (
        (exchange / "inbox", staging / "inbox") if direction == "inbox" else (staging / "outbox", exchange / "outbox")
    )
    bundle = _bundle(source / "job")
    outside = _bundle(tmp_path / "outside")
    original = mover._renameat2
    swapped: list[bytes] = []

    def swap(source_fd: int, name: bytes, target_fd: int, target_name: bytes, flags: int) -> int:
        if name == b"job" and not swapped:
            swapped.append(name)
            (bundle / "payload").unlink()
            bundle.rmdir()
            bundle.symlink_to(outside, target_is_directory=True)
        return original(source_fd, name, target_fd, target_name, flags)

    mover._renameat2 = swap
    with caplog.at_level(logging.WARNING):
        _poll(mover)
    assert swapped
    assert not (source / "job").exists(follow_symlinks=False)
    for name in os.listdir(target):
        assert name.startswith(".") or name == "managers.json" or stat.S_ISDIR(os.lstat(target / name).st_mode)
    quarantined = [name for name in os.listdir(target) if name.startswith(".quarantine-")]
    assert len(quarantined) == 1 and (target / quarantined[0]).is_symlink()
    assert "daemon_exchange_quarantined" in caplog.text
    assert (outside / "payload").is_file()


def test_status_is_copied_only_when_changed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mover, exchange, staging = _site(tmp_path)
    (staging / "outbox/status.json").write_bytes(b'{"jobs": []}')
    published: list[str] = []
    original = ExchangeMover._replace

    def record(self: ExchangeMover, directory: int, name: str, prefix: str, data: bytes) -> bool:
        published.append(name)
        return original(self, directory, name, prefix, data)

    monkeypatch.setattr(ExchangeMover, "_replace", record)
    _poll(mover)
    _poll(mover)
    assert published.count("status.json") == 1
    assert (exchange / "outbox/status.json").read_bytes() == b'{"jobs": []}'
    assert (staging / "outbox/status.json").exists()
    (staging / "outbox/status.json").write_bytes(b"not json at all")
    _poll(mover)
    assert published.count("status.json") == 2
    assert (exchange / "outbox/status.json").read_bytes() == b"not json at all"
    assert not [name for name in os.listdir(exchange / "outbox") if name.startswith(".")]


@pytest.mark.parametrize("kind", ["fifo", "oversized", "symlink", "directory"])
def test_unsafe_status_is_skipped_without_blocking(tmp_path: Path, kind: str) -> None:
    mover, exchange, staging = _site(tmp_path)
    status = staging / "outbox/status.json"
    if kind == "fifo":
        os.mkfifo(status)
    elif kind == "oversized":
        status.write_bytes(b" " * (1024 * 1024 + 1))
    elif kind == "symlink":
        secret = tmp_path / "secret.json"
        secret.write_text("{}", encoding="utf-8")
        status.symlink_to(secret)
    else:
        status.mkdir()
    _poll(mover)
    assert not (exchange / "outbox/status.json").exists()
    assert not [name for name in os.listdir(exchange / "outbox") if name.startswith(".")]


def test_missing_staging_is_logged_and_other_directions_continue(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    mover, exchange, staging = _site(tmp_path)
    for directory in ("inbox", "outbox/rejected", "outbox", "records"):
        (staging / directory).rmdir()
    staging.rmdir()
    _bundle(exchange / "inbox/job")
    with caplog.at_level(logging.WARNING):
        _poll(mover, [MANAGER])
    assert "daemon_exchange_unavailable" in caplog.text
    assert (exchange / "inbox/job").is_dir()
    assert json.loads((exchange / "outbox/managers.json").read_bytes())["managers"] == [MANAGER]


def test_missing_root_is_logged_without_raising(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    mover = ExchangeMover(tmp_path / "absent", "exchange", "workspace", ENROLLMENT_ID)
    with caplog.at_level(logging.WARNING):
        _poll(mover)
    assert "daemon_exchange_unavailable" in caplog.text


def test_managers_document_is_rewritten_only_when_rows_change(tmp_path: Path) -> None:
    mover, exchange, _ = _site(tmp_path)
    path = exchange / "outbox/managers.json"
    _poll(mover, [MANAGER])
    document = json.loads(path.read_bytes())
    assert document["format"] == "httk-workspace-daemon-managers"
    assert document["format_version"] == 1
    assert document["enrollment_id"] == ENROLLMENT_ID
    assert document["generated_at"].endswith("Z")
    assert document["managers"] == [MANAGER]
    inode = path.stat().st_ino
    _poll(mover, [dict(MANAGER)])
    assert path.stat().st_ino == inode
    _poll(mover, [{**MANAGER, "state": "uncertain"}])
    assert path.stat().st_ino != inode
    assert json.loads(path.read_bytes())["managers"][0]["state"] == "uncertain"
    assert sorted(os.listdir(exchange / "outbox")) == ["managers.json", "rejected"]


def test_missing_renameat2_fails_at_construction(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(exchange_module.ctypes, "CDLL", lambda *_args, **_kwargs: SimpleNamespace())
    with pytest.raises(RuntimeError, match="renameat2"):
        ExchangeMover(tmp_path, "exchange", "workspace", ENROLLMENT_ID)


def test_staging_directory_swapped_for_symlink_is_refused(tmp_path: Path) -> None:
    mover, exchange, staging = _site(tmp_path)
    _poll(mover)
    decoy = tmp_path / "decoy"
    decoy.mkdir()
    (staging / "inbox").rmdir()
    (staging / "inbox").symlink_to(decoy, target_is_directory=True)
    _bundle(exchange / "inbox/job")
    _bundle(staging / "outbox/done")
    _poll(mover)
    assert os.listdir(decoy) == []
    assert (exchange / "inbox/job").is_dir()
    assert (exchange / "outbox/done").is_dir()
