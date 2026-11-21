"""``compress_files``/``decompress_files`` with levels, workers and Zstandard."""

import bz2
import gzip
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.datastream.compression import open_compressed

from httk.workflow.runtime_utils import DIGEST_MANIFEST_NAME, compress_files, decompress_files, read_digest_manifest

zstd = pytest.importorskip("compression.zstd" if sys.version_info >= (3, 14) else "backports.zstd")

_SHELL = Path(__file__).parents[1] / "src" / "httk" / "workflow" / "languages" / "bash" / "httk-workflow.sh"
_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def _payload(index: int) -> bytes:
    return b"".join(f"line {index} {n} {n * n}\n".encode() for n in range(20000))


@pytest.mark.parametrize(("level", "workers"), [(None, 1), (19, 1), (3, 2), (-5, 2)])
def test_zst_round_trip(tmp_path: Path, level: int | None, workers: int) -> None:
    source = tmp_path / "OUTCAR"
    source.write_bytes(_payload(0))
    (output,) = compress_files([source], method="zst", level=level, workers=workers, remove_source=True)
    assert output == tmp_path / "OUTCAR.zst"
    assert not source.exists()
    packed = output.read_bytes()
    assert packed.startswith(_ZSTD_MAGIC)
    assert zstd.decompress(packed) == _payload(0)
    assert decompress_files([output], remove_source=True) == (source,)
    assert source.read_bytes() == _payload(0) and not output.exists()
    assert [path.name for path in tmp_path.iterdir()] == ["OUTCAR"]


def test_bz2_workers_compress_several_files(tmp_path: Path) -> None:
    sources = [tmp_path / f"f{index}" for index in range(5)]
    for index, source in enumerate(sources):
        source.write_bytes(_payload(index))
    outputs = compress_files(sources, workers=2, level=1)
    assert outputs == tuple(source.with_name(source.name + ".bz2") for source in sources)
    for index, output in enumerate(outputs):
        assert bz2.decompress(output.read_bytes()) == _payload(index)
    assert all(source.exists() for source in sources)


def test_existing_output_is_replaced(tmp_path: Path) -> None:
    source = tmp_path / "WAVECAR"
    source.write_bytes(b"new")
    (tmp_path / "WAVECAR.zst").write_bytes(b"stale partial")
    (output,) = compress_files([source], method="zst", remove_source=True)
    assert zstd.decompress(output.read_bytes()) == b"new"


@pytest.mark.parametrize(
    ("method", "level", "workers"),
    [("bz2", 0, 1), ("gz", 10, 1), ("xz", -1, 1), ("zst", 23, 1), ("zst", True, 1), ("zst", None, 0), ("lz4", None, 1)],
)
def test_invalid_arguments(tmp_path: Path, method: str, level: object, workers: int) -> None:
    source = tmp_path / "file"
    source.write_bytes(b"x")
    with pytest.raises(ValueError):
        compress_files([source], method=method, level=level, workers=workers)  # type: ignore[arg-type]
    assert [path.name for path in tmp_path.iterdir()] == ["file"]


def test_symlink_is_refused_before_any_work(tmp_path: Path) -> None:
    (tmp_path / "a").write_bytes(b"a")
    (tmp_path / "link").symlink_to(tmp_path / "a")
    with pytest.raises(ValueError, match="regular file"):
        compress_files([tmp_path / "a", tmp_path / "link"], method="zst")
    assert not (tmp_path / "a.zst").exists()


def test_bash_httk_compress_zst(tmp_path: Path) -> None:
    source = tmp_path / "OUTCAR"
    source.write_bytes(_payload(1))
    environment = {**os.environ, "HTTK_WORKFLOW_PYTHON": sys.executable}
    completed = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; httk_compress --method zst --level 3 --workers 2 --remove-source "$2"',
            "bash",
            str(_SHELL),
            str(source),
        ],
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == str(tmp_path / "OUTCAR.zst")
    assert not source.exists()
    assert zstd.decompress((tmp_path / "OUTCAR.zst").read_bytes()) == _payload(1)


@pytest.mark.parametrize("method", ["bz2", "gz", "xz", "zst"])
def test_output_keeps_the_source_mode(tmp_path: Path, method: str) -> None:
    source = tmp_path / "data"
    source.write_bytes(b"payload")
    source.chmod(0o604)
    (output,) = compress_files([source], method=method, remove_source=True)
    assert output.stat().st_mode & 0o7777 == 0o604
    assert decompress_files([output], remove_source=True)[0].read_bytes() == b"payload"


def test_gzip_header_has_no_name_or_time(tmp_path: Path) -> None:
    source = tmp_path / "OUTCAR"
    source.write_bytes(b"payload")
    (output,) = compress_files([source], method="gz")
    header = output.read_bytes()
    flags, mtime = header[3], int.from_bytes(header[4:8], "little")
    assert flags & 0x08 == 0 and mtime == 0  # FNAME unset: the temporary's name is not recorded
    assert gzip.decompress(header) == b"payload"


def test_a_file_named_twice_is_compressed_once(tmp_path: Path) -> None:
    sources = [tmp_path / f"f{index}" for index in range(3)]
    for source in sources:
        source.write_bytes(_payload(2))
    named = [sources[0], tmp_path / "." / "f0", *sources[1:], sources[2]]
    outputs = compress_files(named, workers=4, remove_source=True)
    assert outputs == tuple(source.with_name(source.name + ".bz2") for source in sources)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["f0.bz2", "f1.bz2", "f2.bz2"]


@pytest.mark.parametrize("method", ["bz2", "gz", "xz", "zst"])
@pytest.mark.parametrize("workers", [1, 2])
def test_an_empty_file_round_trips(tmp_path: Path, method: str, workers: int) -> None:
    source = tmp_path / "vasp.err"
    source.write_bytes(b"")
    (output,) = compress_files([source], method=method, workers=workers, remove_source=True)
    assert output.stat().st_size > 0
    with output.open("rb") as raw, open_compressed(raw, compression="extension", name=output.name) as stream:
        assert stream.read() == b""
    assert decompress_files([output]) == (source,) and source.read_bytes() == b""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize(("method", "workers"), [("zst", 1), ("gz", 3)])
def test_record_digests_writes_a_sorted_sha256sum_manifest(tmp_path: Path, method: str, workers: int) -> None:
    payloads = {name: _payload(index) for index, name in enumerate(["OUTCAR", "EIGENVAL", "DOSCAR", "vasprun.xml"])}
    for name, data in payloads.items():
        (tmp_path / name).write_bytes(data)
    compress_files([tmp_path / name for name in payloads], method=method, workers=workers, record_digests=True)
    expected = "".join(f"{_sha(payloads[name])}  {name}\n" for name in sorted(payloads))
    assert (tmp_path / DIGEST_MANIFEST_NAME).read_text() == expected
    assert read_digest_manifest(tmp_path) == {name: _sha(data) for name, data in payloads.items()}


def test_manifest_merges_drops_vanished_entries_and_survives_source_removal(tmp_path: Path) -> None:
    for name in ("A", "B", "C"):
        (tmp_path / name).write_bytes(name.encode())
    compress_files([tmp_path / "A", tmp_path / "B"], method="zst", record_digests=True, remove_source=True)
    assert sorted(path.name for path in tmp_path.iterdir()) == ["A.zst", "B.zst", "C", DIGEST_MANIFEST_NAME]
    (tmp_path / "B.zst").unlink()
    compress_files([tmp_path / "C"], method="gz", record_digests=True, remove_source=True)
    assert read_digest_manifest(tmp_path) == {"A": _sha(b"A"), "C": _sha(b"C")}
    assert sorted(path.name for path in tmp_path.iterdir()) == ["A.zst", "C.gz", DIGEST_MANIFEST_NAME]


def test_manifest_is_not_compressed_while_recording(tmp_path: Path) -> None:
    manifest = tmp_path / DIGEST_MANIFEST_NAME
    manifest.write_text("")
    with pytest.raises(ValueError, match="digest manifest"):
        compress_files([manifest], method="zst", record_digests=True)
    assert compress_files([manifest], method="zst") == (tmp_path / f"{DIGEST_MANIFEST_NAME}.zst",)


def test_without_record_digests_no_manifest(tmp_path: Path) -> None:
    (tmp_path / "A").write_bytes(b"A")
    compress_files([tmp_path / "A"], method="zst")
    assert not (tmp_path / DIGEST_MANIFEST_NAME).exists()
    assert read_digest_manifest(tmp_path) == {}


def test_an_empty_source_records_the_empty_digest(tmp_path: Path) -> None:
    (tmp_path / "vasp.err").write_bytes(b"")
    compress_files([tmp_path / "vasp.err"], method="zst", record_digests=True)
    assert read_digest_manifest(tmp_path) == {"vasp.err": _sha(b"")}


def test_a_name_with_a_backslash_is_compressed_but_not_recorded(tmp_path: Path) -> None:
    (tmp_path / "a\\b").write_bytes(b"x")
    (tmp_path / "c").write_bytes(b"y")
    compress_files([tmp_path / "a\\b", tmp_path / "c"], method="zst", record_digests=True)
    assert (tmp_path / "a\\b.zst").is_file()
    assert read_digest_manifest(tmp_path) == {"c": _sha(b"y")}


@pytest.mark.parametrize(
    "text", [f"{'A' * 64}  name\n", f"{'a' * 64} name\n", f"{'a' * 64}  \n", "\n", f"{'a' * 63}  name\n"]
)
def test_read_digest_manifest_rejects_a_malformed_line(tmp_path: Path, text: str) -> None:
    (tmp_path / DIGEST_MANIFEST_NAME).write_text(f"{'0' * 64}  ok\n{text}")
    with pytest.raises(ValueError, match=f"{DIGEST_MANIFEST_NAME}:2:"):
        read_digest_manifest(tmp_path)


def test_unparsable_manifest_lines_are_dropped_on_update(tmp_path: Path) -> None:
    (tmp_path / DIGEST_MANIFEST_NAME).write_text("garbage\n")
    (tmp_path / "A").write_bytes(b"A")
    compress_files([tmp_path / "A"], method="zst", record_digests=True)
    assert (tmp_path / DIGEST_MANIFEST_NAME).read_text() == f"{_sha(b'A')}  A\n"


def test_many_files_update_the_manifest_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    names = [f"f{index:03d}" for index in range(300)]
    for name in names:
        (tmp_path / name).write_bytes(name.encode())
    writes: list[str] = []
    replace = os.replace

    def counting(source: object, destination: os.PathLike[str]) -> None:
        writes.append(Path(destination).name)
        replace(source, destination)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "replace", counting)
    compress_files([tmp_path / name for name in names], method="gz", workers=4, record_digests=True, remove_source=True)
    assert writes.count(DIGEST_MANIFEST_NAME) == 1
    assert read_digest_manifest(tmp_path) == {name: _sha(name.encode()) for name in names}


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a mode-000 file")
def test_a_failed_source_leaves_every_source_and_no_manifest(tmp_path: Path) -> None:
    sources = [tmp_path / name for name in ("A", "B", "C")]
    for source in sources:
        source.write_bytes(source.name.encode())
    sources[1].chmod(0)
    try:
        with pytest.raises(PermissionError):
            compress_files(sources, method="zst", record_digests=True, remove_source=True)
    finally:
        sources[1].chmod(0o644)
    assert all(source.is_file() for source in sources)
    assert not (tmp_path / DIGEST_MANIFEST_NAME).exists()
