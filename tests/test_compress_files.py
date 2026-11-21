"""``compress_files``/``decompress_files`` with levels, workers and Zstandard."""

import bz2
import gzip
import os
import subprocess
import sys
from pathlib import Path

import pytest
from httk.core.datastream.compression import open_compressed

from httk.workflow.runtime_utils import compress_files, decompress_files

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
