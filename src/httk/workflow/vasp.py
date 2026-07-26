"""Small, dependency-free VASP runner helpers for native v2 workflows.

This is an independent, data-oriented interface rather than a port of the
historic ``VASP_*`` Python or shell APIs. Historical authorship is documented
in ``v1_runtime/NOTICE``.
"""

import bz2
import gzip
import hashlib
import logging
import lzma
import math
import os
import random
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ._util import read_json, sha256_file, utc_now, write_json_atomic
from .models import JOB_STATE_DIRECTORY
from .runtime_builders import ReplayableWorkdirBatch
from .supervision import (
    Diagnostic,
    FollowSource,
    ProcessReport,
    ProcessSupervisor,
    SourceEvent,
)

_LOGGER = logging.getLogger(__name__)

#: The k-point centering every entry point starts from. Monkhorst-Pack is the
#: starting point rather than Gamma because the reviewed remedy ladder promotes
#: ``("centering", "Gamma")`` as the *fix* for the ``kpoints_class`` and
#: ``kpoint_shifts`` failure classes: a workflow that already starts at Gamma has
#: no such remedy left to apply.
DEFAULT_KPOINT_CENTERING = "Monkhorst-Pack"
KPOINT_CENTERINGS = ("Gamma", "Monkhorst-Pack")

#: Outputs :func:`clean_vasp_outputs` keeps unless they are named explicitly:
#: the remedy machinery and restart promotion read exactly these two files.
VASP_RESTART_ARTIFACTS: tuple[str, ...] = ("CONTCAR", "vasp-run-report.json")

#: Where a remedy history lives when a caller names no other place. The
#: job-scoped location of :func:`job_remedy_history_path` is what a runner
#: should use; this workdir-relative name is the pre-0.2 location, still read for
#: compatibility.
DEFAULT_REMEDY_HISTORY = ".httk-vasp/remedies.json"
REMEDY_HISTORY_NAME = "vasp-remedies.json"


@dataclass(frozen=True)
class PoscarHeader:
    """The VASP-5 header information needed by execution helpers."""

    comment: str
    scale: float
    lattice: tuple[tuple[float, float, float], ...]
    species: tuple[str, ...]
    counts: tuple[int, ...]


def read_poscar_header(path: str | os.PathLike[str] = "POSCAR") -> PoscarHeader:
    """Read a VASP-5 POSCAR header without interpreting site coordinates."""

    lines = Path(path).read_text(encoding="utf-8").splitlines()
    if len(lines) < 7:
        raise ValueError("POSCAR has fewer than seven header lines")
    try:
        scale = float(lines[1].split()[0])
        raw_lattice = [tuple(float(item) for item in lines[index].split()) for index in range(2, 5)]
        counts = tuple(int(item) for item in lines[6].split())
    except (IndexError, ValueError) as exc:
        raise ValueError("POSCAR contains an invalid scale, lattice, or count line") from exc
    if len(raw_lattice) != 3 or any(len(row) != 3 for row in raw_lattice):
        raise ValueError("POSCAR lattice must contain three three-component rows")
    lattice = (
        (raw_lattice[0][0], raw_lattice[0][1], raw_lattice[0][2]),
        (raw_lattice[1][0], raw_lattice[1][1], raw_lattice[1][2]),
        (raw_lattice[2][0], raw_lattice[2][1], raw_lattice[2][2]),
    )
    species = tuple(lines[5].split())
    if not species or len(species) != len(counts) or any(value < 0 for value in counts):
        raise ValueError("POSCAR requires matching VASP-5 species and nonnegative counts")
    return PoscarHeader(lines[0], scale, lattice, species, counts)


def suggested_magnetic_moments(path: str | os.PathLike[str] = "POSCAR") -> str:
    """Return the explicit comment override or a five-per-atom default.

    The five Bohr magnetons per atom are a heuristic starting guess, not a
    physical prediction: a deliberately high initial moment lets a spin-polarized
    relaxation fall into a low-spin solution, whereas starting too low tends to keep
    it there, so overestimating is the safer direction. The value of five is the
    empirical default carried over from *httk* v1. Encode a per-structure choice in
    the POSCAR comment as ``[MAGMOM=...]``.
    """

    header = read_poscar_header(path)
    override = re.search(r"\[MAGMOM=([^\]]+)\]", header.comment)
    if override is not None:
        return override.group(1)
    return " ".join(f"{count}*5" for count in header.counts)


def _cross(left: Sequence[float], right: Sequence[float]) -> tuple[float, float, float]:
    return (
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    )


def _dot(left: Sequence[float], right: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(left, right, strict=True))


def automatic_kpoint_grid(
    density: float,
    *,
    poscar: str | os.PathLike[str] = "POSCAR",
    minimum: int = 3,
    equal: bool = False,
    bump: int = 0,
) -> tuple[int, int, int]:
    """Calculate a reciprocal-length automatic grid."""

    if density <= 0 or minimum < 1 or bump < 0:
        raise ValueError("density must be positive; minimum must be positive; bump cannot be negative")
    header = read_poscar_header(poscar)
    first, second, third = header.lattice
    determinant = _dot(first, _cross(second, third))
    if abs(determinant) < 1e-12:
        raise ValueError("POSCAR lattice is singular")
    scale = header.scale
    if scale < 0:
        scale = (-scale / abs(determinant)) ** (1.0 / 3.0)
    if scale == 0:
        raise ValueError("POSCAR scale cannot be zero")
    reciprocal = (
        _cross(second, third),
        _cross(third, first),
        _cross(first, second),
    )
    lengths = tuple(math.sqrt(_dot(row, row)) / abs(determinant * scale) for row in reciprocal)
    values = tuple(max(minimum, math.ceil(density * length + 0.5) + bump) for length in lengths)
    grid = (values[0], values[1], values[2])
    if equal:
        largest = max(grid)
        return largest, largest, largest
    return grid


def write_automatic_kpoints(
    grid: Sequence[int],
    path: str | os.PathLike[str] = "KPOINTS",
    *,
    centering: str = DEFAULT_KPOINT_CENTERING,
) -> Path:
    """Write a standard automatic KPOINTS file.

    The centering defaults to :data:`DEFAULT_KPOINT_CENTERING` here, in
    :class:`VaspPreparationOptions`, and in the Bash bridge, so a workflow that
    hits a k-point failure class still has the ``Gamma`` remedy available.
    """

    values = tuple(grid)
    if len(values) != 3 or any(isinstance(value, bool) or value < 1 for value in values):
        raise ValueError("grid must contain three positive integers")
    if centering not in KPOINT_CENTERINGS:
        raise ValueError("centering must be 'Gamma' or 'Monkhorst-Pack'")
    destination = Path(path)
    destination.write_text(
        "Automatic mesh generated by httk\n" "0\n" f"{centering}\n" f"{values[0]} {values[1]} {values[2]}\n" "0 0 0\n",
        encoding="utf-8",
    )
    return destination


def read_incar(path: str | os.PathLike[str] = "INCAR") -> dict[str, str]:
    """Read the last value of each simple INCAR assignment."""

    result: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        content = re.split(r"[#!]", line, maxsplit=1)[0]
        for statement in content.split(";"):
            name, separator, value = statement.partition("=")
            key = name.strip().upper()
            if separator and key:
                result[key] = value.strip()
    return result


def _incar_statement_tag(statement: str) -> str | None:
    """Return the tag one ``NAME = VALUE`` statement assigns, if it assigns one."""

    name, separator, _ = statement.partition("=")
    key = name.strip().upper()
    return key if separator and key else None


def update_incar(
    values: Mapping[str, object],
    path: str | os.PathLike[str] = "INCAR",
) -> Path:
    """Atomically replace selected INCAR tags while preserving other lines.

    VASP allows several ``;``-separated assignments per line, and
    :func:`read_incar` reads them all, so an update has to rewrite the individual
    statements of a line rather than drop or keep the whole line: updating
    ``ISYM`` in ``ISPIN = 2 ; ISYM = 2`` leaves ``ISPIN = 2`` and appends the new
    ``ISYM``, instead of leaving a line that still assigns the old value.
    """

    destination = Path(path)
    normalized = {str(name).strip().upper(): str(value) for name, value in values.items()}
    if not normalized or any(not name or re.fullmatch(r"[A-Z][A-Z0-9_]*", name) is None for name in normalized):
        raise ValueError("INCAR updates require valid nonempty tag names")
    kept: list[str] = []
    for line in destination.read_text(encoding="utf-8").splitlines():
        comment_match = re.search(r"[#!]", line)
        content = line if comment_match is None else line[: comment_match.start()]
        comment = "" if comment_match is None else line[comment_match.start() :]
        statements = content.split(";")
        if not any(_incar_statement_tag(item) in normalized for item in statements):
            # Nothing on this line is replaced, so it survives byte for byte.
            kept.append(line)
            continue
        surviving = [item.strip() for item in statements if _incar_statement_tag(item) not in normalized]
        remainder = " ; ".join(item for item in surviving if item)
        if remainder and comment:
            kept.append(f"{remainder} {comment}")
        elif remainder or comment:
            kept.append(remainder or comment)
    # Appended in tag order: the same set of updates then always produces the same
    # file, whatever order the caller's mapping happened to have.
    kept.extend(f"{name} = {value}" for name, value in sorted(normalized.items()))
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write("\n".join(kept) + "\n")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def _read_potential(path: Path) -> bytes:
    if path.name == "POTCAR":
        return path.read_bytes()
    if path.name == "POTCAR.gz":
        return gzip.decompress(path.read_bytes())
    if path.name == "POTCAR.bz2":
        return bz2.decompress(path.read_bytes())
    if path.name in {"POTCAR.xz", "POTCAR.lzma"}:
        return lzma.decompress(path.read_bytes())
    if path.name == "POTCAR.Z":
        executable = shutil.which("gzip") or shutil.which("uncompress")
        if executable is None:
            raise RuntimeError("reading legacy POTCAR.Z requires gzip or uncompress")
        completed = subprocess.run(
            [executable, "-cd", str(path)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if completed.returncode:
            raise ValueError(f"cannot decompress {path}: {completed.stderr.decode(errors='replace').strip()}")
        return completed.stdout
    raise ValueError(f"unsupported pseudopotential compression: {path}")


@dataclass(frozen=True)
class PotcarChoice:
    """Which pseudopotential of a library one species was given.

    The suffix policy of :func:`assemble_potcar` silently prefers one PAW variant
    over another, and the choice changes the numbers a calculation produces, so
    every choice is recorded: the variant directory, the suffix that selected it,
    the full source path, its digest, and the ``TITEL`` the potential names
    itself with.
    """

    species: str
    variant: str
    suffix: str
    source: Path
    sha256: str
    titel: str | None

    def as_mapping(self) -> dict[str, object]:
        return {
            "species": self.species,
            "variant": self.variant,
            "suffix": self.suffix,
            "source": str(self.source),
            "sha256": self.sha256,
            "titel": self.titel,
        }


@dataclass(frozen=True)
class PotcarAssembly:
    """One assembled POTCAR and the provenance of every piece in it."""

    path: Path
    library: Path
    choices: tuple[PotcarChoice, ...]
    provenance: Path

    def as_mapping(self) -> dict[str, object]:
        return {
            "format": "httk-vasp-potcar-provenance",
            "format_version": 1,
            "potcar": str(self.path),
            "library": str(self.library),
            "assembled_at": utc_now(),
            "potentials": [item.as_mapping() for item in self.choices],
        }


def assemble_potcar(
    library: str | os.PathLike[str],
    *,
    poscar: str | os.PathLike[str] = "POSCAR",
    output: str | os.PathLike[str] = "POTCAR",
    suffix_preference: Iterable[str] = ("_3", "_2", "_d", "_sv", "_pv", "", "_h", "_s"),
    provenance: str | os.PathLike[str] | None = None,
) -> PotcarAssembly:
    """Assemble POTCAR from explicit species and a configurable suffix policy.

    The suffix policy decides which PAW variant each species gets, so the result
    describes what it chose: the returned :class:`PotcarAssembly` names every
    potential, and the same record is written next to the POTCAR as
    ``<POTCAR>.provenance.json`` unless *provenance* names another file.
    """

    root = Path(library).expanduser().resolve()
    header = read_poscar_header(poscar)
    suffixes = tuple(suffix_preference)
    pieces: list[bytes] = []
    choices: list[PotcarChoice] = []
    for species in header.species:
        found: Path | None = None
        variant = ""
        for suffix in suffixes:
            directory = root / f"{species}{suffix}"
            for filename in ("POTCAR", "POTCAR.gz", "POTCAR.bz2", "POTCAR.xz", "POTCAR.lzma", "POTCAR.Z"):
                candidate = directory / filename
                if candidate.is_file():
                    found = candidate
                    variant = suffix
                    break
            if found is not None:
                break
        if found is None:
            raise FileNotFoundError(f"no pseudopotential found for species {species!r} below {root}")
        content = _read_potential(found)
        pieces.append(content)
        titel = re.search(r"^\s*TITEL\s*=\s*(.+)$", content.decode("utf-8", errors="replace"), re.MULTILINE)
        choices.append(
            PotcarChoice(
                species,
                f"{species}{variant}",
                variant,
                found,
                sha256_file(found),
                None if titel is None else titel.group(1).strip(),
            )
        )
    destination = Path(output)
    destination.write_bytes(b"".join(pieces))
    record = (
        Path(provenance) if provenance is not None else destination.with_name(f"{destination.name}.provenance.json")
    )
    assembly = PotcarAssembly(destination, root, tuple(choices), record)
    write_json_atomic(record, assembly.as_mapping())
    _LOGGER.info(
        "assembled %s from %s: %s",
        destination,
        root,
        ", ".join(f"{item.species}->{item.variant}" for item in choices),
    )
    return assembly


def last_oszicar_energy(path: str | os.PathLike[str] = "OSZICAR") -> float | None:
    """Return the final ``E0`` value, or ``None`` when it is absent."""

    found: float | None = None
    pattern = re.compile(r"\bE0=\s*([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?)")
    for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line)
        if match is not None:
            found = float(match.group(1))
    return found


def contcar_to_poscar(
    contcar: str | os.PathLike[str] = "CONTCAR",
    *,
    reference: str | os.PathLike[str] = "POSCAR",
    output: str | os.PathLike[str] = "POSCAR",
) -> Path:
    """Replace CONTCAR's comment with the reference POSCAR comment."""

    reference_lines = Path(reference).read_text(encoding="utf-8").splitlines(keepends=True)
    lines = Path(contcar).read_text(encoding="utf-8").splitlines(keepends=True)
    if not reference_lines or not lines:
        raise ValueError("CONTCAR and reference POSCAR must both be nonempty")
    destination = Path(output)
    destination.write_text(reference_lines[0].rstrip("\r\n") + "\n" + "".join(lines[1:]), encoding="utf-8")
    return destination


def normalize_poscar_handedness(path: str | os.PathLike[str] = "POSCAR") -> Path:
    """Make a left-handed POSCAR lattice right-handed without moving sites."""

    destination = Path(path)
    lines = destination.read_text(encoding="utf-8").splitlines()
    header = read_poscar_header(destination)
    determinant = _dot(header.lattice[0], _cross(header.lattice[1], header.lattice[2]))
    if determinant >= 0:
        return destination
    for index in range(2, 5):
        values = [-float(item) for item in lines[index].split()]
        lines[index] = " ".join(f"{value:.16g}" for value in values)
    _write_text_atomic(destination, "\n".join(lines) + "\n")
    return destination


def scale_poscar_lattice(
    factor: float,
    path: str | os.PathLike[str] = "POSCAR",
) -> Path:
    """Multiply POSCAR's universal linear scale by a positive factor."""

    if not math.isfinite(factor) or factor <= 0:
        raise ValueError("lattice scale factor must be positive and finite")
    destination = Path(path)
    lines = destination.read_text(encoding="utf-8").splitlines()
    if len(lines) < 2:
        raise ValueError("POSCAR has no scale line")
    try:
        scale = float(lines[1].split()[0])
    except (IndexError, ValueError) as exc:
        raise ValueError("POSCAR has an invalid scale line") from exc
    lines[1] = f"{scale * factor:.16g}"
    _write_text_atomic(destination, "\n".join(lines) + "\n")
    return destination


def derive_seed(entropy: str) -> int:
    """Derive one reproducible 63-bit seed from a caller-supplied string.

    The string is the caller's own identity of the perturbation — an attempt
    ordinal, a job key, a remedy step — so the same attempt always derives the
    same seed and two different attempts derive different ones, without anything
    reading a clock or a global random state.
    """

    if not isinstance(entropy, str) or not entropy:
        raise ValueError("seed entropy must be a nonempty string")
    return int.from_bytes(hashlib.sha256(entropy.encode("utf-8")).digest()[:8], "big") >> 1


def rattle_poscar(
    path: str | os.PathLike[str] = "POSCAR",
    *,
    amplitude: float = 0.01,
    seed: int | None = None,
    entropy: str | None = None,
) -> Path:
    """Apply a deterministic bounded perturbation to POSCAR site coordinates.

    A perturbation has to be reproducible *and* different between two attempts of
    the same calculation, so this function never invents entropy of its own:
    either *seed* names the stream explicitly, or *entropy* is a string
    :func:`derive_seed` turns into one — typically something attempt-derived, such
    as ``f"{job_key}:{attempt_ordinal}"``. Giving neither is refused rather than
    silently repeated, because two retries that rattle identically are two
    identical calculations.
    """

    if not math.isfinite(amplitude) or amplitude < 0:
        raise ValueError("rattle amplitude must be finite and nonnegative")
    if seed is None and entropy is None:
        raise ValueError(
            "rattle_poscar needs an explicit seed or an entropy string to derive one from; "
            "pass entropy=f'{job_key}:{attempt_ordinal}' to make every retry differ reproducibly"
        )
    if entropy is not None:
        seed = derive_seed(entropy if seed is None else f"{seed}:{entropy}")
    destination = Path(path)
    lines = destination.read_text(encoding="utf-8").splitlines()
    header = read_poscar_header(destination)
    coordinate_start = 7
    if len(lines) > coordinate_start and lines[coordinate_start].strip().lower().startswith("s"):
        coordinate_start += 1
    if len(lines) <= coordinate_start:
        raise ValueError("POSCAR has no coordinate mode")
    coordinate_start += 1
    total = sum(header.counts)
    if len(lines) < coordinate_start + total:
        raise ValueError("POSCAR has fewer coordinate rows than declared atoms")
    generator = random.Random(seed)
    for index in range(coordinate_start, coordinate_start + total):
        fields = lines[index].split()
        if len(fields) < 3:
            raise ValueError(f"invalid POSCAR coordinate row {index + 1}")
        values = [float(fields[column]) + generator.uniform(-amplitude, amplitude) for column in range(3)]
        lines[index] = " ".join([*(f"{value:.14f}" for value in values), *fields[3:]])
    _write_text_atomic(destination, "\n".join(lines) + "\n")
    return destination


def _write_text_atomic(path: Path, value: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(value)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def calculate_nbands(
    *,
    poscar: str | os.PathLike[str] = "POSCAR",
    potcar: str | os.PathLike[str] = "POTCAR",
    incar: str | os.PathLike[str] = "INCAR",
    divisor: int | None = None,
) -> int:
    """Calculate a conservative VASP band count from input metadata.

    The count is a heuristic, deliberately generous margin over VASP's own
    default of roughly half the valence electrons plus half the atom count: too
    few bands is a run that stops with ``TOO FEW BANDS`` or converges to the wrong
    state, while a handful of extra empty bands costs only time. The spin-polarized
    branch adds ``0.6 * electrons`` plus a magnetization- or atom-count-derived
    margin, and the maximum of the candidate formulas wins; the numbers themselves
    are empirical defaults carried over from *httk* v1. The result is rounded up to
    an even number, and to a multiple of *divisor* when a parallel band divisor
    (``NPAR``) is in play, because VASP would otherwise round NBANDS up itself and
    report a changed value.
    """

    if divisor is not None and (isinstance(divisor, bool) or divisor < 1):
        raise ValueError("NBANDS divisor must be a positive integer")
    header = read_poscar_header(poscar)
    text = Path(potcar).read_text(encoding="utf-8", errors="replace")
    zvals = [float(value) for value in re.findall(r"\bZVAL\s*=\s*([-+0-9.Ee]+)", text)]
    if len(zvals) < len(header.counts):
        raise ValueError("POTCAR contains fewer ZVAL entries than POSCAR species")
    electrons = sum(count * zval for count, zval in zip(header.counts, zvals, strict=False))
    tags = read_incar(incar)
    ispin = int(float(tags.get("ISPIN", "1")))
    natoms = max(6, sum(header.counts))
    candidates: tuple[int, ...]
    if ispin == 2:
        magnetic = _expanded_sum(tags.get("MAGMOM", suggested_magnetic_moments(poscar)))
        candidates = (
            math.floor(0.6 * electrons + 1) + math.ceil(natoms / 2),
            math.floor(0.6 * electrons + 1) + math.ceil(abs(magnetic) / 2),
            math.floor(0.6 * electrons + 1) + 20,
        )
    else:
        candidates = (
            math.floor(electrons / 2 + 2) + math.ceil(natoms / 2),
            math.ceil(electrons / 2) + 20,
        )
    result = max(candidates)
    if result % 2:
        result += 1
    if divisor is not None and result % divisor:
        result += divisor - result % divisor
    return result


def _expanded_sum(value: str) -> float:
    total = 0.0
    for item in value.split():
        if "*" in item:
            count, number = item.split("*", 1)
            total += int(count) * float(number)
        else:
            total += float(item)
    return total


def last_vasprun_volume(path: str | os.PathLike[str] = "vasprun.xml") -> float | None:
    """Return the final volume reported by ``vasprun.xml``."""

    found: float | None = None
    pattern = re.compile(r'<i\s+name=["\']volume["\']>\s*([-+0-9.Ee]+)\s*</i>')
    for match in pattern.finditer(Path(path).read_text(encoding="utf-8", errors="replace")):
        found = float(match.group(1))
    return found


def outcar_potim(path: str | os.PathLike[str] = "OUTCAR") -> float | None:
    """Return the last optimizer step size from OUTCAR."""

    found: float | None = None
    pattern = re.compile(r"^\s*opt step\s*=\s*([-+0-9.Ee]+)", re.MULTILINE)
    for match in pattern.finditer(Path(path).read_text(encoding="utf-8", errors="replace")):
        found = float(match.group(1))
    return found


def outcar_plane_wave_count(path: str | os.PathLike[str] = "OUTCAR") -> int | None:
    """Return OUTCAR's maximum plane-wave count."""

    match = re.search(
        r"^\s*maximum number of plane-waves\s*:\s*([0-9]+)",
        Path(path).read_text(encoding="utf-8", errors="replace"),
        re.MULTILINE,
    )
    return None if match is None else int(match.group(1))


def potcar_summary(
    path: str | os.PathLike[str] = "POTCAR",
    output: str | os.PathLike[str] = "POTCAR.summary",
) -> Path:
    """Write a non-potential metadata summary suitable for logs."""

    text = Path(path).read_text(encoding="utf-8", errors="replace")
    blocks: list[str] = []
    for block in re.split(r"(?=^\s*TITEL\s*=)", text, flags=re.MULTILINE):
        if "TITEL" not in block:
            continue
        keep: list[str] = []
        for line in block.splitlines():
            if re.match(r"^\s*(TITEL|POMASS|ZVAL|ENMAX|ENMIN|LEXCH|EATOM)\b", line):
                keep.append(line.rstrip())
        if keep:
            blocks.append("\n".join(keep))
    destination = Path(output)
    _write_text_atomic(destination, "\n\n".join(blocks) + ("\n" if blocks else ""))
    return destination


def clean_outcar(
    path: str | os.PathLike[str] = "OUTCAR",
    output: str | os.PathLike[str] = "OUTCAR.cleaned",
) -> Path:
    """Remove the largest reproducible k-point detail blocks from OUTCAR."""

    lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    result: list[str] = []
    skipping = False
    starts = (
        "k-points in units of 2pi/SCALE and weight:",
        "Following reciprocal coordinates:",
        "Following cartesian coordinates:",
        "k-points in reciprocal lattice and weights",
    )
    for line in lines:
        stripped = line.strip()
        if any(stripped.startswith(item) for item in starts) or re.match(r"^k-point\s+\d+\s+.*plane waves", stripped):
            result.append("VASP_CLEAN_OUTCAR: removed k-point detail block")
            skipping = True
            continue
        if skipping:
            if not stripped:
                skipping = False
            continue
        result.append(line)
    destination = Path(output)
    _write_text_atomic(destination, "\n".join(result) + "\n")
    return destination


def validate_vasp_workdir(
    path: str | os.PathLike[str] = ".",
    *,
    maximum_length: int = 240,
) -> Path:
    """Validate VASP's conservative absolute-path length constraint."""

    resolved = Path(path).resolve()
    if maximum_length < 1:
        raise ValueError("maximum path length must be positive")
    if len(os.fspath(resolved).encode()) > maximum_length:
        raise ValueError(f"VASP workdir path exceeds {maximum_length} bytes: {resolved}")
    return resolved


def clean_vasp_outputs(
    directory: str | os.PathLike[str] = ".",
    *,
    keep: Iterable[str] = (),
    also_remove: Iterable[str] = (),
) -> tuple[Path, ...]:
    """Remove standard rerun outputs while preserving declared names.

    The files in :data:`VASP_RESTART_ARTIFACTS` — ``CONTCAR`` and
    ``vasp-run-report.json`` — are kept even without *keep*, because they are what
    the remedy machinery and restart promotion read: a pre-run cleanup that
    deletes them destroys the evidence of the run it is cleaning up after. Name
    them in *also_remove* to delete them anyway.
    """

    root = Path(directory)
    retained = frozenset(keep) | (frozenset(VASP_RESTART_ARTIFACTS) - frozenset(also_remove))
    names = (
        "CHG",
        "CHGCAR",
        "CONTCAR",
        "DOSCAR",
        "EIGENVAL",
        "IBZKPT",
        "OSZICAR",
        "OUTCAR",
        "PCDAT",
        "PROCAR",
        "REPORT",
        "vasprun.xml",
        "WAVECAR",
        "XDATCAR",
        "vasp.out",
        "vasp.err",
        "vasp-run-report.json",
    )
    removed: list[Path] = []
    for name in names:
        path = root / name
        if name in retained or not path.exists():
            continue
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"refusing to clean non-regular VASP output: {path}")
        path.unlink()
        removed.append(path)
    return tuple(removed)


@dataclass(frozen=True)
class VaspPreparationOptions:
    """Options for dependency-free VASP input preparation.

    ``accuracy_per_atom`` is the target total-energy accuracy in eV per atom:
    ``EDIFFG`` is set to that budget for the whole cell and ``EDIFF`` to the same
    budget divided by ``ediff_margin``, so the electronic loop converges roughly
    one and a half orders of magnitude tighter than the ionic loop it feeds. The
    margin of ``33`` is a heuristic, empirical default carried over from *httk*
    v1, not a derived quantity.
    """

    kpoint_density: float = 20.0
    centering: str = DEFAULT_KPOINT_CENTERING
    accuracy_per_atom: float | None = 0.001
    ediff_margin: float = 33.0
    pseudopotential_library: str | os.PathLike[str] | None = None
    parallel_tag: str | None = None
    parallel_value: int | None = None
    normalize_handedness: bool = True
    incar_tags: Mapping[str, object] = field(default_factory=dict)


def prepare_vasp_inputs(
    options: VaspPreparationOptions = VaspPreparationOptions(),
    *,
    directory: str | os.PathLike[str] = ".",
) -> dict[str, object]:
    """Prepare POSCAR, POTCAR, KPOINTS, and INCAR with recorded choices.

    ``incar_tags`` is applied to the INCAR first and wins over everything derived
    afterwards: the derived values are defaults for what the caller did not say,
    so an explicit ``EDIFF``, ``MAGMOM``, or ``NBANDS`` survives preparation, and
    an explicit ``ISPIN`` is what the band-count heuristic reads.
    """

    root = validate_vasp_workdir(directory)
    poscar = root / "POSCAR"
    incar = root / "INCAR"
    if options.normalize_handedness:
        normalize_poscar_handedness(poscar)
    explicit = {str(name).strip().upper(): value for name, value in options.incar_tags.items()}
    if explicit:
        update_incar(explicit, incar)
    potcar: dict[str, object] | None = None
    if options.pseudopotential_library is not None:
        potcar = assemble_potcar(
            options.pseudopotential_library,
            poscar=poscar,
            output=root / "POTCAR",
        ).as_mapping()
    grid = automatic_kpoint_grid(options.kpoint_density, poscar=poscar)
    write_automatic_kpoints(grid, root / "KPOINTS", centering=options.centering)
    updates: dict[str, object] = {}
    header = read_poscar_header(poscar)
    if options.accuracy_per_atom is not None:
        atoms = sum(header.counts)
        updates["EDIFF"] = max(options.accuracy_per_atom * atoms / options.ediff_margin, 1e-6)
        updates["EDIFFG"] = max(options.accuracy_per_atom * atoms, 1e-4)
    current = read_incar(incar)
    if "MAGMOM" not in current:
        updates["MAGMOM"] = suggested_magnetic_moments(poscar)
    if "NBANDS" not in current and (root / "POTCAR").is_file():
        divisor = options.parallel_value if options.parallel_tag == "NPAR" else None
        updates["NBANDS"] = calculate_nbands(poscar=poscar, potcar=root / "POTCAR", incar=incar, divisor=divisor)
    if options.parallel_tag is not None:
        if options.parallel_tag not in {"NPAR", "NCORE", "KPAR"}:
            raise ValueError("parallel_tag must be NPAR, NCORE, or KPAR")
        if options.parallel_value is None or options.parallel_value < 1:
            raise ValueError("parallel_value must be a positive integer")
        updates[options.parallel_tag] = options.parallel_value
    updates = {name: value for name, value in updates.items() if name not in explicit}
    if updates:
        update_incar(updates, incar)
    result: dict[str, object] = {"kpoint_grid": grid, "incar_updates": updates, "incar_tags": explicit}
    if potcar is not None:
        result["potcar"] = potcar
    return result


_VASP_PATTERNS: tuple[tuple[re.Pattern[str], str, str, bool], ...] = (
    (re.compile(r"chargedensity file is incomplete", re.I), "chgcar_incomplete", "error", True),
    (re.compile(r"ZPOTRF failed", re.I), "zpotrf", "fatal", True),
    (re.compile(r"FEXCF: supplied exchange-correlation table", re.I), "fexcf", "error", True),
    (re.compile(r"Reciprocal lattice and k-lattice belong to different class", re.I), "kpoints_class", "error", True),
    (
        re.compile(r"Tetrahedron method fails|Fatal error.*k-mesh|unable to match k-point|TETIRR needs", re.I),
        "tetrahedron_kpoints",
        "error",
        True,
    ),
    (re.compile(r"inverse of rotation matrix was not found", re.I), "inverse_rotation", "error", True),
    (re.compile(r"Found some non-integer element in rotation matrix", re.I), "rotation_matrix", "error", True),
    (re.compile(r"BRMIX: very serious problems", re.I), "brmix", "warning", False),
    (re.compile(r"Could not get correct shifts", re.I), "kpoint_shifts", "warning", False),
    (re.compile(r"REAL_OPTLAY: internal error", re.I), "real_optlay", "error", True),
    (re.compile(r"(?:internal ERROR RSPHER|RSPHER: internal ERROR)", re.I), "rspher", "fatal", True),
    (re.compile(r"DENTET: can't reach specified precision", re.I), "dentet", "warning", False),
    (re.compile(r"TOO FEW BANDS", re.I), "too_few_bands", "error", False),
    (re.compile(r"triple product of the basis vectors", re.I), "triple_product", "fatal", True),
    (re.compile(r"BRIONS problems: POTIM should be increased", re.I), "brions", "warning", False),
    (re.compile(r"small aliasing.*errors", re.I), "aliasing", "warning", False),
    (re.compile(r"distance between some ions is very small", re.I), "ions_too_close", "fatal", True),
    (re.compile(r"set LREAL=.FALSE", re.I), "lreal_false", "error", True),
    (re.compile(r"number of cells and number of vectors did not agree", re.I), "pricell", "error", True),
    (re.compile(r"internal error in RAD_INT", re.I), "radint", "fatal", True),
    (re.compile(r"internal ERROR in NONLR_ALLOC", re.I), "nonlr_alloc", "fatal", True),
    (re.compile(r"Error EDDDAV: Call to ZHEGV failed", re.I), "edddav_zhegv", "error", False),
    (re.compile(r"CNORMN: search vector ill defined", re.I), "cnormn", "warning", False),
    (re.compile(r"ZBRENT: fatal error in bracketing", re.I), "zbrent_bracketing", "error", False),
    (re.compile(r"One of the lattice vectors is very long", re.I), "lattice_vector_too_long", "fatal", True),
)


class _VaspMonitor:
    def __call__(self, event: SourceEvent) -> Sequence[Diagnostic]:
        if event.event != "line" or event.line is None:
            return ()
        result: list[Diagnostic] = []
        allocation = re.search(r"total allocation\s*:\s*([0-9]+)\s*KBytes", event.line, re.I)
        if allocation is not None and int(allocation.group(1)) > 500_000:
            result.append(
                Diagnostic(
                    "realspace_allocation_too_large",
                    "fatal",
                    "VASP requested more than 500000 KBytes for real-space projection",
                    event.source,
                    event.line,
                    True,
                )
            )
        for pattern, code, severity, stop in _VASP_PATTERNS:
            if pattern.search(event.line):
                result.append(
                    Diagnostic(
                        code,
                        severity,  # type: ignore[arg-type]
                        event.line.strip(),
                        event.source,
                        event.line,
                        stop,
                    )
                )
        return result


def diagnose_vasp_files(directory: str | os.PathLike[str] = ".") -> tuple[Diagnostic, ...]:
    """Diagnose completion and final convergence from VASP output files."""

    root = Path(directory)
    diagnostics: list[Diagnostic] = []
    outcar = root / "OUTCAR"
    oszicar = root / "OSZICAR"
    outcar_text = outcar.read_text(encoding="utf-8", errors="replace") if outcar.is_file() else ""
    if (
        re.search(
            r"General timing and accounting information(?:s)? for this job:",
            outcar_text,
            re.I,
        )
        is None
    ):
        diagnostics.append(Diagnostic("incomplete_outcar", "error", "OUTCAR has no normal completion footer", "OUTCAR"))
    nelm_match = re.search(r"^\s*NELM\s*=\s*([0-9]+)", outcar_text, re.MULTILINE)
    nsw_match = re.search(r"^\s*NSW\s*=\s*([0-9]+)", outcar_text, re.MULTILINE)
    nelm = None if nelm_match is None else int(nelm_match.group(1))
    nsw = None if nsw_match is None else int(nsw_match.group(1))
    if oszicar.is_file():
        electronic_step = 0
        ionic_steps = 0
        last_energy: float | None = None
        for line in oszicar.read_text(encoding="utf-8", errors="replace").splitlines():
            match = re.match(r"^[A-Za-z]+:\s+([0-9]+)\s+", line)
            if match:
                electronic_step = int(match.group(1))
            ionic = re.match(r"^\s*[0-9]+\s+F=\s*([-+0-9.Ee]+)", line)
            if ionic:
                ionic_steps += 1
                last_energy = float(ionic.group(1))
        if nelm is not None and electronic_step >= nelm:
            diagnostics.append(
                Diagnostic("electronic_nonconvergence", "error", "final electronic step reached NELM", "OSZICAR")
            )
        if nsw is not None and nsw > 1 and ionic_steps >= nsw:
            diagnostics.append(Diagnostic("ionic_nonconvergence", "error", "ionic steps reached NSW", "OSZICAR"))
        if last_energy is not None and last_energy > 0:
            diagnostics.append(Diagnostic("positive_final_energy", "error", "final free energy is positive", "OSZICAR"))
    return tuple(diagnostics)


@dataclass(frozen=True)
class VaspRunReport:
    """Classified result of one supervised VASP execution."""

    process: ProcessReport
    classification: str
    diagnostics: tuple[Diagnostic, ...]

    def as_mapping(self) -> dict[str, object]:
        return {
            "format": "httk-vasp-run-report",
            "format_version": 1,
            "process": self.process.as_mapping(),
            "classification": self.classification,
            "diagnostics": [item.as_mapping() for item in self.diagnostics],
        }

    def write(self, path: str | os.PathLike[str]) -> Path:
        destination = Path(path)
        write_json_atomic(destination, self.as_mapping())
        return destination


def run_vasp(
    argv: Sequence[str],
    *,
    directory: str | os.PathLike[str] = ".",
    timeout: float | None = None,
    termination_grace: float = 10.0,
    report_path: str | os.PathLike[str] = "vasp-run-report.json",
) -> VaspRunReport:
    """Run VASP with live VASP-5/6 diagnostics and a structured report."""

    root = Path(directory).resolve()
    supervisor = ProcessSupervisor(
        monitors=(_VaspMonitor(),),
        follow=(
            FollowSource(root / "OSZICAR", "OSZICAR"),
            FollowSource(root / "OUTCAR", "OUTCAR"),
        ),
    )
    process = supervisor.run(
        argv,
        timeout=timeout,
        cwd=root,
        termination_grace=termination_grace,
        stdout_path=root / "vasp.out",
        stderr_path=root / "vasp.err",
    )
    diagnostics = _deduplicate((*process.diagnostics, *diagnose_vasp_files(root)))
    if process.timed_out:
        classification = "timeout"
    elif any(item.stop for item in diagnostics):
        classification = "diagnosed_stop"
    elif process.returncode:
        classification = "process_failure"
    elif any(
        item.code in {"electronic_nonconvergence", "ionic_nonconvergence", "positive_final_energy"}
        for item in diagnostics
    ):
        classification = "nonconverged"
    elif any(item.code == "incomplete_outcar" for item in diagnostics):
        classification = "process_failure"
    else:
        classification = "completed"
    report = VaspRunReport(process, classification, diagnostics)
    report.write(root / report_path)
    return report


def _deduplicate(values: Sequence[Diagnostic]) -> tuple[Diagnostic, ...]:
    result: list[Diagnostic] = []
    seen: set[tuple[str, str, str | None]] = set()
    for item in values:
        key = item.code, item.source, item.evidence
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)


type RemedyChange = tuple[str, object]
type RemedySequence = tuple[tuple[RemedyChange, ...], ...]

_REVIEWED_SEQUENCES: dict[str, RemedySequence] = {
    "kpoints_class": (
        (("bump_kpoints", 1),),
        (("centering", "Gamma"),),
        (("bump_kpoints", 1), ("centering", "Gamma")),
        (("equal_kpoints", True),),
        (("bump_kpoints", 1), ("equal_kpoints", True)),
        (("incar.ISYM", 0),),
    ),
    "dentet": (
        (("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),
        (("bump_kpoints", 1),),
        (("bump_kpoints", 1), ("centering", "Gamma")),
        (("incar.NEDOS", 1000),),
    ),
    "kpoint_shifts": ((("centering", "Gamma"),),),
    "lreal_false": ((("incar.LREAL", ".FALSE."),),),
    "inverse_rotation": ((("incar.ISYM", 0),),),
    "rotation_matrix": ((("incar.ISYM", 0),),),
    "fexcf": (
        (("incar.ICHARG", 2), ("incar.AMIX", 0.10), ("incar.BMIX", 0.01)),
        (("incar.ICHARG", 2), ("incar.BMIX", 3.0), ("incar.AMIN", 0.01)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.4)),
        (("incar.ALGO", "Damped"), ("incar.TIME", 0.05)),
    ),
    "electronic_nonconvergence": (
        (("incar.ICHARG", 2), ("incar.AMIX", 0.10), ("incar.BMIX", 0.01)),
        (("incar.ICHARG", 2), ("incar.BMIX", 3.0), ("incar.AMIN", 0.01)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.4)),
        (("incar.ALGO", "All"), ("incar.TIME", 0.05)),
        (("incar.ALGO", "Damped"), ("incar.TIME", 0.05)),
    ),
    "pricell": (
        (("incar.SYMPREC", 1e-4),),
        (("incar.SYMPREC", 1e-6),),
        (("incar.ISYM", 0),),
    ),
    "tetrahedron_kpoints": ((("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),),
    "zbrent_bracketing": ((("scale_ediff", 0.1),), (("scale_ediff", 0.1),)),
    "real_optlay": ((("incar.LREAL", ".FALSE."),),),
    "too_few_bands": ((("bump_bands", 2),),),
    "ionic_nonconvergence": ((("contcar_to_poscar", True),),),
    "zpotrf": (
        (("scale_lattice", 1.05),),
        (("bump_kpoints", 1),),
        (("bump_bands", 2),),
        (("incar.ISMEAR", 0), ("incar.SIGMA", 0.05)),
        (("incar.ISYM", 0),),
    ),
    "ions_too_close": ((("scale_lattice", 1.05),),),
}
_REVIEWED_PRECEDENCE = (
    "pricell",
    "zpotrf",
    "tetrahedron_kpoints",
    "zbrent_bracketing",
    "real_optlay",
    "ions_too_close",
    "nonlr_alloc",
    "kpoints_class",
    "dentet",
    "kpoint_shifts",
    "lreal_false",
    "inverse_rotation",
    "rotation_matrix",
    "fexcf",
    "electronic_nonconvergence",
    "ionic_nonconvergence",
    "too_few_bands",
)
_REVIEWED_REFUSALS = {"nonlr_alloc": "memory allocation failure has no safe input remedy"}


@dataclass(frozen=True)
class RemedyPolicy:
    """One named, ordered ladder of bounded remedies.

    A policy is data, not code: *sequences* maps a diagnosed problem to the
    escalating steps tried for it, *precedence* orders the problems so one
    diagnosis is remedied at a time, and *refusals* names the problems this policy
    deliberately has no input remedy for. A group with its own reviewed practice
    registers its own policy with :func:`register_remedy_policy` instead of
    editing this module.
    """

    name: str
    sequences: Mapping[str, RemedySequence]
    precedence: tuple[str, ...]
    refusals: Mapping[str, str] = field(default_factory=dict)


_REMEDY_POLICIES: dict[str, RemedyPolicy] = {}

#: Every remedy operation :func:`apply_vasp_remedy` implements, besides the
#: ``incar.<TAG>`` form that assigns one INCAR tag.
REMEDY_OPERATIONS = (
    "bump_bands",
    "bump_kpoints",
    "centering",
    "contcar_to_poscar",
    "equal_kpoints",
    "scale_ediff",
    "scale_lattice",
)


def register_remedy_policy(
    name: str,
    sequences: Mapping[str, Sequence[Sequence[tuple[str, object]]]],
    precedence: Sequence[str],
    *,
    refusals: Mapping[str, str] | None = None,
    replace: bool = False,
) -> RemedyPolicy:
    """Register one named remedy policy and return the normalized result.

    Every problem named in *sequences* must also appear in *precedence*, which is
    what decides which of several simultaneous diagnostics is acted on, and every
    change must spell one supported remedy operation, so a policy that cannot be
    executed is refused when it is registered rather than when a run needs it.
    """

    if not isinstance(name, str) or not name:
        raise ValueError("a remedy policy name must be a nonempty string")
    if name in _REMEDY_POLICIES and not replace:
        raise ValueError(f"remedy policy {name!r} is already registered; pass replace to redefine it")
    order = tuple(precedence)
    if len(set(order)) != len(order):
        raise ValueError(f"remedy policy {name!r} lists a problem twice in its precedence")
    normalized: dict[str, RemedySequence] = {}
    for problem, sequence in sequences.items():
        if problem not in order:
            raise ValueError(f"remedy policy {name!r} has a sequence for {problem!r}, which its precedence omits")
        steps: list[tuple[RemedyChange, ...]] = []
        for step in sequence:
            changes = tuple((str(operation), value) for operation, value in step)
            if not changes:
                raise ValueError(f"remedy policy {name!r} has an empty remedy step for {problem!r}")
            for operation, _ in changes:
                if operation not in REMEDY_OPERATIONS and not operation.startswith("incar."):
                    raise ValueError(
                        f"remedy policy {name!r} uses unsupported remedy operation {operation!r}; "
                        f"supported operations: {', '.join(REMEDY_OPERATIONS)}, or incar.<TAG>"
                    )
            steps.append(changes)
        normalized[problem] = tuple(steps)
    for problem in refusals or {}:
        if problem not in order:
            raise ValueError(f"remedy policy {name!r} refuses {problem!r}, which its precedence omits")
        if problem in normalized:
            raise ValueError(f"remedy policy {name!r} both refuses {problem!r} and has a remedy sequence for it")
    policy = RemedyPolicy(name, normalized, order, dict(refusals or {}))
    _REMEDY_POLICIES[name] = policy
    return policy


def remedy_policy_names() -> tuple[str, ...]:
    """Return the names of every registered remedy policy, in registration order."""

    return tuple(_REMEDY_POLICIES)


def remedy_policy(name: str) -> RemedyPolicy:
    """Return one registered remedy policy, naming the alternatives if absent."""

    policy = _REMEDY_POLICIES.get(name)
    if policy is None:
        raise ValueError(
            f"unknown VASP remedy policy {name!r}; registered policies: {', '.join(_REMEDY_POLICIES) or 'none'}"
        )
    return policy


register_remedy_policy(
    "reviewed-v1",
    _REVIEWED_SEQUENCES,
    _REVIEWED_PRECEDENCE,
    refusals=_REVIEWED_REFUSALS,
)


@dataclass(frozen=True)
class VaspRemedyDecision:
    """One explicit bounded remedy proposal."""

    policy: str
    problem: str
    step: int
    changes: tuple[tuple[str, object], ...]
    give_up: bool
    reason: str

    def as_mapping(self) -> dict[str, object]:
        return {
            "format": "httk-vasp-remedy-decision",
            "format_version": 1,
            "policy": self.policy,
            "problem": self.problem,
            "step": self.step,
            "changes": [{"operation": name, "value": value} for name, value in self.changes],
            "give_up": self.give_up,
            "reason": self.reason,
        }


def job_remedy_history_path(payload: str | os.PathLike[str]) -> Path:
    """Return the job-scoped remedy history file of one job payload.

    The escalation ladder is a property of the *job*, not of the directory one
    attempt happened to run in, so it lives beside the job state in
    ``<payload>/.httk-job/`` rather than in the workdir. A job with an isolated
    workdir therefore keeps escalating instead of silently starting the ladder
    from the beginning on every attempt.
    """

    return Path(payload).resolve() / JOB_STATE_DIRECTORY / REMEDY_HISTORY_NAME


def _remedy_history_file(root: Path, history_path: str | os.PathLike[str]) -> Path:
    """Resolve one history location the same way in planning and in application.

    A relative path is relative to the VASP directory in both, never to the
    process working directory: a plan and the application of that plan must read
    and write one file.
    """

    candidate = Path(history_path)
    return candidate if candidate.is_absolute() else root / candidate


def _empty_remedy_history() -> dict[str, Any]:
    return {"format": "httk-vasp-remedy-history", "format_version": 1, "attempts": {}, "events": []}


def _read_remedy_history(root: Path, history_path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read the remedy history, falling back to the pre-0.2 workdir location."""

    history_file = _remedy_history_file(root, history_path)
    if history_file.exists():
        return read_json(history_file)
    legacy = root / DEFAULT_REMEDY_HISTORY
    if legacy != history_file and legacy.exists():
        return read_json(legacy)
    return _empty_remedy_history()


def _remedy_obstacle(root: Path, changes: Sequence[RemedyChange]) -> str | None:
    """Return why *changes* cannot be applied in *root*, or ``None`` when they can.

    Planning and application ask exactly this question, so a planned remedy is by
    construction one that can be executed: proposing ``bump_bands`` for a
    calculation whose INCAR never set ``NBANDS`` would otherwise reach
    :func:`apply_vasp_remedy` and kill the runner with an uncaught error.
    """

    tags: dict[str, str] | None = read_incar(root / "INCAR") if (root / "INCAR").is_file() else None
    for operation, _ in changes:
        if operation.startswith("incar."):
            if tags is None:
                return f"{operation} needs an INCAR in {root}"
            continue
        if operation in {"scale_ediff", "bump_bands"}:
            name = "EDIFF" if operation == "scale_ediff" else "NBANDS"
            if tags is None:
                return f"{operation} needs an INCAR in {root}"
            if name not in tags:
                return f"{operation} needs {name} in INCAR, which does not set it"
            try:
                float(tags[name])
            except ValueError:
                return f"{operation} needs a numeric {name} in INCAR, which reads {tags[name]!r}"
            continue
        if operation in {"bump_kpoints", "equal_kpoints", "centering"}:
            kpoints = root / "KPOINTS"
            if not kpoints.is_file():
                return f"{operation} needs a staged KPOINTS in {root}"
            lines = kpoints.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(lines) < 4:
                return f"{operation} needs a KPOINTS with at least four lines"
            if operation != "centering":
                grid = lines[3].split()[:3]
                if len(grid) != 3 or any(re.fullmatch(r"[-+]?[0-9]+", item) is None for item in grid):
                    return f"{operation} needs an explicit three-integer KPOINTS grid line"
            continue
        if operation == "scale_lattice":
            poscar = root / "POSCAR"
            if not poscar.is_file():
                return f"{operation} needs a POSCAR in {root}"
            lines = poscar.read_text(encoding="utf-8", errors="replace").splitlines()
            if len(lines) < 2 or not lines[1].split():
                return f"{operation} needs a POSCAR scale line"
            try:
                float(lines[1].split()[0])
            except ValueError:
                return f"{operation} needs a numeric POSCAR scale line"
            continue
        if operation == "contcar_to_poscar":
            if not (root / "CONTCAR").is_file() or not (root / "CONTCAR").read_text(errors="replace").strip():
                return "contcar_to_poscar needs a nonempty CONTCAR from the interrupted run"
            if not (root / "POSCAR").is_file():
                return "contcar_to_poscar needs the reference POSCAR"
            continue
        return f"unsupported remedy operation: {operation}"
    return None


def plan_vasp_remedy(
    diagnostics: Sequence[Diagnostic],
    *,
    directory: str | os.PathLike[str] = ".",
    history_path: str | os.PathLike[str] = DEFAULT_REMEDY_HISTORY,
    policy: str = "reviewed-v1",
) -> VaspRemedyDecision:
    """Return, but do not apply, the next remedy of *policy*.

    The decision is validated against the real contents of *directory*: a ladder
    step whose changes cannot be executed there is skipped, and the first
    executable step wins. When nothing is left the decision gives up, so a runner
    only ever hands :func:`apply_vasp_remedy` a remedy it can perform.
    """

    resolved = remedy_policy(policy)
    root = Path(directory).resolve()
    history = _read_remedy_history(root, history_path)
    attempts = history.get("attempts")
    if not isinstance(attempts, Mapping):
        raise ValueError("VASP remedy history has invalid attempts")
    codes = {item.code for item in diagnostics}
    problem = next((item for item in resolved.precedence if item in codes), "unknown")
    step = int(attempts.get(problem, 0))
    refusal = resolved.refusals.get(problem)
    if refusal is not None:
        return VaspRemedyDecision(policy, problem, step, (), True, refusal)
    sequence = resolved.sequences.get(problem, ())
    skipped: list[str] = []
    while step < len(sequence):
        obstacle = _remedy_obstacle(root, sequence[step])
        if obstacle is None:
            return VaspRemedyDecision(policy, problem, step, sequence[step], False, "reviewed remedy available")
        _LOGGER.info("remedy step %d for %s is not applicable in %s: %s", step, problem, root, obstacle)
        skipped.append(f"step {step}: {obstacle}")
        step += 1
    reason = f"no further {policy} remedy is available"
    if skipped:
        reason = f"{reason} ({'; '.join(skipped)})"
    return VaspRemedyDecision(policy, problem, step, (), True, reason)


def apply_vasp_remedy(
    decision: VaspRemedyDecision,
    *,
    directory: str | os.PathLike[str] = ".",
    history_path: str | os.PathLike[str] = DEFAULT_REMEDY_HISTORY,
) -> Path:
    """Explicitly apply a proposed remedy through a replayable workdir batch.

    The history is recorded before the inputs change, so an application
    interrupted halfway advances the ladder rather than repeating one remedy for
    ever. A history file inside the VASP directory is written by the same
    replayable batch as the inputs; a job-scoped one, which the batch cannot
    reach, is written atomically just before the batch commits.
    """

    if decision.give_up:
        raise ValueError(f"cannot apply give-up decision: {decision.reason}")
    root = Path(directory).resolve()
    obstacle = _remedy_obstacle(root, decision.changes)
    if obstacle is not None:
        raise ValueError(f"cannot apply the {decision.policy} remedy for {decision.problem}: {obstacle}")
    staging = Path(tempfile.mkdtemp(prefix=".httk-vasp-remedy.", dir=root))
    try:
        changed: dict[str, Path] = {}
        before: dict[str, str] = {}
        incar_source = root / "INCAR"
        if incar_source.is_file():
            shutil.copy2(incar_source, staging / "INCAR")
            before["INCAR"] = sha256_file(incar_source)
        kpoints_source = root / "KPOINTS"
        if kpoints_source.is_file():
            shutil.copy2(kpoints_source, staging / "KPOINTS")
            before["KPOINTS"] = sha256_file(kpoints_source)
        poscar_source = root / "POSCAR"
        if poscar_source.is_file():
            shutil.copy2(poscar_source, staging / "POSCAR")
            before["POSCAR"] = sha256_file(poscar_source)
        incar_updates: dict[str, object] = {}
        for operation, value in decision.changes:
            if operation.startswith("incar."):
                incar_updates[operation.removeprefix("incar.")] = value
            elif operation == "scale_ediff":
                tags = read_incar(staging / "INCAR")
                if "EDIFF" not in tags:
                    raise ValueError("cannot scale absent EDIFF")
                incar_updates["EDIFF"] = float(tags["EDIFF"]) * float(str(value))
            elif operation == "bump_bands":
                tags = read_incar(staging / "INCAR")
                if "NBANDS" not in tags:
                    raise ValueError("cannot bump absent NBANDS")
                bands = int(tags["NBANDS"]) + int(str(value))
                if "NPAR" in tags:
                    divisor = int(tags["NPAR"])
                    if divisor < 1:
                        raise ValueError("NPAR must be positive")
                    remainder = bands % divisor
                    if remainder:
                        bands += divisor - remainder
                incar_updates["NBANDS"] = bands
            elif operation in {"bump_kpoints", "equal_kpoints", "centering"}:
                _modify_kpoints(staging / "KPOINTS", operation, value)
                changed["KPOINTS"] = staging / "KPOINTS"
            elif operation == "scale_lattice":
                scale_poscar_lattice(float(str(value)), staging / "POSCAR")
                changed["POSCAR"] = staging / "POSCAR"
            elif operation == "contcar_to_poscar":
                contcar_to_poscar(root / "CONTCAR", reference=root / "POSCAR", output=staging / "POSCAR")
                changed["POSCAR"] = staging / "POSCAR"
            else:
                raise ValueError(f"unsupported remedy operation: {operation}")
        if incar_updates:
            update_incar(incar_updates, staging / "INCAR")
            changed["INCAR"] = staging / "INCAR"
        history_file = _remedy_history_file(root, history_path)
        history = _read_remedy_history(root, history_path)
        attempts = dict(history.get("attempts", {}))
        attempts[decision.problem] = decision.step + 1
        events = list(history.get("events", []))
        evidence = [
            {
                "path": name,
                "before_sha256": before.get(name),
                "after_sha256": sha256_file(source),
            }
            for name, source in sorted(changed.items())
        ]
        events.append(
            {
                **decision.as_mapping(),
                "applied_at": utc_now(),
                "files": evidence,
            }
        )
        history.update({"attempts": attempts, "events": events})
        batch = ReplayableWorkdirBatch.create(root)
        for index, (name, source) in enumerate(sorted(changed.items())):
            batch.transaction.put_file(f"input-{index}", source, name)
        if history_file.is_relative_to(root):
            staged_history = staging / REMEDY_HISTORY_NAME
            write_json_atomic(staged_history, history)
            batch.transaction.put_file("remedy-history", staged_history, history_file.relative_to(root).as_posix())
        else:
            write_json_atomic(history_file, history)
        return batch.commit()
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _modify_kpoints(path: Path, operation: str, value: object) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 4:
        raise ValueError("KPOINTS has fewer than four lines")
    if operation == "centering":
        lines[2] = str(value)
    else:
        grid = [int(item) for item in lines[3].split()[:3]]
        if len(grid) != 3:
            raise ValueError("KPOINTS grid line is invalid")
        if operation == "bump_kpoints":
            grid = [item + int(str(value)) for item in grid]
        elif operation == "equal_kpoints" and bool(value):
            grid = [max(grid)] * 3
        lines[3] = " ".join(str(item) for item in grid)
    _write_text_atomic(path, "\n".join(lines) + "\n")
