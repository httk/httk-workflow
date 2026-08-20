"""POSCAR/INCAR/KPOINTS/POTCAR preparation and provenance helpers.

This is the input-side of the dependency-free VASP interface: reading and
rewriting the four VASP input files, assembling a POTCAR with recorded
provenance, and the :func:`prepare_vasp_inputs` entry point that ties them
together. Historical authorship is documented in ``v1_runtime/NOTICE``.
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

from httk.core.digests import sha256_file

from .._util import utc_now, write_json_atomic

_LOGGER = logging.getLogger(__name__)

#: The k-point centering every entry point starts from. Monkhorst-Pack is the
#: starting point rather than Gamma because the reviewed remedy ladder promotes
#: ``("centering", "Gamma")`` as the *fix* for the ``kpoints_class`` and
#: ``kpoint_shifts`` failure classes: a workflow that already starts at Gamma has
#: no such remedy left to apply.
DEFAULT_KPOINT_CENTERING = "Monkhorst-Pack"
KPOINT_CENTERINGS = ("Gamma", "Monkhorst-Pack")


@dataclass(frozen=True)
class PoscarHeader:
    """The VASP-5 header information needed by execution helpers.

    :param comment: Preserve the POSCAR comment line.
    :param scale: Preserve the universal POSCAR scale.
    :param lattice: Preserve the three lattice vectors.
    :param species: Preserve the VASP-5 species names.
    :param counts: Preserve the atom count for each species.
    """

    comment: str
    scale: float
    lattice: tuple[tuple[float, float, float], ...]
    species: tuple[str, ...]
    counts: tuple[int, ...]


def read_poscar_header(path: str | os.PathLike[str] = "POSCAR") -> PoscarHeader:
    """Read a VASP-5 POSCAR header without interpreting site coordinates.

    :param path: Read the POSCAR file at this path.
    :return: The parsed POSCAR header.
    :raises ValueError: If the header is too short or malformed.
    """

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

    :param path: Read the POSCAR comment and species counts from this path.
    :return: The explicit or generated MAGMOM value.
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
    """Calculate a reciprocal-length automatic grid.

    :param density: Scale reciprocal lengths by this positive density.
    :param poscar: Read lattice vectors from this POSCAR.
    :param minimum: Enforce this minimum value in each grid direction.
    :param equal: Use the largest direction value for all directions.
    :param bump: Add this nonnegative amount to each calculated direction.
    :return: The three automatic grid dimensions.
    :raises ValueError: If an argument is invalid or the lattice is singular.
    """

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
    :class:`~httk.workflow.vasp.inputs.VaspPreparationOptions`, and in the Bash bridge, so a workflow that
    hits a k-point failure class still has the ``Gamma`` remedy available.

    :param grid: Write these three positive grid dimensions.
    :param path: Write the KPOINTS file to this path.
    :param centering: Use this supported k-point centering.
    :return: The written KPOINTS path.
    :raises ValueError: If the grid or centering is unsupported.
    """

    values = tuple(grid)
    if len(values) != 3 or any(isinstance(value, bool) or value < 1 for value in values):
        raise ValueError("grid must contain three positive integers")
    if centering not in KPOINT_CENTERINGS:
        raise ValueError("centering must be 'Gamma' or 'Monkhorst-Pack'")
    destination = Path(path)
    destination.write_text(
        f"Automatic mesh generated by httk\n0\n{centering}\n{values[0]} {values[1]} {values[2]}\n0 0 0\n",
        encoding="utf-8",
    )
    return destination


def read_incar(path: str | os.PathLike[str] = "INCAR") -> dict[str, str]:
    """Read the last value of each simple INCAR assignment.

    :param path: Read the INCAR file at this path.
    :return: Upper-case INCAR tags mapped to their last values.
    """

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

    :param values: Replace these INCAR tag assignments.
    :param path: Update the INCAR file at this path.
    :return: The updated INCAR path.
    :raises ValueError: If a tag name is empty or invalid.
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
            capture_output=True,
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

    :param species: Record the POSCAR species name.
    :param variant: Record the selected pseudopotential directory name.
    :param suffix: Record the suffix selected for the species.
    :param source: Record the source potential path.
    :param sha256: Record the source file digest.
    :param titel: Record the potential's ``TITEL`` value, when present.
    """

    species: str
    variant: str
    suffix: str
    source: Path
    sha256: str
    titel: str | None

    def as_mapping(self) -> dict[str, object]:
        """Serialize the potential choice.

        :return: The JSON-compatible choice mapping.
        """
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
    """One assembled POTCAR and the provenance of every piece in it.

    :param path: Record the assembled POTCAR path.
    :param library: Record the pseudopotential library root.
    :param choices: Record the selected potential for each species.
    :param provenance: Record the provenance output path.
    """

    path: Path
    library: Path
    choices: tuple[PotcarChoice, ...]
    provenance: Path

    def as_mapping(self) -> dict[str, object]:
        """Serialize the assembly provenance.

        :return: The JSON-compatible provenance mapping.
        """
        return {
            "format": "httk-vasp-potcar-provenance",
            "format_version": 2,
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

    :param library: Search this pseudopotential library root.
    :param poscar: Read species names from this POSCAR.
    :param output: Write the assembled POTCAR to this path.
    :param suffix_preference: Try these species suffixes in order.
    :param provenance: Write provenance to this path, or beside the POTCAR when unset.
    :return: The assembled POTCAR and its provenance.
    :raises FileNotFoundError: If a species has no matching potential.
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


def contcar_to_poscar(
    contcar: str | os.PathLike[str] = "CONTCAR",
    *,
    reference: str | os.PathLike[str] = "POSCAR",
    output: str | os.PathLike[str] = "POSCAR",
) -> Path:
    """Replace CONTCAR's comment with the reference POSCAR comment.

    :param contcar: Read the relaxed structure from this CONTCAR path.
    :param reference: Read the replacement comment from this POSCAR path.
    :param output: Write the resulting POSCAR to this path.
    :return: The written POSCAR path.
    :raises ValueError: If either input file is empty.
    """

    reference_lines = Path(reference).read_text(encoding="utf-8").splitlines(keepends=True)
    lines = Path(contcar).read_text(encoding="utf-8").splitlines(keepends=True)
    if not reference_lines or not lines:
        raise ValueError("CONTCAR and reference POSCAR must both be nonempty")
    destination = Path(output)
    destination.write_text(reference_lines[0].rstrip("\r\n") + "\n" + "".join(lines[1:]), encoding="utf-8")
    return destination


def normalize_poscar_handedness(path: str | os.PathLike[str] = "POSCAR") -> Path:
    """Make a left-handed POSCAR lattice right-handed without moving sites.

    :param path: Normalize the POSCAR file at this path.
    :return: The normalized POSCAR path.
    """

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
    """Multiply POSCAR's universal linear scale by a positive factor.

    :param factor: Multiply the current scale by this positive finite value.
    :param path: Update the POSCAR file at this path.
    :return: The updated POSCAR path.
    :raises ValueError: If the factor or POSCAR scale line is invalid.
    """

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

    :param entropy: Identify the perturbation with this nonempty string.
    :return: The reproducible seed.
    :raises ValueError: If entropy is not a nonempty string.
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

    :param path: Update site coordinates in this POSCAR.
    :param amplitude: Bound each coordinate perturbation by this value.
    :param seed: Use this random seed when supplied.
    :param entropy: Derive a seed from this string when supplied.
    :return: The updated POSCAR path.
    :raises ValueError: If the perturbation settings or coordinate rows are invalid.
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

    :param poscar: Read species and atom counts from this POSCAR.
    :param potcar: Read valence counts from this POTCAR.
    :param incar: Read spin and magnetization tags from this INCAR.
    :param divisor: Round the result up to a multiple of this positive divisor.
    :return: The conservative even band count.
    :raises ValueError: If the divisor or required input metadata is invalid.
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


def potcar_summary(
    path: str | os.PathLike[str] = "POTCAR",
    output: str | os.PathLike[str] = "POTCAR.summary",
) -> Path:
    """Write a non-potential metadata summary suitable for logs.

    :param path: Read POTCAR metadata from this path.
    :param output: Write the summary to this path.
    :return: The written summary path.
    """

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


@dataclass(frozen=True)
class VaspPreparationOptions:
    """Options for dependency-free VASP input preparation.

    ``accuracy_per_atom`` is the target total-energy accuracy in eV per atom:
    ``EDIFFG`` is set to that budget for the whole cell and ``EDIFF`` to the same
    budget divided by ``ediff_margin``, so the electronic loop converges roughly
    one and a half orders of magnitude tighter than the ionic loop it feeds. The
    margin of ``33`` is a heuristic, empirical default carried over from *httk*
    v1, not a derived quantity.

    :param kpoint_density: Set the reciprocal-length k-point density.
    :param centering: Select the automatic k-point centering.
    :param accuracy_per_atom: Set the target total-energy accuracy, or disable derived accuracy tags when unset.
    :param ediff_margin: Divide the cell accuracy by this margin for ``EDIFF``.
    :param pseudopotential_library: Assemble POTCAR from this library when set.
    :param parallel_tag: Set the selected parallel INCAR tag when set.
    :param parallel_value: Set the value for the selected parallel tag.
    :param normalize_handedness: Normalize a left-handed POSCAR before preparation.
    :param incar_tags: Apply these explicit INCAR tags before derived defaults.
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
    options: VaspPreparationOptions | None = None,
    *,
    directory: str | os.PathLike[str] = ".",
) -> dict[str, object]:
    """Prepare POSCAR, POTCAR, KPOINTS, and INCAR with recorded choices.

    ``incar_tags`` is applied to the INCAR first and wins over everything derived
    afterwards: the derived values are defaults for what the caller did not say,
    so an explicit ``EDIFF``, ``MAGMOM``, or ``NBANDS`` survives preparation, and
    an explicit ``ISPIN`` is what the band-count heuristic reads.

    :param options: Use these preparation options, or their defaults when unset.
    :param directory: Prepare the VASP inputs in this directory.
    :return: A record of prepared files and selected input choices.
    :raises ValueError: If the workdir, options, or input metadata is invalid.
    """

    if options is None:
        options = VaspPreparationOptions()

    # Imported lazily to keep the input helpers a leaf module: ``diagnostics``
    # imports :func:`_write_text_atomic` from here, so importing it at module
    # scope would close a cycle.
    from .diagnostics import validate_vasp_workdir

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
