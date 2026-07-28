"""VASP output diagnosis, report reading, and workdir cleanup.

This is the read-side of the dependency-free VASP interface: extracting single
values from OSZICAR/vasprun/OUTCAR, the diagnostic pattern table that classifies
VASP-5/6 failure lines, the file-level completion diagnosis, and the workdir
validation and cleanup helpers. Historical authorship is documented in
``v1_runtime/NOTICE``.
"""

import os
import re
from collections.abc import Iterable
from pathlib import Path

from ..supervision import Diagnostic
from .inputs import _write_text_atomic

#: Outputs :func:`clean_vasp_outputs` keeps unless they are named explicitly:
#: the remedy machinery and restart promotion read exactly these two files.
VASP_RESTART_ARTIFACTS: tuple[str, ...] = ("CONTCAR", "vasp-run-report.json")


def last_oszicar_energy(path: str | os.PathLike[str] = "OSZICAR") -> float | None:
    """Return the final ``E0`` value, or ``None`` when it is absent."""

    found: float | None = None
    pattern = re.compile(r"\bE0=\s*([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][-+]?\d+)?)")
    for line in Path(path).read_text(encoding="utf-8", errors="replace").splitlines():
        match = pattern.search(line)
        if match is not None:
            found = float(match.group(1))
    return found


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


_VASP_PATTERNS: tuple[tuple[re.Pattern[str], str, str, bool], ...] = (
    (re.compile(r"chargedensity file is incomplete", re.IGNORECASE), "chgcar_incomplete", "error", True),
    (re.compile(r"ZPOTRF failed", re.IGNORECASE), "zpotrf", "fatal", True),
    (re.compile(r"FEXCF: supplied exchange-correlation table", re.IGNORECASE), "fexcf", "error", True),
    (
        re.compile(r"Reciprocal lattice and k-lattice belong to different class", re.IGNORECASE),
        "kpoints_class",
        "error",
        True,
    ),
    (
        re.compile(r"Tetrahedron method fails|Fatal error.*k-mesh|unable to match k-point|TETIRR needs", re.IGNORECASE),
        "tetrahedron_kpoints",
        "error",
        True,
    ),
    (re.compile(r"inverse of rotation matrix was not found", re.IGNORECASE), "inverse_rotation", "error", True),
    (re.compile(r"Found some non-integer element in rotation matrix", re.IGNORECASE), "rotation_matrix", "error", True),
    (re.compile(r"BRMIX: very serious problems", re.IGNORECASE), "brmix", "warning", False),
    (re.compile(r"Could not get correct shifts", re.IGNORECASE), "kpoint_shifts", "warning", False),
    (re.compile(r"REAL_OPTLAY: internal error", re.IGNORECASE), "real_optlay", "error", True),
    (re.compile(r"(?:internal ERROR RSPHER|RSPHER: internal ERROR)", re.IGNORECASE), "rspher", "fatal", True),
    (re.compile(r"DENTET: can't reach specified precision", re.IGNORECASE), "dentet", "warning", False),
    (re.compile(r"TOO FEW BANDS", re.IGNORECASE), "too_few_bands", "error", False),
    (re.compile(r"triple product of the basis vectors", re.IGNORECASE), "triple_product", "fatal", True),
    (re.compile(r"BRIONS problems: POTIM should be increased", re.IGNORECASE), "brions", "warning", False),
    (re.compile(r"small aliasing.*errors", re.IGNORECASE), "aliasing", "warning", False),
    (re.compile(r"distance between some ions is very small", re.IGNORECASE), "ions_too_close", "fatal", True),
    (re.compile(r"set LREAL=.FALSE", re.IGNORECASE), "lreal_false", "error", True),
    (re.compile(r"number of cells and number of vectors did not agree", re.IGNORECASE), "pricell", "error", True),
    (re.compile(r"internal error in RAD_INT", re.IGNORECASE), "radint", "fatal", True),
    (re.compile(r"internal ERROR in NONLR_ALLOC", re.IGNORECASE), "nonlr_alloc", "fatal", True),
    (re.compile(r"Error EDDDAV: Call to ZHEGV failed", re.IGNORECASE), "edddav_zhegv", "error", False),
    (re.compile(r"CNORMN: search vector ill defined", re.IGNORECASE), "cnormn", "warning", False),
    (re.compile(r"ZBRENT: fatal error in bracketing", re.IGNORECASE), "zbrent_bracketing", "error", False),
    (re.compile(r"One of the lattice vectors is very long", re.IGNORECASE), "lattice_vector_too_long", "fatal", True),
)


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
            re.IGNORECASE,
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
