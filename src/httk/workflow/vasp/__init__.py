"""Small, dependency-free VASP runner helpers for native v2 workflows.

This is an independent, data-oriented interface rather than a port of the
historic ``VASP_*`` Python or shell APIs. Historical authorship is documented
in ``v1_runtime/NOTICE``.

The implementation is split across cohesive sibling modules — ``inputs``,
``diagnostics``, ``remedies``, and ``reports`` — and this package is a thin
public facade that re-exports their surface unchanged. Importing the package
also registers the packaged VASP templates with the generic scaffold (see the
``templates`` module), which is how ``httk workflow job new --template
vasp-relax`` resolves a runner the scaffold never names.
"""

# Imported for its registration side effect: the packaged VASP templates join
# the scaffold's provider registry when this package is imported.
from . import templates as _templates  # noqa: F401
from .diagnostics import (
    VASP_RESTART_ARTIFACTS,
    clean_outcar,
    clean_vasp_outputs,
    diagnose_vasp_files,
    last_oszicar_energy,
    last_vasprun_volume,
    outcar_plane_wave_count,
    outcar_potim,
    validate_vasp_workdir,
)
from .inputs import (
    DEFAULT_KPOINT_CENTERING,
    KPOINT_CENTERINGS,
    PoscarHeader,
    PotcarAssembly,
    PotcarChoice,
    VaspPreparationOptions,
    assemble_potcar,
    automatic_kpoint_grid,
    calculate_nbands,
    contcar_to_poscar,
    derive_seed,
    normalize_poscar_handedness,
    potcar_summary,
    prepare_vasp_inputs,
    rattle_poscar,
    read_incar,
    read_poscar_header,
    scale_poscar_lattice,
    suggested_magnetic_moments,
    update_incar,
    write_automatic_kpoints,
)
from .remedies import (
    DEFAULT_REMEDY_HISTORY,
    REMEDY_OPERATIONS,
    RemedyPolicy,
    VaspRemedyDecision,
    apply_vasp_remedy,
    job_remedy_history_path,
    plan_vasp_remedy,
    register_remedy_policy,
    remedy_policy,
    remedy_policy_names,
)
from .reports import VaspRunReport, run_vasp

__all__ = [
    "DEFAULT_KPOINT_CENTERING",
    "KPOINT_CENTERINGS",
    "VASP_RESTART_ARTIFACTS",
    "DEFAULT_REMEDY_HISTORY",
    "REMEDY_OPERATIONS",
    "PoscarHeader",
    "PotcarChoice",
    "PotcarAssembly",
    "VaspPreparationOptions",
    "VaspRunReport",
    "VaspRemedyDecision",
    "RemedyPolicy",
    "read_poscar_header",
    "suggested_magnetic_moments",
    "automatic_kpoint_grid",
    "write_automatic_kpoints",
    "read_incar",
    "update_incar",
    "assemble_potcar",
    "last_oszicar_energy",
    "contcar_to_poscar",
    "normalize_poscar_handedness",
    "scale_poscar_lattice",
    "derive_seed",
    "rattle_poscar",
    "calculate_nbands",
    "last_vasprun_volume",
    "outcar_potim",
    "outcar_plane_wave_count",
    "potcar_summary",
    "clean_outcar",
    "validate_vasp_workdir",
    "clean_vasp_outputs",
    "prepare_vasp_inputs",
    "diagnose_vasp_files",
    "run_vasp",
    "plan_vasp_remedy",
    "apply_vasp_remedy",
    "register_remedy_policy",
    "remedy_policy",
    "remedy_policy_names",
    "job_remedy_history_path",
]
