"""Small, dependency-free VASP runner helpers for native v2 workflows.

This is an independent, data-oriented interface rather than a port of the
historic ``VASP_*`` Python or shell APIs. Historical authorship is documented
in ``v1_runtime/NOTICE``.

The implementation is split across cohesive sibling modules — ``inputs``,
``diagnostics``, ``remedies``, ``reports``, and ``steps`` (the attempt-level
steps a Python VASP runner declares) — and this package is a thin public facade
that re-exports their surface unchanged. The VASP workflows
built on these helpers live in https://github.com/httk/workflows-vasp.
"""

from httk.core import register_citation

register_citation(
    applies_to="VASP workflow automation and results handling build on httk v1 contributions by Henrik Levämäki",
    references=(
        {
            "authors": (
                {"name": "Henrik Levämäki"},
                {"name": "Ferenc Tasnádi"},
                {"name": "Davide G. Sangiovanni"},
                {"name": "Lars J. S. Johnson"},
                {"name": "Rickard Armiento"},
                {"name": "Igor A. Abrikosov"},
            ),
            "title": "Predicting elastic properties of hard-coating alloys using ab-initio and machine learning methods",
            "journal": "npj Computational Materials",
            "volume": "8",
            "pages": "17",
            "year": "2022",
            "doi": "10.1038/s41524-022-00698-7",
            "bib_type": "article",
        },
    ),
)
register_citation(
    applies_to="VASP relaxation workflows and task scheduling build on httk v1 contributions by Christopher Tholander",
    references=(
        {
            "authors": (
                {"name": "Christopher Tholander"},
                {"name": "C. B. Andreas Andersson"},
                {"name": "Rickard Armiento"},
                {"name": "Ferenc Tasnádi"},
                {"name": "Björn Alling"},
            ),
            "title": "Strong piezoelectric response in stable TiZnN2, ZrZnN2, and HfZnN2 found by ab initio high-throughput approach",
            "journal": "Journal of Applied Physics",
            "volume": "120",
            "pages": "225102",
            "year": "2016",
            "doi": "10.1063/1.4971248",
            "bib_type": "article",
        },
    ),
)

from . import collect as _collect  # noqa: F401
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
from .steps import (
    DEFAULT_STATIC_TAGS,
    DEFAULT_VASP_COLLECT,
    promote_vasp_relaxation,
    publish_vasp_files,
    run_vasp_step,
    stage_vasp_inputs,
    vasp_command,
    vasp_data_prefix,
    vasp_preparation_options,
    vasp_static_tags,
)

__all__ = [
    "DEFAULT_KPOINT_CENTERING",
    "DEFAULT_REMEDY_HISTORY",
    "DEFAULT_STATIC_TAGS",
    "DEFAULT_VASP_COLLECT",
    "KPOINT_CENTERINGS",
    "REMEDY_OPERATIONS",
    "VASP_RESTART_ARTIFACTS",
    "PoscarHeader",
    "PotcarAssembly",
    "PotcarChoice",
    "RemedyPolicy",
    "VaspPreparationOptions",
    "VaspRemedyDecision",
    "VaspRunReport",
    "apply_vasp_remedy",
    "assemble_potcar",
    "automatic_kpoint_grid",
    "calculate_nbands",
    "clean_outcar",
    "clean_vasp_outputs",
    "contcar_to_poscar",
    "derive_seed",
    "diagnose_vasp_files",
    "job_remedy_history_path",
    "last_oszicar_energy",
    "last_vasprun_volume",
    "normalize_poscar_handedness",
    "outcar_plane_wave_count",
    "outcar_potim",
    "plan_vasp_remedy",
    "potcar_summary",
    "prepare_vasp_inputs",
    "promote_vasp_relaxation",
    "publish_vasp_files",
    "rattle_poscar",
    "read_incar",
    "read_poscar_header",
    "register_remedy_policy",
    "remedy_policy",
    "remedy_policy_names",
    "run_vasp",
    "run_vasp_step",
    "scale_poscar_lattice",
    "stage_vasp_inputs",
    "suggested_magnetic_moments",
    "update_incar",
    "validate_vasp_workdir",
    "vasp_command",
    "vasp_data_prefix",
    "vasp_preparation_options",
    "vasp_static_tags",
    "write_automatic_kpoints",
]
