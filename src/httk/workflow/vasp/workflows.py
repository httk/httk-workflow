"""Packaged VASP workflow providers."""

from httk.workflow.scaffold import WorkflowProvider, register_workflow

from .postprocess import postprocess_vasp_relax, postprocess_vasp_relax_static, postprocess_vasp_static

RUNNER_PACKAGE = "httk.workflow.vasp.runners"
_RELAX_ID = "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax"

_RELAX_DECLARATION = {
    "$id": _RELAX_ID,
    "title": "VASP structure relaxation",
    "description": "This workflow declaration defines the httk vasp-relax workflow: it relaxes the geometry of a crystal structure with VASP.\nIt defines the meaning of its roles: input role initial_structure is a structures entry containing the structure to relax.\nOutput role relaxed_structure is a structures entry containing the relaxed geometry, and output role total_energy is a records entry containing the final total energy of the relaxed structure.",
    "x-httk-definition": {
        "kind": "workflow_declaration",
        "format": "0.1",
        "version": "0.1.0",
        "name": "vasp-relax",
        "label": "vasp_relax_workflow_httk",
    },
    "inputs": [
        {
            "name": "initial_structure",
            "entry_type": "structures",
            "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
            "description": "The structure to relax.",
        }
    ],
    "outputs": [
        {
            "name": "relaxed_structure",
            "entry_type": "structures",
            "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
            "description": "The relaxed geometry.",
        },
        {
            "name": "total_energy",
            "entry_type": "records",
            "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
            "description": "The final total energy of the relaxed structure.",
        },
    ],
}
_STATIC_DECLARATION = {
    "$id": "https://schemas.httk.org/defs/v0.1/workflows/vasp-static",
    "title": "VASP static calculation",
    "description": "This workflow declaration defines the httk vasp-static workflow: it performs a single-point total-energy evaluation of a fixed structure with VASP.\nIt defines the meaning of its roles: input role initial_structure is a structures entry containing the structure to evaluate.\nOutput role total_energy is a records entry containing the total energy of the structure.",
    "x-httk-definition": {
        "kind": "workflow_declaration",
        "format": "0.1",
        "version": "0.1.0",
        "name": "vasp-static",
        "label": "vasp_static_workflow_httk",
    },
    "inputs": [
        {
            "name": "initial_structure",
            "entry_type": "structures",
            "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
            "description": "The structure to evaluate.",
        }
    ],
    "outputs": [
        {
            "name": "total_energy",
            "entry_type": "records",
            "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
            "description": "The total energy of the structure.",
        }
    ],
}
_RELAX_STATIC_DECLARATION = {
    "$id": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax-static",
    "title": "VASP relaxation and static calculation",
    "description": "This workflow declaration defines the httk vasp-relax-static workflow: it relaxes the geometry, then evaluates the relaxed structure with a final static calculation.\nIt defines the meaning of its roles: input role initial_structure is a structures entry containing the structure to relax.\nOutput role relaxed_structure is a structures entry containing the relaxed geometry, and output role total_energy is a records entry containing the total energy of the relaxed structure from the final static calculation.",
    "x-httk-definition": {
        "kind": "workflow_declaration",
        "format": "0.1",
        "version": "0.1.0",
        "name": "vasp-relax-static",
        "label": "vasp_relax_static_workflow_httk",
    },
    "inputs": [
        {
            "name": "initial_structure",
            "entry_type": "structures",
            "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
            "description": "The structure to relax.",
        }
    ],
    "outputs": [
        {
            "name": "relaxed_structure",
            "entry_type": "structures",
            "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
            "description": "The relaxed geometry.",
        },
        {
            "name": "total_energy",
            "entry_type": "records",
            "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
            "description": "The total energy of the relaxed structure from the final static calculation.",
        },
    ],
}

_RELAX_OUTPUTS = {
    "relaxed_structure": {
        "entry_type": "structures",
        "role": "relaxed_structure",
        "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
        "description": "The relaxed geometry.",
        "product_of": "initial_structure",
    },
    "total_energy": {
        "entry_type": "records",
        "role": "total_energy",
        "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
        "description": "The final total energy of the relaxed structure.",
        "product_of": "relaxed_structure",
    },
}
_STATIC_OUTPUTS = {
    "total_energy": {
        "entry_type": "records",
        "role": "total_energy",
        "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
        "description": "The total energy of the structure.",
        "product_of": "initial_structure",
    },
}
_RELAX_STATIC_OUTPUTS = {
    "relaxed_structure": {
        "entry_type": "structures",
        "role": "relaxed_structure",
        "ref": "https://schemas.optimade.org/defs/v1.3/entrytypes/optimade/structures",
        "description": "The relaxed geometry.",
        "product_of": "initial_structure",
    },
    "total_energy": {
        "entry_type": "records",
        "role": "total_energy",
        "ref": "https://schemas.httk.org/defs/v0.1/properties/core/total_energy",
        "description": "The total energy of the relaxed structure from the final static calculation.",
        "product_of": "relaxed_structure",
    },
}

PROVIDERS = (
    WorkflowProvider(
        workflow_id="httk.vasp.relax",
        alias="vasp-relax",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax.py",
        initial_step="prepare",
        steps=("publish", "prepare", "run"),
        data_mode="transactional",
        inputs={"structure": "POSCAR"},
        outputs=_RELAX_OUTPUTS,
        summary="relax one structure with the reviewed remedy ladder",
        declarations={"workflow": _RELAX_DECLARATION},
        postprocessor=postprocess_vasp_relax,
    ),
    WorkflowProvider(
        workflow_id="httk.vasp.relax-bash",
        alias="vasp-relax-bash",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax.sh",
        initial_step="prepare",
        steps=("publish", "prepare", "run"),
        data_mode="transactional",
        inputs={"structure": "POSCAR"},
        outputs=_RELAX_OUTPUTS,
        summary="the same relaxation, authored in Bash",
        declarations={"workflow": _RELAX_DECLARATION},
        postprocessor=postprocess_vasp_relax,
    ),
    WorkflowProvider(
        workflow_id="httk.vasp.static",
        alias="vasp-static",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_static.py",
        initial_step="prepare",
        steps=("publish", "prepare", "run"),
        data_mode="transactional",
        inputs={"structure": "POSCAR"},
        outputs=_STATIC_OUTPUTS,
        summary="one single-point calculation of one structure",
        declarations={"workflow": _STATIC_DECLARATION},
        postprocessor=postprocess_vasp_static,
    ),
    WorkflowProvider(
        workflow_id="httk.vasp.relax-static",
        alias="vasp-relax-static",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax_static.py",
        initial_step="prepare",
        steps=("publish", "prepare", "promote", "run", "static"),
        data_mode="transactional",
        inputs={"structure": "POSCAR"},
        outputs=_RELAX_STATIC_OUTPUTS,
        summary="relax, promote the relaxed structure, then run it statically",
        declarations={"workflow": _RELAX_STATIC_DECLARATION},
        postprocessor=postprocess_vasp_relax_static,
    ),
)


def register() -> None:
    """Register the packaged VASP workflow providers.

    The registered workflow IDs are ``httk.vasp.relax``/``vasp-relax``,
    ``httk.vasp.relax-bash`` (with the same declaration ID),
    ``httk.vasp.static``, and ``httk.vasp.relax-static``. Every provider ends
    at the terminal ``publish`` step.
    """
    for provider in PROVIDERS:
        register_workflow(provider)


register()
