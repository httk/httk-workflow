"""Directory workflow package manifests and scaffold integration."""

import json
from pathlib import Path
from typing import Any, cast

import pytest

from httk.workflow import Workspace, scaffold
from httk.workflow._util import tree_digest
from httk.workflow.models import MAXIMUM_DECLARATIONS_BYTES, JobDefinition
from httk.workflow.packages import (
    load_workflow_package,
    parse_workflow_manifest,
    workflow_declaration_from_manifest,
)
from httk.workflow.scaffold import new_job, resolve_workflow, workflow_provider

_MANIFEST = '''
[workflow]
id = "tests.package"
alias = "test-package"
description = "A package for tests."
declaration_uri = "https://example.test/workflows/package"

[workflow.runner]
steps = ["start"]

[workflow.instantiate]
file = "instantiate.py"

[workflow.postprocess]
file = "postprocess.py"

[workflow.parameters.structure]
destination = "POSCAR"
entry_type = "structures"
ref = "https://example.test/structures"
description = "The input structure."
role = "initial_structure"

[workflow.inputs.label]
type = "string"
default = "test"

[workflow.outputs.relaxed]
entry_type = "structures"
ref = "https://example.test/structures"
description = "The output structure."
product_of = "initial_structure"
role = "relaxed_structure"
'''

_DECLARATION = {
    "$id": "https://example.test/workflows/package",
    "description": "External declaration.",
    "parameters": [{"name": "initial_structure", "entry_type": "structures"}],
    "output_types": [{"name": "relaxed_structure", "entry_type": "structures"}],
}


def _package(root: Path, manifest: str = _MANIFEST) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    (root / "instantiate.py").write_text(
        "def instantiate(context):\n    (context.payload / 'instantiated.txt').write_text(__file__)\n",
        encoding="utf-8",
    )
    (root / "postprocess.py").write_text(
        "def postprocess(record):\n    return {}\n",
        encoding="utf-8",
    )
    return root


def test_manifest_parses_and_generates_the_declared_roles(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_package(tmp_path / "package"))
    assert provider.workflow_id == "tests.package"
    assert provider.directory == (tmp_path / "package").resolve()
    assert provider.entry == "run"
    assert provider.initial_step == "start"
    assert provider.parameters == {"structure": "POSCAR"}
    assert provider.instantiate_file == "instantiate.py"
    assert provider.postprocess_file == "postprocess.py"
    assert callable(provider.postprocessor) and cast(Any, provider.postprocessor)(object()) == {}
    assert workflow_declaration_from_manifest(provider) == {
        "$id": "https://example.test/workflows/package",
        "description": "A package for tests.",
        "parameters": [
            {
                "name": "initial_structure",
                "entry_type": "structures",
                "ref": "https://example.test/structures",
                "description": "The input structure.",
            }
        ],
        "output_types": [
            {
                "name": "relaxed_structure",
                "entry_type": "structures",
                "ref": "https://example.test/structures",
                "description": "The output structure.",
            }
        ],
    }
    assert provider.declarations["workflow"] == workflow_declaration_from_manifest(provider)
    assert provider.outputs["relaxed"]["product_of"] == "initial_structure"


def test_manifest_rejects_duplicate_output_roles(tmp_path: Path) -> None:
    manifest = _MANIFEST + '\n[workflow.outputs.other]\nentry_type = "records"\nrole = "relaxed_structure"\n'
    with pytest.raises(ValueError, match=r"\[workflow.outputs.other\]\.role is duplicated"):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


def test_external_declaration_is_embedded_verbatim(tmp_path: Path) -> None:
    package = _package(
        tmp_path / "package",
        _MANIFEST.replace(
            'declaration_uri = "https://example.test/workflows/package"',
            'declaration_uri = "https://example.test/workflows/external"\ndeclaration_file = "declaration.json"',
        ),
    )
    declaration = dict(_DECLARATION)
    declaration["$id"] = "https://example.test/workflows/external"
    (package / "declaration.json").write_text(json.dumps(declaration), encoding="utf-8")
    assert parse_workflow_manifest(package).declarations == {"workflow": declaration}


def test_external_declaration_rejects_product_of(tmp_path: Path) -> None:
    package = _package(
        tmp_path / "package",
        _MANIFEST.replace(
            'declaration_uri = "https://example.test/workflows/package"',
            'declaration_uri = "https://example.test/workflows/external"\ndeclaration_file = "declaration.json"',
        ),
    )
    declaration = dict(_DECLARATION)
    declaration["$id"] = "https://example.test/workflows/external"
    declaration["output_types"] = [
        {"name": "relaxed_structure", "entry_type": "structures", "product_of": "initial_structure"}
    ]
    (package / "declaration.json").write_text(json.dumps(declaration), encoding="utf-8")
    with pytest.raises(ValueError, match="must not carry product_of"):
        parse_workflow_manifest(package)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("$id", "https://example.test/workflows/wrong", "declaration \\$id does not match"),
        ("output_name", "not_an_output", "is not a manifest output role"),
        ("output_type", "records", "incompatible entry_type"),
        ("parameter_name", "not_a_role", "is not a manifest parameter/input role"),
    ],
)
def test_external_declaration_must_match_the_manifest(tmp_path: Path, field: str, value: str, message: str) -> None:
    manifest = _MANIFEST.replace(
        'declaration_uri = "https://example.test/workflows/package"',
        'declaration_uri = "https://example.test/workflows/external"\ndeclaration_file = "declaration.json"',
    )
    package = _package(tmp_path / "package", manifest)
    declaration = json.loads(json.dumps(_DECLARATION))
    declaration["$id"] = "https://example.test/workflows/external"
    if field == "$id":
        declaration["$id"] = value
    elif field == "output_name":
        declaration["output_types"][0]["name"] = value
    elif field == "output_type":
        declaration["output_types"][0]["entry_type"] = value
    else:
        declaration["parameters"][0]["name"] = value
    (package / "declaration.json").write_text(json.dumps(declaration), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(package)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ("id = \"tests.package\"", r"\[workflow\]\.id is required"),
        ("alias = \"test-package\"", r"\[workflow\]\.alias must match"),
        ("[workflow.runner]\nsteps = [\"start\"]\nunknown = true", r"unknown key \[workflow\.runner\.unknown\]"),
        (
            "[workflow.parameters.structure]\ndestination = \"POSCAR\"\nunknown = true",
            r"unknown key \[workflow\.parameters\.structure\.unknown\]",
        ),
        ("steps = []", "steps must be a nonempty list"),
        ("steps = [\"one\", \"two\"]", "initial_step is required"),
        ("product_of = \"missing\"", "product_of names unknown parameter or output role"),
        ("[workflow.inputs.label]\ntype = \"integer\"\ndefault = \"bad\"", "default does not match type"),
    ],
)
def test_manifest_validation_errors_are_pathful(tmp_path: Path, change: str, message: str) -> None:
    manifest = _MANIFEST
    if change.startswith("id ="):
        manifest = manifest.replace('id = "tests.package"', "")
    elif change.startswith("alias ="):
        manifest = manifest.replace('alias = "test-package"', 'alias = "Bad Alias"')
    elif change.startswith("[workflow.runner]"):
        manifest = manifest.replace(
            '[workflow.runner]\nsteps = ["start"]', change.split("\nunknown", 1)[0] + "\nunknown = true"
        )
    elif change.startswith("[workflow.parameters"):
        manifest = manifest.replace('role = "initial_structure"', 'unknown = true\nrole = "initial_structure"')
    elif change == "steps = []" or change.startswith("steps ="):
        manifest = manifest.replace('steps = ["start"]', change)
    elif change.startswith("product_of"):
        manifest = manifest.replace('product_of = "initial_structure"', change)
    else:
        manifest = manifest.replace('type = "string"\ndefault = "test"', 'type = "integer"\ndefault = "bad"')
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


@pytest.mark.parametrize(
    ("product", "extra", "message"),
    [
        ("relaxed_structure", "", "cannot reference its own output role"),
        (
            "other",
            '\n[workflow.outputs.other]\nentry_type = "records"\nproduct_of = "relaxed_structure"',
            "forms an output cycle",
        ),
    ],
)
def test_manifest_rejects_output_product_cycles(tmp_path: Path, product: str, extra: str, message: str) -> None:
    manifest = _MANIFEST.replace('product_of = "initial_structure"', f'product_of = "{product}"') + extra
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


def test_manifest_rejects_ambiguous_product_role(tmp_path: Path) -> None:
    manifest = _MANIFEST + '\n[workflow.outputs.initial]\nentry_type = "structures"\nrole = "initial_structure"\n'
    with pytest.raises(ValueError, match="both a parameter and output role"):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


def test_manifest_rejects_missing_and_unsafe_members_and_toml_errors(tmp_path: Path) -> None:
    for member in ("missing.py", "../instantiate.py", str(tmp_path / "outside.py"), "instantiate.txt"):
        manifest = _MANIFEST.replace('file = "instantiate.py"', f'file = "{member}"')
        with pytest.raises(ValueError, match="instantiate"):
            parse_workflow_manifest(_package(tmp_path / member.replace("/", "_"), manifest))
    bad = _package(tmp_path / "bad", _MANIFEST + "\nnot = [\n")
    with pytest.raises(ValueError, match=r"bad:.*line"):
        parse_workflow_manifest(bad)
    no_hook = _MANIFEST.replace('[workflow.instantiate]\nfile = "instantiate.py"\n\n', "")
    no_hook = no_hook.replace('destination = "POSCAR"\n', "")
    with pytest.raises(ValueError, match="hook-consumed"):
        parse_workflow_manifest(_package(tmp_path / "no-hook", no_hook))


def test_manifest_size_limit_is_enforced(tmp_path: Path) -> None:
    huge = "x" * MAXIMUM_DECLARATIONS_BYTES
    manifest = _MANIFEST.replace('description = "A package for tests."', f'description = "{huge}"')
    with pytest.raises(ValueError, match="exceeds"):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


def test_load_register_alias_and_register_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package(tmp_path / "package")
    assert workflow_provider("tests.package") is None
    provider = load_workflow_package(package, register=False)
    assert workflow_provider(provider.workflow_id) is None
    loaded = load_workflow_package(package)
    assert workflow_provider("tests.package") == loaded
    assert workflow_provider("test-package") == loaded
    assert resolve_workflow("tests.package").directory == package.resolve()

    collision_manifest = _MANIFEST.replace('id = "tests.package"', 'id = "tests.other"')
    collision = parse_workflow_manifest(_package(tmp_path / "collision", collision_manifest))
    with pytest.raises(ValueError, match="collides"):
        load_workflow_package(tmp_path / "collision")
    assert collision.workflow_id == "tests.other"
    monkeypatch.delitem(scaffold._WORKFLOW_PROVIDERS, loaded.workflow_id)


def test_directory_workflow_scaffolds_from_the_published_tree_and_pins_declarations(tmp_path: Path) -> None:
    package = _package(tmp_path / "package")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, parameters={"structure": structure}, inputs={"label": "job"})
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.workflow == "tests.package"
    assert definition.runner_source == "workspace"
    assert definition.runner_sha256 == tree_digest(package)
    assert definition.declarations["workflow"]["$id"] == "https://example.test/workflows/package"
    assert str(workspace.runners) in (job.payload / "instantiated.txt").read_text(encoding="utf-8")
    assert workspace.runner_store_path(job.runner["path"]).is_dir()  # type: ignore[arg-type]

    stored = workspace.runner_store_path(str(job.runner["path"]))
    stored.chmod(0o755)
    (stored / "postprocess.py").chmod(0o644)
    (stored / "postprocess.py").write_text("def postprocess(record):\n    return {'changed': True}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="published workflow tree"):
        new_job(workspace, package, parameters={"structure": structure})
