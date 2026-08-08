"""Directory workflow package manifests and scaffold integration."""

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from httk.workflow import Workspace, languages, scaffold
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

[workflow.collect]
file = "collect.py"

[workflow.inputs.structure]
destination = "POSCAR"
entry_type = "structures"
ref = "https://example.test/structures"
description = "The input structure."
role = "initial_structure"

[workflow.parameters.label]
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
    "inputs": [{"name": "initial_structure", "entry_type": "structures"}],
    "outputs": [{"name": "relaxed_structure", "entry_type": "structures"}],
}

_LANGUAGE_CWL = """
cwlVersion: v1.2
class: CommandLineTool
baseCommand: echo
inputs:
  message:
    type: string
    inputBinding: {position: 1}
outputs:
  spoken:
    type: stdout
stdout: spoken.txt
"""

_LANGUAGE_PWD = {
    "version": "0.1.0",
    "nodes": [
        {"id": 0, "type": "function", "value": "module.echo"},
        {"id": 1, "type": "input", "value": "hello", "name": "message"},
        {"id": 2, "type": "output", "name": "result"},
    ],
    "edges": [
        {"target": 0, "targetPort": "message", "source": 1, "sourcePort": None},
        {"target": 2, "targetPort": None, "source": 0, "sourcePort": None},
    ],
}


def _package(root: Path, manifest: str = _MANIFEST) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "run").chmod(0o755)
    (root / "instantiate.py").write_text(
        "def instantiate(context):\n    (context.payload / 'instantiated.txt').write_text(__file__)\n",
        encoding="utf-8",
    )
    (root / "collect.py").write_text(
        "def collect(record):\n    return {}\n",
        encoding="utf-8",
    )
    return root


def _language_package(root: Path, language: str = "cwl", manifest_extra: str = "") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    document = "echo.cwl" if language == "cwl" else "workflow.json"
    output_name = "spoken" if language == "cwl" else "result"
    content = f'''[workflow]
id = "tests.{language}"

[workflow.runner]
language = "{language}"
document = "{document}"
{manifest_extra}

[workflow.inputs.message]
entry_type = "strings"

[workflow.outputs.{output_name}]
entry_type = "strings"
'''
    (root / "httk_workflow.toml").write_text(content, encoding="utf-8")
    if language == "cwl":
        (root / "echo.cwl").write_text(_LANGUAGE_CWL, encoding="utf-8")
    else:
        (root / "workflow.json").write_text(json.dumps(_LANGUAGE_PWD), encoding="utf-8")
        (root / "module.py").write_text("def echo(message):\n    return message\n", encoding="utf-8")
    return root


def test_manifest_parses_and_generates_the_declared_roles(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_package(tmp_path / "package"))
    assert provider.workflow_id == "tests.package"
    assert provider.directory == (tmp_path / "package").resolve()
    assert provider.entry == "run"
    assert provider.initial_step == "start"
    assert provider.inputs == {"structure": "POSCAR"}
    assert provider.instantiate_file == "instantiate.py"
    assert provider.collect_file == "collect.py"
    assert callable(provider.collector) and cast(Any, provider.collector)(object()) == {}
    assert workflow_declaration_from_manifest(provider) == {
        "$id": "https://example.test/workflows/package",
        "description": "A package for tests.",
        "inputs": [
            {
                "name": "initial_structure",
                "entry_type": "structures",
                "ref": "https://example.test/structures",
                "description": "The input structure.",
            }
        ],
        "outputs": [
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


def test_cwl_language_manifest_uses_registry_defaults(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(_language_package(tmp_path / "cwl"))
    assert provider.language == "cwl"
    assert provider.document == "echo.cwl"
    assert provider.steps == ("start", "enter", "advance", "collect")
    assert provider.instantiate is True and provider.inputs == {"message": None}
    assert provider.collector == "httk.workflow.languages.cwl:collect"
    assert workflow_declaration_from_manifest(provider)["inputs"] == [{"name": "message", "entry_type": "strings"}]


def test_pwd_language_manifest_keeps_runner_options(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(
        _language_package(
            tmp_path / "pwd",
            "pwd",
            'modules = ["module.py"]\nallowed_modules = ["module"]',
        )
    )
    assert provider.language == "pwd"
    assert provider.steps == ("execute",) and provider.initial_step == "execute"
    assert provider.runner_options == {"modules": ["module.py"], "allowed_modules": ["module"]}
    assert provider.inputs == {"message": None}


def _jobflow_package(root: Path, runner: str, *, document: bool = False) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    document_line = 'document = "maker.json"\n' if document else ""
    (root / "httk_workflow.toml").write_text(
        f'''[workflow]
id = "tests.jobflow"

[workflow.runner]
language = "jobflow"
{document_line}{runner}

[workflow.inputs.structure]
entry_type = "records"
port = "make_structure"

[workflow.outputs.result]
entry_type = "records"
port = "dynamic_output"
''',
        encoding="utf-8",
    )
    if document:
        (root / "maker.json").write_text(json.dumps({"@module": "atomate2", "@class": "Maker"}), encoding="utf-8")
    return root


def test_jobflow_language_manifest_accepts_maker_and_open_ports(tmp_path: Path) -> None:
    provider = parse_workflow_manifest(
        _jobflow_package(tmp_path / "maker", 'maker = "atomate2.vasp.flows.core:DoubleRelaxMaker"')
    )
    assert provider.language == "jobflow"
    assert provider.document is None
    assert provider.steps == ("start", "advance", "enter")
    assert provider.runner_options == {"maker": "atomate2.vasp.flows.core:DoubleRelaxMaker"}
    assert provider.inputs == {"structure": None}
    assert provider.collector == "httk.workflow.languages.jobflow:collect"


def test_jobflow_manifest_rejects_duplicate_input_ports(tmp_path: Path) -> None:
    package = _jobflow_package(tmp_path / "duplicate-input", 'maker = "atomate2:Maker"')
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest
        + '''
[workflow.inputs.structure_alias]
entry_type = "records"
port = "make_structure"
''',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"structure_alias.*port duplicates.*structure.*make_structure"):
        parse_workflow_manifest(package)


def test_jobflow_manifest_rejects_duplicate_output_ports(tmp_path: Path) -> None:
    package = _jobflow_package(tmp_path / "duplicate-output", 'maker = "atomate2:Maker"')
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest
        + '''
[workflow.outputs.result_alias]
entry_type = "records"
port = "dynamic_output"
''',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"result_alias.*port duplicates.*result.*dynamic_output"):
        parse_workflow_manifest(package)


def test_jobflow_manifest_preserves_declared_maker_parameters(tmp_path: Path) -> None:
    package = _jobflow_package(tmp_path / "parameters", 'maker = "atomate2:Maker"')
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest + '\n[workflow.parameters.relax_steps]\ntype = "integer"\ndefault = 300\n', encoding="utf-8"
    )

    provider = parse_workflow_manifest(package)

    assert provider.parameters == {"relax_steps": {"type": "integer", "default": 300}}


def test_language_environment_declarations_are_under_manifest_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = languages.language

    def language_with_environment(name: str):
        return replace(
            original(name),
            environment={
                "auto": {"type": "string", "default": "language"},
                "shared": {"type": "string", "default": "language"},
            },
        )

    monkeypatch.setattr(languages, "language", language_with_environment)
    package = _language_package(
        tmp_path / "language-environment",
        manifest_extra='''

[workflow.environment.shared]
type = "string"
default = "manifest"

[workflow.environment.manifest]
type = "integer"
default = 1
''',
    )
    provider = parse_workflow_manifest(package)
    assert provider.environment == {
        "auto": {"type": "string", "default": "language"},
        "shared": {"type": "string", "default": "manifest"},
        "manifest": {"type": "integer", "default": 1},
    }


def test_environment_manifest_is_typed_and_round_trips_in_job_json(tmp_path: Path) -> None:
    manifest = (
        _MANIFEST
        + '''
[workflow.environment.command]
type = "string"
description = "The command to run."
setting = "tool.command"

[workflow.environment.retries]
type = "integer"
default = 2
'''
    )
    package = _package(tmp_path / "environment", manifest)
    provider = load_workflow_package(package, register=False)
    assert provider.environment == {
        "command": {"type": "string", "description": "The command to run.", "setting": "tool.command"},
        "retries": {"type": "integer", "default": 2},
    }
    workspace = Workspace.initialize(tmp_path / "workspace")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure", encoding="utf-8")
    job = new_job(workspace, package, inputs={"structure": structure}, environment={"command": "echo"})
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.environment == {
        "declared": provider.environment,
        "overrides": {"command": "echo"},
    }


@pytest.mark.parametrize(
    ("table", "message"),
    [
        ('type = "integer"\ndefault = "bad"', "default does not match type"),
        ('setting = "bad-name"', "setting must be a nonempty dotted identifier"),
        ('unknown = true', "unknown key"),
    ],
)
def test_environment_manifest_rejects_bad_declarations(tmp_path: Path, table: str, message: str) -> None:
    package = _package(tmp_path / "invalid", _MANIFEST + f"\n[workflow.environment.value]\n{table}\n")
    with pytest.raises(ValueError, match=message):
        load_workflow_package(package, register=False)


@pytest.mark.parametrize(
    "entry",
    [
        '[workflow.environment.value]\nsetting = "workflow.python"',
        '[workflow.environment."workflow.python"]',
    ],
)
def test_environment_manifest_rejects_manager_owned_setting_variables(tmp_path: Path, entry: str) -> None:
    package = _package(tmp_path / "reserved-environment", _MANIFEST + f"\n{entry}\n")
    with pytest.raises(ValueError, match="reserved 'HTTK_WORKFLOW_' variable"):
        load_workflow_package(package, register=False)


def test_environment_overrides_are_validated_at_submission(tmp_path: Path) -> None:
    package = _package(
        tmp_path / "submission-environment",
        _MANIFEST + '\n[workflow.environment.retries]\ntype = "integer"\n',
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure", encoding="utf-8")
    with pytest.raises(ValueError, match="declared names: retries"):
        new_job(workspace, package, inputs={"structure": structure}, environment={"missing": 1})
    with pytest.raises(ValueError, match="does not match type 'integer'"):
        new_job(workspace, package, inputs={"structure": structure}, environment={"retries": "bad"})
    job = new_job(workspace, package, inputs={"structure": structure}, environment={"retries": 3})
    assert JobDefinition.from_path(job.payload / "job.json").environment["overrides"] == {"retries": 3}


def test_declared_parameters_apply_defaults_enforce_types_and_warn_on_undeclared(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    package = _package(tmp_path / "parameters")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")

    # A declared default is applied for the name nobody supplied, and the
    # declared parameter and input metadata ride along in job.json's 'declared'.
    job = new_job(workspace, package, inputs={"structure": structure})
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.parameters["label"] == "test"
    assert definition.declared["parameters"]["label"] == {"type": "string", "default": "test"}
    assert definition.declared["inputs"]["structure"]["required"] is True

    # A declared type mismatch is an error that names the remedy.
    with pytest.raises(ValueError, match="parameter 'label' does not match type 'string'"):
        new_job(workspace, package, inputs={"structure": structure}, parameters={"label": 5})

    # An undeclared name only warns, through the logging channel, and is kept.
    with caplog.at_level(logging.WARNING, logger="httk.workflow.scaffold"):
        warned = new_job(workspace, package, inputs={"structure": structure}, parameters={"kpont_density": 1})
    assert "job parameter 'kpont_density' is not declared by this workflow; declared: label" in caplog.text
    assert JobDefinition.from_path(warned.payload / "job.json").parameters["kpont_density"] == 1


def test_required_input_is_enforced_and_entry_type_defaults_required(tmp_path: Path) -> None:
    # A distinct workflow id keeps this package's tree digest distinct from the
    # shared _MANIFEST package: the hook-module cache is content-addressed, and
    # byte-identical trees deliberately share one loaded module.
    package = _package(
        tmp_path / "required", _MANIFEST.replace('id = "tests.package"', 'id = "tests.package.required"')
    )
    workspace = Workspace.initialize(tmp_path / "workspace")
    # The structure input declares entry_type, so it defaults to required.
    with pytest.raises(ValueError, match="workflow input 'structure' is required and was not supplied"):
        new_job(workspace, package)

    # An explicit required = false makes the same entry-typed input optional.
    optional = _package(
        tmp_path / "optional",
        _MANIFEST.replace('destination = "POSCAR"', 'destination = "POSCAR"\nrequired = false'),
    )
    job = new_job(workspace, optional)
    assert JobDefinition.from_path(job.payload / "job.json").declared["inputs"]["structure"]["required"] is False


def test_format_rejects_manifest_package_directory(tmp_path: Path) -> None:
    package = _package(tmp_path / "format-package")
    with pytest.raises(ValueError, match="language comes from the manifest"):
        resolve_workflow(package, format="cwl")


@pytest.mark.parametrize(
    ("runner", "document", "message"),
    [
        ('maker = "atomate2:Maker"', True, "both"),
        ("", False, "neither"),
        ('maker = "atomate2:Maker"\nmaker_options = {}', False, "unknown runner option"),
    ],
)
def test_jobflow_language_manifest_validates_source_and_options(
    tmp_path: Path, runner: str, document: bool, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_jobflow_package(tmp_path / message.replace(" ", "-"), runner, document=document))


def test_required_and_forbidden_language_documents_remain_enforced(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "cwl")
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8").replace('document = "echo.cwl"\n', "")
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(ValueError, match="requires.*document"):
        parse_workflow_manifest(package)

    v1 = tmp_path / "v1"
    v1.mkdir()
    (v1 / "httk_workflow.toml").write_text(
        '''[workflow]
id = "tests.v1"

[workflow.runner]
language = "httk-v1"
document = "maker.json"
''',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="not used"):
        parse_workflow_manifest(v1)


def test_language_collect_file_overrides_default(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "cwl", manifest_extra='\n[workflow.collect]\nfile = "collect.py"')
    (package / "collect.py").write_text("def collect(record):\n    return {}\n", encoding="utf-8")
    provider = parse_workflow_manifest(package)
    assert provider.collect_file == "collect.py"
    assert callable(provider.collector)


def test_manifest_parses_curated_postprocess_scripts_in_order(tmp_path: Path) -> None:
    manifest = (
        _MANIFEST
        + """

[workflow.postprocess.relaxation-report]
file = "scripts/relaxation_report"
description = "write a relaxation report"

[workflow.postprocess.dos-plot]
file = "scripts/plot_dos.sh"
"""
    )
    package = _package(tmp_path / "package", manifest)
    scripts = package / "scripts"
    scripts.mkdir()
    (scripts / "relaxation_report").write_text("#!/bin/sh\n", encoding="utf-8")
    (scripts / "plot_dos.sh").write_text("#!/bin/sh\n", encoding="utf-8")

    provider = parse_workflow_manifest(package)

    assert list(provider.postprocess_scripts) == ["relaxation-report", "dos-plot"]
    assert provider.postprocess_scripts == {
        "relaxation-report": {"file": "scripts/relaxation_report", "description": "write a relaxation report"},
        "dos-plot": {"file": "scripts/plot_dos.sh", "description": None},
    }


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ("\n[workflow.postprocess]\nfile = \"postprocess.py\"\n", r"\[workflow\.collect\]"),
        ("\npostprocess = \"postprocess.py\"\n", r"\[workflow\.collect\]"),
    ],
)
def test_manifest_teaches_the_postprocess_shape(tmp_path: Path, extra: str, message: str) -> None:
    manifest = _MANIFEST + extra
    if extra.startswith("\npostprocess ="):
        manifest = _MANIFEST.replace("\n[workflow.runner]", extra + "\n[workflow.runner]")
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_package(tmp_path / "package", manifest))


@pytest.mark.parametrize(
    ("file", "message"),
    [("missing.sh", "does not exist"), ("scripts/report.txt", "unknown key")],
)
def test_manifest_validates_postprocess_members_and_keys(tmp_path: Path, file: str, message: str) -> None:
    manifest = _MANIFEST + f'\n[workflow.postprocess.report]\nfile = "{file}"\n'
    if message == "unknown key":
        manifest += "unknown = true\n"
    package = _package(tmp_path / "package", manifest)
    if file.startswith("scripts/"):
        (package / "scripts").mkdir()
        (package / file.removeprefix("scripts/")).write_text("#!/bin/sh\n", encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(package)


def test_manifest_rejects_bad_postprocess_names_and_empty_descriptions(tmp_path: Path) -> None:
    manifest = (
        _MANIFEST
        + """

[workflow.postprocess.""]
file = "report.sh"
description = ""
"""
    )
    package = _package(tmp_path / "package", manifest)
    (package / "report.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.postprocess\] name"):
        parse_workflow_manifest(package)

    manifest = _MANIFEST + '\n[workflow.postprocess.report]\nfile = "report.sh"\ndescription = ""\n'
    package = _package(tmp_path / "empty-description", manifest)
    (package / "report.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.postprocess\.report\]\.description"):
        parse_workflow_manifest(package)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ('entry = "run"', "entry is implied by language"),
        ('steps = ["start"]', "steps is implied by language"),
        ('initial_step = "start"', "initial_step is implied by language"),
    ],
)
def test_language_runner_fields_are_forbidden(tmp_path: Path, extra: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(_language_package(tmp_path / "package", manifest_extra=extra))


def test_language_manifest_forbids_instantiate_and_destinations(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "instantiate")
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    (package / "httk_workflow.toml").write_text(
        manifest + '\n[workflow.instantiate]\nfile = "run.py"\n', encoding="utf-8"
    )
    (package / "run.py").write_text("def instantiate(context): pass\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.instantiate\].*implied"):
        parse_workflow_manifest(package)
    package = _language_package(tmp_path / "destination")
    manifest = (
        (package / "httk_workflow.toml")
        .read_text(encoding="utf-8")
        .replace('entry_type = "strings"', 'entry_type = "strings"\ndestination = "message"')
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.inputs\.message\]\.destination.*implied"):
        parse_workflow_manifest(package)


@pytest.mark.parametrize(
    "change",
    [
        'document = "missing.cwl"',
        'language = "unknown"\ndocument = "echo.cwl"',
        'modules = []',
    ],
)
def test_language_manifest_reports_missing_or_invalid_runner_data(tmp_path: Path, change: str) -> None:
    package = _language_package(tmp_path / "package")
    manifest = (package / "httk_workflow.toml").read_text(encoding="utf-8")
    if change.startswith("document"):
        manifest = manifest.replace('document = "echo.cwl"', change)
        message = "document.*does not exist"
    elif change.startswith("language"):
        manifest = manifest.replace('language = "cwl"\ndocument = "echo.cwl"', change)
        message = "available languages.*cwl.*pwd"
    else:
        manifest = manifest.replace('document = "echo.cwl"', 'document = "echo.cwl"\n' + change)
        message = "unknown runner option"
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        parse_workflow_manifest(package)


def test_language_ports_default_and_validate_names(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "ports", manifest_extra='')
    manifest = (
        (package / "httk_workflow.toml")
        .read_text(encoding="utf-8")
        .replace('[workflow.outputs.spoken]', '[workflow.outputs.spoken]\nport = "missing"')
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.outputs\.spoken\].*known ports: spoken"):
        parse_workflow_manifest(package)
    package = _language_package(tmp_path / "default")
    manifest = (
        (package / "httk_workflow.toml")
        .read_text(encoding="utf-8")
        .replace("[workflow.inputs.message]", '[workflow.inputs.message]\nport = "message"')
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    provider = parse_workflow_manifest(package)
    assert provider._input_metadata["message"]["port"] == "message"


def test_language_inputs_reject_duplicate_effective_ports(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "inputs")
    manifest = (
        (package / "httk_workflow.toml").read_text(encoding="utf-8")
        + """
[workflow.inputs.message_alias]
entry_type = "strings"
port = "message"
"""
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(
        ValueError, match=r"\[workflow\.inputs\.message_alias\].*\[workflow\.inputs\.message\].*message"
    ):
        parse_workflow_manifest(package)


def test_language_outputs_reject_duplicate_effective_ports(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "outputs")
    manifest = (
        (package / "httk_workflow.toml").read_text(encoding="utf-8")
        + """
[workflow.outputs.spoken_alias]
entry_type = "strings"
port = "spoken"
"""
    )
    (package / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    with pytest.raises(ValueError, match=r"\[workflow\.outputs\.spoken_alias\].*\[workflow\.outputs\.spoken\].*spoken"):
        parse_workflow_manifest(package)


def test_port_is_rejected_in_entry_manifests(tmp_path: Path) -> None:
    manifest = _MANIFEST.replace('[workflow.inputs.structure]', '[workflow.inputs.structure]\nport = "structure"')
    with pytest.raises(ValueError, match=r"\[workflow\.inputs\.structure\].*port"):
        parse_workflow_manifest(_package(tmp_path / "entry", manifest))


def test_language_resolution_scaffolds_without_publishing(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "package")
    provider = load_workflow_package(package, register=False)
    resolved = resolve_workflow(package)
    assert resolved.language == provider.language
    assert resolved.document_path == package.resolve() / "echo.cwl"
    with pytest.raises(ValueError, match="never published"):
        _ = resolved.store_name
    job = new_job(Workspace.initialize(tmp_path / "workspace"), package)
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.runner_path.as_posix() == "pkg:httk.workflow.languages.cwl/cwl_runner.py"
    assert definition.runner_source == "installed"
    assert definition.parameters["workflow_language"] == "cwl"
    assert definition.parameters["cwl_document"] == "files/workflow.cwl.json"
    assert definition.parameters["cwl_output_roles"] == {"spoken": "spoken"}
    assert (job.payload / "files" / "workflow.cwl.json").is_file()
    assert (job.payload / "files" / "inputs.json").is_file()


def test_pwd_module_members_are_package_files(tmp_path: Path) -> None:
    package = _language_package(tmp_path / "missing", "pwd", 'modules = ["missing.py"]')
    with pytest.raises(ValueError, match="modules.*does not exist"):
        parse_workflow_manifest(package)
    package = _language_package(tmp_path / "text", "pwd", 'modules = ["module.txt"]')
    (package / "module.txt").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match=r"\.py"):
        parse_workflow_manifest(package)


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
    declaration["outputs"] = [
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
        ("parameter_name", "not_a_role", "is not a manifest input role"),
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
        declaration["outputs"][0]["name"] = value
    elif field == "output_type":
        declaration["outputs"][0]["entry_type"] = value
    else:
        declaration["inputs"][0]["name"] = value
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
            "[workflow.inputs.structure]\ndestination = \"POSCAR\"\nunknown = true",
            r"unknown key \[workflow\.inputs\.structure\.unknown\]",
        ),
        ("steps = []", "steps must be a nonempty list"),
        ("steps = [\"one\", \"two\"]", "initial_step is required"),
        ("product_of = \"missing\"", "product_of names unknown input or output role"),
        ("[workflow.parameters.label]\ntype = \"integer\"\ndefault = \"bad\"", "default does not match type"),
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
    elif change.startswith("[workflow.inputs"):
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
    with pytest.raises(ValueError, match="both an input and output role"):
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
    scaffold._WORKFLOW_PROVIDERS.pop(loaded.workflow_id, None)


def test_workflow_package_precedes_document_matching(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package(tmp_path / "package")
    from httk.workflow import languages

    monkeypatch.setattr(languages, "match_document", lambda path: pytest.fail("package reached document matching"))

    assert resolve_workflow(package).workflow_id == "tests.package"


def test_directory_workflow_scaffolds_from_the_published_tree_and_pins_declarations(tmp_path: Path) -> None:
    package = _package(tmp_path / "package")
    structure = tmp_path / "POSCAR"
    structure.write_text("structure", encoding="utf-8")
    workspace = Workspace.initialize(tmp_path / "workspace")
    job = new_job(workspace, package, inputs={"structure": structure}, parameters={"label": "job"})
    definition = JobDefinition.from_path(job.payload / "job.json")
    assert definition.workflow == "tests.package"
    assert definition.runner_source == "workspace"
    assert definition.runner_sha256 == tree_digest(package)
    assert definition.declarations["workflow"]["$id"] == "https://example.test/workflows/package"
    # The hook module cache is content-addressed: byte-identical package trees
    # share one loaded module, so __file__ may name any identical published
    # tree. Assert the hook ran from a published runner store, not which one.
    instantiated = (job.payload / "instantiated.txt").read_text(encoding="utf-8")
    assert instantiated.endswith("instantiate.py")
    assert "/.httk-workflow/runners/" in instantiated
    digest = str(job.runner["path"]).rsplit(".", 1)[-1]
    assert Path(instantiated).parent.name.endswith(digest)
    assert workspace.runner_store_path(job.runner["path"]).is_dir()  # type: ignore[arg-type]

    stored = workspace.runner_store_path(str(job.runner["path"]))
    stored.chmod(0o755)
    (stored / "collect.py").chmod(0o644)
    (stored / "collect.py").write_text("def collect(record):\n    return {'changed': True}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="published workflow tree"):
        new_job(workspace, package, inputs={"structure": structure})
