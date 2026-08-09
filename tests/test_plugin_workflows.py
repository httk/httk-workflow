import logging
from collections.abc import Iterator
from pathlib import Path, PurePosixPath

import pytest
from httk.core.plugins.install import install_plugin

from httk.workflow import Workspace, scaffold
from httk.workflow._runner_builds import register_build
from httk.workflow.packages import _reset_plugin_workflow_cache
from httk.workflow.scaffold import (
    WorkflowProvider,
    register_workflow,
    registered_workflow_labels,
    registered_workflows,
    resolve_workflow,
    workflow_provider,
)


def _plugin(
    root: Path,
    name: str,
    workflows: list[tuple[str, str | None, bool]],
) -> Path:
    root.mkdir()
    members: list[str] = []
    for index, (workflow_id, alias, build) in enumerate(workflows):
        member = f"workflows/{index}"
        package = root / member
        package.mkdir(parents=True)
        members.append(member)
        alias_line = f'alias = "{alias}"\n' if alias is not None else ""
        build_section = "\n[workflow.build]\ncommand = \"sh build.sh\"\nartifacts = [\"build\"]\n" if build else ""
        (package / "httk_workflow.toml").write_text(
            f'[workflow]\nid = "{workflow_id}"\n{alias_line}\n[workflow.runner]\nsteps = ["run"]\n{build_section}',
            encoding="utf-8",
        )
        (package / "run").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        (package / "run").chmod(0o755)
        if build:
            (package / "build.sh").write_text(
                "#!/bin/sh\nset -eu\nmkdir -p build\ncp run build/run\nchmod +x build/run\n",
                encoding="utf-8",
            )
            (package / "build.sh").chmod(0o755)
    (root / "httk_plugin.toml").write_text(
        f'[plugin]\nname = "{name}"\nworkflows = {members!r}\n',
        encoding="utf-8",
    )
    return root


@pytest.fixture(autouse=True)
def _plugin_cache() -> Iterator[None]:
    scaffold._WORKFLOW_PROVIDERS.pop("test.plugin.flow", None)
    _reset_plugin_workflow_cache()
    yield
    scaffold._WORKFLOW_PROVIDERS.pop("test.plugin.flow", None)
    _reset_plugin_workflow_cache()


def test_installed_plugin_workflow_resolves_by_id_and_alias(tmp_path: Path) -> None:
    assert workflow_provider("test.plugin.flow") is None
    source = _plugin(tmp_path / "plugin", "plugin-one", [("test.plugin.flow", "plugin-flow", False)])
    install_plugin(source)
    _reset_plugin_workflow_cache()

    provider = workflow_provider("test.plugin.flow")
    assert provider is not None and provider.workflow_id == "test.plugin.flow"
    assert workflow_provider("plugin-flow") == provider
    assert resolve_workflow("plugin-flow").workflow_id == "test.plugin.flow"


def test_in_process_registration_wins_over_plugin(tmp_path: Path) -> None:
    source = _plugin(tmp_path / "plugin", "plugin-one", [("test.plugin.flow", "plugin-flow", False)])
    install_plugin(source)
    _reset_plugin_workflow_cache()
    stub = WorkflowProvider("test.plugin.flow", runner_package="stub", runner_file="run", alias="plugin-flow")
    register_workflow(stub)

    assert workflow_provider("test.plugin.flow") is stub
    assert workflow_provider("plugin-flow") == stub


def test_plugin_name_collisions_are_poisoned_but_other_workflows_work(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "one", "plugin-one", [("test.plugin.flow", "one-flow", False)]))
    install_plugin(_plugin(tmp_path / "two", "plugin-two", [("test.plugin.flow", "two-flow", False)]))
    install_plugin(_plugin(tmp_path / "three", "plugin-three", [("test.plugin.other", "other-flow", False)]))
    _reset_plugin_workflow_cache()

    with pytest.raises(ValueError, match="plugin-one.*plugin-two"):
        workflow_provider("test.plugin.flow")
    assert workflow_provider("test.plugin.other") is not None


def test_malformed_plugin_workflow_is_skipped_with_a_warning(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    bad = _plugin(tmp_path / "bad", "plugin-bad", [("test.plugin.bad", "bad-flow", False)])
    (bad / "workflows/0/httk_workflow.toml").write_text("[workflow\n", encoding="utf-8")
    install_plugin(bad)
    install_plugin(_plugin(tmp_path / "good", "plugin-good", [("test.plugin.good", None, False)]))
    _reset_plugin_workflow_cache()

    with caplog.at_level(logging.WARNING, logger="httk.workflow.packages"):
        assert workflow_provider("test.plugin.good") is not None
    assert "plugin-bad" in caplog.text and "workflows/0" in caplog.text


def test_plugin_workflows_appear_in_listings_and_hints(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "plugin", "plugin-one", [("test.plugin.flow", "plugin-flow", False)]))
    _reset_plugin_workflow_cache()

    assert "test.plugin.flow" in registered_workflows()
    assert "test.plugin.flow (plugin-flow) [plugin plugin-one]" in registered_workflow_labels()
    with pytest.raises(ValueError) as excinfo:
        resolve_workflow("test.plugin.flo")
    assert "test.plugin.flow" in str(excinfo.value)


def test_plugin_workflow_build_publishes_and_registers_artifacts(tmp_path: Path) -> None:
    install_plugin(_plugin(tmp_path / "plugin", "plugin-builder", [("test.plugin.build", None, True)]))
    _reset_plugin_workflow_cache()
    provider = workflow_provider("test.plugin.build")
    assert provider is not None and provider.directory is not None and provider.build is not None

    resolved = resolve_workflow("test.plugin.build")
    workspace = Workspace.initialize(tmp_path / "workspace")
    reference = workspace.publish_runner(resolved.source, name="plugin-build")
    artifacts = register_build(
        workspace,
        workspace.runner_store_path("plugin-build"),
        PurePosixPath("plugin-build"),
        provider.build,
        source_sha256=str(reference["sha256"]),
    )
    assert (artifacts / "build" / "run").is_file()
