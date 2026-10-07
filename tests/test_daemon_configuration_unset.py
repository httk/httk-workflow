"""Daemon configuration clearing preserves required fields and validates before saving."""

from pathlib import Path

import pytest

from httk.workflow import _daemon_cli
from test_daemon_setup import (
    _configure,
    _host,  # noqa: F401 -- shared autouse scheduler fixture
    _initialize,
    _layout,
    _state,
    _stored_configuration,
)


def test_unset_optional_and_default_settings(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _initialize(layout, changes=[("set", "max_submissions=7"), ("set", "force=true")])
    assert (
        _daemon_cli.command(
            [
                "configure",
                str(layout.workspace.root),
                "--unset",
                "max_submissions",
                "--unset",
                "force",
                "--unset",
                "sacct",
                "--unset",
                "slurm_conf",
            ],
            program="httk daemon",
        )
        == 0
    )
    settings = _stored_configuration(layout)
    assert settings["max_submissions"] == 128
    assert settings["force"] is False
    assert settings["sacct"] is None and settings["slurm_conf"] is None


@pytest.mark.parametrize("key", ["bwrap", "launchers", "authorized_keys", "cluster", "typo", "force=false"])
def test_unset_required_or_unknown_setting_never_saves(tmp_path: Path, key: str) -> None:
    layout = _layout(tmp_path)
    _initialize(layout)
    path = _state(layout) / "configuration.json"
    before = path.read_bytes()
    with pytest.raises(ValueError):
        _configure(layout, ("set", "max_submissions=7"), ("unset", key))
    assert path.read_bytes() == before
