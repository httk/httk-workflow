"""CLI-level named operator identity configuration.

The library-level behaviour of operator identity — resolution order, literal
selectors, signing, seed handling, key permissions — now lives in
``httk.core.identity`` and is covered by httk-core's
``tests/test_operator_identity.py``. What remains here is only the workflow CLI
wiring: that ``httk workflow config identity add|list|default|remove`` drives the
core identity store in ``identity.json`` and reports it the way it used to.
"""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext
from httk.core.identity import (
    identity_key_paths,
    read_identity_config,
    write_identity_config,
)

from httk.workflow.configuration import set_config_key
from httk.workflow.workflow_cli import command


def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("HTTK_CONFIG_HOME", raising=False)
    monkeypatch.delenv("HTTK_DATA_HOME", raising=False)


def _command(tmp_path: Path, *arguments: str) -> int:
    return command(list(arguments), CLIContext("httk", tmp_path))


def test_identity_cli_round_trip_and_key_permissions(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)

    assert (
        _command(tmp_path, "config", "identity", "add", "alice", "--name", "Alice", "--email", "alice@example.test")
        == 0
    )
    first = json.loads(capsys.readouterr().out)
    assert first["default"]
    assert identity_key_paths("alice")[0].stat().st_mode & 0o777 == 0o600
    assert identity_key_paths("alice")[1].exists()

    assert (
        _command(
            tmp_path,
            "config",
            "identity",
            "add",
            "ci_bot",
            "--name",
            "CI",
            "--email",
            "ci@example.test",
            "--default",
        )
        == 0
    )
    capsys.readouterr()
    assert _command(tmp_path, "config", "identity", "list", "--json") == 0
    listed = json.loads(capsys.readouterr().out)
    assert [item["short"] for item in listed] == ["alice", "ci_bot"]
    assert listed[1]["default"] and listed[1]["public_key"].startswith("ed25519:")
    stored = read_identity_config()
    configured = stored["identities"]
    assert stored["default_identity"] == "ci_bot"
    assert isinstance(configured, dict) and set(configured) == {"alice", "ci_bot"}

    assert _command(tmp_path, "config", "identity", "default", "ci_bot") == 0
    capsys.readouterr()
    assert _command(tmp_path, "config", "identity", "remove", "ci_bot") == 0
    output = capsys.readouterr().out
    assert "identity-ci_bot.seed" in output and identity_key_paths("ci_bot")[0].exists()
    assert read_identity_config()["default_identity"] == "alice"


def test_add_succeeds_without_default_in_existing_multi_identity_config(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    write_identity_config(
        {
            "identities": {
                "alice": {"name": "Alice", "email": "alice@example.test"},
                "bob": {"name": "Bob", "email": "bob@example.test"},
            },
        }
    )

    assert (
        _command(tmp_path, "config", "identity", "add", "carol", "--name", "Carol", "--email", "carol@example.test")
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["default"] is False
    values = read_identity_config()
    configured = values["identities"]
    assert isinstance(configured, dict)
    assert set(configured) == {"alice", "bob", "carol"}
    assert "default_identity" not in values


def test_dangling_default_is_loud_warned_and_cleared(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    write_identity_config(
        {
            "identities": {"alice": {"name": "Alice", "email": "alice@example.test"}},
            "default_identity": "missing",
        }
    )

    assert _command(tmp_path, "config", "identity", "list", "--json") == 0
    captured = capsys.readouterr()
    assert "warning: config identity default" in captured.err

    assert _command(tmp_path, "config", "identity", "remove", "alice") == 0
    assert "default_identity" not in read_identity_config()


def test_config_set_refuses_identity_members_and_invalid_short(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    # Identity members are no longer workflow config keys at all; they live in
    # identity.json, so ``config set`` rejects them as unknown.
    with pytest.raises(ValueError, match="unknown configuration key"):
        set_config_key("identities", "{}")
    assert _command(tmp_path, "config", "identity", "add", "Not-valid", "--name", "N", "--email", "e") == 2
    assert "must match" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("name", "email"),
    (("Bad\nName", "bad@example.test"), ("Bad<Name", "bad@example.test"), ("Good", "bad"), ("Good", "bad > @x")),
)
def test_identity_add_rejects_unforwardable_labels(tmp_path: Path, monkeypatch, capsys, name: str, email: str) -> None:
    _isolate(tmp_path, monkeypatch)
    assert _command(tmp_path, "config", "identity", "add", "bad", "--name", name, "--email", email) == 2
    assert "identity" in capsys.readouterr().err
    assert not identity_key_paths("bad")[0].exists()
