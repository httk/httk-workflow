"""Named operator identity configuration and signing."""

import json
from pathlib import Path

import pytest
from httk.core.cli import CLIContext

from httk.workflow.configuration import (
    config_path,
    ensure_identity_key,
    identity_key_paths,
    identity_seed,
    read_config,
    resolve_operator_identity,
    set_config_key,
    sign_document,
    verify_document,
    write_config,
)
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
    assert _command(tmp_path, "config", "show") == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["default_identity"] == "ci_bot" and set(shown["identities"]) == {"alice", "ci_bot"}

    assert _command(tmp_path, "config", "identity", "default", "ci_bot") == 0
    capsys.readouterr()
    assert _command(tmp_path, "config", "identity", "remove", "ci_bot") == 0
    output = capsys.readouterr().out
    assert "identity-ci_bot.seed" in output and identity_key_paths("ci_bot")[0].exists()
    assert read_config()["default_identity"] == "alice"


def test_identity_resolution_order_and_literal_selector(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="config identity add"):
        resolve_operator_identity(None)

    _command(tmp_path, "config", "identity", "add", "alice", "--name", "Alice", "--email", "alice@example.test")
    _command(tmp_path, "config", "identity", "add", "bob", "--name", "Bob", "--email", "bob@example.test")
    assert resolve_operator_identity("alice").label == "Alice <alice@example.test>"
    assert resolve_operator_identity(None).short == "alice"
    assert resolve_operator_identity("<forward@example.test>").label == " <forward@example.test>"
    assert resolve_operator_identity("<forward@example.test>").seed_path == resolve_operator_identity("alice").seed_path
    with pytest.raises(ValueError, match="alice, bob"):
        resolve_operator_identity("missing")

    values = read_config()
    values.pop("default_identity")
    values["name"] = "Legacy"
    values["email"] = "legacy@example.test"
    config_path().write_text(json.dumps(values), encoding="utf-8")
    assert resolve_operator_identity(None).short is None


def test_literal_selector_without_config_is_unsigned(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    identity = resolve_operator_identity("Ext <e@x>")
    document = {"format": "test"}
    assert identity.seed_path is None
    assert sign_document(document, seed_path=identity.seed_path) == document


def test_literal_selector_does_not_hide_dangling_or_corrupt_config(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": {"alice": {"name": "Alice", "email": "alice@example.test"}},
            "default_identity": "missing",
        }
    )
    with pytest.raises(ValueError, match="config identity default"):
        resolve_operator_identity("Ext <e@x>")

    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": "corrupt",
        }
    )
    with pytest.raises(ValueError, match="identities.*object"):
        resolve_operator_identity("Ext <e@x>")


@pytest.mark.parametrize(
    "selector",
    ("Alice <", "Alice <mail", "Alice <mail@x> trailing", "Alice <>", "Alice <mail>", "Alice<mail@x>"),
)
def test_malformed_literal_selector_is_rejected(tmp_path: Path, monkeypatch, selector: str) -> None:
    _isolate(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='NAME <EMAIL>'):
        resolve_operator_identity(selector)


def test_identity_key_paths_refuse_symlinks(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    seed_path, public_path = identity_key_paths("alice")
    seed_path.parent.mkdir(parents=True)
    target = tmp_path / "target"
    target.write_text("not a key\n", encoding="ascii")

    seed_path.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        ensure_identity_key("alice")

    seed_path.unlink()
    public_path.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        ensure_identity_key("alice")
    public_path.unlink()

    ensure_identity_key("alice")
    seed_path.chmod(0o644)
    ensure_identity_key("alice")
    assert seed_path.stat().st_mode & 0o777 == 0o600


def test_add_succeeds_without_default_in_existing_multi_identity_config(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
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
    values = read_config()
    configured = values["identities"]
    assert isinstance(configured, dict)
    assert set(configured) == {"alice", "bob", "carol"}
    assert "default_identity" not in values


def test_dangling_default_is_loud_warned_and_cleared(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": {"alice": {"name": "Alice", "email": "alice@example.test"}},
            "default_identity": "missing",
        }
    )

    with pytest.raises(ValueError, match="config identity default"):
        resolve_operator_identity(None)
    with pytest.raises(ValueError, match="config identity default"):
        identity_seed()

    assert _command(tmp_path, "config", "identity", "list", "--json") == 0
    captured = capsys.readouterr()
    assert "warning: config identity default" in captured.err

    assert _command(tmp_path, "config", "identity", "remove", "alice") == 0
    assert "default_identity" not in read_config()


def test_single_identity_and_legacy_resolution_and_named_signing(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    _command(tmp_path, "config", "identity", "add", "alice", "--name", "Alice", "--email", "alice@example.test")
    assert resolve_operator_identity(None).short == "alice"
    default_signed = sign_document({"format": "test"})
    assert (
        default_signed["operator_key"]
        == "ed25519:" + identity_key_paths("alice")[1].read_text(encoding="ascii").strip()
    )
    signed = sign_document({"format": "test"}, seed_path=resolve_operator_identity("alice").seed_path)
    assert verify_document(signed).valid
    assert signed["operator_key"] == "ed25519:" + identity_key_paths("alice")[1].read_text(encoding="ascii").strip()

    values = read_config()
    values.pop("identities")
    values.pop("default_identity")
    values["name"] = "Legacy"
    values["email"] = "legacy@example.test"
    config_path().write_text(json.dumps(values), encoding="utf-8")
    ensure_identity_key()
    assert resolve_operator_identity(None).label == "Legacy <legacy@example.test>"


def test_config_set_refuses_identity_members_and_invalid_short(tmp_path: Path, monkeypatch, capsys) -> None:
    _isolate(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="cannot be set"):
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


def test_ambiguous_identities_refuse_signing_like_job_request(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    write_config(
        {
            "format": "httk-config",
            "format_version": 2,
            "identities": {
                "alice": {"name": "Alice", "email": "alice@example.test"},
                "bob": {"name": "Bob", "email": "bob@example.test"},
            },
        }
    )
    # Two identities and no default is ambiguous. Signing (a transfer
    # acknowledgement) must refuse just as ``job request`` does, rather than
    # silently falling back to the legacy identity key.
    with pytest.raises(ValueError, match="configure an identity"):
        resolve_operator_identity(None)
    with pytest.raises(ValueError, match="configure an identity"):
        identity_seed()
    with pytest.raises(ValueError, match="configure an identity"):
        sign_document({"format": "test"})


def test_symlinked_seed_is_refused_not_silently_unsigned(tmp_path: Path, monkeypatch) -> None:
    _isolate(tmp_path, monkeypatch)
    real_seed, _ = ensure_identity_key("alice")
    link = tmp_path / "linked.seed"
    link.symlink_to(real_seed)
    # A symlinked (or otherwise refused) but valid seed must raise, not map to
    # None, which would silently produce unsigned documents.
    with pytest.raises(ValueError):
        identity_seed(link)
    # Only a genuinely missing file reads as unsigned.
    assert identity_seed(tmp_path / "absent.seed") is None
