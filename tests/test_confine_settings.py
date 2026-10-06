"""Parse and validate the ``manager.confine`` and ``confine.*`` settings."""

import os
import shutil
import sys
import types
from pathlib import Path

import pytest

import httk
from httk.workflow import _confine
from httk.workflow._confine import (
    CONFINE_OVERRIDE_KEYS,
    ConfineSettings,
    confine_settings,
    default_readonly_paths,
    filtered_attempt_environment,
    is_override_key,
    launch_mpi_setting,
)


def test_defaults_leave_attempts_unconfined_with_isolated_network() -> None:
    settings = confine_settings({"slurm.partition": "debug", "manager.workers": 4})
    found = shutil.which("bwrap")
    assert settings == ConfineSettings(
        mode="none",
        readonly_paths=default_readonly_paths(),
        isolate_network=True,
        bwrap=None if found is None else Path(found).resolve(),
        devices=(),
        pmix_roots=(),
        shm_root=Path("/dev/shm"),
        environment=(),
    )
    # A null value reads as unset.
    assert confine_settings({"manager.confine": None, "confine.isolate_network": None}) == settings


def test_default_readonly_paths_drop_nested_entries_and_include_the_prefix() -> None:
    paths = default_readonly_paths()
    assert all(path.is_absolute() for path in paths)
    assert not any(left != right and left.is_relative_to(right) for left in paths for right in paths)
    prefix = Path(__import__("sys").prefix).resolve()
    assert any(prefix == path or prefix.is_relative_to(path) for path in paths)


def test_default_readonly_paths_follow_the_interpreter_and_the_httk_import_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An editable checkout outside the prefix, a regular install under it, and an editable-finder hook entry.
    editable = tmp_path / "checkout" / "src"
    (editable / "httk").mkdir(parents=True)
    installed = Path(sys.prefix).resolve() / "lib" / "site-packages" / "httk"
    monkeypatch.setattr(
        httk, "__path__", [str(editable / "httk"), str(installed), "__editable__.x.finder.__path_hook__"]
    )
    hooked = tmp_path / "hooked" / "src"
    monkeypatch.setattr(_confine, "_editable_finder_roots", lambda: {hooked})
    candidates = {Path(path).resolve() for path in ("/usr", "/bin", "/lib", "/lib64", "/etc") if os.path.exists(path)}
    candidates |= {Path(sys.prefix).resolve(), Path(sys.base_prefix).resolve(), editable, hooked}
    expected = {
        path for path in candidates if not any(path != other and path.is_relative_to(other) for other in candidates)
    }
    paths = default_readonly_paths()
    assert set(paths) == expected
    assert editable in paths and installed.parent not in paths


def test_editable_finder_hooks_yield_their_import_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    finder = types.ModuleType("__editable___fake_1_0_finder")
    finder.MAPPING = {  # type: ignore[attr-defined]
        "httk.codes": "/checkout/src/httk/codes",
        "httk.registry.codes": "/checkout/src/httk/registry/codes",
        "other": "/elsewhere/other",
    }
    monkeypatch.setitem(sys.modules, finder.__name__, finder)
    roots = _confine._editable_finder_roots()
    assert Path("/checkout/src").resolve() in roots
    assert not any(root.is_relative_to("/elsewhere") for root in roots)


def test_default_readonly_paths_include_etc() -> None:
    # Alternatives symlinks, ld.so.cache, passwd and localtime live there; it is read-only and same-principal.
    etc = Path("/etc").resolve()
    assert any(etc == path or etc.is_relative_to(path) for path in default_readonly_paths())


def test_every_key_is_parsed() -> None:
    settings = confine_settings(
        {
            "manager.confine": "bwrap",
            "confine.readonly_paths": "/usr:/opt/site/modules",
            "confine.isolate_network": "FALSE",
            "confine.bwrap": "/opt/bwrap/bin/bwrap",
            "confine.devices": "/dev/infiniband/uverbs0:/dev/nvidia0",
            "confine.pmix_roots": "/var/spool/slurmd:/tmp/pmix",
            "confine.shm_root": "/scratch/shm",
            "confine.environment.OMPI_MCA_btl_vader_single_copy_mechanism": "none",
            "confine.environment.A_NUMBER": 3,
        }
    )
    assert settings == ConfineSettings(
        mode="bwrap",
        readonly_paths=(Path("/usr"), Path("/opt/site/modules")),
        isolate_network=False,
        bwrap=Path("/opt/bwrap/bin/bwrap"),
        devices=(Path("/dev/infiniband/uverbs0"), Path("/dev/nvidia0")),
        pmix_roots=(Path("/var/spool/slurmd"), Path("/tmp/pmix")),
        shm_root=Path("/scratch/shm"),
        environment=(("A_NUMBER", "3"), ("OMPI_MCA_btl_vader_single_copy_mechanism", "none")),
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("True", True),
        ("false", False),
        ("FALSE", False),
        ("1", True),
        ("0", False),
        (1, True),
        (0, False),
    ],
)
def test_booleans_accept_words_and_zero_or_one(value: object, expected: bool) -> None:
    assert confine_settings({"confine.isolate_network": value}).isolate_network is expected


@pytest.mark.parametrize("value", ["yes", "", "2", 2, True, 1.0, "on"])
def test_malformed_booleans_are_refused(value: object) -> None:
    with pytest.raises(ValueError, match="confine.isolate_network must be true or false"):
        confine_settings({"confine.isolate_network": value})


@pytest.mark.parametrize("value", ["Bwrap", "landlock", "", 1])
def test_unknown_modes_are_refused(value: object) -> None:
    with pytest.raises(ValueError, match="manager.confine must be none or bwrap"):
        confine_settings({"manager.confine": value})


@pytest.mark.parametrize("key", ["confine.termination_grace", "confine.readonly", "confine.", "confine.environment"])
def test_unknown_confine_keys_are_refused_by_name(key: str) -> None:
    with pytest.raises(ValueError, match=f"unknown confinement setting '{key}'"):
        confine_settings({key: "x"})


@pytest.mark.parametrize("key", ["confine.readonly_paths", "confine.devices", "confine.pmix_roots"])
@pytest.mark.parametrize("value", ["relative/path", "/usr::/opt", "", "/usr:", "/opt/../etc", "/opt/\0x", 7])
def test_bad_path_lists_are_refused(key: str, value: object) -> None:
    with pytest.raises(ValueError, match=key):
        confine_settings({key: value})


@pytest.mark.parametrize("key", ["confine.bwrap", "confine.shm_root"])
@pytest.mark.parametrize("value", ["bwrap", "", "/a/../b", 3])
def test_bad_single_paths_are_refused(key: str, value: object) -> None:
    with pytest.raises(ValueError, match=key):
        confine_settings({key: value})


@pytest.mark.parametrize("path", ["/", "/tmp", "/tmp/home", "/tmp/home/x", "/proc", "/proc/sys", "/dev", "/dev/shm"])
def test_readonly_paths_cannot_cover_the_private_sandbox_mounts(path: str) -> None:
    with pytest.raises(ValueError, match="must not cover"):
        confine_settings({"confine.readonly_paths": f"/usr:{path}"})
    # Below the private /tmp is fine: it is bound onto the sandbox's own tmpfs.
    assert confine_settings({"confine.readonly_paths": "/tmp/site"}).readonly_paths == (Path("/tmp/site"),)


@pytest.mark.parametrize("device", ["/dev", "/opt/dev/gpu0", "/devices/x"])
def test_devices_must_lie_below_dev(device: str) -> None:
    with pytest.raises(ValueError, match="below /dev"):
        confine_settings({"confine.devices": device})


@pytest.mark.parametrize(
    "name", ["HTTK_WORKFLOW_LAUNCH", "HTTK_X", "1BAD", "BAD-NAME", "A" * 129, "with.dot", "with space"]
)
def test_bad_environment_names_are_refused(name: str) -> None:
    with pytest.raises(ValueError, match="must name a variable"):
        confine_settings({f"confine.environment.{name}": "1"})


@pytest.mark.parametrize("value", ["x" * 4097, "a\0b", True, None, ["list"]])
def test_bad_environment_values_are_refused(value: object) -> None:
    with pytest.raises(ValueError, match="confine.environment.NAME"):
        confine_settings({"confine.environment.NAME": value})
    assert confine_settings({"confine.environment.NAME": "é" * 2048}).environment == (("NAME", "é" * 2048),)


def test_missing_bwrap_is_not_a_settings_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_confine.shutil, "which", lambda _name: None)
    assert confine_settings({"manager.confine": "bwrap"}).bwrap is None


def test_override_keys_are_the_manager_launch_keys_and_every_confine_key() -> None:
    assert CONFINE_OVERRIDE_KEYS == {
        "manager.confine",
        "manager.launch_template",
        "manager.launch_mpi",
        "manager.bind_cpus",
    }
    for key in (*CONFINE_OVERRIDE_KEYS, "confine.readonly_paths", "confine.environment.X", "confine.unknown"):
        assert is_override_key(key)
    for key in ("manager.workers", "manager.launch", "manager.allocation", "slurm.partition", "confined.x", "confine"):
        assert not is_override_key(key)


@pytest.mark.parametrize("value", ["pmi2", "pmix", "none"])
def test_launch_mpi_accepts_plugin_names(value: str) -> None:
    assert launch_mpi_setting({"manager.launch_mpi": value}) == value
    assert launch_mpi_setting({}) is None


@pytest.mark.parametrize("value", ["PMI2", "pmi 2", "", "a" * 33, 2])
def test_launch_mpi_refuses_other_values(value: object) -> None:
    with pytest.raises(ValueError, match="manager.launch_mpi"):
        launch_mpi_setting({"manager.launch_mpi": value})


def test_filtered_environment_hides_the_scheduler_and_sets_private_paths() -> None:
    source = {
        "PATH": "/usr/bin",
        "HOME": "/home/user",
        "TMPDIR": "/scratch",
        "SLURM_JOB_ID": "1",
        "SLURM_CONF": "/etc/slurm.conf",
        "SRUN_DEBUG": "1",
        "SBATCH_ACCOUNT": "a",
        "SALLOC_PARTITION": "p",
        "PMI_RANK": "0",
        "PMIX_RANK": "0",
        "OMPI_MCA_x": "kept",
        "HTTK_WORKFLOW_JOB_DIR": "/ws/job",
    }
    filtered = filtered_attempt_environment(source)
    assert filtered == {
        "PATH": "/usr/bin",
        "HOME": "/tmp/home",
        "TMPDIR": "/tmp",
        "OMPI_MCA_x": "kept",
        "HTTK_WORKFLOW_JOB_DIR": "/ws/job",
        "HTTK_WORKFLOW_CONFINED": "1",
    }
    assert source["HOME"] == "/home/user"
