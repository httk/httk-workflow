"""Tests for the daemon launcher settings schema."""

from pathlib import Path

import pytest

from httk.workflow._daemon_launcher import DaemonSettings, parse_daemon_settings


def parse(settings: dict[str, object], force: bool = False) -> DaemonSettings:
    return parse_daemon_settings(settings, force=force)


def test_unknown_keys_are_refused_sorted() -> None:
    with pytest.raises(ValueError, match="unsupported daemon launcher setting: a.b, z.y"):
        parse({"z.y": 1, "a.b": 2, "slurm.partition": "p"})


def test_resources_parse() -> None:
    value = parse(
        {
            "slurm.cpus_per_task": "4",
            "slurm.mem": "2G",
            "slurm.time_limit": "1:30:00",
            "slurm.partition": "debug",
            "slurm.account": "acct",
            "slurm.reservation": "r1",
            "slurm.gres": "gpu:a100=2",
            "manager.workers": 3,
            "manager.command": " httk ",
            "environment.prelude": "module load x",
        }
    )
    assert (value.cpus, value.memory_mb, value.time_minutes) == (4, 2048, 90)
    assert (value.partition, value.account, value.reservation, value.gres) == ("debug", "acct", "r1", "gpu:a100=2")
    assert (value.workers, value.manager_command, value.prelude) == (3, "httk", "module load x")
    assert (value.mpi, value.nodes, value.ranks, value.ntasks_per_node) == (False, 1, 1, None)
    assert parse({}) == DaemonSettings()


def test_sanity_bounds_need_force() -> None:
    with pytest.raises(ValueError, match="sanity limit; pass --force"):
        parse({"slurm.mem": "2T"})
    assert parse({"slurm.mem": "2T"}, force=True).memory_mb == 2 * 1024 * 1024
    with pytest.raises(ValueError, match="pass --force"):
        parse({"slurm.cpus_per_task": 5000})
    assert parse({"slurm.cpus_per_task": 5000}, force=True).cpus == 5000


@pytest.mark.parametrize("key", ["slurm.nodes", "slurm.ntasks", "slurm.ntasks_per_node"])
def test_serial_geometry(key: str) -> None:
    with pytest.raises(ValueError, match=f"{key}=4 requires slurm.mpi=pmix"):
        parse({key: 4})
    for one in (1, "1"):
        assert parse({key: one}).ranks == 1
    with pytest.raises(ValueError, match="requires slurm.mpi"):
        parse({key: True})


def test_mpi_geometry() -> None:
    value = parse({"slurm.mpi": "pmix", "slurm.nodes": 2, "slurm.ntasks_per_node": 8})
    assert (value.mpi, value.nodes, value.ranks, value.ntasks_per_node) == (True, 2, 16, 8)
    value = parse({"slurm.mpi": "pmix", "slurm.nodes": 2, "slurm.ntasks": 5, "slurm.ntasks_per_node": 4})
    assert (value.nodes, value.ranks) == (2, 5)
    assert parse({"slurm.mpi": "pmix"}).ranks == 1
    with pytest.raises(ValueError, match="slurm.ntasks must be from"):
        parse({"slurm.mpi": "pmix", "slurm.nodes": 4, "slurm.ntasks": 2})
    with pytest.raises(ValueError, match="exceeds the approved node placement"):
        parse({"slurm.mpi": "pmix", "slurm.nodes": 1, "slurm.ntasks": 9, "slurm.ntasks_per_node": 8})


def test_mpi_requires_one_worker_and_pmix() -> None:
    with pytest.raises(ValueError, match="manager.workers=1"):
        parse({"slurm.mpi": "pmix", "manager.workers": 2})
    with pytest.raises(ValueError, match="must be 'pmix'"):
        parse({"slurm.mpi": "openmpi"})


def test_site_paths() -> None:
    value = parse(
        {
            "daemon.readonly_paths": "/usr:/opt/x",
            "daemon.sbatch": "/usr/bin/sbatch",
            "daemon.cluster": "c-1.x",
            "daemon.max_submissions": "9",
            "daemon.mpi.max_steps": 7,
        }
    )
    assert value.site == {
        "daemon.readonly_paths": (Path("/usr"), Path("/opt/x")),
        "daemon.sbatch": Path("/usr/bin/sbatch"),
        "daemon.cluster": "c-1.x",
        "daemon.max_submissions": 9,
        "daemon.mpi.max_steps": 7,
    }
    for bad in ("", "rel/x", "/usr::/opt", "/a/../b", "/a:", "/a\0b"):
        with pytest.raises(ValueError):
            parse({"daemon.readonly_paths": bad})
    # The broker and allocation service see the host read-only, so the former broker-only mounts are gone.
    with pytest.raises(ValueError):
        parse({"daemon.broker_paths": "/opt/slurm"})
    with pytest.raises(ValueError):
        parse({"daemon.python": "python"})
    with pytest.raises(ValueError):
        parse({"daemon.cluster": "-x"})


def test_mpi_environment_names() -> None:
    assert parse({"daemon.mpi.environment.FOO_X": "1"}).mpi_environment == {"FOO_X": "1"}
    assert parse({"daemon.mpi.environment.FOO_X": "1"}).site == {}
    for key in ("daemon.mpi.environment.1X", "daemon.mpi.environment.", "daemon.mpi.environment.A-B"):
        with pytest.raises(ValueError, match="unsupported daemon launcher setting"):
            parse({key: "v"})
    with pytest.raises(ValueError):
        parse({"daemon.mpi.environment.A": "v\0"})


def test_termination_grace_bounds() -> None:
    assert parse({"daemon.mpi.termination_grace": "0.5"}).site["daemon.mpi.termination_grace"] == 0.5
    assert parse({"daemon.mpi.termination_grace": 60}).site["daemon.mpi.termination_grace"] == 60.0
    for bad in (0.05, 61, "nan", "x", True):
        with pytest.raises(ValueError, match="termination_grace"):
            parse({"daemon.mpi.termination_grace": bad})


def test_gres_pattern() -> None:
    for bad in ("", "gpu 1", "gpu;1", "x" * 256):
        with pytest.raises(ValueError, match="slurm.gres"):
            parse({"slurm.gres": bad})


def test_max_submissions_is_capped_at_max_records() -> None:
    assert parse({"daemon.max_submissions": 4096}).site["daemon.max_submissions"] == 4096
    with pytest.raises(ValueError, match="from 1 through 4096"):
        parse({"daemon.max_submissions": 4097})


@pytest.mark.parametrize("name", ["PMIX_SERVER_URI", "PMI_RANK", "SLURM_JOB_ID", "OMPI_COMM_WORLD_RANK", "OPAL_PREFIX"])
def test_mpi_environment_reserved_names_refused(name: str) -> None:
    with pytest.raises(ValueError, match="must not override"):
        parse({f"daemon.mpi.environment.{name}": "x"})
    assert parse({"daemon.mpi.environment.OMPI_MCA_btl": "x"}).mpi_environment == {"OMPI_MCA_btl": "x"}


def test_null_geometry_has_its_own_message() -> None:
    with pytest.raises(ValueError, match=r"slurm\.nodes must be a positive integer"):
        parse({"slurm.nodes": None})


def test_isolate_network_flag_spellings() -> None:
    for value, expected in (("false", False), ("FALSE", False), (0, False), ("true", True), ("True", True), (1, True)):
        assert parse({"daemon.isolate_network": value}).site["daemon.isolate_network"] is expected
    assert "daemon.isolate_network" not in parse({}).site
    for bad in ("maybe", 2, "", "0", True, None):
        with pytest.raises(ValueError, match=r"daemon\.isolate_network must be 'true', 'false', 1 or 0"):
            parse({"daemon.isolate_network": bad})
