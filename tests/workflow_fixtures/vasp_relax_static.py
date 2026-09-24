#!/usr/bin/env python3
"""A thin VASP relax-then-static runner built only from the ``httk.workflow.vasp`` step API."""

from httk.workflow import Attempt, Runner
from httk.workflow.vasp import (
    promote_vasp_relaxation,
    publish_vasp_files,
    run_vasp_step,
    stage_vasp_inputs,
    vasp_data_prefix,
)

run = Runner("tests.vasp-relax-static")


@run.step
def prepare(a: Attempt) -> None:
    """Stage and derive the relaxation inputs."""
    if stage_vasp_inputs(a) is not None:
        a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    """Relax with the reviewed remedy ladder."""
    run_vasp_step(a, next_step="promote")


@run.step
def promote(a: Attempt) -> None:
    """Turn the relaxation into single-point inputs."""
    promote_vasp_relaxation(a, next_step="static")


@run.step
def static(a: Attempt) -> None:
    """Run the single point with the reviewed remedy ladder."""
    run_vasp_step(a, next_step="publish")


@run.step
def publish(a: Attempt) -> None:
    """Publish both stages and complete the job."""
    prefix = vasp_data_prefix(a, "")
    publish_vasp_files(a, prefix=f"{prefix}/static" if prefix else "static")
    publish_vasp_files(a, prefix=f"{prefix}/relax" if prefix else "relax", directory=a.workdir / "relax")
    a.state["static_energy"] = a.state.get("energy")
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
