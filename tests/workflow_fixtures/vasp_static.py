#!/usr/bin/env python3
"""A thin VASP single-point runner built only from the ``httk.workflow.vasp`` step API."""

from httk.workflow import Attempt, Runner
from httk.workflow.vasp import (
    publish_vasp_files,
    run_vasp_step,
    stage_vasp_inputs,
    vasp_data_prefix,
    vasp_static_tags,
)

run = Runner("tests.vasp-static")


@run.step
def prepare(a: Attempt) -> None:
    """Stage and derive single-point inputs."""
    if stage_vasp_inputs(a, extra_tags=vasp_static_tags(a)) is not None:
        a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    """Run VASP with the reviewed remedy ladder."""
    run_vasp_step(a, next_step="publish")


@run.step
def publish(a: Attempt) -> None:
    """Publish the calculation and complete the job."""
    publish_vasp_files(a, prefix=vasp_data_prefix(a))
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
