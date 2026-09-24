#!/usr/bin/env python3
"""A three-step stand-in for a packaged relaxation runner: every step just advances."""

from httk.workflow import Attempt, Runner

run = Runner("tests.relax")


@run.step
def prepare(a: Attempt) -> None:
    """Advance to the run step."""
    a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    """Advance to the publish step."""
    a.advance("publish")


@run.step
def publish(a: Attempt) -> None:
    """Complete the job."""
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
