#!/usr/bin/env python3
"""A complete campaign in one runner file: spawn a child per site, then triage.

Four steps, and nothing declares the shape of the workflow in advance:

``characterize``
    decides how many defect sites there are and spawns one child job per site,
    then waits for all of them.
``relax``
    the step every child runs. It publishes its result as transactional data, or
    fails by its own declared failure code.
``aggregate``
    runs once the children are terminal, writes a report of the ones that
    succeeded, and either completes the campaign or advances to triage.
``triage``
    records which children failed and fails the campaign with a named failure.

Every child job runs *this same file* at a different step: ``ChildSpec`` needs no
payload, and ``RunnerRef.inherit()`` — the default — points at the runner of the
job that spawned it. That is why the runner must live outside the payload, which
is exactly what scaffolding a job does with it.

Run it with the mock relaxation this file performs (no VASP, no site
characterization — the point is the campaign, not the physics):

.. code-block:: console

    httk project init --name campaign
    httk workflow job new \\
        --workflow examples/defect_campaign.py --step characterize \\
        --input sites=3 --input diverging=1 --tag campaign
    httk workflow run
    httk workflow job list

The campaign then fails by design, with ``defects.child_failed``, because
``diverging=1`` made site 1 fail: ``httk workflow job show`` on the parent and on
one child is what this example is for. ``examples/defect_campaign.sh`` is the same
campaign authored in Bash, publishing the same outcomes byte for byte.

Job inputs
----------

* ``sites`` — how many sites to relax, one child job each.
* ``diverging`` (default none) — a comma-separated list of site numbers that fail.
"""

from httk.workflow import Attempt, ChildSpec, Runner

run = Runner("examples.defects")


@run.step
def characterize(a: Attempt) -> None:
    """Spawn one child job per site and wait for every one of them."""

    sites = int(str(a.input("sites")))
    diverging = str(a.input("diverging", "")).split(",")
    # Job state is the runner's own memory of this job; it survives every attempt.
    a.state["sites"] = sites
    for site in range(sites):
        a.spawn(
            ChildSpec(
                step="relax",
                inputs={"site": site, "diverge": str(site) in diverging},
                data_mode="transactional",
                maximum_attempts_per_activation=1,
            ),
            label=f"site-{site}",
            placement="project/children",
        )
    # all_terminal, so one failing child does not hide the ones that worked;
    # on_impossible is the step that runs when the condition can no longer be met.
    a.gather("aggregate", when="all_terminal", on_impossible="triage")


@run.step
def relax(a: Attempt) -> None:
    """Relax one site: the step every spawned child runs."""

    site = a.input("site")
    (a.workdir / "site.txt").write_text(f"{site}\n", encoding="utf-8")
    if a.input("diverge"):
        # A declared failure code is what a parent triages on and what a job lists
        # in retry_on; it is never a stack trace.
        a.fail("relax.diverged", f"site {site} did not relax")
    else:
        # Staged now, applied by the manager exactly once when this outcome commits.
        a.put(a.workdir / "site.txt", "results/site.txt")
        a.succeed()


@run.step
def aggregate(a: Attempt) -> None:
    """Report the children that succeeded, and triage the campaign if any failed."""

    rows = [
        f"{child.label}\t{(child.workdir / 'site.txt').read_text(encoding='utf-8').strip()}\n"
        for child in a.children.succeeded
        if child.workdir is not None
    ]
    (a.workdir / "report.tsv").write_text("".join(rows), encoding="utf-8")
    failed = ",".join(child.label for child in a.children.failed if child.label is not None)
    if failed:
        a.advance("triage", state={"failed": failed})
    else:
        a.succeed()


@run.step
def triage(a: Attempt) -> None:
    """Record which children failed and end the campaign by name."""

    failed = str(a.state.get("failed", ""))
    (a.workdir / "triage.txt").write_text(f"{failed}\n", encoding="utf-8")
    a.log.append("note", f"triaged after {failed or 'no'} failing children")
    a.fail("defects.child_failed", f"failed: {failed}")


if __name__ == "__main__":
    raise SystemExit(run.main())
