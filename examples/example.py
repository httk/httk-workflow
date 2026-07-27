#!/usr/bin/env python3
"""A tour of the *httk-workflow* Python API: init, scaffold, run, harvest.

The same path as ``docs/quickstart.md``, one call per command:

* :meth:`httk.workflow.Workspace.initialize` creates the workspace;
* importing :mod:`httk.workflow.vasp` registers its packaged templates, which is
  what lets :func:`~httk.workflow.scaffold.new_job` resolve ``vasp-relax`` by
  name — the generic scaffold never names a domain itself;
* :func:`httk.workflow.scaffold.new_job` builds and submits one job of the
  packaged ``vasp-relax`` template;
* :class:`httk.workflow.TaskManager` runs everything that is ready;
* :func:`httk.workflow.harvest` reads the finished jobs back.

Run it in an empty directory:

.. code-block:: console

    python examples/example.py

It creates ``example-workflow-workspace`` beside the ``POSCAR`` it writes. Without
VASP installed, the mock VASP beside this file is used; set ``HTTK_VASP_COMMAND``
to use the real thing.
"""

import os
from pathlib import Path

import httk.workflow.vasp  # noqa: F401 - registers the packaged vasp-relax template used below
from httk.workflow import TaskManager, Workspace, harvest
from httk.workflow.scaffold import new_job

POSCAR = """silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
"""


def main() -> int:
    """Submit one relaxation, run it, and report what it produced."""

    # How VASP is invoked belongs to this machine, not to the job; the mock one
    # beside this file writes plausible outputs when no real command is configured.
    os.environ.setdefault("HTTK_VASP_COMMAND", str(Path(__file__).with_name("mock_vasp.py")))

    # One workspace is the whole state of the work. The extension lets its jobs
    # publish results as transactional data.
    workspace = Workspace.initialize(
        Path("example-workflow-workspace"),
        extensions=["transactional-data-v1"],
    )
    print(f"workspace {workspace.workspace_id} at {workspace.root}")

    # One job of the packaged relaxation runner. The runner file is published into
    # the workspace store and pinned by digest; the structure is staged where the
    # runner reads it, as files/POSCAR.
    structure = Path("POSCAR")
    structure.write_text(POSCAR, encoding="utf-8")
    job = new_job(
        workspace,
        "vasp-relax",
        files={"POSCAR": structure},
        inputs={"kpoint_density": 20.0, "incar_tags": {"ENCUT": 320}},
        tag="silicon",
    )
    print(f"submitted {job.job_key} at {job.placement}, running {job.runner['path']}")

    # One manager, in this process, until nothing is ready. A deployment runs the
    # same manager as `httk workflow manager run WORKSPACE` instead.
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=300.0)

    # Reading results back is a read-only iteration over the finished jobs.
    for record in harvest(workspace, states=("succeeded", "failed")):
        print(f"{record.state} {record.job_key} ({record.job['workflow']})")
        if record.failure is not None:
            print(f"  failure {record.failure.code}: {record.failure.message}")
        data = record.data
        if data is None:
            continue
        for path in sorted(data.rglob("*")):
            if path.is_file():
                print(f"  published {path.relative_to(data)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
