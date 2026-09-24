#!/usr/bin/env python3
"""A tour of the *httk-workflow* Python API: instantiate, run, collect.

The same path as ``docs/quickstart.md``, one call per command:

* :meth:`httk.workflow.Workspace.initialize` creates the workspace;
* :func:`httk.workflow.scaffold.new_job` builds and submits one job of the
  ``vasp.relax`` workflow, referenced by its git URI ``git+https://github.com/httk/workflows-vasp#vasp-relax``
  (fetching it needs git and, the first time, network access);
* :class:`httk.workflow.TaskManager` runs everything that is ready;
* :func:`httk.workflow.collect` reads the finished jobs back.

Run it in an empty directory:

.. code-block:: console

    python examples/example.py

It creates ``example-workflow-workspace`` beside the ``POSCAR`` it writes. Without
VASP installed, the mock VASP beside this file is used; set ``HTTK_VASP_COMMAND``
to use the real thing. Install ``httk-atomistic`` to read the finished VASP results.
"""

import os
from pathlib import Path

from httk.workflow import TaskManager, Workspace, collect
from httk.workflow.scaffold import new_job

WORKFLOW = "git+https://github.com/httk/workflows-vasp#vasp-relax"

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

    # One workspace is the whole state of the work. VASP results stay in the
    # persistent workdir by default; data_mode="transactional" opts into a copy.
    workspace = Workspace.initialize(Path("example-workflow-workspace"))
    print(f"workspace {workspace.workspace_id} at {workspace.root}")

    # One job of the vasp.relax workflow. Referencing its URI fetches and installs
    # it; the job records the URI pinned to the full commit, and the structure is
    # staged where the runner reads it, as files/POSCAR.
    structure = Path("POSCAR")
    structure.write_text(POSCAR, encoding="utf-8")
    job = new_job(
        workspace,
        WORKFLOW,
        files={"POSCAR": structure},
        parameters={"kpoint_density": 20.0, "incar_tags": {"ENCUT": 320}},
        tag="silicon",
    )
    print(f"submitted {job.job_key} at {job.placement}, running {job.runner['path']}")

    # One manager, in this process, until nothing is ready. A deployment runs the
    # same manager as `httk workflow manager run --workspace WORKSPACE` instead.
    with TaskManager(workspace) as manager:
        manager.run_until_idle(timeout=300.0)

    # Reading results back is a read-only iteration over the finished jobs.
    for item in collect(workspace, states=("succeeded", "failed")):
        record = item.record
        print(f"{record.state} {record.job_key} ({record.job['workflow']})")
        if record.failure is not None:
            print(f"  failure {record.failure.code}: {record.failure.message}")
        result = record.data if record.data is not None else record.workdir
        if result is None:
            continue
        label = "published" if record.data is not None else "workdir"
        for path in sorted(result.rglob("*")):
            if path.is_file():
                print(f"  {label} {path.relative_to(result)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
