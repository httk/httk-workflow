# Quickstart

*For anyone meeting httk-workflow for the first time; run this walkthrough from a clone of httk-workflow. No VASP is required.*

The mock VASP used below lives at `examples/mock_vasp.py` in that checkout; the
walkthrough does not run from an arbitrary empty directory.

Six commands from a checkout to a finished VASP relaxation whose results
you can read back. Nothing here needs a runner to be written, a graph to be
declared, or a database to exist.

```{admonition} No VASP? Run the mock one
:class: tip

Every command below works without VASP installed: `examples/mock_vasp.py` writes
the output files a finished run leaves behind, so the whole path — prepare, run,
publish, collect — is exercised for real, with meaningless numbers.

The complete sequence of this page is also `examples/quickstart.sh`, which runs
it in whatever directory you start it in.
```

## A structure to start from

Any VASP-5 `POSCAR` in an empty working directory will do. If you have none at
hand, this is one:

```console
$ cat >POSCAR <<'END'
silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
END
```

## The five commands

```console
$ httk project init --name quickstart
$ httk workflow workspace init . --name default
$ httk workflow job new --workflow vasp-relax --parameter structure=POSCAR --tag silicon
$ httk workflow workspace settings set vasp.command "$PWD/examples/mock_vasp.py"
$ httk workflow run
$ httk workflow collect
```

On a VASP machine, set `vasp.command` to a command such as
`"srun -n 32 vasp_std"` instead.

## What each command did

**`project init`** created the project anchor. The next command initialized and
registered the workspace at the project root as `default`; project creation
does not create or contain a workspace.
The workspace is the state of the work, and transactional data makes a finished
calculation readable without looking inside a workdir.

**`job new`** built and submitted one job. `--workflow vasp-relax` is the packaged
relaxation runner — one file, three steps, the reviewed remedy ladder — so no
runner had to be written. The runner file is published into the workspace and the
job pins its digest, so upgrading *httk-workflow* underneath a queued campaign
cannot change what its jobs execute. `--parameter structure=POSCAR` copied the
creation-time structure parameter as the
`files/POSCAR` the runner reads, and `--tag silicon` made the job's key readable.
The command printed the job key and the payload directory:

```console
silicon--0c4f…	/…/jobs/silicon--0c4f…
```

**`settings set`** stored workspace state that travels with the job wherever it
runs. The manager exports scalar settings into each attempt environment, so
`vasp.command` becomes `HTTK_VASP_COMMAND`; a real VASP machine can set it to
`"srun -n 32 vasp_std"`. A real environment variable remains a deployment
override and wins over the workspace setting.

**`run`** ran a task manager until nothing was ready, driving the job through
`prepare`, `run`, and `publish`. With `--idle` the same manager keeps serving
the workspace, which is how a campaign is run.

**`collect`** printed one JSON summary per finished job: what ran, where its files
are, and everything that happened on the way. That record is the boundary to a
data layer — see {doc}`collecting`.

## Looking at a job

```console
$ httk workflow job list
JOB                                  STATE       STEP             PRI PLACEMENT
silicon--0c4f…                       succeeded   publish          500 jobs

$ httk workflow job show silicon
$ httk workflow job why silicon
```

Any job UUID, complete `tag--uuid` key, or unique prefix of either names a job.
`job show` describes it from its authoritative state, and `job why` explains a job
that is *not* progressing — an unmet capability, a paused job, no manager
running. When something did go wrong, the finished job's results and logs are in
the payload directory `job new` printed: `run/` is the workdir, `data/` holds the
published files, and `job log` prints the transitions.

`job debug WORKSPACE JOB` drives one job in the foreground and prints every
transition, which is the fastest loop while a runner is still being written.

## Many jobs at once

Point `--parameter-from structure` at a *directory* and every readable structure
file in it becomes one job, each tagged after its file:

```console
$ httk workflow job new quickstart-workspace --workflow vasp-relax --parameter-from structure structures/ \
      --input kpoint_density=30.0 --placement project/screening
```

The runner is published once for the whole set, and the jobs are submitted as they
are generated. In Python the same thing streams, which is how a campaign of any
size is built:

```python
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.scaffold import new_jobs, structure_tag

workspace = Workspace.default()
items = ({"parameters": {"structure": path}, "tag": structure_tag(path)} for path in Path("structures").glob("POSCAR.*"))
for job in new_jobs(workspace, "vasp-relax", items, inputs={"kpoint_density": 30.0}):
    print(job.job_key)
```

Neither side of that loop is ever materialized: one runner publication is
amortized over every job, and each job costs one payload directory and one state
marker.

## Where to go next

- {doc}`vasp_runners` — what the packaged VASP runners do, every job input they
  read, and the failure codes they publish.
- {doc}`runtime_helpers` — authoring a runner of your own. `job new --workflow
  ./my_runner.py --step characterize` scaffolds jobs for it exactly as for a
  packaged one; the file is published into the workspace and pinned by digest.
  Two complete campaign runners are in `examples/defect_campaign.py` and
  `examples/defect_campaign.sh`.
- {doc}`native_bash_api` — the same runner protocol from Bash.
- {doc}`importing_workflows` — a Python Workflow Definition or CWL workflow you
  already have becomes one job with `httk workflow import pwd` or
  `httk workflow import cwl`, without being rewritten and without a runner file.
- {doc}`collecting` — turning finished jobs into stored results.
- Running on a cluster — add and configure a remote, initialize `R:NAME`, then
  `transfer LOCAL R:NAME --job JOB` puts jobs there and `run R:NAME` submits a
  manager through its scheduler; a very large run spread across many workspaces
  is a {doc}`campaigns`. See
  {doc}`workflow_cli`.
- {doc}`taskmanager` and {doc}`workflow_cli` — running managers for real, and the
  complete command tree.
