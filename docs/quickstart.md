# Quickstart

*For anyone meeting httk-workflow for the first time; no runner, no graph, and no VASP required.*

Five commands from an empty directory to a finished VASP relaxation whose results
you can read back. Nothing here needs a runner to be written, a graph to be
declared, or a database to exist.

```{admonition} No VASP? Run the mock one
:class: tip

Every command below works without VASP installed: `examples/mock_vasp.py` writes
the output files a finished run leaves behind, so the whole path — prepare, run,
collect, harvest — is exercised for real, with meaningless numbers. Point
`HTTK_VASP_COMMAND` at it, exactly as a deployment points it at `vasp_std`.

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
$ httk workflow workspace init quickstart-workspace --extension transactional-data-v1
$ httk workflow job new quickstart-workspace --template vasp-relax --from POSCAR --tag silicon
$ export HTTK_VASP_COMMAND="$PWD/examples/mock_vasp.py"
$ httk workflow manager run quickstart-workspace --until-idle
$ httk workflow harvest quickstart-workspace
```

On a machine with VASP, the third command names the real one instead — for
example `export HTTK_VASP_COMMAND="srun -n 32 vasp_std"`.

## What each command did

**`init`** created the workspace: one directory that is the whole state of your
work. `--extension transactional-data-v1` lets jobs publish their results as
transactional data, which is what makes a finished calculation readable without
looking inside a workdir.

**`job new`** built and submitted one job. `--template vasp-relax` is the packaged
relaxation runner — one file, three steps, the reviewed remedy ladder — so no
runner had to be written. The runner file is published into the workspace and the
job pins its digest, so upgrading *httk-workflow* underneath a queued campaign
cannot change what its jobs execute. `--from POSCAR` staged the structure as the
`files/POSCAR` the runner reads, and `--tag silicon` made the job's key readable.
The command printed the job key and the payload directory:

```console
silicon--0c4f…	/…/quickstart-workspace/jobs/silicon--0c4f…
```

**`HTTK_VASP_COMMAND`** is deployment state, not job state: the machine that runs
a job decides how VASP is invoked there, so the same job runs on a laptop and on
32 ranks of a cluster without being resubmitted.

**`run --until-idle`** ran a task manager in the foreground until nothing was
ready, driving the job through `prepare`, `run`, and `collect`. Without
`--until-idle` the same manager keeps serving the workspace, which is how a
campaign is run.

**`harvest`** printed one JSON record per finished job: what ran, where its files
are, and everything that happened on the way. That record is the boundary to a
data layer — see {doc}`harvest`.

## Looking at a job

```console
$ httk workflow job list quickstart-workspace
JOB                                  STATE       STEP             PRI PLACEMENT
silicon--0c4f…                       succeeded   collect          500 jobs

$ httk workflow job show quickstart-workspace silicon
$ httk workflow job why quickstart-workspace silicon
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

Point `--from` at a *directory* and every `POSCAR*` or `*.vasp` file in it becomes
one job, each tagged after its file:

```console
$ httk workflow job new quickstart-workspace --template vasp-relax --from structures/ \
      --input kpoint_density=30.0 --placement project/screening
```

The runner is published once for the whole set, and the jobs are submitted as they
are generated. In Python the same thing streams, which is how a campaign of any
size is built:

```python
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.scaffold import new_jobs, structure_tag

workspace = Workspace("quickstart-workspace")
items = ({"files": {"POSCAR": path}, "tag": structure_tag(path)} for path in Path("structures").glob("POSCAR.*"))
for job in new_jobs(workspace, "vasp-relax", items, inputs={"kpoint_density": 30.0}):
    print(job.job_key)
```

Neither side of that loop is ever materialized: one runner publication is
amortized over every job, and each job costs one payload directory and one state
marker.

## Where to go next

- {doc}`vasp_runners` — what the packaged VASP runners do, every job input they
  read, and the failure codes they publish.
- {doc}`runtime_helpers` — authoring a runner of your own. `job new --template
  ./my_runner.py --step characterize` scaffolds jobs for it exactly as for a
  packaged one; the file is published into the workspace and pinned by digest.
  Two complete campaign runners are in `examples/defect_campaign.py` and
  `examples/defect_campaign.sh`.
- {doc}`native_bash_api` — the same runner protocol from Bash.
- {doc}`importing_workflows` — a Python Workflow Definition or CWL workflow you
  already have becomes one job with `httk workflow import pwd` or
  `httk workflow import cwl`, without being rewritten and without a runner file.
- {doc}`harvest` — turning finished jobs into stored results.
- Running on a cluster — `transfer send` puts jobs on a remote, `transfer
  start-manager` runs them there, and `transfer fetch` brings the finished ones
  back into this workspace for `harvest`; see {doc}`workflow_cli`.
- {doc}`taskmanager` and {doc}`workflow_cli` — running managers for real, and the
  complete command tree.
