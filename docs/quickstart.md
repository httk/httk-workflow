# Quickstart

*For anyone meeting httk-workflow for the first time; run this walkthrough from a clone of httk-workflow. No VASP is required.*

The mock VASP used below lives at `examples/mock_vasp.py` in that checkout; the
walkthrough does not run from an arbitrary empty directory.

Seven commands from a checkout to a finished VASP relaxation whose results
are stored and plotted. Nothing here needs a runner to be written or a graph
to be declared.

```{admonition} No VASP? Run the mock one
:class: tip

Every command below works without VASP installed: `examples/mock_vasp.py` writes
the output files a finished run leaves behind, so the whole path — prepare, run,
publish, collect — is exercised for real, with meaningless numbers. Install
`httk-store` for `collect --into results.sqlite --id-base httk.quickstart`; without it, that command reports
a teaching error (the shell example skips storage and continues).

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

## The seven commands

```console
$ httk project init --name quickstart .
$ httk workspace init --name default .
$ httk job new --workflow vasp-relax --input structure=POSCAR --tag silicon
$ httk workspace settings set --key vasp.command --value "$PWD/examples/mock_vasp.py" default
$ httk workflow run
$ httk workflow collect --into results.sqlite --id-base httk.quickstart
$ httk workflow postprocess --script relaxation-plot
```

On a VASP machine, set `vasp.command` to a command such as
`"srun -n 32 vasp_std"` instead. If the machine needs shell setup first — a
`module load`, a `source activate` — put it in a prelude rather than in
`vasp.command`; see [Environment preludes](taskmanager.md#environment-preludes).

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
cannot change what its jobs execute. `--input structure=POSCAR` staged the
declared structure input as the
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

**`collect --into`** printed one JSON summary per finished job and stored its
entries, run, and products in the file-backed SQLite database `results.sqlite`.
The required `--id-base httk.quickstart` selects the namespace for the store's
minted entry ids.
The stored results are readable with `httk-store`; the collection record is the
boundary to that data layer — see {doc}`collecting`.

**`postprocess`** ran the registered `relaxation-plot` script against the
published OUTCAR and wrote
`<payload>/run/postprocess/relaxation-plot/relaxation_energies.svg`.

## Looking at a job

```console
$ httk job list
JOB                                  STATE       STEP             PRI PLACEMENT
silicon--0c4f…                       succeeded   publish          500 jobs

$ httk job show silicon
$ httk job why silicon
```

The job commands also accept a path inside the workspace, such as `jobs` or
`jobs/silicon--...`.

Any job UUID, complete `tag--uuid` key, or unique prefix of either names a job.
`job show` describes it from its authoritative state, and `job why` explains a job
that is *not* progressing — an unmet capability, a paused job, no manager
running. When something did go wrong, the finished job's results and logs are in
the payload directory `job new` printed: `run/` is the workdir, `data/` holds the
published files, and `job log` prints the transitions.

`job debug --workspace WORKSPACE JOB` drives one job in the foreground and prints every
transition, which is the fastest loop while a runner is still being written.

## Many jobs at once

Point `--input-from structure` at a *directory* and every readable structure
file in it becomes one job, each tagged after its file:

```console
$ httk job new --workspace quickstart-workspace --workflow vasp-relax --input-from structure structures/ \
      --parameter kpoint_density=30.0 --placement project/screening
```

The runner is published once for the whole set, and the jobs are submitted as they
are generated. In Python the same thing streams, which is how a campaign of any
size is built:

```python
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.scaffold import new_jobs, structure_tag

workspace = Workspace.default()
items = ({"inputs": {"structure": path}, "tag": structure_tag(path)} for path in Path("structures").glob("POSCAR.*"))
for job in new_jobs(workspace, "vasp-relax", items, parameters={"kpoint_density": 30.0}):
    print(job.job_key)
```

Neither side of that loop is ever materialized: one runner publication is
amortized over every job, and each job costs one payload directory and one state
marker.

## Launchers

Managers normally run in-process when `httk workflow run` uses the built-in
`process` launcher. On a cluster, set `manager.launch` to a named launcher so
the same command submits managers through that launcher's scheduler profile.
Use `--inline` for a one-manager debugging run or `--launcher NAME` for a
one-invocation choice; see {doc}`launchers` for setup and scheduler settings.

## Remotes

A remote lets you reach another machine, move jobs to its workspace, and run
the same workflow commands there. Initialize and address that workspace with
`NAME:WORKSPACE`, such as `kappa:runs`; the remote invokes the manager on its
owning machine, where that workspace's launcher still controls scheduling.
See {doc}`remotes` for SSH setup, transfers, and the `local` adapter.

## Where to go next

- {doc}`vasp_runners` — what the packaged VASP runners do, every job input and parameter they
  read, and the failure codes they publish.
- {doc}`runtime_helpers` — authoring a runner of your own. `job new --workflow
  ./my_runner.py --step characterize` scaffolds jobs for it exactly as for a
  packaged one; the file is published into the workspace and pinned by digest.
  Two complete campaign runners are in `examples/defect_campaign.py` and
  `examples/defect_campaign.sh`.
- {doc}`workflow_packages` — authoring a directory workflow with hooks and a
  manifest.
- {doc}`sdks/native_bash_api` — the same runner protocol from Bash.
- {doc}`workflow_languages` — a Python Workflow Definition, CWL workflow,
  jobflow Maker document, or explicitly selected httk-v1 template becomes one
  job with `httk job new --workflow DOCUMENT`, without being rewritten
  and without a runner file.
- {doc}`collecting` — turning finished jobs into stored results.
- Running on a cluster — add and configure a remote, initialize `R:NAME`, then
  `transfer --job JOB LOCAL R:NAME` puts jobs there and `run --workspace R:NAME` invokes
  the manager on its owning machine, where its workspace launcher starts it; a very large run spread across many workspaces
  is a {doc}`campaigns`. See
  {doc}`workflow_cli`.
- {doc}`taskmanager` and {doc}`workflow_cli` — running managers for real, and the
  complete command tree.
