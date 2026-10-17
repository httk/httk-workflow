# Quickstart

*For anyone meeting httk-workflow for the first time; run this walkthrough from a clone of httk-workflow. No VASP is required.*

The mock VASP used below lives at `examples/mock_vasp.py` in that checkout; the
walkthrough does not run from an arbitrary empty directory.

Eight commands from a checkout to a finished VASP relaxation whose results
are stored and plotted. Nothing here needs a runner to be written or a graph
to be declared: the relaxation workflow is fetched from the
[workflows-vasp](https://github.com/httk/workflows-vasp) repository by its git
URI, which needs `git` and network access the first time.

```{admonition} No VASP? Run the mock one
:class: tip

Every command below works without VASP installed: `examples/mock_vasp.py` writes
the output files a finished run leaves behind, so the whole path — prepare, run,
publish, collect — is exercised for real, with meaningless numbers. Install
`httk-atomistic` to read the VASP results and `httk-store[db]` for
`collect --into results.sqlite --id-base httk.quickstart`; without the store, that command reports
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

## The eight commands

```console
$ httk init --name "Your Name" --email you@example.org
$ httk project init --name quickstart .
$ httk workspace init --name default workspace
$ httk job new --workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax' --install --input structure=POSCAR --tag silicon
$ httk workspace settings set --key vasp.command --value "$PWD/examples/mock_vasp.py" default
$ httk workflow run
$ httk collect --into results.sqlite --id-base httk.quickstart
$ httk workflow postprocess --script relaxation-plot
```

On a VASP machine, set `vasp.command` to the bare program, such as
`vasp_std`, instead; the parallel start (`srun` or `mpirun`) comes from the
attempt's launch prefix, not from the command. If the machine needs shell setup first — a
`module load`, a `source activate` — put it in a prelude rather than in
`vasp.command`; see [Environment preludes](workspaces.md#environment-preludes).

## What each command did

**`init`** sets up your per-user operator identity used to attribute and sign
what you publish. It is idempotent, so an existing setup is reported and left
unchanged.

**`project init`** created the project anchor. The next command initialized and
registered the workspace at the project root as `default`; project creation
does not create or contain a workspace.
The workspace is the state of the work. The VASP workflows keep their results
in the persistent `run/` workdir and create no `data/` copy; add
`--parameter publish_data=true` to `job new` to also publish a curated copy
into `data/`.

**`job new`** built and submitted one job. `--workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax'`
names the `vasp.relax` workflow of the workflows-vasp repository (one runner,
three steps, the reviewed remedy ladder), so no runner had to be written. A job
runs only a workflow installed in its workspace, and `--install` fetched the
repository and installed the workflow there first; `httk workflow install
--workspace default URI` does the same on its own. The job records the
canonical URI, pinned to the full commit, and runs the installed copy, so a new
commit of the repository cannot change what a queued job executes. Once
installed, the short name `vasp.relax` selects it; see {doc}`vasp_runners`.
`--input structure=POSCAR` staged the declared structure input as the
`files/POSCAR` the runner reads, and `--tag silicon` made the job's key
readable. The command printed the job key and the directory the job is in now:

```console
silicon--0c4f…	/…/workspace/jobs/ready/silicon--0c4f…~p500~…
```

A job's directory moves with its state (`jobs/ready/`, `jobs/owned/…`,
`jobs/succeeded/`), so name a job by its key or UUID, not by its path.

**`settings set`** stored workspace state that travels with the job wherever it
runs. The manager exports scalar settings into each attempt environment, so
`vasp.command` becomes `HTTK_VASP_COMMAND`; a real VASP machine can set it to
`vasp_std`. The setting names only the program; the manager supplies the
parallel start through the attempt's launch prefix, and the process count comes
from the job's resources. A real environment variable remains a deployment
override and wins over the workspace setting.

**`run`** ran a task manager until nothing was ready, driving the job through
`prepare`, `run`, and `publish`, and sealed it when it succeeded. With `--idle` the same manager keeps serving
the workspace, which is how a campaign is run.

**`collect --into`** printed one JSON summary per finished job and stored its
entries, run, and products in the file-backed SQLite database `results.sqlite`.
The required `--id-base httk.quickstart` selects the namespace for the store's
minted entry ids.
The stored results are readable with `httk-store`; the collection record is the
boundary to that data layer — see {doc}`collecting`.

**`postprocess`** ran the registered `relaxation-plot` script against the
workdir OUTCAR and wrote
`postprocess/<placement>/<job_key>/relaxation-plot/relaxation_energies.svg`
under the workspace root. Postprocess output never lands in the payload, so a
finished job can be sealed and still be postprocessed; change the root with the
`postprocess.directory` setting or `--output-dir`. To postprocess one job
instead of every succeeded job, pass its id:
`httk workflow postprocess --script relaxation-plot <job-id>`.

## Looking at a job

```console
$ httk job list
JOB                                     STATE      STEP             PRI PLACEMENT
silicon--0c4f…                          succeeded  publish          500 -

$ httk job show silicon
$ httk job why silicon
```

The job commands also accept a path inside the workspace, such as
`workspace/jobs/succeeded`.

Any job UUID, complete `tag--uuid` key, or unique prefix of either names a job.
`job show` describes it from its authoritative state, and `job why` explains a job
that is *not* progressing — an unmet capability, a paused job, no manager
running. The finished job's results are in its directory, which `job show`
prints: `run/` holds the results, `data/` exists only when the workflow
publishes data, and `logs/` holds its run log and output. `job log` prints the
transitions.

`job debug --workspace WORKSPACE JOB` drives one job in the foreground and prints every
transition, which is the fastest loop while a runner is still being written.

## Many jobs at once

Point `--input-from structure` at a *directory* and every readable structure
file in it becomes one job, each tagged after its file:

```console
$ httk job new --workspace default --workflow vasp.relax --input-from structure structures/ \
      --parameter kpoint_density=30.0 --placement project/screening
```

The workflow is installed once for the whole set, and the jobs are submitted as
they are generated. In Python the same thing streams, which is how a campaign
of any size is built:

```python
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.scaffold import new_jobs, structure_tag

workspace = Workspace.default()
items = ({"inputs": {"structure": path}, "tag": structure_tag(path)} for path in Path("structures").glob("POSCAR.*"))
for job in new_jobs(workspace, "vasp.relax", items, parameters={"kpoint_density": 30.0}):
    print(job.job_key)
```

Neither side of that loop is ever materialized, and each job costs one
directory.

## Launchers, remotes, and other machines

Managers run as local processes until the workspace's `manager.launch` setting
names a launcher such as `slurm`; a remote reaches another machine, so that jobs
can be moved to a workspace there and run by its own launcher. A whole project is
moved by copying its directory and running `httk project repair` inside it.
{doc}`workspaces` sets all of this up, and {doc}`running` covers the job
cycle in full.

## Where to go next

- {doc}`workspaces` and {doc}`running`: setting up workspaces, launchers and
  remotes, and creating, running, inspecting, transferring and sealing jobs.
- {doc}`vasp_runners`: the VASP workflows of the workflows-vasp repository.
- {doc}`runtime_helpers`: writing a runner of your own, which
  `job new --from-runner ./my_runner.py` installs into the workspace ad hoc;
  {doc}`sdks/bash_api` is the same protocol from Bash.
- {doc}`workflow_packages`: packaging a workflow with its manifest and hooks.
- {doc}`workflow_compat`: running CWL, PWD, jobflow and httk v1 workflows as jobs.
- {doc}`collecting`: turning finished jobs into stored results.
- {doc}`campaigns`: one very large run spread across many workspaces.
