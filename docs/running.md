# Running workflows

With a workspace in place ({doc}`workspaces`), work goes through the same
cycle every time: create jobs from a workflow, run managers until they are
done, look at what happened, collect the results, and seal what is finished.

```console
httk job new --workflow vasp.relax --input structure=POSCAR --tag silicon
httk workflow run
httk job list
httk collect --into results.sqlite --id-base httk.campaign
```

## Creating jobs

`httk job new` instantiates one job from a workflow. The workflow can be named
in several ways:

| `--workflow` / option | Selects |
| --- | --- |
| `vasp.relax` | an installed or registered workflow by short name |
| `'git+https://github.com/httk/workflows-vasp#vasp-relax'` | a workflow package in a Git repository, fetched and installed on first use |
| `--workflow-dir ./my-workflow` | a local workflow package directory |
| `--from-runner ./relax.py` | a runner file of your own, published into the workspace |
| `--from-runner flow.cwl` | a CWL, PWD or jobflow document ({doc}`workflow_compat`) |

Declared **inputs** are files or entries the workflow stages into the payload
(`--input structure=POSCAR`); **parameters** are plain values
(`--parameter encut=520`). `--tag` gives the job a readable key, and
`--placement project/screening` puts it in a subtree of the workspace that
managers and collects can address on its own; job directories never nest, so
no placement component may look like a job key (`name--uuid`).
`httk workflow describe NAME`
shows what a workflow declares before anything is created, and
`httk workflow precheck` reports whether the workspace can run it.

Many jobs at once come from a directory of inputs, one job per file:

```console
httk job new --workflow vasp.relax --input-from structure structures/ --parameter kpoint_density=30.0
```

The same thing streams from Python, which is how a campaign of any size is
built without materializing it:

```python
from pathlib import Path

from httk.workflow import Workspace
from httk.workflow.scaffold import new_jobs, structure_tag

workspace = Workspace.default()
items = ({"inputs": {"structure": p}, "tag": structure_tag(p)} for p in Path("structures").glob("POSCAR.*"))
for job in new_jobs(workspace, "vasp.relax", items, parameters={"kpoint_density": 30.0}):
    print(job.job_key)
```

The job records the workflow it was made from, pinned by digest (and by commit
for a git workflow), so a later upgrade cannot change what a queued job runs.
A payload prepared some other way is submitted as it is with
`httk job submit --workspace NAME --placement project/00 prepared-job`.

## Running managers

```console
httk workflow run                      # serve ready jobs until nothing is left
httk workflow run --idle               # keep serving; this is how a campaign runs
httk workflow run --count 4 --workers 8
httk workflow run --workspace kappa:runs --count 4
```

A manager claims jobs, runs their steps and records every transition. `--count`
starts several managers through the workspace launcher, `--workers` sets how
many attempts each runs at once, and `--worker-resource procs 32` advertises
capacities for jobs that declare resource needs. Inside a Slurm allocation the
capacities are derived from it. A manager that cannot meet a job's declared
requirements leaves it for another manager and says why in its idle summary.

Running against a remote workspace starts the managers on the owning machine,
through that workspace's own launcher and preludes.

To keep jobs from writing anything but their own job directory, confine them:

```console
httk workspace settings set --key manager.confine --value bwrap default
```

Each attempt then runs in a Bubblewrap sandbox that sees the workspace
read-only and the software listed in `confine.readonly_paths`; the manager
itself stays unconfined, and parallel programs still start through the launch
prefix. {doc}`details/taskmanager` describes the sandbox and
{doc}`details/launchers` its settings.

## Inspecting and controlling jobs

```console
httk job list --placement project/screening
httk job show silicon
httk job why silicon                   # why is this job not progressing?
httk job log silicon
httk job debug --workspace default silicon
```

Any job UUID, full `tag--uuid` key, or unique prefix of either names a job.
`job why` explains a stalled job: an unmet requirement, a paused job, no
manager running. `job debug` drives one job in the foreground and prints each
transition, which is the fastest loop while a runner is being written. A
finished job's files are in the payload directory `job new` printed: `run/`
holds the persistent workdir and `data/` the committed results of a job with
transactional data.

Operator requests change a job's course without touching its files:

```console
httk job request pause --reason "hold for quota" silicon
httk job request continue --reason "quota restored" silicon
httk job request cancel --reason "wrong structure" silicon
httk job delete silicon                # finished or not yet started jobs only
```

`httk workspace status` summarizes the workspace; `workspace fsck` verifies the
state tree and repairs what it can with `--repair`, `workspace gc` frees
journal and trash space, and `workspace unlock` clears a stale maintenance
lock. {doc}`details/taskmanager` explains each of them.

## Moving jobs between machines

Jobs are created locally and moved to the workspace that will run them; when
they have stopped, the reverse transfer brings them home:

```console
httk job transfer --job silicon default kappa:runs
httk workflow run --workspace kappa:runs --count 4
httk job transfer kappa:runs default
```

A transfer moves the job's sealed bundle, imports it on the other side, and
retires the source only after the destination has acknowledged it, so an
interrupted transfer is resumed rather than duplicated. A job that spawned
children moves together with its whole tree. {doc}`details/remotes` covers
job trees, the mount-based remotes and their restrictions;
{doc}`details/transfer_reclamation` describes what is left on disk by
completed and interrupted transfers.

## Collecting results

`httk collect` reads the stopped jobs of a workspace back out as records,
one summary line per job, and `--into` stores their entries, run provenance and
products in a *httk-store* database:

```console
httk collect --workspace default
httk collect --workspace default --into results.sqlite --id-base httk.campaign
```

Collecting is read-only and stateless, so it can be repeated. Jobs fetched back
from a remote collect exactly like local ones. See {doc}`collecting`.

## Postprocessing

Workflow packages may ship curated scripts that run against a finished job's
files, such as the relaxation plot of the VASP workflows:

```console
httk workflow postprocess --script relaxation-plot
httk workflow postprocess --script relaxation-plot silicon
```

Output lands under `<workspace>/postprocess/`, never in the payload, so a
sealed job can still be postprocessed.

## Sealing

A seal is a signed statement of what a job, workspace or project contained, so
that a later change is detected when the seal is verified. Managers seal each
job as it succeeds, using the keys named by the `seal.keys` setting. Workspaces
and projects are sealed on top, bottom-up, and unsealed top-down:

```console
httk job seal silicon
httk workspace seal --force            # seals the remaining jobs first
httk project seal
httk seal verify
httk project unseal
```

A sealed job refuses state changes, deletion and runner republication; reading,
`fsck`, `gc`, postprocessing and transfers still work, and a seal travels with
its job. {doc}`details/sealing` covers key refs, verdicts and what each level
records.

## Further reading

- {doc}`details/taskmanager` for resources and scheduling, placement, requests,
  and repair.
- {doc}`details/workflow_cli` for every command and option.
- {doc}`campaigns` for one run spread across many workspaces.
