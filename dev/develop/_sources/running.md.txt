# Running workflows

With a workspace in place ({doc}`workspaces`), work goes through the same
cycle every time: install a workflow, create jobs from it, run managers until
they are done, look at what happened, and collect the results.

```console
httk workflow install --workspace default 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workflow vasp.relax --input structure=POSCAR --tag silicon
httk workflow run
httk job list
httk collect --into results.sqlite --id-base httk.campaign
```

## Installing workflows and creating jobs

A job runs only a workflow installed in its workspace. Install it once, from a
Git repository, a package directory or a known name, and its declared calls
come along:

```console
httk workflow install --workspace default 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow install --workspace default ./my-workflow
httk workspace workflows default
```

Without `--workspace`, `httk workflow install URI` only fetches the repository
into this machine's cache. `httk job new` then instantiates jobs:

| `--workflow` / option | Selects |
| --- | --- |
| `vasp.relax` | an installed workflow by short name (or id) |
| `--workflow 'git+…#vasp-relax' --install` | a workflow not installed yet, installed first |
| `--workflow-dir ./my-workflow --install` | a local workflow package directory |
| `--from-runner ./relax.py` | a runner file of your own, installed ad hoc |
| `--from-runner flow.cwl` | a CWL, PWD or jobflow document ({doc}`workflow_compat`) |

Declared **inputs** are files or entries the workflow stages into the payload
(`--input structure=POSCAR`); **parameters** are plain values
(`--parameter encut=520`). `--tag` gives the job a readable key, and
`--placement project/screening` puts it in a subtree of the workspace that
managers and collects can address on its own; job directories never nest, so
no placement component may look like a job key (`name--uuid`). Workflows decide
their own data handling through parameters; the VASP workflows, for example,
copy a curated set of results into `data/` only with
`--parameter publish_data=true`. `httk workflow describe NAME` shows what a
workflow declares before anything is created, and `httk workflow precheck`
reports whether the workspace can run its jobs.

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

The job records the installed workflow it was made from (pinned to the commit
for a git workflow). A payload prepared some other way is submitted as it is
with `httk job submit --workspace NAME prepared-job`; its placement comes from
its `job.json`.

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
requirements, or lacks its workflow, leaves it for another manager and says why
in its idle summary.

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
transition, which is the fastest loop while a runner is being written. A job's
directory moves with its state, so `job show` prints where it is: `run/` holds
the workdir, `data/` the results a job committed, and `logs/` its run log.

Operator requests change a job's course without touching its files:

```console
httk job request pause --reason "hold for quota" silicon
httk job request continue --reason "quota restored" silicon
httk job request cancel --reason "wrong structure" silicon
httk job delete silicon                # terminal or paused jobs only
```

The job's owner applies each request at its next boundary; a manager claims an
idle job to apply it. `job delete`, `job seal`, `job unseal` and `job detach`
apply at once to a job no owner holds, print `queued` (exit `0`) for one an
owner holds, and `refused` (exit `1`) when the request cannot apply. A
succeeded job is protected: `httk job unseal` it before `job delete`.

A job changes hands only when its owner is proven dead: a manager that is
killed, or whose node or allocation ended, has its jobs returned to where they
were by the next manager tick or `workspace gc`. `httk workspace owners` lists
the owners and their liveness, and when no probe can decide (a host that is
gone), `workspace attest-dead OWNER --reason TEXT` declares the death; it
refuses a provably live owner and needs `--force` for an undecidable one, and
attesting a still-running owner can run work twice. `workspace status`
summarizes the workspace, `workspace fsck` checks the job tree (only while
nothing else uses the workspace, like `fsck` for a filesystem), and
`workspace gc` frees what the retention policy allows.
{doc}`details/taskmanager` explains each of them.

## Moving jobs between machines

Jobs are created locally and moved to the workspace that will run them, which
needs the workflow installed too; when they have stopped, the reverse transfer
(naming each job by its UUID) brings them home:

```console
httk workflow install --workspace kappa:runs 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job transfer --job silicon default kappa:runs
httk workflow run --workspace kappa:runs --count 4
httk job transfer --job JOB-UUID kappa:runs default
```

A transfer holds the jobs on the source, copies them to the destination,
adopts them there and only then releases the hold; an interrupted transfer is
finished by running it again or with `httk job transfer --resume`, and
`httk transfer status` lists the holds. With `--tree` a job moves together with
its descendants.

Without a registered workspace at either end, `job eject` and `job adopt` move
jobs through a plain directory:

```console
httk job eject --tree silicon /scratch/outgoing
httk job adopt --workspace other /scratch/outgoing/silicon--…
```

`job eject --wait` pauses a running job first, and `job adopt --move` removes a
bundle it had to copy from another filesystem. {doc}`details/remotes` covers
the mount-based remotes and the exchange, and {doc}`details/workflow_cli` the
transfer's recovery.

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
httk job seal silicon                  # a succeeded job that has no seal yet
httk workspace seal
httk project seal
httk seal verify
httk project unseal
```

A succeeded job is never changed until `job unseal` releases it, and a sealed
workspace refuses modifying commands; reading, `fsck`, `gc` and postprocessing
still work, and a seal travels with its job. {doc}`details/sealing` covers key refs, verdicts and what each level
records.

## Further reading

- {doc}`details/taskmanager` for resources and scheduling, placement, requests,
  and repair.
- {doc}`details/workflow_cli` for every command and option.
- {doc}`campaigns` for one run spread across many workspaces.
