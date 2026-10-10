# Task-manager usage in detail

This guide covers setting up workspaces, installing workflows, submitting jobs,
running managers, and inspecting, controlling and maintaining workspaces. See
[the project and workflow command line](workflow_cli.md) for the complete
command tree and {doc}`workflow_filesystem_api` for the on-disk protocol.

## Setting up a workspace

### Initializing

`WORKSPACE` is optional. Without it, a command uses the closest enclosing
workspace, then the project's recorded default, the registry default, and
finally the per-user default workspace. A project does not contain workspaces;
record its routing explicitly with `workspace default NAME`.

`workspace init PATH` creates a named local workspace in a directory of its
own, which must be new or empty (it is never a project root), or adopts and
registers an existing workspace. The first local workspace initialized inside a
project becomes the project's default. *httk-workflow* owns the workspace top
level: `.httk-workspace/`, `jobs/`, `workflows/`, `logs/`, `postprocess/` and,
once enabled, `exchange/`. `REMOTE:PATH` initializes and names a workspace on
that remote.

```console
httk workspace init --name WORKSPACE runs/WORKSPACE
```

Library callers construct `Workspace(path)` directly; the registry is the
command-line contract. See {doc}`workflow_cli` for the whole `workspace` group
and {doc}`/campaigns` for spreading a very large run across many workspaces.

### Durability

Protocol publications are synchronized to storage by default. `--no-durable`
skips the `fsync` calls for throwaway workspaces: submission and transitions get
faster, but a node crash may lose the latest publications. `--durable` is still
accepted and has no effect, since it is the default.

### Workspaces on a cluster

Manager launch is a property of the workspace. On the cluster, install the
packaged Slurm launcher and configure the workspace it owns:

```console
httk launcher add --template slurm --global cluster
httk workspace init --name runs /scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster runs
httk workspace settings set --key slurm.partition --value batch runs
httk workspace settings set --key vasp.command --value vasp_std runs
httk workflow run --workspace runs --count 4
```

From a desk, the same operations reach that workspace through a remote as
`kappa:runs`; `run --workspace kappa:runs` invokes
`httk workflow manager run --workspace runs --count 4 --detach` on kappa, which
uses the owning workspace's launcher. Move jobs there and back with
`job transfer` ({doc}`remotes`).

### Workspace policy

Policy tunables belong to the workspace rather than to a process, so every
manager, CLI and independent implementation attaching it agrees on them. They
live in `.httk-workspace/format.json`:

```console
httk workspace policy show WORKSPACE
httk workspace policy set --key visibility_deadline_seconds --value 30 WORKSPACE
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace policy unset --key retention.trash_days WORKSPACE
```

| Key | Default | Meaning |
| --- | --- | --- |
| `visibility_deadline_seconds` | `5.0` | How long another node's change may take to become visible; see [the visibility deadline](#the-visibility-deadline). |
| `retention.attempt_control_days` | absent (keep) | Age after which `gc` removes old attempt directories. |
| `retention.trash_days` | `1.0` | Age after which `gc` removes the logs of departed managers. |
| `retention.owner_tombstone_days` | `30.0` | Age after which `gc` removes the tombstone of a recovered owner. |

Values are JSON, validated on write; `null` or `"keep"` keeps a retention
category forever, and an unknown key is refused. Concurrent policy writes are
atomic but not serialized: the last writer wins. Managers read the policy when
they attach, so restart long-running managers after a change.

### Application settings

A workspace also holds *application settings*: a flat map of dotted names to
small values that a runner resolves at run time, such as the VASP command or a
pseudopotential library. The manager launch profile is also a workspace
setting, so each workspace carries its own scheduler requirements.

```console
httk workspace settings set --key vasp.command --value vasp_std WORKSPACE
httk workspace settings show WORKSPACE
```

The `slurm.*` settings become batch directives of the workspace's Slurm
launcher ({doc}`launchers`). A runner reads a setting with
`a.setting("vasp.command")`, resolved in layers: the job's inputs, a real
`HTTK_VASP_COMMAND` deployment override, the workspace setting, then the
runner's default. The manager exports scalar settings into each attempt's
environment and snapshots them into `HTTK_WORKFLOW_CONTEXT`, so settings are
non-secret configuration: do not store credentials there.

## Shared filesystems

Several nodes under the same account may attach one workspace, so metadata
visibility is part of the filesystem configuration. The protocol uses no file
locks and no hard links; all coordination is atomic renames of directories
whose names are fresh tokens.

### Mount options

- NFS: use a **hard** mount; a soft mount can apply a rename after reporting a
  failure. Bound attribute caching, starting with `actimeo=5` (or
  `acdirmin=1,acdirmax=5`) and `lookupcache=positive`. With the default
  `acdirmax=60`, another node may legally serve a stale directory listing for
  up to a minute. `noac` is correct but usually far too slow for a workspace
  with many jobs. `nolock` is fine. Use NFSv4.1 or newer where available.
- Lustre and GPFS: no special options.
- Mounts backed by an object store, and FUSE caches without rename atomicity,
  are not supported. The protocol requires `rename(2)` to be atomic and a
  rename onto a non-empty directory to fail.

### The visibility deadline

A decision that rests on something being absent (another owner's job gone, a
join child vanished, a request's job not found) is only taken after the
observation has been repeated across `visibility_deadline_seconds`. Set it
comfortably above the mount's worst-case staleness window:

| Filesystem | Recommended `visibility_deadline_seconds` |
| --- | --- |
| Local disk, tmpfs, single node | `5` (the default) |
| Lustre, GPFS, BeeGFS | `10` |
| NFS with `actimeo=5` | `30` |
| NFS with default caching (`acdirmax=60`) | `120` |

The deadline costs nothing in the common case: it delays only destructive
decisions and recovery of dead owners.

### Clocks

No decision that hands over a job or removes data depends on a clock. Clocks
pace retries, schedule deadlines and drains, date records and drive retention.
Two policies use them to fail or set aside, never to let a second writer in: the
join grace and the quarantine of stale request files. Heartbeats are
informational only.

## Installing workflows

A job runs only a workflow installed in its workspace, together with every
workflow it calls. Install once, then create jobs by id or short name:

```console
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow install --workspace WORKSPACE ./my-workflow
httk workspace workflows WORKSPACE
httk job new --workspace WORKSPACE --workflow vasp.relax --input structure=POSCAR
```

`job new --install` installs a missing workflow first, and a runner file,
command template or bare workflow document given to `job new` is always
installed ad hoc. Installation copies the package into
`workflows/<slug>--<h16>/`, installs its declared calls (unless `--no-calls`)
and builds a `[workflow.build]` package for this platform (unless
`--no-build`). `httk workflow build` builds an installed workflow for another
platform, and `httk workflow uninstall --workspace WORKSPACE SELECTOR` removes
one; `--check` warns about unfinished jobs that still use it.

Workflows never travel with jobs. A job transferred, ejected or adopted into a
workspace that lacks its workflow stays `ready` and unclaimed until the workflow
is installed there; `job adopt` and `job transfer` warn about it, and `job why`
and `workflow precheck` name it. Do not reinstall a workflow that running jobs
use: the installed digest is recorded at launch (`workflow_pin`), not enforced.

## Submitting jobs

`job new` scaffolds and submits jobs; see {doc}`workflow_cli`. A payload
prepared some other way, a directory with an immutable `job.json` naming an
installed workflow, is submitted as it is:

```console
httk job submit --workspace WORKSPACE PAYLOAD
```

The placement comes from the payload's `job.json`. Submission copies by
default; `--move` does a same-filesystem rename and consumes the source
directory. Job directories never nest: no placement component may itself parse
as a job key (`<tag>--<uuid>`). Submission, adoption and spawned children all
refuse such a placement; a spawn that names one fails the parent attempt with
`protocol_error`.

## Checking readiness

### The precheck

Run the read-only precheck before starting managers:

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --json
```

For `ready`, `waiting` and `paused` jobs it reports environment entries and
how they resolve, whether the job's workflow and its calls are installed and
built here, jobs **no live manager can claim** (naming the closest manager's
unmet requirements), jobs of a built-in format (CWL, PWD, jobflow, httk v1)
whose engine modules are absent (with the pip extra to install;
`indeterminate` while a live manager may have them), and declared **required
inputs** missing from the payload. Any problem gives exit status `1`. The
report is advisory; the authoritative checks run at claim and at attempt start.

### Which owners serve a workspace

`httk workspace owners WORKSPACE` (alias `workspace managers`) lists every
registered owner (managers, CLI processes, daemons) with its kind, host,
process, allocation end and its liveness from the read-only death proof. It
answers what serves the workspace without a `job why` on an arbitrary job.
`--kind manager` narrows it.

## Running managers

### Starting a manager

```console
httk manager run --workspace WORKSPACE --workers 8
httk manager run --workspace WORKSPACE --pool vasp --capability gpu --workers 4
```

Without pool configuration, a manager advertises the reserved `default` pool.
By default it runs until idle, which suits batch invocations and tests; pass
`--idle` to keep serving.

A manager registers as an owner in `.httk-workspace/owners/<owner-id>/`, claims
a job by renaming its directory into `jobs/owned/<owner-id>/`, and is from then
on the only writer of that job. It acts only on jobs whose directory and
`job.json` its own account owns. A job directory is written by the job it
holds, so the manager treats what a job wrote as hostile: it reads outcomes,
transactions, children and the payload it seals only once the attempt and
every launch it made have been reaped, and never follows a symlink there.

### Launching managers

`run` and `manager run` launch managers through the launcher bundle that the
workspace's `manager.launch` setting selects: the built-in `process` launcher
starts detached local processes, and a named bundle, such as the Slurm launcher
`cluster` above, starts them as it defines. `manager.count`, `manager.workers`,
`manager.command`, `manager.allocation` and the launch-prefix and confinement
settings shape the launch; {doc}`launchers` lists them all, and which of them a
`slurm` launcher pins on its managers
([pinned settings](launchers.md#pinned-settings)).

`--inline` runs one manager in the current process, ignoring the workspace
launcher, and combines only with `--count 1`. `--detach` starts the managers
and returns immediately. A remote workspace always uses this detached
invocation once the remote adapter has reached its owning machine.

The packaged Slurm launcher writes one mode-0700 batch script below
`logs/batch/`, submits it once per manager, and returns the Slurm job ids. If
submission fails after one or more jobs were accepted, the command refuses with
text containing `submitted: N` and `job_ids: [...]`; cancel those jobs before
retrying.

### Confining attempts

With `manager.confine=bwrap` the manager, which stays trusted and unconfined,
starts each attempt inside a Bubblewrap sandbox:

- the workspace is visible read-only at its real path, so cross-job reads,
  `stage_input` and `Attempt.parent` keep working, and the attempt's own job
  directory is writable at its real path; the job's trusted entries
  (`job.json`, `state.json`, `.httk-job/seal.json`, `logs/`) are read-only
  overlays, and the attempt cannot write the workspace root,
  `.httk-workspace/` or another job;
- the paths of `confine.readonly_paths` are read-only, and `/tmp`, `/proc`,
  `/dev/shm`, the user, PID, IPC and UTS namespaces and (unless
  `confine.isolate_network=false`) the network are private.

The full sandbox is described in
[confined attempts](workspace_daemon.md#confined-attempts). Whatever a prelude
or code needs at run time (module trees, conda or virtual-environment prefixes,
code binaries) must be listed in `confine.readonly_paths`. The PID namespace is
also what makes an attempt's end provable: an unconfined attempt can leave
processes behind. The keys and their defaults are in {doc}`launchers`.

A manager probes Bubblewrap when it starts and refuses to start if it cannot
build the sandbox or a confinement setting is malformed. If confinement becomes
unavailable after it started, it stops claiming work, logs the reason and
reports the ready jobs under `ready_blocked["confinement"]` until confinement is
available again. A workspace with the exchange extension requires
`manager.confine=bwrap` of every manager.

### Startup banner and idle summary

`run` and `manager run` print one startup line (manager id, workspace, log
file, pools, capabilities, resources) and, on exiting idle, one summary line:

```text
idle: 3 succeeded, 1 failed, 2 not claimable here (capability=gpu: 2), 0 waiting on children, 0 paused
```

*Not claimable here* breaks ready jobs down by what this manager lacks: a pool,
a capability, an unmet `requires` of the installed workflow, a workflow or call
that is not installed or not built here, a resource beyond its capacity, a
`mintime` beyond the time it has left, or unavailable confinement. If the
manager reaches `--idle-timeout`, the advice names the mismatches and the flags
that would clear them rather than just suggesting a longer timeout.

### Logging

The console shows warnings and errors; the complete info-level record goes to
the manager's own file `logs/managers/<owner-id>.log`. `--log-level` raises or
lowers both, `--json-logs` writes one JSON object per line, and `--log-file
PATH` names a fixed file for one in-process manager. A manager rotates its log
at start and past 16 MiB, keeping one backup; `gc` (category `manager_logs`)
removes the logs of departed managers after `trash_days`. `logs/batch/` is
never collected.

Each job also keeps its own timeline, `logs/runlog.jsonl`, appended only by its
owner, and the attempt output `logs/stdio.out`; `job log` prints them.

### Draining

A draining manager stops claiming, sends `SIGTERM` to its running attempts,
keeps committing their outcomes for up to `--drain-timeout` seconds (sending
`SIGKILL` to any attempt still running 10 s into the drain), gives back the
jobs it still holds, and exits successfully. Three things start a drain:

- `SIGTERM` or `SIGINT`, which batch systems send at walltime, for an `--idle`
  manager;
- `SIGTERM` (not `SIGINT`, which still stops at once) for a manager running
  until idle;
- reaching the drain point of a known allocation end (see
  [Time requirements](#time-requirements)), in either mode.

A signal during a drain the deadline started, or a second signal during a
signal drain, kills the attempts and exits immediately; the jobs it still holds
are recovered once its death is proven. An attempt the drain stopped that
published no outcome fails with `owner_lost` (an unclean restart, retried only
if `retry_policy.retry_on` lists it), unless it had already exceeded its
`maxtime`, which stays `timeout`; an outcome the runner publishes on `SIGTERM`
wins as usual. After a drain the manager prints
`drained (<reason>): N attempt(s) left for recovery` instead of the idle
summary.

### Owners, death and recovery

A job changes hands only when its owner is **proven dead** or an operator
**attests** that it is. There is no lease and no time-based takeover: a manager
that stops heartbeating, freezes, or loses its node keeps its jobs until one of
those happens.

Every tick, a manager probes a few other registered owners and recovers those
proven dead. The proof comes from the process table of the host it runs on or
from the batch scheduler: the owner's process is gone on its recorded host
(boot id, pid and start ticks), or, observed from another host, its recorded
allocation has provably ended; and every launch it recorded under
`owners/<id>/launches/` has ended too. A probe that cannot decide says
`unknown` and changes nothing. `workspace status` writes the tombstone of an
owner it proves dead, and `workspace gc` (category `dead_owners`) recovers
every recoverable owner; a CLI owner killed by a signal is provable only on
its own host, so run `gc` there.

Recovery never continues another owner's work. It returns each job, unchanged,
to the state it was claimed from, and the next claimant reconciles it from its
`state.json`: a decided commit is finished, a pending release applied, an
attempt that never started is launched again, and an attempt that was running
without a published outcome fails with `owner_lost` under the retry policy. A
published outcome is committed, not rerun.

When no probe can decide (a host that is gone, an allocation nobody can be
asked about), the operator attests the death:

```console
httk workspace owners WORKSPACE
httk workspace attest-dead OWNER WORKSPACE --reason "node17 rebooted"
```

`attest-dead` runs the death proof first: an owner proven alive is refused, and
one the proof cannot decide needs `--force`. The next manager tick or
`workspace gc` then recovers its jobs. Attesting an owner that is still running
can apply a request twice and run work twice until that owner notices its
tombstone and stops (fail-stop: it kills every attempt it started and touches no
job). Even for a dead owner a narrow window remains in which a request can apply
twice; see {doc}`workflow_filesystem_api` under accepted limitations. Attest
only after confirming that the owner process and every launch it started are
gone.

Managers that share one allocation cannot recover each other's jobs before that
allocation (or the sibling's processes, on this host) ends.

### Unresolvable join children

A job `waiting` on a child that cannot be found in this workspace fails with
`dependency_failure` after `--join-grace-seconds` (default `3600`) instead of
waiting forever. `job why` on the waiting job shows each child and what blocks
it.

## Resources

### Advertising capacities

Managers may advertise integer resource capacities, such as
`resources={"procs": 8, "mem": 32768}`. On the command line, repeat
`--worker-resource` once per resource; for example, four workers sharing 32
CPUs and 128000 MB:

```console
httk manager run --workspace WORKSPACE --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000
```

Command-line capacities must be non-negative integers and override any
same-named capacity detected from the allocation. Labels other than `procs` and
`mem` are opaque to the manager. `manager.workers` remains the concurrency
limit, and there is no `manager.resources` workspace setting.

### How jobs are packed

A manager skips a ready job when one of its declared resources is missing,
zero-capacity, or larger than the manager's capacity. Such a job is never
claimed by that manager and is reported in the idle summary under
`ready_blocked["resources"]`. Jobs that fit are packed against the reservations
of running attempts.

`procs` and `mem` are special: a job that omits one gets the manager's fair
share (`capacity // workers`, or the whole capacity when that quotient is zero),
so undeclared jobs still occupy one worker's share. Only jobs declaring both can
pack more densely than one per worker.

A dynamic requirement published by an `advance` or `wait` outcome applies to the
next activation and is kept across retries. From the SDK:
`a.advance("analyse", resources={"procs": 1, "mem": 2000, "matlab_license_slots": 1})`.
The Bash bridge equivalent is
`httk_workflow_advance analyse --resource procs=1 --resource mem=2000 --resource matlab_license_slots=1`.

### Example

A workflow mixing a wide relaxation with dense analysis steps declares:

```toml
[workflow.resources]
procs = 4
mem = 16000            # MB

[workflow.steps.relax]
resources = { procs = 32, mem = 120000 }

[workflow.steps.analyse]
resources = { procs = 1, mem = 2000, matlab_license_slots = 1 }
```

and is served by:

```console
httk workflow run --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000 \
  --worker-resource matlab_license_slots 2
```

`relax` then runs alone, while one-proc `analyse` steps run together, at most
two at a time because of the two `matlab_license_slots`.

### Time requirements

`maxtime` (the time limit of one attempt) and `mintime` (the least remaining
allocation time needed to start one) are reserved labels. Jobs declare them as
Slurm `--time` strings, such as `maxtime = "24:00:00"` in a manifest,
`resources={"maxtime": "2:00:00"}` from the SDK, or `--resource maxtime=2:00:00`
from the Bash bridge; `job.json` and the attempt context hold seconds. They are
job requirements, never capacities: `--worker-resource`,
`TaskManager(resources=...)` and campaign manager resources refuse them.

Each time label resolves on its own (dynamic requirement, then step, then
job). A manager enforces `maxtime`: an attempt still running `maxtime` seconds
after its launch gets `SIGTERM`, then `SIGKILL` after the cancellation grace
(10 s), and fails with the reserved code `timeout` unless it published an
outcome meanwhile; `retry_policy.retry_on` can list `timeout`. The attempt is
told when the `SIGTERM` comes: the context member `deadline`,
`HTTK_WORKFLOW_DEADLINE` and `Attempt.deadline`. A spawned child never inherits
`mintime`, and its `maxtime` is capped at the spawning attempt's.

A manager may also know when its own allocation ends: `--time-limit DURATION`
(a Slurm `--time` string, counted from the manager's start), its
[allocation probe](#allocations), or `SLURM_JOB_END_TIME`; the earliest wins.
Its drain point is `--deadline-margin` seconds (120 by default) before that
end. With a known end, a ready job whose `mintime` exceeds the time left until
the drain point stays ready, and from the drain point on nothing is claimed;
attempts are told the earlier of their `maxtime` deadline and the drain point.
`workspace owners` and `job why` show each manager's time left. At the drain
point the manager [drains](#draining) and exits. An allocation end that has
passed is never evidence of death on its own.

### Allocations

A manager learns what its allocation consists of from an allocation probe
selected by `--allocation SPEC`: a list of nodes, each with its host name,
`procs`, `mem`, `gpus` and, when known, GPU device ids and CPU lists, plus the
allocation's end time. The allocation's capacity is the node sums (`procs`,
`gpus`, `mem` when every node knows it, and `nodes`) plus any extra resources it
reports; `--worker-resource` still overrides any same-named capacity. The probe
runs once, when the manager starts, and the allocation is recorded in the
manager's `owner.json`, where the death proof later asks about it.

| SPEC | Meaning |
| --- | --- |
| `auto` (default) | `slurm` when `SLURM_JOB_ID` is set, else `none` |
| `none` | no allocation; capacities come only from `--worker-resource` |
| `slurm` | the Slurm probe below |
| `host` | this machine as one node: its usable processors, half its physical memory, and the GPUs announced in `CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES` or `ZE_AFFINITY_MASK` |
| `exec:PATH` | run executable PATH inside the allocation; it prints one allocation envelope (see [launcher authoring](launcher_authoring.md#allocation-probes)) and later answers whether the allocation has ended |

The Slurm probe lists the job's hosts with `scontrol show hostnames
$SLURM_JOB_NODELIST` (a one-node job needs no `scontrol`), splits the tasks
over them by `SLURM_TASKS_PER_NODE` (`16(x2),8`), splits memory accordingly,
and reads the GPU device ids of the batch host from the first of the variables
above. Multiple GPU nodes require `SLURM_GPUS_PER_NODE` consistent with the
total. The probe also records `SLURM_CPUS_PER_TASK` and the job's identity
(`SLURM_JOB_ID` and, when set, `SLURM_CLUSTER_NAME`), so that another host can
later ask `squeue` whether the allocation has ended. When the nodes cannot be
described consistently, it warns and keeps the aggregate counts only.

Launchers choose the probe for the managers they start: the Slurm launcher
passes `--allocation slurm` (or its `manager.allocation` setting), and the
process launcher passes `host` for one manager and `none` for several. Binding
to devices needs one manager per allocation, so with `--count` above one every
child gets `none`.

### Placement and binding

A manager whose allocation lists its nodes keeps a per-node inventory and
places every attempt on it: `procs`, `gpus`, `mem` and `nodes` are then decided
by the inventory, while other labels are still counted. Without listed nodes
everything is counted as described above. `mem` is placed only when every node
knows its memory.

- An attempt goes on one node when it fits there, on the node with the fewest
  free `procs` that holds its `procs`, `gpus` and `mem` (ties go to the first
  node in allocation order).
- Otherwise it spills: nodes with the most free `gpus` first when it needs
  GPUs, then the nodes with the largest remaining capacity, with `mem` split in
  proportion to the slots taken on each node. A `mem`-only requirement never
  spills.
- `nodes=N` gives the attempt N whole idle nodes to itself, the smallest ones
  in allocation order that together hold its `procs`, `gpus` and `mem`.

Concurrent attempts never share a processor slot or GPU id. A job that can
never be placed even on an idle inventory, such as `nodes=3` on two nodes, is
reported under `ready_blocked["resources"]` with the first of `nodes`, `procs`,
`gpus`, `mem` that does not fit.

Every placed attempt is told what it got. Its context member `binding` holds
`nodes` (per node: `host`, `procs`, `gpus` and, when placed, `mem`),
`nodefile`, `file` and, when one applies, `launch`. `file` is `binding.json` in
the attempt directory: the same object with each node's `gpu_ids` and `cpus`
added when known. The runner environment carries `HTTK_WORKFLOW_NODELIST` (the
hosts, comma-separated), `HTTK_WORKFLOW_NODEFILE` (a file in the manager's
launch record below `.httk-workspace/owners/` with exactly one host line per
reserved processor slot, the `PBS_NODEFILE` convention) and
`HTTK_WORKFLOW_LAUNCH` (the launch prefix, shell-quoted). The Python SDK
exposes the member as `Attempt.binding`, and Bash reads it with
`httk_workflow_context binding`.

The launch prefix is what a runner puts before its parallel command. It is
`shlex`-quoted, so in Bash run `eval "$HTTK_WORKFLOW_LAUNCH vasp_std"` (or split
it with `read -ra`). Inside a Slurm allocation it is

```text
env SLURM_HOSTFILE=NODEFILE srun [--mpi=M] --ntasks=T --distribution=arbitrary
     --exact --cpus-per-task=C
     [--mem=MBM | --mem-per-cpu=MBM] [--gpus=G | --gres=none]
```

with `T` the nodefile's line count, equal to the reserved processor slots,
and `M` the workspace setting `manager.launch_mpi` when it is set. The hostfile
alone names the nodes: `srun` refuses `--nodes` with the arbitrary
distribution, and a `--nodelist` would replace the hostfile, so the prefix
passes neither (a custom `manager.launch_template` using
`--distribution=arbitrary` must not either). A one-node attempt with `mem` gets
`--mem` (its share); a multi-node one gets `--mem-per-cpu`. `--gpus=G` is added
when it has GPUs and `--gres=none` when the allocation has GPUs it does not
get. This built-in prefix still needs acceptance on a real Slurm cluster. The
workspace setting `manager.launch_template` replaces it for every kind of
allocation; it is split like shell words, and the placeholders are substituted
in each word:

```console
httk workspace settings set --key manager.launch_template \
  --value 'mpirun -np {procs} --hostfile {nodefile}' WORKSPACE
```

A template naming any other placeholder fails the attempt's preparation with
`protocol_error`. Outside Slurm and without a template there is no launch
prefix.

The code run helpers (`run_vasp`, `run_pw`, `run_cp2k`, `run_abinit`,
`run_lammps`, the mdrun step of `run_gromacs`, and the `httk_<code>_run` Bash
functions and `<code>-run` bridge verbs) prepend this prefix themselves, so a
code command setting names only the program (`vasp.command = "vasp_std"`). ORCA
gets no prefix by default because it starts its own MPI. A code command that
itself starts with `srun` or `mpirun` is refused when a prefix applies. The
generic `run` verb, `Attempt.run` and `httk_workflow_run` never prepend the
prefix.

In a [confined attempt](#confining-attempts) `HTTK_WORKFLOW_LAUNCH` is a
launch client instead, used the same way: the trusted manager records the
launch under `owners/<owner-id>/launches/` and starts each rank in its own
sandbox, and the attempt is not committed until every launch is reaped. See
[parallel launches](workspace_daemon.md#parallel-launches).

An attempt placed on one node that is the manager's own host is executed
locally, so the manager also binds it to its devices:

- When the share has GPUs whose ids are known, the runner environment sets the
  variable the ids came from (`CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES`
  or `ZE_AFFINITY_MASK`) to exactly those ids. A local attempt that requests no
  GPUs on a node with known GPUs sees none (except for Level Zero, whose empty
  mask does not hide devices).
- With `manager.bind_cpus` true, the `slurm` (batch host only) and `host`
  probes split the manager's CPU affinity into equal slots, one per processor
  slot, and the runner is pinned to the union of its slots' CPUs before it
  starts. A pin the kernel refuses is logged as `attempt_pin_failed`. Thread
  counts such as `OMP_NUM_THREADS` are not set.

```console
httk workspace settings set --key manager.bind_cpus --value true WORKSPACE
```

Placement is in memory only: a replacement manager places its own attempts on
its own inventory. The binding is information for well-behaved runners; there
is no backfill or reservation, and device identity holds only for attempts
executed locally.

### Capacities from SLURM

Inside a SLURM batch allocation, a manager derives capacities, but only when
`SLURM_JOB_ID` is present:

| SLURM variable | Manager resource |
| --- | --- |
| `SLURM_NTASKS` | `procs` |
| `SLURM_GPUS` (`4`, `a100:4` or `a100:2,v100:2`) | `gpus` |
| `SLURM_JOB_NUM_NODES` | `nodes` |
| `SLURM_MEM_PER_CPU`, `SLURM_CPUS_PER_TASK`, `SLURM_NTASKS` | `mem = MEM_PER_CPU × CPUS_PER_TASK (default 1) × NTASKS` |
| `SLURM_MEM_PER_NODE`, `SLURM_JOB_NUM_NODES` | fallback `mem = MEM_PER_NODE × JOB_NUM_NODES` when `SLURM_MEM_PER_CPU` is absent |

Memory is recorded in MB. A trailing `M`, `G` or `K` is accepted. Missing or
invalid input omits only the affected capacity, with a warning for invalid
input. For multiple local managers, the launcher supplies host `procs` and half
the host physical memory in MB when the caller did not, split across managers;
explicit resource pairs are per-manager values and stay unchanged.

## Scheduling

A manager never reads the whole workspace on a tick. Each tick it checks that
it is still alive (fail-stop otherwise), gives back any job of its own it does
not hold, probes a few other owners, serves one bounded window of request
files, evaluates waiting jobs, commits the attempts that ended, and claims from
one bounded window of `ready` jobs (`discovery_budget`, 4096 by default),
resumed from a cursor on the next tick. The terminal state directories are
never opened by scheduling, so a tick grows with the work in flight, not with
accumulated history.

Within a window, eligible jobs are claimed in priority order (`p000` first) up
to the free worker slots. Priority is therefore best-within-window, not
exact-global: the price of bounded discovery. Eligibility is checked from
`job.json` and `state.json` before claiming, in this order: pool, capabilities,
the workflow and its calls installed and built, the installed `requires`, the
resources, and `mintime` against the drain point. A job that fails a check stays
`ready`, unclaimed and not failed.

A claim is one atomic rename of the job directory into `jobs/owned/<owner-id>/`.
Of two managers that try, exactly one wins; the other moves on.

### Restricting a manager to placement prefixes

Placement prefixes restrict what a manager scans:

```console
httk manager run --workspace WORKSPACE \
  --placement-prefix project-a \
  --placement-prefix project-b/2026
```

The flag is repeatable, and every scheduling scan is confined to those
subtrees. Overlapping assignments are safe, since the claim rename still
arbitrates each job, and disjoint ones divide the scanning. A manager with
prefixes does not serve the exchange. `job why` reports a prefix mismatch when a
live manager's prefixes exclude the diagnosed job.

## Inspecting a workspace and its jobs

### Workspace status

```console
httk workspace status WORKSPACE
httk workspace status --json WORKSPACE
```

`status` prints the job counts per state, whether the workspace is sealed, and
the owners with their liveness; an owner it proves dead gets its tombstone, for
the next manager tick or `workspace gc` to recover. `httk workflow monitor`
gives an interactive view of the same counts, with pages, details and controls;
see {doc}`monitor`.

### Listing and reading jobs

For large workspaces, list jobs a page at a time with
`job list --json --limit N`, passing its `next_after` value back as `--after`.
`--kind` selects states, `--placement` prunes to a placement prefix, and
`--tag-contains` narrows the stream before state is read.

These commands find a job by listing its placement in each state and read its
`job.json`, `state.json` and run logs; none writes anything:

```console
httk job list --workspace WORKSPACE --kind ready --placement project-a
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
```

`JOB` is a job UUID, a complete `tag--uuid` job key, a unique prefix of either,
or a path inside the workspace. A job directory names one job; a placement
directory names every job below it. Globs such as `jobs/ready/silicon*` expand
from the current working directory. An ambiguous prefix is refused, listing the
jobs it matched. A job's directory name changes at every move, so script with
UUIDs or keys, never with paths.

`job show` reports the state and phase, placement, priority, workflow, step,
the activation and attempt counters against the retry budgets, the last
failure, a waiting job's children, and the payload, workdir and attempt paths.
`job log` prints the owner's run log (`claimed`, `attempt_started`, `outcome`,
`committed`, `released`, `recovered`, ...), oldest first, then the runner's own
annotations.

### Why a job is not running

`job why` answers this for every state:

- `ready`: every claim precondition, one line each: the installed and built
  workflow and its calls, the claim pool, required capabilities, each live
  manager's verdict, and the attempt budgets;
- `owned`: the owner's liveness from the death proof (alive, dead and about to
  be recovered, or undecidable from this host) and the phase (`idle`,
  `launching` or `running`);
- `waiting`: the join condition, every child with its state, and which children
  block;
- `failed`: the failure, whether an operator `continue` still fits in the retry
  budget, and the last attempt's `error.json` breadcrumb;
- `paused`, `succeeded` and `cancelled`: the state and how to proceed;
- **held**: a job held for a transfer, which is in neither state tree, is shown
  with its hold path, transfer id, destination and the state it returns to,
  and how to take it back.

For `ready`, `owned` and `failed` jobs it also folds the run log into one
attempt-history line and flags a job under an unlimited retry budget that has
attempted well past a small threshold as flapping. Operator requests still
pending for the job are shown.

### Reading results

Reading results rather than status is collecting: `httk collect WORKSPACE`
streams `CollectedJob` summaries, and `--raw` exposes the `JobRecord` stream for
a data layer; see {doc}`/details/collecting`.

## Controlling jobs

### Requests

Operators change jobs only through request files: one
`.httk-workspace/requests/<job-uuid>.<request-uuid>.json` per job, applied by
the job's owner. A manager claims an unowned job to apply its requests and
releases it; a job a manager is running gets a `cancel` at once and every other
request at the next attempt boundary.

```console
httk job request pause --workspace WORKSPACE --reason "inspection" JOB
httk job request override_step --step relax --reason "redo relax" JOB
httk job request set_priority --priority 100 --reason "urgent" JOB
```

| Action | Applies to | Effect |
| --- | --- | --- |
| `cancel` | any non-terminal job | `cancelled`; a running attempt is stopped first |
| `pause` | a job not terminal, paused or waiting | `paused`, at the next boundary when running |
| `continue` | a failed or paused job | `ready`; the failure is cleared and a new attempt counted against the budget |
| `override_step` | a failed or paused job | `ready` with a new activation of `--step` |
| `set_priority` | any job | the new `--priority` (0 is highest) |
| `detach` | a spawned job | independent of its parent, permanently |
| `eject` | a quiescent job | moved out as a bundle to `--destination` |
| `delete` | a terminal or paused job; a succeeded one only after `unseal`; no waiting parent | removed |
| `seal` | a succeeded job without a seal | sealed |
| `unseal` | a sealed succeeded job | released from its seal |

Requests apply once, in posting order. A request that does not apply where the
job now is is dropped and recorded in the job's `state.json` history with the
reason; `job why` shows it. A request whose signature fails to verify, or whose
job cannot be found, is moved to quarantine by `gc` after a day. `continue` and
`override_step` of a child that a decided join has already consumed are refused
unless `--force` is given.

`job request` exits `0` once the files are posted, and warns when no live
manager serves the job. `--wait` (only for `pause`) waits until each job was
observed `paused`, and exits `1` when a pause was superseded, dropped or timed
out (`--timeout SECONDS`). `override_step` is checked against the steps the
runner recorded after its first attempt; `--force` publishes anyway.

When the operator has an identity from `httk init`, the request carries an
Ed25519 signature over its canonical JSON; it is attribution, not
authorization. See [the command guide](workflow_cli.md#operator-identity).

### Applying requests now

`job delete`, `job seal`, `job unseal` and `job detach` post the request and,
as a short-lived CLI owner, apply it at once to an unowned job. Each job prints
one line:

| Line | Meaning | Exit status |
| --- | --- | --- |
| `removed`, `sealed`, `unsealed`, `detached` | applied now | `0` |
| `queued` | an owner holds the job and applies the request at its next boundary | `0` |
| `refused` | the request cannot apply where the job is (the reason follows) | `1` |

### Cancelling a running job

A `cancel` of a running attempt sends `SIGTERM` to its process group and its
launches, then `SIGKILL` after the cancellation grace, and the job commits as
`cancelled` once the attempt is reaped, whatever it published. Transactions the
attempt had committed still apply. An attempt that ended without an outcome
while a `cancel` was pending is cancelled too.

### Deleting jobs

`httk job delete JOB...` removes a terminal or paused job. A succeeded job is
never changed by the protocol until it is released from its seal with
`httk job unseal`, so delete it with:

```console
httk job unseal --force JOB
httk job delete --force JOB
```

A child that a non-terminal parent still waits on is refused. `--force` skips
only the confirmation. Cancel a running or ready job first. Never delete a job
directory with `rm -r`: a manager may hold it at that moment.

## Freeing disk

After a commit, the manager removes the attempt directory unless the job
failed or was cancelled, which keep it as evidence. Everything else that
accumulates is collected by `workspace gc`, explicitly or from a long-lived
manager:

```console
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace gc --dry-run WORKSPACE
httk workspace gc WORKSPACE
httk workspace gc --category requests --category tmp_entries WORKSPACE
httk manager run --workspace WORKSPACE --gc-interval 3600
```

| Category | What goes |
| --- | --- |
| `dead_owners` | owners proven dead are recovered: their jobs return to the states they were claimed from |
| `attempt_control` | attempt directories of unowned jobs older than `attempt_control_days`; a failed or cancelled job keeps the one its `state.json` names |
| `placement_directories` | empty placement directories of the unowned states |
| `requests` | request files older than a day that are malformed or whose job is not found go to quarantine; old exchange translation records are removed |
| `manager_logs` | `logs/managers/<owner-id>.log` of departed owners older than `trash_days` |
| `owner_tombstones` | tombstones of recovered owners older than `owner_tombstone_days` |
| `tmp_entries` | write temporaries and removal leftovers older than a day, and leftovers of a crashed workflow reinstall |

`gc` prints one row per category with the candidates, what was removed and the
bytes reclaimed; `--json` lists every entry, and `--category` (repeatable)
selects categories. A collection killed halfway leaves the workspace consistent,
and running it again finishes it. There is no age-based collection of scratch,
held transfers or quarantine: ownership decides, and quarantine is removed only
by hand. A manager with `--gc-interval` collects every category but
`dead_owners`, which its ticks handle anyway; it is off by default.

## Checking a workspace

`workspace fsck` walks `jobs/` and reports what the kernel cannot read or
resolve on its own:

```console
httk workspace fsck WORKSPACE
httk workspace fsck --json WORKSPACE
httk workspace fsck --repair WORKSPACE
```

| Finding | Meaning and remedy |
| --- | --- |
| `unparsable_name` | an entry that is neither a placement directory nor a job name of its position; `--repair` moves it to quarantine, the only repair |
| `duplicate_job` | one job UUID in two places, after a cross-filesystem crash, a duplicate delivery or two concurrent adoptions; inspect both and delete one |
| `orphan_owned` | `jobs/owned/<id>/` without its owner's directory; resolve it with `workspace attest-dead` |
| `tombstoned_owner_with_jobs` | a recovered owner holds jobs, launches or scratch again; `workspace gc` recovers it |
| `unreadable_state` | a `state.json` that exists but does not decode; the next claimant fails the job with `protocol_error` |
| `foreign_owner` | a job directory another account owns, which managers skip |

Without `--repair` nothing is written. The command exits `0` when nothing is
left for an operator and `1` otherwise. `httk project repair` adds the
workspace health checks: dead owners not yet recovered, scratch of unknown
owners, old temporaries, and holds or incoming copies older than seven days.

## The foreground debug runner

```console
httk job debug --workspace WORKSPACE --step relax PAYLOAD
httk job debug --workspace WORKSPACE --follow-children JOB
```

`job debug` drives one job to a terminal state in the foreground, streaming the
job's `logs/stdio.out` to the console as it grows. A private task manager
restricted to that job performs every transition, so the job runs through the
same code paths as in production and no unrelated work is claimed.
`--log-level` raises the private manager's console log, which is quiet by
default.

The first argument is a payload directory, submitted fresh at `--placement`
(default `debug`), or a selector of an existing job. `--step` overrides a fresh
payload's initial step; for a job with history use an `override_step` request.
`--follow-children` drives a waiting job's spawned children, depth first, then
resumes the parent. The exit status is `0` when the job succeeded, `3` when it
failed, and `4` when it stopped without finishing (paused, cancelled, or
waiting for children without `--follow-children`).

## Runner contract

The runner executes in the job's workdir `run/`. It reads the context in
`HTTK_WORKFLOW_CONTEXT` and publishes its outcome by renaming a complete
`outcome.tmp.<nonce>/` to `outcome.ready/` in its attempt directory
`HTTK_WORKFLOW_CONTROL_DIR`. Data reaches the payload only through
transactions, applied by the manager after the attempt ended. See
{doc}`workflow_filesystem_api` for the complete protocol, and
{doc}`runtime_helpers`, {doc}`/sdks/bash_api` or the {doc}`/sdks/sdk_parity`
table for the SDKs that implement it.

The manager puts its own interpreter's directory first on the runner's `PATH`,
so `#!/usr/bin/env python3` (and `HTTK_WORKFLOW_PYTHON`) is the interpreter in
which the installed workflow's `requires` were checked at claim. Runners start
behind a one-byte launch gate that opens only after the launch is recorded, so
a launch the manager did not record never ran. Converted `httk-v1` packages run
through the same manager; select their `taskset` pool with `--pool` (see
[*httk* v1 task compatibility](v1_compatibility.md)).
