# Task-manager usage in detail

This guide covers setting up workspaces, submitting jobs, running managers, and
inspecting and repairing workspaces. See
[the project and workflow command line](workflow_cli.md) for the complete
command tree.

## Setting up a workspace

### Initializing

`WORKSPACE` is optional. Without it, a command uses the closest enclosing
workspace, then the project's recorded default, the registry default, and
finally the per-user default workspace. A project does not contain workspaces;
record its routing explicitly with `workspace default NAME`.

`workspace init PATH` creates a named local workspace, or adopts and registers
an existing path. `REMOTE:PATH` initializes and names a workspace on that
remote.

```console
httk workspace init --name WORKSPACE runs/WORKSPACE
```

A cluster workspace is created there over the adapter, and the owning machine
registers the basename (or `--name`) in its own registry. `workspace list`
shows this machine's names and paths; `workspace list kappa:` asks kappa.
`workspace forget` deregisters a name; `workspace delete --force` destroys the
workspace and deregisters it. Library callers construct `Workspace(path)`
directly; the registry is the command-line contract. See {doc}`workflow_cli`
for the whole `workspace` group and {doc}`/campaigns` for spreading a very
large run across many workspaces.

### Durability

Protocol publications are synchronized to storage by default. `--no-durable`
disables this for throwaway workspaces. Submission and transitions get faster,
but after a node crash an unsynchronized journal frame can be lost while the
marker naming it survives; the job's state is then unreadable until
`workspace fsck --repair` restores it. `--durable` is still accepted and has no
effect, since it is the default.

### Workspaces on a cluster

Manager launch is a property of the workspace. On the cluster, install the
packaged Slurm launcher and configure the workspace it owns:

```console
httk workflow launcher add --template slurm --global cluster
httk workspace init --name runs /scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster runs
httk workspace settings set --key slurm.partition --value batch runs
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" runs
httk workflow run --workspace runs --count 4
```

From a desk, configure an `ssh` remote to that machine and use the same
workspace operations through `kappa:`:

```console
httk workflow remote add --template ssh kappa
httk workflow remote configure \
    --set host=kappa.example.org --set username=rar \
    --set check_connectivity=yes kappa
httk workspace init kappa:/scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster kappa:runs
httk workspace settings set --key slurm.partition --value batch kappa:runs
httk workflow run --workspace kappa:runs --count 4
```

The remote is only transport: it moves files and invokes commands on kappa.
`run --workspace kappa:runs` invokes
`httk workflow manager run --workspace runs --count 4 --detach` there, which
uses the owning workspace's launcher. The result is the same as running on the
cluster, or addressing that machine through a configured `machine_names` alias.
Transfer jobs to the workspace as needed, run `transfer kappa:runs default`
after they stop, then `httk collect` locally.

### Workspace policy

Four tunables belong to the workspace rather than to a process, so every
manager, CLI and independent implementation attaching it agrees on them. They
live in `.httk-workspace/format.json`:

```console
httk workspace policy show WORKSPACE
httk workspace policy set --key visibility_deadline_seconds --value 60 WORKSPACE
httk workspace policy set --key retention.journal_days --value 90 WORKSPACE
```

| Key | Default | Meaning |
| --- | --- | --- |
| `visibility_deadline_seconds` | `5.0` | How long a marker rename or a referenced journal frame may take to become visible before it is called damage. |
| `lease_seconds` | `900.0` | The claim lease of a manager started without `--lease-seconds`. |
| `journal_segment_bytes` | `67108864` | The size at which a journal writer rotates to its next segment. |
| `retention` | `{"journal_days": 1.0, "trash_days": 1.0}` | `journal_days` and `trash_days` collect after one day; `attempt_control_days` is unset. Set a member to `null` or `"keep"` to keep that category forever. |

Values are JSON, validated on write; an unknown key is refused. A manager reads
the policy when it attaches, so restart long-running managers after a change.
Concurrent policy writes are atomic but not serialized: the last writer wins.

### Application settings

A workspace also holds *application settings*: a flat map of dotted names to
small values that a runner resolves at run time, such as the VASP command or a
pseudopotential library. The manager launch profile is also a workspace
setting, so each workspace carries its own scheduler requirements.

```console
httk workspace settings set --key vasp.command --value '"srun -n 32 vasp_std"' WORKSPACE
httk workspace settings show WORKSPACE
```

For a Slurm manager, set the launcher profile in the target workspace too.
`slurm.account`, `slurm.partition`, `slurm.time_limit`, `slurm.nodes`,
`slurm.cpus_per_task`, `slurm.ntasks`, `slurm.ntasks_per_node`, `slurm.mem`,
`slurm.gres` and `slurm.reservation` become batch directives when the workspace
launcher composes the batch script, and `manager.workers` supplies the default
worker count.

A runner reads a setting with `a.setting("vasp.command")`, resolved in layers:
the job's inputs, a real `HTTK_VASP_COMMAND` deployment override, the workspace
setting, then the runner's default. The manager exports scalar settings into
each attempt's environment (`vasp.command` becomes `HTTK_VASP_COMMAND`) and
snapshots them into the `HTTK_WORKFLOW_CONTEXT` JSON environment value, so a
runner sees the values the workspace held when its job was claimed. See
{doc}`/vasp_runners` and {doc}`/sdks/sdk_parity`.

Because they are snapshotted and exported this way, workspace settings are
non-secret configuration: do not store credentials there. Remote credentials
live elsewhere.

## Shared filesystems

Several nodes under the same account may attach one workspace, so metadata
visibility is part of the filesystem configuration.

### Mount options

Other clients must see renames and directory listings promptly, so bound the
attribute caching of an aggressively cached mount:

- NFS: start with `actimeo=5` (or the pair `acdirmin=1,acdirmax=5`) and
  `lookupcache=positive`. With the default `acdirmax=60`, another node may
  legally serve a stale directory listing for up to a minute, which must be
  waited out. `noac` removes the staleness and is correct, but disables
  attribute caching and close-to-open optimization altogether and is usually
  far too slow for a workspace with many jobs. `nolock` is fine: the protocol
  never takes a POSIX lock. Use NFSv4.1 or newer where available.
- Lustre and GPFS: no special options; their metadata coherence suits the
  local-filesystem defaults.
- Mounts backed by an object store, and FUSE caches without rename atomicity,
  are not supported. The protocol requires `rename(2)` to be atomic and to fail
  rather than silently overwrite.

### The visibility deadline

Set `visibility_deadline_seconds` comfortably above the mount's worst-case
staleness window:

| Filesystem | Recommended `visibility_deadline_seconds` |
| --- | --- |
| Local disk, tmpfs, single node | `5` (the default) |
| Lustre, GPFS, BeeGFS | `10` |
| NFS with `actimeo=5` | `30` |
| NFS with default caching (`acdirmax=60`) | `120` |

The deadline costs nothing when all is well: polling starts at 10 ms and stops
once the rename or frame is visible. Time is spent only when the filesystem
gives a client a stale view.

### Clocks

Leases are advisory evidence, not a fence. A manager decides that another
manager's claim has expired by comparing its own wall clock with that manager's
heartbeat timestamp, so nodes sharing a workspace should run NTP. Skew larger
than `lease_seconds` causes premature or delayed recovery of abandoned claims.
Safety does not depend on clocks: the fence is the marker rename, which only one
actor can win, so a wrong expiry decision costs a lost claim, never two runners
in one job.

## Submitting jobs

### Submitting a payload

A prepared payload is a directory with an immutable `job.json` and its runner.
Submit it at any placement:

```console
httk job submit --workspace WORKSPACE --placement project-a/00/17 PAYLOAD
```

Submission copies by default; `--move` does a same-filesystem rename and
consumes the source directory.

### Sharing one runner between many jobs

A partitioned campaign should publish its runner once into the workspace runner
store instead of copying it into every payload:

```console
httk workflow runner publish --workspace WORKSPACE --name relax.py ./relax.py
# A runner directory is published the same way and pinned by its tree digest.
httk workflow runner publish --workspace WORKSPACE --name relax-runner ./relax-runner
```

The command prints the reference to embed in every `job.json` that uses it:

```json
{"path": "relax.py", "sha256": "…", "source": "workspace"}
```

Publication is content addressed: publishing identical bytes again changes
nothing, and replacing a stored name with different content requires
`--replace`, because live jobs already reference the stored digest.

Before each attempt the manager verifies the runner in place and runs it with
the job workdir as cwd. A mismatch fails the job with `runner_mismatch`; an
unresolvable or non-executable runner fails it with `runner_unavailable`. A
detached transfer carries the runners its jobs reference, and the import
installs missing ones at the destination. Compiled package runners find their
registered binaries under `HTTK_WORKFLOW_RUNNER_ARTIFACTS`. A file runner is
invoked through its verified `/dev/fd/<N>` descriptor, so code that needs
sibling files uses `HTTK_WORKFLOW_RUNNER_ROOT`.

Runners deployed outside any workspace use `"source": "installed"` and resolve
against the manager's ordered `--runner-search-path` roots.

## Checking readiness

### The precheck

Run the read-only precheck before starting managers:

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --json
httk workflow precheck --workspace WORKSPACE --runner-search-path PATH
```

For pending jobs it reports:

- environment entries, resolved from the current process environment,
  workspace settings or declared defaults, and runner-reference problems;
- jobs **no live manager can claim**, naming the closest manager's unmet
  requirements in `job why`'s wording, including a runner-module allowlist the
  manager does not carry;
- jobs of a compat **format** (the collect gate's
  `workflow_realization = language` pair) whose engine modules are absent, with
  the pip extra to install (for example `pip install httk-workflow[jobflow]`).
  The extras belong on the machine that runs the job, so this fails only when no
  live manager serves the job's executor; otherwise it is `indeterminate` and
  non-failing. A job whose `requires` are unmet in this process is reported the
  same way, naming each unmet requirement and its installed version;
- declared **required inputs** whose staged destination is missing from the
  payload.

With no live manager, one workspace-level notice replaces the per-job claim
findings. An unresolved entry, broken runner, unclaimable job, missing and
unserved engine, or missing required input gives exit status `1`.

The repeatable `--runner-search-path` option checks installed runner
references. A plain installed reference without a configured path is
`indeterminate`, not a failure, and alone does not give exit status `1`.

The report is advisory and can go stale; the authoritative environment check
is at attempt start. The `HTTK_*` layer is this process's environment, not a
promise about a later compute node's.

### Which managers serve a workspace

`httk workspace managers WORKSPACE` prints one line per registered manager, live
or stale, with its pools, capabilities, executors and runner modules. It
answers what serves the workspace directly, without a `job why` on an arbitrary
job.

### Environment checks on transfer

Transfers check the environment against destination settings, job overrides and
declared defaults, not the client process environment. They warn about
unresolved entries without a default; `--strict-environment` blocks before any
job state is moved. Remote settings are checked through an isolated read when
reachable. An unreachable destination gets one immediate warning and fails only
in strict mode.

## Running managers

### Starting a manager

```console
httk workflow manager run --workspace WORKSPACE --workers 8
```

Without pool configuration, a manager advertises the reserved `default` pool.
Additional routing and capability labels are explicit:

```console
httk workflow manager run --workspace WORKSPACE \
  --pool vasp \
  --capability gpu \
  --workers 4
```

A task manager claims and runs only jobs whose marker, payload directory and
`job.json` are regular, non-symlink entries owned by the account running it.
Child jobs belong to the manager's account; imported jobs belong to the
importing account.

A manager claims work under the workspace's `lease_seconds` unless
`--lease-seconds` overrides it for that manager. By default it runs until idle,
which suits batch invocations and tests; pass `--idle` to keep serving.

### Launching managers

A launcher is to starting managers what a remote is to reaching a machine.
`run` and `manager run` launch managers through the launcher bundle that the
workspace's `manager.launch` setting selects. The built-in local `process`
launcher starts detached local processes; a named bundle, such as the Slurm
launcher `cluster` above, starts them as it defines. Three workspace settings
shape the launch:

- `manager.count`: the default number of managers; `--count` overrides it at
  the launch site.
- `manager.workers`: the default number of attempts each manager runs
  concurrently; `--workers` overrides it per manager.
- `manager.command`: the command used after an environment prelude (default
  `httk`).

`--inline` runs one manager in the current process, ignoring the workspace
launcher, and combines only with `--count 1`. `--detach` starts the managers
and returns immediately. A remote workspace always uses this detached
invocation once the remote adapter has reached its owning machine.

The launcher receives the manager's complete argument vector, the workspace
path, the count and the workspace settings. The packaged Slurm launcher writes
one mode-0700 batch script below `.httk-workspace/batch/`, submits it once per
manager, and returns the Slurm job ids and the script path. The script is reused
for the requested count and stays there, beside the scheduler's manager output
files, for inspection; the directory and scripts are launcher output, not
remote-adapter state. If submission fails after one or more jobs were accepted,
the command refuses with text containing `submitted: N` and
`job_ids: [...]`; cancel those jobs before retrying.

`environment.prelude` runs under `set -e` before the manager. With a prelude,
the launcher resolves `manager.command` on the resulting `PATH`; without one, it
keeps the caller's Python interpreter command. A module-loaded environment can
thus select the intended `httk`, while direct process launches stay faithful to
the invoking interpreter.

### Startup banner and idle summary

At any console log level, `run` and `manager run` print one startup line: the
manager id, the workspace, the log file path, and the pools, capabilities,
executors and advertised resources this manager serves.

On exiting idle, the manager prints one summary line classifying every
remaining job:

- succeeded and failed;
- *not claimable here*: ready or unregisterable-submitted jobs, broken down by
  the pool, capability or executor this manager does not serve, the job
  `requires` entry its environment does not meet, or the resource label beyond
  its capacity;
- waiting on children, and paused;
- committing or cancelling jobs with an unreadable definition.

A job this manager cannot progress, including one with a corrupt `job.json`, is
reported instead of keeping the manager awake until the idle timeout. If the
manager does reach `--idle-timeout`, the advice names the actual pool,
capability, executor and resource mismatches and the flags that would clear
them, and points an unreadable definition at `workspace fsck`, rather than just
suggesting a longer timeout.

### Logging

Every claim, launch, transition, recovery decision and refused request is
logged. The console shows warnings and errors; the complete info-level record
goes to `.httk-workspace/managers.log`, with the manager id on each record.
`--log-level` raises or lowers both, `--log-file` moves the file, and
`--json-logs` writes one JSON object per line for ingestion.

The shared log rotates when a manager starts, or every 1000 records once it
exceeds 16 MiB, keeping one backup, `managers.log.1`. A manager that has not yet
reopened the file keeps appending to the backup.

### Draining on signals

A manager drains on `SIGTERM` or `SIGINT`, which batch systems send at walltime.
The first signal stops claiming, terminates the running attempts, and keeps
committing their outcomes for `--drain-timeout` seconds before exiting
successfully. A second signal exits immediately. The next manager recovers
anything left behind from its expired lease.

### Taking over another manager's attempt

An expired lease means a manager stopped heartbeating, not that its attempt
stopped, so neither workdir mode relaunches on lease expiry alone:

| Workdir mode | What admits a takeover | Relaxed by |
| --- | --- | --- |
| `persistent` | The recorded process is provably gone on this host. A second writer would corrupt the shared directory. | `--unsafe-persistent-takeover` |
| `isolated` | The recorded process is provably gone, *or* the heartbeat has been silent for `--takeover-grace-factor` leases (default `2.0`). A second attempt corrupts nothing but costs a second allocation. | `--unsafe-isolated-takeover` |

Both unsafe options and the evidence for every takeover (the admitting rule and
the heartbeat's age) are recorded in the new attempt's state frame, so
`job log` shows why a job was relaunched.

Only the launching host can ask its kernel whether a process is gone. A manager
on another host therefore cannot prove a persistent-workdir attempt stopped; it
leaves the attempt alone and logs that decision at info level. `job why` reports
the job as blocked, names the writer's host, and says to run a manager on that
host or pass `--unsafe-persistent-takeover`, rather than claiming the expired
lease will be recovered here.

### Unresolvable join children

A job `waiting` on a child that cannot be resolved in this workspace fails with
`dependency_failure` after `--join-grace-seconds` (default `3600`) instead of
waiting forever. The grace runs from when a manager first records the child as
unresolvable. That moment is persisted in the waiting job's state frame, so the
deadline survives manager restarts and takeovers. `job why` on the waiting job
shows the recorded moment and what the grace will do.

## Resources

### Advertising capacities

Managers may advertise integer resource capacities, such as
`resources={"procs": 8, "mem": 32768}`. On the command line, repeat
`--worker-resource` once per resource; for example, four workers sharing 32
CPUs and 128000 MB:

```console
httk workflow manager run --workspace WORKSPACE --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000
```

Command-line capacities must be non-negative integers and override any
same-named SLURM capacity detected by a local manager. Labels other than
`procs` and `mem` are opaque to the manager. `manager.workers` remains the
concurrency limit, and there is no `manager.resources` workspace setting.

### How jobs are packed

A manager permanently skips a ready job when one of its declared resources is
missing, zero-capacity, or larger than the manager's capacity. Such a job is
never claimed by that manager and is reported in the idle census and summary
under `ready_blocked["resources"]`. Jobs that fit are packed against the
reservations of running attempts.

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

A workflow mixing a wide relaxation with dense analysis steps declares in its
manifest:

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

`relax` then runs alone, while several one-proc `analyse` steps run together,
at most two at a time because of the two `matlab_license_slots`. A manager
started without `--worker-resource matlab_license_slots` never runs `analyse`
and reports it as `ready_blocked["resources"]` and in the idle summary.

### Time requirements

`maxtime` (the time limit of one attempt) and `mintime` (the least remaining
allocation time needed to start one) are reserved labels. Jobs declare them as
Slurm `--time` strings, such as `maxtime = "24:00:00"` in a manifest,
`resources={"maxtime": "2:00:00"}` from the SDK, or `--resource maxtime=2:00:00`
from the Bash bridge; `job.json`, state frames and the attempt context hold
seconds. They are job requirements, never capacities: managers do not count
them against capacity, and `--worker-resource`, `TaskManager(resources=...)`
and campaign manager resources refuse them.

Unlike the other labels, each time label resolves on its own (dynamic
requirement, then step, then job), so a job-level `maxtime` still applies to a
step that overrides only `procs`; a mapping that names only time labels leaves
the consumable selection to the next level, and a resolved `mintime` above the
resolved `maxtime` is lowered to it. The resolved values are part of the
attempt's reservation and its context `resources`. In this release a manager
records them in the reservation and the attempt context; it does not yet
enforce a time limit or a start gate. A child spawned from a `ChildSpec` or
by `Attempt.call` never inherits `mintime`, and every `maxtime` it carries is
capped at the spawning attempt's `maxtime`; a prepared payload directory passed
to `Attempt.spawn` is registered as written.

### Capacities from SLURM

Inside a SLURM batch allocation, a manager derives capacities, but only when
`SLURM_JOB_ID` is present:

| SLURM variable | Manager resource |
| --- | --- |
| `SLURM_NTASKS` | `procs` |
| `SLURM_GPUS` | `gpus` |
| `SLURM_JOB_NUM_NODES` | `nodes` |
| `SLURM_MEM_PER_CPU`, `SLURM_CPUS_PER_TASK`, `SLURM_NTASKS` | `mem = MEM_PER_CPU × CPUS_PER_TASK (default 1) × NTASKS` |
| `SLURM_MEM_PER_NODE`, `SLURM_JOB_NUM_NODES` | fallback `mem = MEM_PER_NODE × JOB_NUM_NODES` when `SLURM_MEM_PER_CPU` is absent |

Memory is recorded in MB. A trailing `M`, `G` or `K` is accepted; `G`
multiplies by 1024 and `K` divides by 1024. Missing or invalid input omits only
the affected capacity, with a warning for invalid input.

Under SLURM, `procs`, `gpus`, `nodes` and `mem` come from the allocation unless
given on the command line. Each manager owns its own allotment, and a
replacement manager taking over a job brings its own capacity. SLURM adapters
let the allocation variables describe the real allocation.

### Local capacities

Local adapters supply host `procs` and total host physical memory in MB when the
caller did not. For multiple local managers, explicit resource pairs are
per-manager values and stay unchanged; only injected host capacities are split
across managers, quotient plus remainder.

## Scheduling

A manager never reads the whole workspace on a tick. Each scheduling pass
streams the state tree of one active kind (`submitted`, `ready`, `claimed`,
`running`, `committing`, `waiting` or `cancelling`) and never opens the
terminal `succeeded`, `failed` or `cancelled` trees. The in-memory marker index
and every scheduling scan therefore grow with the work in flight, not with
years of accumulated history.

### Bounded streaming discovery

A pass walks directory entries with `os.scandir` rather than materializing an
`rglob` of the tree. It stops at either of two budgets: `discovery_budget`
directory entries visited (default `4096`) or `maximum_pass_markers` markers
collected (default `256`), then yields the tick. It also takes a heartbeat
opportunity every 512 entries inside the walk, so even one huge flat placement
directory keeps the lease alive during the scan, not only between passes. A
workspace too large to scan within one lease is thus served round-robin instead
of making the manager look abandoned to its peers.

The walk keeps a resume cursor per top-level placement root, in memory only.
Nothing is written to disk, so managers of one workspace never contend on a
shared position, and a restarted manager simply starts a fresh cycle. Roots are
served round-robin with per-root resume, so a large placement subtree cannot
starve a smaller sibling, and the next tick continues where this one stopped. A
concurrent transition that renames or removes a marker during the walk is
tolerated silently, as a vanished marker is a miss, not a fault.

A pass that still takes half the lease is logged as a warning, and nine tenths
as an error. Raise `lease_seconds`, split the workspace, or reduce what the
manager scans.

### Exhaustive scans

The exhaustive operations `fsck`, `gc`, `collect` and `status` use their own
exhaustive scans. `job list` uses a separate cursor-stable scandir walker, not
the manager's fair `MarkerStream`, and prunes its `--placement` prefix before
opening descendants. A cursor whose kind is not among the selected `--kind`
values is rejected.

### Best-within-window priority

Claiming ready work scans one bounded window and claims the best-priority
candidates in it, in stable order among equal priorities, up to the number of
free worker slots. Priority is therefore best-within-window, not exact-global:
the price of bounded discovery. The round-robin rotation reaches a starved
subtree on a later tick. Exact global order would need a derived priority index,
which is not built; it could be added where a deployment measures a need.

### Restricting a manager to placement prefixes

Like pools and capabilities restrict what a manager claims, placement prefixes
restrict what it scans:

```console
httk workflow manager run --workspace WORKSPACE \
  --placement-prefix project-a \
  --placement-prefix project-b/2026
```

The flag is repeatable, and every scheduling scan, bounded window and
exhaustive walk alike, is confined to those subtrees. Without
`--placement-prefix`, a manager scans the whole workspace.

Overlapping assignments are safe: the marker rename still arbitrates each
claim, so two managers on one subtree never both run a job. Disjoint
assignments divide the scanning, so neither manager walks the other's trees.

The assignment is deployment policy, not a protocol change; placement values
remain project-owned semantics that the engine only validates and filters on.
It is recorded in the manager's manifest, so `job why` reports a prefix
mismatch when a live manager's prefixes exclude the diagnosed job's placement.
A prefix that currently matches no job (a typo, or a manager started before its
jobs are submitted) is logged as one warning at manager start, naming the
prefix and noting that the manager will serve that subtree once work arrives.

Laying out placements for a large campaign and assigning subtrees to managers
by a written recipe is out of scope here; the Phase 14 campaign recipes add it.

## Inspecting a workspace and its jobs

### Workspace status

```console
httk workspace status WORKSPACE
httk workspace status --json WORKSPACE
```

`httk workflow monitor` gives an interactive view of the same status counts,
with pages, details and controls. It keeps local reads bounded and also shows
registered remote workspaces through their adapter.

### Listing and reading jobs

For large workspaces, list jobs a page at a time with
`job list --json --limit N`, passing its `next_after` value back as `--after`
to continue in stable order without materializing the whole job set.
`--placement` prunes to a placement prefix, and `--tag-contains` narrows the
stream before state details are read.

These commands read a job as a manager does (the authoritative marker, the
journal frame it names, and the immutable `job.json`), and none writes protocol
state:

```console
httk job list --workspace WORKSPACE --kind ready --placement project-a
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
```

`JOB` is a job UUID, a complete `tag--uuid` job key, a unique prefix of either,
or a path inside the workspace. A job directory names one job; a placement
directory such as `jobs` names every live job below it. Globs such as
`jobs/silicon*` expand from the current working directory. Paths must be inside
the selected workspace. An ambiguous prefix is refused, listing the jobs it
matched. Every command also accepts `--json` and prints one object: a report, a
frame array, a diagnosis or a job array.

`job show` reports the state kind, placement, priority, generation, job digest,
runner identity, retry budgets against consumption, the current and initial
step, any step set the runner declared, the last failure, a waiting job's join
and per-child state, and the payload, workdir and data paths.

`job log` walks the journal backward from the marker through
`previous_record_ref` and prints one line per state frame, oldest first: the
timestamp, transition, step, attempt ordinal, reason and any failure code. An
unreadable frame is reported in place, and the remaining readable history is
still shown.

### Why a job is not running

`job why` answers this for every state:

- `submitted`: whether any manager has registered it, and which live managers
  serve its runner executor;
- `ready`: every claim precondition, one line each: runner executor, claim
  pool, required capabilities, the job's `requires` (checked in the operator's
  process; a manager re-checks in its own), the maintenance lock, the workspace
  core profile, the attempt budgets, and which live manager would accept the
  job;
- `claimed` and `running`: the owning manager, its heartbeat age against the
  recorded lease, and whether an expired lease means recovery rather than a
  stuck job;
- `committing`: that a published outcome is being committed, which any manager
  serving the executor resumes. If a commit anomaly has repeated for the same
  attempt, the recorded error is shown and the job is reported as a blocked,
  wedged commit rather than as needing no action;
- `waiting`: the join condition, every child with its label and state, which
  children block, and which cannot be resolved in this workspace;
- `failed`: the failure, whether an operator `continue` still fits in the retry
  budget, and the last attempt's `error.json` breadcrumb;
- `paused`, `succeeded` and `cancelled`: the state and how to proceed.

For `ready`, `running` and `failed` jobs, `job why` also folds the journal into
one attempt-history line, `N attempts across M activations at step 'X'; K after unclean exits`,
and flags a job under an unlimited retry budget that has attempted well past a
small threshold as flapping rather than progressing.

A runner-allowlist refusal is reported whenever a live manager's
`runner_modules` or search paths cannot reach the job's runner, so a repeating
`runner_unavailable` claim loop is named instead of shown as a manager that
"offers everything this job requires". Operator requests still pending in
`requests/ready`, and the reason recorded for the most recent retired one, are
shown on the states where they apply.

The job side of every precondition comes from `job.json` and cannot drift. The
other side (pools, capabilities and served executors) is the running manager's
deployment policy, read from the manifest each manager publishes, so a manager
that is not running is reported as absent rather than assumed.

### Reading results

Reading results rather than status is collecting: `httk collect WORKSPACE`
streams `CollectedJob` summaries, and `--raw` exposes the `JobRecord` stream for
a data layer; see {doc}`/details/collecting`.

## Controlling jobs

### Pausing and continuing

```console
httk job request pause --workspace WORKSPACE \
  --reason "inspection" JOB_UUID

httk job request continue --workspace WORKSPACE \
  --reason "inputs repaired" JOB_UUID
```

### Overriding the step

An `override_step --step X` request is pre-validated on the client. Once the
job's state frame records the runner's `runner_steps` (after its first attempt),
a step outside that set is refused before publication, listing the recorded
steps. `--force` turns the refusal into a stderr note and publishes anyway,
since a payload runner is mutable and an operator may have added the step.

Before the first attempt nothing is recorded, so the request is allowed with a
stderr note that it could not be pre-validated. In both allowed cases the
runner, not the manager, refuses the step at the next attempt if it does not
implement it; the manager only checks the request's shape.

### How requests apply

Requests capture the exact current marker generation and record reference, so a
delayed request cannot change a newer job state. A request that can never apply
again, because the job has moved on, is moved to
`.httk-workspace/requests/retired/` with the reason beside it rather than being
reread on every pass. A request for a runner executor this manager does not
serve is left for a manager that does.

When the publishing installation has an operator identity from `httk init`, the
request also carries a detached Ed25519 signature over its canonical JSON, and
the manager records the verified `operator_key` in the journalled state frame
beside `operator` and `reason`. The signature is optional both ways: a request
without one is applied as before, so mixed deployments need no flag day, but a
request whose signature fails to verify is quarantined with that reason instead
of applied. It is attribution, not authorization; see
[the project CLI guide](workflow_cli.md#operator-identity).

### Cancelling a running job

Cancellation is fenced and verified, not a single signal:

```console
httk job request cancel --workspace WORKSPACE \
  --reason "wrong inputs" JOB_UUID
```

1. The manager renames the marker `running` → `cancelling`, fencing the attempt
   so it can no longer commit an outcome.
2. It sends `SIGTERM` to the process group, then `SIGKILL` if the group has not
   exited within the grace period, and verifies that it is gone.
3. Only a verified exit moves the job to `cancelled`; the terminal frame records
   how the exit was verified.

A manager that dies mid-cancellation leaves a `cancelling` marker, and the next
manager completes the same procedure. A process recorded on another host cannot
be proven stopped from here: the job stays `cancelling`, the reason is
journaled, and every retry logs a warning. This is the safe answer, because
`cancelled` asserts that nothing is still writing the workdir.

### Deleting jobs

`httk job delete JOB...` cleanly removes a finished (`succeeded`, `failed` or
`cancelled`), `submitted` or `ready` job. It removes payload and marker together
and asks for confirmation on a terminal. It refuses a child referenced by a
non-terminal join parent unless `--force` is given, which skips both the
confirmation and that guard.

For finished jobs, `rm -r` of the directory followed by GC or a manager also
works. For queued `ready` or `submitted` jobs prefer `job delete`, since
removing the directory first can race a manager claiming the job at that
moment. A job in any other state must first be cancelled with
`job request cancel`.

## Freeing disk on a quota'd filesystem

### What a manager cleans up

After a durable commit, once it has reaped the local process, a manager removes
an attempt's control directory when the actual destination is `ready`,
`waiting`, `paused` or `succeeded`. Failed and cancelled attempts remain as
evidence. Transaction trash normally goes with that control tree. A manager that
inherits a commit leaves the tree for GC.

A manager is never required to run policy-gated cleanup, so it can disappear
between any two instructions. It runs always-safe cleanup at startup and the
full policy-gated collection at a clean exit. A clean manager removes its own
metadata directory; a crash leaves it for `journal_days` collection.

On a quota'd HPC filesystem, what remains to manage is failed and cancelled
attempt evidence, retained journal history, interrupted transaction trash and
acknowledged bundles.

### Running a collection

A workspace that no manager visits needs explicit collection. Configure the
retention limits once, then collect from a maintenance job or by hand:

```console
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
httk workspace policy set --key retention.journal_days --value 90 WORKSPACE
httk workspace policy set --key retention.journal_days --value null WORKSPACE  # keep forever
httk workspace gc --dry-run WORKSPACE
httk workspace gc WORKSPACE
```

`attempt_control_days` is unlimited when omitted; `journal_days` and
`trash_days` default to one day. A `null` or `"keep"` member means keep forever.
See [the command guide](workflow_cli.md#freeing-disk) for the full category
table and the cost of collecting journal history.

Collection is safe on a live workspace:

- A manager still heartbeating keeps its own directory and every journal segment
  it wrote.
- No `claimed`, `running`, `committing`, `cancelling`, `waiting` or `paused`
  marker or payload is touched, beyond the aged attempt-control directories of
  quiescent jobs.
- GC may remove a finished marker, or the marker of a quiescent job no manager
  owns, when its payload was removed.
- Every segment in a non-terminal job's current frame chain is protected;
  terminal jobs protect only their current segment.
- Pruning an empty placement mirror that a transition is recreating meanwhile
  is an ordinary outcome, not an error.

### Collection from a long-lived manager

A long-lived manager can collect too, which helps where no maintenance job
exists:

```console
httk workflow manager run --workspace WORKSPACE --gc-interval 3600
```

It then collects at most once per interval, at the end of a tick and never
between observing a marker and acting on it, under the same `policy.retention`
limits. This is off by default, and a failed collection is logged without
disturbing scheduling. Keep the interval long: a collection walks the state tree
and journal directory, which scheduling does not need often.

## Checking and repairing a workspace

`workspace fsck` verifies the one thing a manager cannot route around: that
every state marker still resolves to its journal frame.

```console
httk workspace fsck WORKSPACE
httk workspace fsck --json WORKSPACE
httk workspace fsck --repair WORKSPACE
httk workspace fsck --repair --quarantine-unrepairable WORKSPACE
```

Run it after a node crashed while writing, after a filesystem was restored from
a snapshot, or whenever `job show` reports an unreadable state frame.

### What fsck checks

Fsck reads every marker of every state kind and checks that its record
reference resolves to a readable frame whose checksum verifies and whose job,
kind and generation match the marker name. It waits up to the visibility
deadline, so a slow network filesystem is not mistaken for damage.

Each problem has a stable code: `missing_segment`, `short_read`,
`checksum_mismatch`, `reference_mismatch`, `identity_mismatch`,
`unparseable_name`, `payload_missing`, and their siblings. `payload_missing`
applies to `claimed`, `running`, `committing`, `cancelling`, `waiting` and
`paused` markers with no payload; it is always reported and never repaired or
quarantined. `submitted` and `ready` markers with no payload are collectable
instead.

Without `--repair` nothing is written. The command exits `0` when the workspace
is clean or everything found was repaired, and `1` when something is left for
an operator. Fsck also dry-run counts always-safe leftovers, reporting the total
and counts for `removed_jobs`, `tmp_entries`, `retired_requests` and
`placement_directories`; these informational counts do not affect the exit
status.

### Repair

`--repair` re-points a damaged marker at its job's last good frame. Since the
frame holding the backward link is the unreadable one, the repair scans the
journal for readable frames naming the job and adopts the newest one *older*
than the marker's generation, never a newer one, which would be the damaged
frame itself or a transition no marker committed. It writes one `fsck_repair`
state frame chained to the recovered frame, carrying its step, activation and
attempt counters forward, and renames the marker onto it at the next
generation. History is added, never rewritten, and the job is schedulable
again.

### What is never repaired

- A `claimed`, `running` or `committing` marker whose manager still heartbeats
  within its lease is reported and left alone, because that manager owns the
  next transition. Stop the manager or wait for its lease to expire, then
  repair again.
- A marker with no readable older frame, typically a job damaged before its
  second transition, cannot be restored. It is reported, and moved into
  `.httk-workspace/quarantine/` with an audit record only with
  `--quarantine-unrepairable`.

## The foreground debug runner

```console
httk job debug --workspace WORKSPACE --step relax PAYLOAD
httk job debug --workspace WORKSPACE --follow-children JOB
```

`job debug` drives one job to a terminal state in the foreground, streaming the
job's `logs/stdio.out` chronicle to the console as it grows. A private task
manager whose scans are restricted to that job performs every transition, so
the job runs through the same code paths as in production and no unrelated work
is claimed. Lines are prefixed with the step that produced them, and `[debug]`
marks each transition the polling loop observed; `job log` holds the complete
record afterwards. `--log-level` raises the private manager's console log,
which is quiet by default.

The first argument is a payload directory, submitted fresh at `--placement`
(default `debug`), or a selector of an existing job. `--step` overrides a fresh
payload's initial step; overriding the step of a job with history is refused,
because the recorded `override_step` request exists for that.
`--follow-children` drives a waiting job's spawned children, depth first, then
resumes the parent.

The exit status is `0` when the job succeeded, `3` when it failed, and `4` when
it stopped without finishing (paused, cancelled, or waiting for children without
`--follow-children`). A live maintenance lock is refused up front, since it
would stop every launch anyway.

## Runner contract

The runner executes in the selected persistent or isolated workdir. It reads the
context in `HTTK_WORKFLOW_CONTEXT` and publishes `outcome.tmp.<nonce>/` as
`outcome.ready/` beneath `HTTK_WORKFLOW_CONTROL_DIR`. See
{doc}`workflow_filesystem_api` for the complete protocol, and
{doc}`runtime_helpers`, {doc}`/sdks/bash_api` or the {doc}`/sdks/sdk_parity`
table for the two authoring SDKs that implement it.

### The runner's interpreter

The manager puts its own interpreter's directory first on the runner's `PATH`,
so `#!/usr/bin/env python3` is the interpreter the manager runs in, the one in
which the job's `requires` were checked at claim. A ready job whose `requires`
that environment does not meet is left unclaimed for another manager; install
the required distribution versions in that manager's environment and restart
it, since it checks them once per process. `HTTK_WORKFLOW_PYTHON` names the same
interpreter. A workflow prelude's login shell puts that directory first again
after its profiles run, and the prelude itself runs last and may still change
`PATH`.

### The launch gate

The local executor starts runners behind a one-byte launch gate and records the
process identity in the `running` frame before releasing it. If the manager
disappears during this short interval, the gated process sees end-of-file and
exits without executing the runner.

### Executors and v1 packages

`httk workflow manager run` executes the normal `path` runner executor.
Converted `httk-v1` packages use the same path through their packaged v1 runner;
select their `taskset` claim pool with the manager's `--pool` option. See
[*httk* v1 task compatibility](v1_compatibility.md).
