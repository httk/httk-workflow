# Task-manager usage in detail

*For operators: initializing workspaces, submitting jobs, running managers, and inspecting or repairing what they leave behind.*

Every command below is under `httk workflow …`; see
[the project and workflow command line](workflow_cli.md) for the complete tree.

A launcher is to starting managers what a remote is to reaching a machine.
Managers can use the built-in local `process` launcher or a named bundle such as
the Slurm launcher. The workspace's `manager.launch` setting selects the bundle
used by `run` and `manager run`.

## Initialize a workspace

`WORKSPACE` is optional: omitting it uses the closest enclosing workspace,
then the project's recorded default, the registry default, or the per-user
default workspace when none is found. A project does not contain workspaces;
record its routing explicitly with `workspace default NAME`. Explicit local
names are created with `workspace init PATH`; an existing path is adopted and
registered. `REMOTE:PATH`
initializes and names a workspace on that remote.

```console
httk workspace init --name WORKSPACE runs/WORKSPACE
```

A workspace on a cluster is created there over the adapter; its owning machine
registers the basename (or `--name`) in its own registry. `workspace list` shows
this machine's names and paths, while `workspace list kappa:` asks kappa.
`workspace forget` deregisters a name, and `workspace delete --force` destroys
the workspace and deregisters it.
A library caller still constructs `Workspace(path)` directly; the registry is the
command-line contract. See {doc}`workflow_cli` for the whole `workspace` group and
{doc}`/campaigns` for spreading a very large run across many workspaces.

Protocol publications are synchronized to storage by default. `--no-durable`
turns that off for throwaway workspaces and makes submission and transitions
faster at the price of correctness after a node crash: an unsynchronized
journal frame can be lost while the marker naming it survives, which leaves a
job whose state cannot be read until `workspace fsck --repair` restores it.
`--durable` is still accepted and does nothing, since it is now the default.

## Running on a remote

Manager launch is a property of the workspace. On the cluster, install the
packaged Slurm launcher and configure the workspace that it owns:

```console
httk workflow launcher add --template slurm --global cluster
httk workspace init --name runs /scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster runs
httk workspace settings set --key slurm.partition --value batch runs
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" runs
httk workflow run --workspace runs --count 4
```

From a desk, configure an `ssh` remote to reach that machine, then use the same
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
`run --workspace kappa:runs` invokes `httk workflow manager run --workspace
runs --count 4 --detach` there, and that command uses the owning workspace's
launcher. The result is the same as running on the cluster, or addressing the
same machine through a configured `machine_names` alias. Transfer jobs to the
workspace as needed and use `transfer kappa:runs default` after they stop, then
`httk workflow collect` locally.

## Workspace policy

Four tunables belong to the workspace rather than to any one process, so that
every manager, CLI, and independent implementation attaching it agrees on them.
They live in `.httk-workspace/format.json` and are read and written with:

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

Values are given as JSON and validated on write; an unknown key is refused
rather than stored. A change reaches a manager when it attaches, so restart
long-running managers after changing policy. Concurrent policy writers are not
serialized: the write itself is atomic, but the last writer wins.

## Application settings

Separate from that engine policy, a workspace also holds *application settings*:
a flat, dotted-name map of small values a runner resolves at run time — the VASP
command and a pseudopotential library. The manager launch profile is also a
workspace setting, so each workspace can carry its own scheduler requirements.

```console
httk workspace settings set --key vasp.command --value '"srun -n 32 vasp_std"' WORKSPACE
httk workspace settings show WORKSPACE
```

For a Slurm manager, set its launcher profile in the target workspace as well:
`slurm.account`, `slurm.partition`, `slurm.time_limit`, `slurm.nodes`,
`slurm.cpus_per_task`, and `slurm.reservation` become batch directives, while
`manager.workers` supplies the default worker count. The workspace launcher
reads these values when it composes the batch script.

A runner reads one through `a.setting("vasp.command")`, resolved in layers — the
job's inputs, a real `HTTK_VASP_COMMAND` deployment override, the workspace
setting, then the runner's default. The manager exports scalar workspace settings
into each attempt environment (`vasp.command` becomes `HTTK_VASP_COMMAND`) and
snapshots them into the `HTTK_WORKFLOW_CONTEXT` JSON environment value, so a runner sees the values the workspace
held when its job was claimed. See {doc}`/vasp_runners` and {doc}`/sdks/sdk_parity`.
Workspace settings are non-secret configuration: they are snapshotted into the
attempt context and exported into the runner environment, so credentials must
not be stored there; remote credentials already live elsewhere.

## Readiness and transfer environment advisories

Use the read-only precheck before starting managers:

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --json
httk workflow precheck --workspace WORKSPACE --runner-search-path PATH
```

It reports environment entries resolved from the current process environment,
workspace settings, or declared defaults, plus runner-reference problems, for
pending jobs. It also measures each pending job against the workspace's live
managers: a job **no live manager can claim** names the closest manager's unmet
requirements (the same wording `job why` uses, including a runner-module
allowlist a manager does not carry), a **language job** (the collect gate's
`workflow_realization = language` pair) whose engine modules are absent names the
pip extra to install (for example `pip install httk-workflow[jobflow]`) — a
failure only when no live manager serves its executor, since the extras belong on
the machine that runs the job; when one does, it is `indeterminate` and
non-failing. A declared **required input** whose staged destination has gone
missing from the payload is flagged. When no manager is live, one workspace-level
notice replaces per-job claim findings. An unresolved entry, broken runner,
unclaimable job, missing-and-unserved engine, or missing required input gives
exit status `1`. The repeatable `--runner-search-path`
option checks installed runner references; a plain installed reference without a
configured path is `indeterminate`, not a failure, and does not by itself give
exit status `1`. The authoritative environment gate is still at attempt start;
this report is advisory and can become stale. The `HTTK_*` layer is this
process's environment, not a promise about the environment of a later compute
node.

`httk workspace managers WORKSPACE` answers "what serves this
workspace?" directly — one line per registered manager, live or stale, with its
pools, capabilities, executors, and runner modules — rather than by reading it
off a `job why` on an arbitrary job.

Transfers run the environment check against destination settings, job overrides,
and declared defaults, without treating the client process environment as the
destination. They warn about unresolved default-less entries; add
`--strict-environment` to block before any job state is moved. Remote settings
are checked through an isolated read when reachable; an unreachable destination
gets one immediate warning and is only a strict-mode failure.

## Freeing disk on a quota'd filesystem

A manager removes an attempt's control directory after a durable commit and
after it has reaped the local process when the actual destination is `ready`,
`waiting`, `paused`, or `succeeded`; failed and cancelled attempts remain as
evidence. Transaction trash is normally removed with that control tree. A
manager inheriting a commit leaves the tree for GC. The manager is never
required to execute policy-gated cleanup code, so it can disappear between any
two instructions. It runs always-safe cleanup at startup and the full
policy-gated collection at clean exit; a clean manager removes its own metadata
directory, while a crash leaves it for `journal_days` collection. On a quota'd
HPC filesystem, failed and cancelled
attempt evidence, retained journal history, interrupted transaction trash, and
acknowledged bundles are what remain to manage.

Collection also runs in full at a clean manager exit. A workspace with no
manager visits still needs an explicit collection. Configure the retention
limits once, then run it from a maintenance job or by hand:

```console
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
httk workspace policy set --key retention.journal_days --value 90 WORKSPACE
httk workspace policy set --key retention.journal_days --value null WORKSPACE  # keep forever
httk workspace gc --dry-run WORKSPACE
httk workspace gc WORKSPACE
```

It is safe to run against a live workspace: a manager that is still
heartbeating keeps its own directory and every journal segment it wrote, no
non-terminal marker or payload is touched beyond the aged attempt-control
directories of terminal jobs; GC may remove a terminal marker whose payload
the operator removed. Pruning an empty placement mirror that a transition is
recreating underneath is an ordinary outcome rather than an error. Every
segment in a non-terminal job's current frame chain is protected; terminal jobs
protect only their current segment. A `null` or
`"keep"` member means keep forever. `attempt_control_days` is unlimited when
omitted; `journal_days` and `trash_days` default to one day. See
[the command guide](workflow_cli.md#freeing-disk) for the full category table
and for what collecting journal history costs.

A long-lived manager can also do this itself, which is convenient where no
maintenance job exists:

```console
httk workflow manager run --workspace WORKSPACE --gc-interval 3600
```

The manager then collects at most once per interval, at the end of a tick and
never between observing a marker and acting on it, obeying exactly the same
`policy.retention` limits. It is off by default, and a failed collection is
logged rather than allowed to disturb scheduling. Keep the interval long: a
collection walks the state tree and the journal directory, which is work the
scheduling passes do not need done often.

To remove a finished (`succeeded`, `failed`, or `cancelled`) job, remove the
payload directory named by `job show` with `rm -r`, then run
`httk workspace gc WORKSPACE`, or let a manager started with
`--gc-interval` perform it. Cancel a non-terminal job first with `job request
cancel`, and remove children only when their parent is terminal: GC's join
guard is best-effort, not a lock, and a parent that publishes a join during the
unbounded TOCTOU window before unlinking may observe a missing child and fail
or stall. This is a scheduling-correctness consequence, not payload data loss.

## Filesystem visibility

A workspace may be attached from several nodes under the same account, which
makes metadata visibility part of the filesystem configuration.

**Mount options.** Renames and directory listings must be seen by other clients
promptly, so an aggressively cached mount needs its attribute caching bounded:

- NFS: `actimeo=5` (or the pair `acdirmin=1,acdirmax=5`) and
  `lookupcache=positive` are a good starting point. The defaults —
  `acdirmax=60` — mean another node may keep serving a stale directory listing
  for up to a minute, which is legal and must simply be waited out. `noac`
  removes the staleness entirely and is correct, but it disables attribute
  caching and close-to-open optimization altogether and is usually far too
  slow for a workspace with many jobs. `nolock` is fine: the protocol never
  takes a POSIX lock. Use NFSv4.1 or newer where available.
- Lustre and GPFS: no special options. Their metadata coherence is strong
  enough that the local-filesystem defaults apply.
- Anything backed by an object store or a FUSE cache without rename atomicity
  is not a supported workspace filesystem at all: the protocol requires
  `rename(2)` to be atomic and to fail rather than silently overwrite.

**The visibility deadline.** Set it to comfortably exceed the worst-case
staleness window of the mount:

| Filesystem | Recommended `visibility_deadline_seconds` |
| --- | --- |
| Local disk, tmpfs, single node | `5` (the default) |
| Lustre, GPFS, BeeGFS | `10` |
| NFS with `actimeo=5` | `30` |
| NFS with default caching (`acdirmax=60`) | `120` |

The deadline costs nothing when nothing is wrong: the schedule starts at 10 ms
and stops the moment the rename or frame becomes visible. It is only spent when
the filesystem is actually lying to one client.

**Clocks.** Leases are advisory evidence, not a fence. A manager decides that
another manager's claim has expired by comparing its own wall clock with the
heartbeat timestamp that manager wrote, so the nodes sharing a workspace should
run NTP; skew larger than `lease_seconds` will cause premature or delayed
recovery of abandoned claims. Safety does not rest on this: the actual fence is
the marker rename, which exactly one actor can win, so a mistaken expiry
decision costs a lost claim rather than two runners in one job.

## Submit a job

A prepared payload is a directory containing an immutable `job.json` and its
runner. Submit it at any arbitrary placement:

```console
httk job submit --workspace WORKSPACE --placement project-a/00/17 PAYLOAD
```

Submission copies by default. `--move` performs a same-filesystem rename and
consumes the source directory.

## Share one runner between many jobs

A partitioned campaign should not copy its runner into every payload. Publish
the runner once into the workspace runner store instead:

```console
httk workflow runner publish --workspace WORKSPACE --name relax.py ./relax.py
# A runner directory is published the same way and pinned by its tree digest.
httk workflow runner publish --workspace WORKSPACE --name relax-runner ./relax-runner
```

The command prints the reference to embed in every `job.json` that uses it:

```json
{"path": "relax.py", "sha256": "…", "source": "workspace"}
```

Publication is content addressed. Publishing identical bytes again changes
nothing, and replacing a stored name whose content differs requires `--replace`,
because live jobs already reference the stored digest. Before each attempt the
manager verifies the runner in place and executes it with the job workdir as
cwd; a mismatch fails the job with `runner_mismatch` and an unresolvable or
non-executable runner with `runner_unavailable`. A detached transfer carries
the runners its job references, and importing installs the missing ones at the
destination. Compiled package runners locate their registered binaries under
`HTTK_WORKFLOW_RUNNER_ARTIFACTS`. A file runner is invoked through its verified
`/dev/fd/<N>` descriptor, so code that needs sibling files uses
`HTTK_WORKFLOW_RUNNER_ROOT`.

Runners deployed outside any workspace use `"source": "installed"` and resolve
against the ordered `--runner-search-path` roots of the manager.

## Run

```console
httk workflow manager run --workspace WORKSPACE --workers 8
```

To advertise a host allocation explicitly, repeat `--worker-resource` once per
resource. For example, four workers sharing 32 CPUs and 128000 MB:

```console
httk workflow manager run --workspace WORKSPACE --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000
```

Safety property: a task manager claims and runs only jobs whose marker, payload
directory, and `job.json` are regular, non-symlink entries owned by the account
running that manager. Child jobs belong to the manager's account; imported jobs
belong to the account that imports them.

Without pool configuration, a manager advertises the reserved `default` pool.
Additional routing and capability labels are explicit:

```console
httk workflow manager run --workspace WORKSPACE \
  --pool vasp \
  --capability gpu \
  --workers 4
```

### Resources

Managers may advertise integer resource capacities, such as
`resources={"procs": 8, "mem": 32768}`. A ready job is skipped permanently by
that manager when one of its declared resources is missing, zero-capacity, or
larger than the manager's capacity; these jobs are reported in the idle census
under `ready_blocked["resources"]`. Jobs that fit are packed against the
reservations of attempts already running. When a job omits `procs` or `mem`,
the manager assigns its fair share (`capacity // workers`, using the whole
capacity when that quotient is zero), so undeclared jobs still occupy one
worker's share. A dynamic requirement published by an `advance` or `wait`
outcome applies to the next activation and is retained across retries.

For a workflow that mixes a wide relaxation with dense analysis steps:

Manifest:

```toml
[workflow.resources]
procs = 4
mem = 16000            # MB

[workflow.steps.relax]
resources = { procs = 32, mem = 120000 }

[workflow.steps.analyse]
resources = { procs = 1, mem = 2000, matlab_license_slots = 1 }
```

Manager:

```console
httk workflow run --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000 \
  --worker-resource matlab_license_slots 2
```

`run` and `manager run` launch managers through the workspace's
`manager.launch` setting. The built-in `process` launcher starts detached local
processes; a named launcher bundle such as `cluster` starts them according to
that bundle. `manager.count` is the workspace default for the number of
managers, and `--count` overrides it at the launch site. `manager.workers` is
the default number of attempts each manager runs concurrently, and `--workers`
overrides it per manager. `manager.command` is the command used after an
environment prelude (default: `httk`).

`--inline` forces one manager in the current process and therefore ignores the
workspace launcher; it can only be combined with `--count 1`. `--detach` starts
the managers and returns immediately. A remote workspace always uses this
detached invocation after the remote adapter has reached its owning machine.

The launcher receives the manager's complete argument vector, workspace path,
count, and workspace settings. The packaged Slurm launcher writes one mode-0700
batch script below `.httk-workspace/batch/`, submits it once per manager, and
returns the Slurm job IDs and script path. One generated script is reused for
the requested count, and it remains in that directory with the scheduler's
manager output files for inspection. The directory and scripts are launcher
output, not remote-adapter state. If submission fails after one or more jobs
were accepted, the command refuses with text containing `submitted: N` and
`job_ids: [...]`; cancel those jobs before retrying.

`environment.prelude` runs under `set -e` before the manager. With a prelude,
the launcher resolves `manager.command` on the resulting `PATH`; without one,
it preserves the Python interpreter command supplied by the caller. This rule
lets a module-loaded environment select the intended `httk` while keeping
direct process launches faithful to the invoking interpreter.

`procs` and `mem` are special: a job that omits them is assumed to need the
manager's fair share (`capacity // --workers`), so only jobs declaring both
can pack more densely than one-per-worker. With the manager above, `relax`
runs alone, while several `analyse` steps (one proc each) can run alongside,
at most two at a time because of the two `matlab_license_slots`. A manager
started without `--worker-resource matlab_license_slots` never runs
`analyse`; it is reported as `ready_blocked["resources"]` and in the idle
summary. A job needing a resource the manager lacks or has at 0 is likewise
never claimed. A dynamic requirement can be supplied by the SDK:
`a.advance("analyse", resources={"procs": 1, "mem": 2000, "matlab_license_slots": 1})`.
The Bash bridge equivalent is:
`httk_workflow_advance analyse --resource procs=1 --resource mem=2000 --resource matlab_license_slots=1`.

Resource labels are otherwise opaque to the manager. `manager.workers` is
unchanged: it remains the concurrency limit and there is no
`manager.resources` workspace setting. The command-line capacities are
repeatable, must be non-negative integers, and override any same-named SLURM
capacity detected by a local manager.

When a manager runs inside a SLURM batch allocation, it derives capacities only
when `SLURM_JOB_ID` is present:

| SLURM variable | Manager resource |
| --- | --- |
| `SLURM_NTASKS` | `procs` |
| `SLURM_GPUS` | `gpus` |
| `SLURM_JOB_NUM_NODES` | `nodes` |
| `SLURM_MEM_PER_CPU`, `SLURM_CPUS_PER_TASK`, `SLURM_NTASKS` | `mem = MEM_PER_CPU × CPUS_PER_TASK (default 1) × NTASKS` |
| `SLURM_MEM_PER_NODE`, `SLURM_JOB_NUM_NODES` | fallback `mem = MEM_PER_NODE × JOB_NUM_NODES` when `SLURM_MEM_PER_CPU` is absent |

Memory values are recorded in MB; a trailing `M`, `G`, or `K` is accepted,
with `G` multiplied by 1024 and `K` divided by 1024. Missing or invalid input
omits only the affected capacity and invalid input is warned about. Local
adapters supply host `procs` and total host physical memory in MB when the
caller did not provide those capacities. For multiple local managers, explicit
resource pairs are per-manager values and remain unchanged; only injected host
capacities are split across managers with quotient-plus-remainder distribution.
Under SLURM the manager reads `procs`, `gpus`, `nodes`, and `mem` from the
allocation unless they are given on the command line. Each manager owns its
own allotment; a replacement manager taking over a job brings its own
capacity. SLURM adapters let the allocation variables describe the real
allocation.

A manager claims work under the workspace's `lease_seconds` unless
`--lease-seconds` overrides it for that manager alone.

The default until-idle behavior is useful for batch invocations and tests;
pass `--idle` to keep serving.

**One banner, then one summary.** Whatever the console log level, `run` and
`manager run` print one line on startup — the manager id, the workspace, the log
file path, the pools, capabilities, executors, and advertised resources this
manager serves — so a
normal run is never silent about which manager is doing what and where its log
is. When it exits idle it prints one closing summary line that classifies every
remaining job: how many succeeded and failed, how many are *not claimable here*
— ready or unregisterable-submitted jobs broken down by the pool, capability,
or executor this manager does not serve, or by resource label beyond its
capacity — how many are waiting on children, how many are paused, and how many
committing or cancelling jobs have an unreadable definition. A job this manager
cannot progress — including one whose `job.json` is corrupt — no longer keeps
it awake to the idle timeout; it is reported instead. If the manager does hit
`--idle-timeout`, the advice names the actual pool, capability, executor, and
resource mismatches, the flags that would clear them, and points an unreadable
definition at `workspace fsck`, rather than a bare suggestion to raise the
timeout.

**Taking over another manager's attempt.** An expired lease says that a manager
stopped heartbeating, which is not the same as its attempt having stopped, so
neither workdir mode relaunches on lease expiry alone:

| Workdir mode | What admits a takeover | Relaxed by |
| --- | --- | --- |
| `persistent` | The recorded process is provably gone on this host. A second writer would corrupt the shared directory. | `--unsafe-persistent-takeover` |
| `isolated` | The recorded process is provably gone, *or* the heartbeat has been silent for `--takeover-grace-factor` leases (default `2.0`). A second attempt corrupts nothing but costs a second allocation. | `--unsafe-isolated-takeover` |

Both unsafe options and the evidence of every takeover — which rule admitted
it and how old the heartbeat was — are recorded in the new attempt's state
frame, so `job log` shows exactly why a job was relaunched.

A persistent-workdir attempt whose recorded process ran on *another host* can
never be proven stopped from here — only the launching host can ask its kernel
about that process — so a manager on a different host leaves it alone and logs
that decision (an info-level line, not a buried debug one). `job why` says the
same truthfully: it reports the job as blocked, names the host the writer ran
on, and tells you to run a manager on that host or pass
`--unsafe-persistent-takeover`, rather than claiming the expired lease will be
recovered here.

**Unresolvable join children.** A job `waiting` on a child that cannot be
resolved in this workspace does not wait forever: after `--join-grace-seconds`
(default `3600`) it fails with `dependency_failure`. The grace is measured from
the instant a manager *first* records the child as unresolvable, and that
instant is persisted into the waiting job's state frame, so the deadline
survives a manager restart instead of resetting to zero each time a new manager
takes over. `job why` on the waiting job shows the recorded instant and what the
grace will do.

**Long scans.** A manager heartbeats between its scheduling passes and inside
long ones, and bounds how many markers of one kind it processes per pass,
resuming the rest on the next pass in a stable order. A workspace too large to
scan inside one lease is therefore served round-robin instead of making the
manager look abandoned to its peers. A pass that still consumes half of the
lease is logged as a warning, and nine tenths of it as an error: raise
`lease_seconds`, split the workspace, or reduce what the manager scans.

Every claim, launch, transition, recovery decision, and refused request is
logged. The console reports warnings and errors, while the complete info-level
record is appended to `.httk-workspace/managers.log` with the manager id on
each record. `--log-level` raises or lowers both, `--log-file` moves the file,
and `--json-logs` emits one
JSON object per line for ingestion.
The shared log is rotated when a manager starts or every 1000 records once the
file exceeds 16 MiB; one backup, `managers.log.1`, is kept. A manager that has
not yet reopened the file keeps appending to the backup.

A manager drains on `SIGTERM` or `SIGINT`, which is what a batch system sends
at walltime. The first signal stops claiming, terminates the running attempts,
and keeps committing their outcomes for `--drain-timeout` seconds before
exiting successfully; a second signal exits immediately. Anything left behind
is recovered from its expired lease by the next manager.

## Scheduling

A manager never reads the whole workspace on a tick. Every scheduling pass
discovers its work by streaming the state tree of one active kind — one of
`submitted`, `ready`, `claimed`, `running`, `committing`, `waiting`, and
`cancelling` — and never opens the terminal `succeeded`, `failed`, or
`cancelled` trees at all. The in-memory marker index and every scheduling scan
therefore grow with the active work in flight rather than with the accumulated
history of a workspace that has run for years.

**Bounded streaming discovery.** A pass walks directory entries with
`os.scandir` instead of materializing an `rglob` of the tree, and it stops early
on two independent budgets: it visits at most `discovery_budget` directory
entries — `4096` by default — and it collects at most `maximum_pass_markers`
markers — `256` by default — before it yields the tick. It also takes a
heartbeat opportunity every 512 entries *inside* the walk, so even one enormous
flat placement directory keeps a manager's lease alive from within the scan
exactly as crossing many placements does, rather than only between passes. The
walk keeps a resume cursor per top-level placement root, held in the manager's
memory alone — nothing is written to disk, so two managers of one workspace
never contend on a shared position and a restarted manager simply begins a fresh
cycle. The roots are served in a round-robin rotation with per-root resume, so a
one large placement subtree can never starve a smaller sibling, and
the next tick continues precisely where this one stopped. A concurrent
transition that renames or removes a marker underneath the walk is tolerated
silently, consistent with how a vanished marker becomes a miss rather than a
fault.

The exhaustive workspace operations — `fsck`, `gc`, `collect`, `status`, and
`job list` — use the same scandir walker in an exhaustive mode with no cursor
and no budget, so their semantics are unchanged; only the bounded scheduling
passes carry the budgets.

**Best-within-window priority.** Claiming ready work scans a single bounded
window and then claims the best-priority candidates found *within that window*,
in a stable order among equal priorities, up to the number of free worker slots.
Priority is therefore best-within-window rather than exact-global: that is the
deliberate price of bounded discovery, and the round-robin rotation is what
eventually reaches a starved subtree on a later tick. Recovering exact global
order would require a derived priority index, which this implementation does not
build; it remains a possible future addition only where a deployment measures
that it needs one.

**Restricting a manager to placement prefixes.** A manager may be told to scan
only part of the tree, exactly the way pools and capabilities restrict what it
claims:

```console
httk workflow manager run --workspace WORKSPACE \
  --placement-prefix project-a \
  --placement-prefix project-b/2026
```

The flag is repeatable, and every scheduling scan — bounded window and
exhaustive walk alike — is then confined to those subtrees. With no
`--placement-prefix` a manager scans the whole workspace, which is the default.
Overlapping assignments stay safe because the marker rename still arbitrates a
claim, so two managers assigned the same subtree never both run one job;
disjoint assignments simply divide the scanning, so neither manager pays to walk
the other's trees. The assignment is deployment policy and not a protocol
change — placement values remain project-owned semantics that the engine only
validates and filters on — and it is recorded in the manager's manifest, so `job
why` reports a prefix mismatch when a live manager's placement prefixes exclude
the placement of the job being diagnosed. A configured prefix that currently
matches no job — whether a typo or simply a manager started before its jobs are
submitted — is logged as one honest warning at manager start, naming the prefix
and noting that the manager will serve that subtree once work arrives there, so
a scan-nothing prefix is a diagnosable condition rather than a silent one.

Laying out placements across a large campaign and assigning their subtrees to
managers by a written recipe rather than by hand is out of scope here; the
Phase 14 campaign recipes add it.

## Inspect and control

```console
httk workspace status WORKSPACE
httk workspace status --json WORKSPACE

httk job request pause --workspace WORKSPACE \
  --reason "inspection" JOB_UUID

httk job request continue --workspace WORKSPACE \
  --reason "inputs repaired" JOB_UUID
```

An `override_step --step X` request is pre-validated on the client: when the
job's state frame already records the runner's `runner_steps` (written after its
first attempt), a step outside that set is refused before the request is
published, listing the recorded steps. `--force` downgrades that refusal to a
stderr note and publishes anyway — a payload runner is mutable, so an operator
may have edited it to add the step. Before the first attempt nothing is
recorded, so the request is allowed with a note on stderr that it could not be
pre-validated; in either allow case the runner, not the manager, refuses the
step at the next attempt if it does not implement it (the manager only
shape-checks the request).

Requests capture the exact current marker generation and record reference.
A delayed request therefore cannot mutate a newer job state. One that can never
apply again — because the job has moved on — is moved to
`.httk-workspace/requests/retired/` with the reason recorded beside it instead
of being reread on every pass; a request for a runner executor this manager does
not serve is left alone for a manager that does.

When the publishing installation has an operator identity key — created by
`httk workflow config init` — the request also carries a detached Ed25519
signature over its canonical JSON, and the manager records the verified
`operator_key` in the journalled state frame beside `operator` and `reason`. The
signature is optional in both directions: a request without one is applied
exactly as before, so a mixed deployment needs no flag day, while a request
whose signature does not verify is quarantined with that reason rather than
applied. It is attribution and not authorization; see
[the project CLI guide](workflow_cli.md#operator-identity).

**Cancelling a running job** is fenced and verified, not a single signal:

```console
httk job request cancel --workspace WORKSPACE \
  --reason "wrong inputs" JOB_UUID
```

The manager first renames the marker `running` → `cancelling`, which fences the
attempt so it can no longer commit an outcome. Only then does it `SIGTERM` the
process group, `SIGKILL` it if it has not exited within the grace period, and
verify that it is actually gone; only a verified exit moves the job to
`cancelled`, and how it was verified is recorded in the terminal frame. A
manager that dies mid-cancellation leaves a `cancelling` marker, and the next
manager finishes exactly the same procedure. A process recorded on another host
cannot be proven stopped here: the job stays `cancelling`, the reason is
journaled, and a warning is logged on every retry — which is the safe answer,
because `cancelled` asserts that nothing is still writing the workdir.

## Checking and repairing a workspace

`workspace fsck` verifies the one thing a manager cannot route around: that
every state marker still resolves to its journal frame.

```console
httk workspace fsck WORKSPACE
httk workspace fsck --json WORKSPACE
httk workspace fsck --repair WORKSPACE
httk workspace fsck --repair --quarantine-unrepairable WORKSPACE
```

It reads every marker of every state kind and checks that its record reference
resolves — within the configured visibility deadline, so a merely slow network
filesystem is never mistaken for damage — to a readable frame whose checksum
verifies and whose job, kind, and generation agree with the marker name. Each
problem is reported with a stable code: `missing_segment`, `short_read`,
`checksum_mismatch`, `reference_mismatch`, `identity_mismatch`,
`unparseable_name`, `payload_missing`, and their siblings. `payload_missing`
means a non-terminal marker has no payload; it is always reported and is never
repaired or quarantined. Without `--repair` nothing is written.
The command exits `0` when the workspace is clean or everything found was
repaired, and `1` when something is left for an operator.
Fsck also performs a dry-run count of always-safe leftovers and reports the
total plus counts for `removed_jobs`, `tmp_entries`, `retired_requests`, and
`placement_directories`; these informational counts do not affect the exit
status.

`--repair` re-points a damaged marker at the last good frame of its job. Since
the frame holding the backward link is the unreadable one, the repair scans the
journal for readable frames naming that job and adopts the newest one *older*
than the marker's own generation — never a newer one, which would be either the
damaged frame itself or a transition no marker ever committed. It then writes
one `fsck_repair` state frame, chained to the recovered frame and carrying its
step, activation, and attempt counters forward, and renames the marker onto it
at the next generation. History is added, never rewritten, and the job is
schedulable again.

Two things are deliberately never repaired:

- a `claimed`, `running`, or `committing` marker whose manager is still
  heartbeating within its lease is reported and left exactly as it is, because
  that manager owns the transition that comes next. Stop the manager, or wait
  for its lease to expire, and run the repair again;
- a marker with no readable older frame — typically a job damaged before its
  second transition — cannot be restored at all. It is reported, and moved into
  `.httk-workspace/quarantine/` with an audit record only if
  `--quarantine-unrepairable` is also given.

Run it when a node crashed while writing, when a filesystem was restored from a
snapshot, or whenever `job show` reports that a state frame is not readable.

## Inspecting jobs

Five commands read one job the way a manager reads it — the authoritative marker,
the journal frame that marker names, and the immutable `job.json` — and none of
them writes protocol state:

```console
httk job list --workspace WORKSPACE --kind ready --placement project-a
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
```

`JOB` is a job UUID, a complete `tag--uuid` job key, any unique prefix of
either, or a path inside the workspace. A job directory names one job, while a
placement directory such as `jobs` names every live job below it; globs such as
`jobs/silicon*` are expanded from the current working directory. Paths must be
inside the selected workspace. An ambiguous prefix is refused with the jobs it
matched. Every command also accepts `--json` and prints one object: a report, a
frame array, a diagnosis, or a job array.

`job show` reports the state kind, placement, priority, generation, job digest,
runner identity, the budgets of the retry policy against what has been consumed,
the current and initial step, any step set the runner declared, the last
failure, the join and per-child state of a waiting job, and the payload,
workdir, and data paths.

`job log` walks the journal backward from the marker through
`previous_record_ref` and prints one line per state frame, oldest first, with the
timestamp, the transition, the step, the attempt ordinal, the reason, and any
failure code. A frame that cannot be read is reported in place; whatever history
remains readable is still shown.

`job why` answers "why is this job not running?" for every state:

- `submitted`: whether any manager has registered it, and which live managers
  serve its runner executor;
- `ready`: every claim precondition, one line each — runner executor, claim pool,
  required capabilities, the maintenance lock, the workspace core profile, the
  attempt budgets, and which live manager would accept the job;
- `claimed` and `running`: the owning manager, its heartbeat age against the
  recorded lease, and whether an expired lease means recovery rather than a stuck
  job;
- `committing`: that a published outcome is being committed and any manager
  serving the executor resumes it — unless a commit anomaly has repeated for the
  same attempt, in which case the recorded error is surfaced and the job is
  reported as a blocked, wedged commit rather than as needing no action;
- `waiting`: the join condition, every child with its label and state, which
  children block, and which cannot be resolved in this workspace;
- `failed`: the failure, whether an operator `continue` still fits inside the
  retry budget, and the `error.json` breadcrumb of the last attempt;
- `paused`, `succeeded`, and `cancelled`: the state and how to proceed.

For `ready`, `running`, and `failed` jobs, `job why` also folds the journal into
one attempt-history line — `N attempts across M activations at step 'X'; K after
unclean exits` — and, when a job under an unlimited retry budget has attempted
well past a small threshold, flags it as flapping rather than progressing. A
runner-allowlist refusal is reported whenever a live manager's `runner_modules`
or search paths cannot reach the job's runner, so a repeating
`runner_unavailable` claim loop is named rather than shown as a manager that
"offers everything this job requires". Any operator request still pending in
`requests/ready`, and the reason recorded for the most recent retired one, are
surfaced on the states where they apply.

The job side of every precondition comes from `job.json` and cannot drift. The
other side — pools, capabilities, and served executors — is deployment policy of
whichever manager is running and is read from the manifest each manager
publishes, so a manager that is not running is reported as absent rather than
assumed.

Reading *results* rather than status is collecting: `httk workflow collect
WORKSPACE` streams `CollectedJob` summaries, while `--raw` exposes the
`JobRecord` stream for a data layer; see {doc}`/collecting`.

## The foreground debug runner

```console
httk job debug --workspace WORKSPACE --step relax PAYLOAD
httk job debug --workspace WORKSPACE --follow-children JOB
```

`job debug` drives exactly one job to a terminal state in the foreground and
streams the job's `logs/stdio.out` chronicle to the console as it grows.
Every transition is performed by a private task manager whose scans are
restricted to that one job, so the debugged job runs through exactly the code
paths a production manager uses and no unrelated work is claimed. Lines are
prefixed with the step that produced them, and `[debug]` marks each transition
the polling loop observed; `job log` always holds the complete record afterwards.
`--log-level` raises the private manager's own console log, which is quiet by
default.

The first argument is either a payload directory, which is submitted fresh at
`--placement` (`debug` by default), or a selector of a job that already exists.
`--step` overrides the initial step of a fresh payload; overriding the step of a
job that already has a history is refused, because rewriting history is what the
recorded `override_step` request is for. `--follow-children` drives the children a
waiting job spawned, depth first, and then resumes the parent.

The exit status is `0` when the job succeeded, `3` when it failed, and `4` when
it stopped without finishing — paused, cancelled, or waiting for children
without `--follow-children`. A live maintenance lock is refused up front, since
it would stop every launch anyway.

## Runner contract

The runner executes in the selected persistent or isolated workdir. It reads
the context supplied by `HTTK_WORKFLOW_CONTEXT` and publishes
`outcome.tmp.<nonce>/` as `outcome.ready/` beneath
`HTTK_WORKFLOW_CONTROL_DIR`. See the
{doc}`workflow_filesystem_api` for the complete protocol, and
{doc}`runtime_helpers`, {doc}`/sdks/native_bash_api`, or the {doc}`/sdks/sdk_parity` table
for the two authoring SDKs that implement it.

The local executor starts runners behind a one-byte launch gate. It records
the process identity in the `running` frame before releasing that gate. If the
manager disappears during this narrow launch interval, the gated process
observes end-of-file and exits without executing the runner.

`httk workflow manager run` executes the normal `path` runner executor. Converted
`httk-v1` packages use that same path through their packaged v1 runner; select
their `taskset` claim pool with the manager's `--pool` option. See
[*httk* v1 task compatibility](v1_compatibility.md).
