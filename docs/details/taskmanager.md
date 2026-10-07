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

`workspace init PATH` creates a named local workspace in a directory of its
own, which must be new or empty (it is never a project root), or adopts and
registers an existing workspace. The first local workspace initialized inside a
project becomes the project's default. *httk-workflow* owns the workspace top
level: `.httk-workspace/`, `jobs/`, `logs/`, `postprocess/` and, once enabled,
`exchange/`. `REMOTE:PATH` initializes and names a workspace on that
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
httk launcher add --template slurm --global cluster
httk workspace init --name runs /scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster runs
httk workspace settings set --key slurm.partition --value batch runs
httk workspace settings set --key vasp.command --value vasp_std runs
httk workflow run --workspace runs --count 4
```

From a desk, configure an `ssh` remote to that machine and use the same
workspace operations through `kappa:`:

```console
httk remote add --template ssh kappa
httk remote configure \
    --set host=kappa.example.org --set username=rar \
    --set check_connectivity=yes kappa
httk workspace init kappa:/scratch/rar/httk/runs
httk workspace settings set --key manager.launch --value cluster kappa:runs
httk workspace settings set --key slurm.partition --value batch kappa:runs
httk workflow run --workspace kappa:runs --count 4
```

The remote is only transport: it moves files and invokes commands on kappa.
`run --workspace kappa:runs` invokes
the frozen peer vector `httk workflow manager run --workspace runs --count 4 --detach` there, which
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
httk workspace settings set --key vasp.command --value vasp_std WORKSPACE
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
  never takes a POSIX lock (the only kernel locks are on node-local tmpfs). Use NFSv4.1 or newer where available.
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

Job directories never nest: no placement component may itself parse as a job
key (`<name>--<uuid>`), so a placement can never put one job inside another's
directory. Submission, import, adoption and spawned children all refuse such a
placement; a spawn that names one fails the parent attempt with
`protocol_error`. Jobs placed before this rule keep working, but a confined
attempt refuses to start in a job directory that contains another job.

### Sharing one runner between many jobs

A partitioned campaign should publish its runner once into the workspace runner
store instead of copying it into every payload:

```console
httk runner publish --workspace WORKSPACE --name relax.py ./relax.py
# A runner directory is published the same way and pinned by its tree digest.
httk runner publish --workspace WORKSPACE --name relax-runner ./relax-runner
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
httk manager run --workspace WORKSPACE --workers 8
```

Without pool configuration, a manager advertises the reserved `default` pool.
Additional routing and capability labels are explicit:

```console
httk manager run --workspace WORKSPACE \
  --pool vasp \
  --capability gpu \
  --workers 4
```

A task manager claims and runs only jobs whose marker, payload directory and
`job.json` are regular, non-symlink entries owned by the account running it.
Child jobs belong to the manager's account; imported jobs belong to the
importing account.

A job directory is written by the job it holds, so the manager treats it as
hostile between attempts. It never follows a symlink on its control paths
below the job directory (logs, attempt control, the outcome, transaction
replay, seals), and a symlink, FIFO or other special file there, or an
oversized control document, fails that job with `protocol_error` instead of
redirecting a manager write or stalling the manager. An attempt that keeps
running after its outcome was committed gets `SIGTERM` and, after the
cancellation grace, `SIGKILL`.

A manager claims work under the workspace's `lease_seconds` unless
`--lease-seconds` overrides it for that manager. By default it runs until idle,
which suits batch invocations and tests; pass `--idle` to keep serving.

### Launching managers

A launcher is to starting managers what a remote is to reaching a machine.
`run` and `manager run` launch managers through the launcher bundle that the
workspace's `manager.launch` setting selects. The built-in local `process`
launcher starts detached local processes; a named bundle, such as the Slurm
launcher `cluster` above, starts them as it defines. These workspace settings
shape the launch:

- `manager.count`: the default number of managers; `--count` overrides it at
  the launch site.
- `manager.workers`: the default number of attempts each manager runs
  concurrently; `--workers` overrides it per manager.
- `manager.command`: the command used after an environment prelude (default
  `httk`).
- `manager.allocation`: the [allocation probe](#allocations) a launcher passes
  its managers (the Slurm launcher's default is `slurm`).
- `manager.launch_template`: the argv template for the attempt launch prefix;
  placeholders `{procs}` `{nodes}` `{hosts}` `{nodefile}` `{gpus}` `{mem}`
  `{cpus_per_proc}`.
- `manager.launch_mpi`: the Slurm MPI plugin appended as `--mpi=<name>` to the
  built-in Slurm launch step (for example `pmi2` for Intel MPI); ignored when
  `manager.launch_template` is set.
- `manager.confine.block_mpi_spawn`: under `manager.confine=bwrap`, `on`
  (default) relays confined ranks' Slurm PMI-1 and refuses `MPI_Comm_spawn`,
  `auto` does so only when Slurm sets `PMI_FD`, and `off` passes Slurm's PMI
  through; see
  [PMI-2 launches](workspace_daemon.md#pmi-2-launches-intel-mpi).
- `manager.bind_cpus`: `true`, `1` or `yes` (any case) pins every locally
  executed attempt to the CPUs of its processor slots; anything else, or no
  value, leaves CPU affinity alone; set it before the manager starts (see
  [placement and binding](#placement-and-binding)).
- `manager.confine` and `confine.*`: attempt confinement; see
  [confining attempts](#confining-attempts).

A `slurm` launcher pins its own values of `manager.confine`,
`manager.launch_template`, `manager.launch_mpi`, `manager.confine.block_mpi_spawn`, `manager.bind_cpus` and
`confine.*` on the managers it starts with the manager option `--setting KEY=VALUE` (not shown in `--help`;
the last occurrence of a key wins). Pinned values override the workspace
settings and stay fixed for the manager's lifetime; all other workspace
settings are read live at each claim. See
[launchers](launchers.md#pinned-settings).

`--inline` runs one manager in the current process, ignoring the workspace
launcher, and combines only with `--count 1`. `--detach` starts the managers
and returns immediately. A remote workspace always uses this detached
invocation once the remote adapter has reached its owning machine.

The launcher receives the manager's complete argument vector, the workspace
path, the count and the workspace settings. The packaged Slurm launcher writes
one mode-0700 batch script below `logs/batch/`, submits it once per
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

### Confining attempts

With `manager.confine=bwrap` the manager, which stays trusted and unconfined,
starts each attempt inside a Bubblewrap sandbox:

- the workspace is visible read-only at its real path, so cross-job reads,
  `stage_input` and `Attempt.parent` keep working, and the attempt's own job
  directory is writable at its real path, so every `HTTK_WORKFLOW_*_DIR` path
  is unchanged; the attempt cannot write the workspace root,
  `.httk-workspace/` or another job;
- the paths of `confine.readonly_paths` are read-only, `/tmp` is private, with
  `HOME=/tmp/home` and `TMPDIR=/tmp`, and `/proc` and a minimal `/dev` with
  a private `/dev/shm` and the `confine.devices` nodes are the sandbox's own.
  The private tmpfs `/tmp` and `/dev/shm`, and the bound launch lock
  directory, are node memory; limiting it is a site matter;
- user, PID, IPC and UTS namespaces are private, the network is too unless
  `confine.isolate_network=false`, capabilities are dropped, and nested user
  namespaces are blocked when Bubblewrap supports it (a warning is logged
  otherwise);
- standard input is `/dev/null`; the environment is the attempt environment
  without `SLURM_*`, `SRUN_*`, `SBATCH_*`, `SALLOC_*`, `PMI_*` and `PMIX_*`,
  with `HTTK_WORKFLOW_CONFINED=1`.

The workflow prelude runs inside the sandbox, while `environment.prelude` runs
before the manager. Whatever either makes a job depend on (module trees, conda
or virtual-environment prefixes, code binaries, installed runner search paths)
must be listed in `confine.readonly_paths`. Job directories must not be
symlinks; placement directories may be, but a job reached through a placement
symlink pointing outside the workspace cannot be confined and its attempt
fails. Timeouts, draining, pausing and cancellation reach the sandboxed runner
as they reach an unconfined one, and parallel programs start through the
[launch prefix](#placement-and-binding). The keys and their defaults are in
{doc}`launchers`; {doc}`workspace_daemon` builds on this feature.

A manager probes Bubblewrap when it starts and refuses to start if it cannot
build the sandbox or a confinement setting is malformed. If confinement becomes
unavailable after it started, it stops claiming work, logs the reason and
reports the ready jobs under `ready_blocked["confinement"]` until confinement is
available again. A workspace enrolled with {doc}`workspace_daemon` requires
`manager.confine=bwrap` of every manager.

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
goes to the manager's own file `logs/managers/<manager-id>.log`, with the
manager id on each record; no file is shared between managers.
`--log-level` raises or lowers both and `--json-logs` writes one JSON object
per line for ingestion. `--log-file PATH` names a fixed file instead. It is
only for one in-process manager and is refused with `--count` above 1,
`--detach`, remote submission, and launchers, which cannot give each manager a
distinct file.

A manager rotates its own log when it starts, and every 1000 records once it
exceeds 16 MiB, keeping one backup, `<manager-id>.log.1`. Logs outlive their
managers so a crash stays diagnosable; garbage collection (category
`manager_logs`) removes the logs of managers whose directory is gone once they
are older than `trash_days`. `logs/batch/` is never collected.

### Draining

A draining manager stops claiming, sends `SIGTERM` to its running attempts,
keeps committing their outcomes for up to `--drain-timeout` seconds (sending
`SIGKILL` to any attempt still running 10 s into the drain), and then exits
successfully. Three things start a drain:

- `SIGTERM` or `SIGINT`, which batch systems send at walltime, for an `--idle`
  manager;
- `SIGTERM` (not `SIGINT`, which still stops at once) for a manager running
  until idle;
- reaching the drain point of a known allocation end (see
  [Time requirements](#time-requirements)), in either mode.

A signal during a drain the deadline started, or a second signal during a
signal drain, kills the attempts and exits immediately. The next manager recovers anything left behind from its expired lease. An attempt
the drain stopped that published no outcome fails with `lease_lost` (an
unclean restart, retried only if `retry_policy.retry_on` lists it), unless it
had already exceeded its `maxtime`, which stays `timeout`; an outcome the
runner publishes on `SIGTERM` wins as usual. `maxtime` is not enforced during
a drain, which already stops every attempt on its own clock. A manager that
knows its allocation end and runs until idle is bounded by its drain point
while attempts run, however long they take; `--idle-timeout` still ends it
when it makes no progress with nothing running. After a drain it prints
`drained (<reason>): N attempt(s) left to lease recovery` instead of the idle
summary.

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

### Taking over another manager's commit

A `committing` job is committed only by the manager its state frame names.
Another manager serving the job's runner executor takes the commit over once
that owner is evidently gone, by one of three kinds of evidence:

| Evidence | Meaning |
| --- | --- |
| `manager_process_dead` | The owner's `managers/<id>/manager.json` names this host and its process is gone, so a restarted manager resumes its predecessor's commits at once. |
| `manager_record_absent` | The owner has no manager directory or readable heartbeat. |
| `lease_grace_expired` | The owner's heartbeat has been silent for its lease times `--takeover-grace-factor`. |

Until then the commit is left alone and does not count as work for
`run_until_idle`. The takeover is itself a marker transition, `committing` →
`committing`, whose frame repeats the commit and records `previous_manager_id`
and `takeover_evidence`, so of two would-be successors exactly one wins.
Because it advances the state generation, an operator request issued against
the earlier generation is retired as stale and must be re-issued.

The owner of a commit first renames the published `outcome.ready` draft to
`commit.<generation>` in the attempt-control directory, after the generation of
the `committing` marker it holds, and reaches the draft by that name for every
step. A takeover renames the draft again, so the previous owner, should it still
be running, stops at its next step with nothing recorded; at most the one step
it had already started can overlap, and the replay tolerates that by checking
whether the step's result is already in place. A manager may therefore die or
be replaced at any point of a commit, and the commit completes exactly once.
One overlap is detected rather than prevented: a `replace-tree` step sets the
old tree aside only after checking that its trash name is free, but Python
offers no rename that refuses to replace, so in the gap between that check and
the rename a stalled previous owner could move the successor's new tree onto an
*empty* set-aside directory. The previous owner then finds a tree it never
observed in its trash and reports an error anomaly (`commit_displaced_data`)
naming both paths, so an operator can move the tree back.

A lingering attempt process that publishes a second `outcome.ready` after its
draft was renamed is ignored: the commit only ever reads its own draft, and the
attempt-control directory is removed (or collected) with it. Cancelling a
`committing` job moves it straight to `cancelled`; a commit already under way
then loses its final transition and records nothing, while transaction
operations it applied before that remain in `data/`.

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
httk manager run --workspace WORKSPACE --workers 4 \
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
attempt's reservation and its context `resources`.

A manager enforces `maxtime`: an attempt still running `maxtime` seconds after
its launch gets `SIGTERM` on its process group, and `SIGKILL` once the
manager's cancellation grace (`TaskManager(cancel_grace_seconds=...)`, 10 s by
default) has passed. Unless the runner published an outcome meanwhile
(a runner may trap `SIGTERM` and publish `retry` or `succeed`), the attempt
fails with the reserved code `timeout`, which `retry_policy.retry_on` can list
to retry it. Every attempt with a `maxtime` is told when it will be stopped:
the context member `deadline` and `HTTK_WORKFLOW_DEADLINE` carry the epoch
second at or shortly after which (never before) the `SIGTERM` comes, and the Python SDK exposes it as `Attempt.deadline`. A child spawned from a `ChildSpec` or
by `Attempt.call` never inherits `mintime`, and every `maxtime` it carries is
capped at the spawning attempt's `maxtime`; a prepared payload directory passed
to `Attempt.spawn` is registered as written.

A manager may also know when its own allocation ends: `--time-limit DURATION`
(a Slurm `--time` string such as `12:00:00`) sets it, and its
[allocation probe](#allocations) may report one. When the probe reports no end
(`none`, `host`, or an envelope without `end_time`), a manager inside a Slurm
job still reads `SLURM_JOB_END_TIME` (only when `SLURM_JOB_ID` is present).
`--time-limit` is counted from each manager's own start, and when both are
known the earlier end wins. Sites whose Slurm does not export
`SLURM_JOB_END_TIME` must pass `--time-limit`. Its drain point is
`--deadline-margin` seconds (120 by default) before that end; a margin shorter
than `--drain-timeout` is raised to it with a warning, and a `--time-limit`
that does not exceed the margin is refused before any manager starts. A
manager that starts past its drain point warns and claims nothing. With a known end, the manager stops claiming work that
cannot fit before its drain point: a ready job whose resolved `mintime` exceeds
the time left until the drain point stays ready, and from the drain point on
no ready job is claimed. Only an explicit `mintime` gates a job before the
drain point; `maxtime` does not imply one. Such jobs are counted under the
census kind `time` (`mintime beyond the time left: N` in the idle summary, or
`past the drain point: N` once it has passed), so
a manager left with only them goes idle and exits; start a manager with a
longer allocation (`slurm.time_limit` / `--time-limit`) or lower the jobs'
`mintime`. An attempt launched by such a manager is told the earlier of its
`maxtime` deadline and the drain point as its `deadline`, so an attempt
without a `maxtime` still gets one. The end time (`end_time`), drain point
(`drain_start`) and capacity (`resources`) are recorded in the manager's
`manager.json`, and `workspace managers` and `job why` show the
time left ("ends in HH:MM:SS", or "ended"). At the drain point the manager
[drains](#draining) and exits.

### Allocations

A manager learns what its allocation consists of from an allocation probe
selected by `--allocation SPEC`: a list of nodes, each with its host name,
`procs`, `mem`, `gpus` and, when known, GPU device ids and CPU lists, plus the
allocation's end time. The allocation's capacity is the node sums (`procs`,
`gpus`, `mem` when every node knows it, and `nodes`) plus any extra resources it
reports; `--worker-resource` still overrides any same-named capacity. The probe
runs once, when the manager starts, and the allocation kind and host names are
recorded in `manager.json` (`allocation`, `nodes`) and shown in the manager's
start line.

| SPEC | Meaning |
| --- | --- |
| `auto` (default) | `slurm` when `SLURM_JOB_ID` is set, else `none` |
| `none` | no allocation; capacities come only from `--worker-resource` |
| `slurm` | the Slurm probe below |
| `host` | this machine as one node: its usable processors, half its physical memory, and the GPUs announced in `CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES` or `ZE_AFFINITY_MASK` |
| `exec:PATH` | run executable PATH inside the allocation; it prints one allocation envelope (see [launcher authoring](launcher_authoring.md#allocation-probes)) |

The Slurm probe lists the job's hosts with `scontrol show hostnames
$SLURM_JOB_NODELIST` (a one-node job needs no `scontrol`), splits the tasks
over them by `SLURM_TASKS_PER_NODE` (`16(x2),8`), splits memory the way
the counts below sum it, and reads the GPU device ids of the batch host from
the first of the variables above. A one-node allocation can use its total GPU
count. Multiple GPU nodes require `SLURM_GPUS_PER_NODE` consistent with the
total; divisibility of the total alone does not establish per-node capacity.
Unknown or contradictory GPU placement falls back to aggregate counts.
The probe also records `SLURM_CPUS_PER_TASK` as the allocation's CPUs per
processor slot. When the nodes cannot be described
consistently with those counts, it warns and keeps the aggregate counts only, so
its capacity is always exactly the table below.

Launchers choose the probe for the managers they start: the Slurm launcher
passes `--allocation slurm` (or its `manager.allocation` setting), the process
launcher passes `host` for one manager and `none` for several, foreground
`--count` children get `none`, and [daemon](workspace_daemon.md) managers get
their approved launcher's `manager.allocation` (default `slurm`). A single detached process-launched manager therefore probes this host:
it advertises `nodes=1`, GPUs from `CUDA_VISIBLE_DEVICES`,
`ROCR_VISIBLE_DEVICES` or `ZE_AFFINITY_MASK`, and `procs` from its CPU
affinity. An explicit `--allocation` (either `--allocation SPEC` or
`--allocation=SPEC`) is kept for one manager. Several managers splitting one
allocation only count capacity, because binding to devices needs one manager
per allocation: with `--count` above one every child gets `none`, and an
explicit `slurm`, `host` or `exec:` spec is overridden with a warning.

### Placement and binding

A manager whose allocation lists its nodes keeps a per-node inventory and
places every attempt on it: `procs`, `gpus`, `mem` and `nodes` are then decided
by the inventory, while other labels are still counted. Without listed nodes
(`--allocation none`, or a probe that knows only aggregate counts) everything is
counted as described above. `mem` is placed only when every node knows its
memory; otherwise it stays a counted label like any other, and a
`--worker-resource mem` capacity applies to it as a counter.

- An attempt goes on one node when it fits there, on the node with the fewest
  free `procs` that holds its `procs`, `gpus` and `mem` (ties go to the first
  node in allocation order), leaving larger holes for larger attempts.
- Otherwise it spills. When the attempt needs `gpus`, the nodes with capacity
  and the most free `gpus` are taken first, each with at least one processor
  slot. The remaining processor slots are filled from the nodes with the
  largest remaining capacity, where a node's capacity is its free slots
  limited so that their proportional share of the attempt's `mem`, rounded up,
  fits its free memory (an attempt without `procs` is spread the same way by
  `gpus`). Its `mem` is then split in proportion to the slots taken on each
  node, with the rounding remainder going to nodes with space. This prefers
  few nodes, without guaranteeing the fewest, and always finds a placement
  when one exists under that rule. A `mem`-only requirement never spills.
- `nodes=N` gives the attempt N whole idle nodes to itself (all their `procs`,
  `gpus` and `mem`), the smallest ones in allocation order that together hold
  its `procs`, `gpus` and `mem`; nothing else is placed on them until it ends,
  and a node holding any other attempt, even one given no `procs`, is not
  idle. Such an attempt gets no fair share of `procs` or `mem`. The selection
  considers nonadjacent combinations too, so a smaller GPU node can be paired
  with a larger CPU node.

Concurrent attempts never share a processor slot or GPU id. A
`--worker-resource` for `procs`, `gpus`, `mem` or `nodes` that differs from the
allocation's own sum is taken from the last nodes backwards (or drops trailing
nodes), and the manager then advertises the inventory's totals for those
labels; one larger than the allocation cannot be placed, so the manager warns
and schedules by counts only. Without explicit `resources`, a `TaskManager`
given an allocation advertises the allocation's capacity. A job that can never be placed even on an idle
inventory, such as `nodes=3` on two nodes or `nodes=1` with more `procs` than
any node has, is reported under `ready_blocked["resources"]` with the first of
`nodes`, `procs`, `gpus`, `mem` that does not fit.

Every placed attempt is told what it got. Its context member `binding` holds
`nodes` (per node: `host`, `procs`, `gpus` and, when placed, `mem`),
`nodefile`, `file` and, when one applies, `launch`. `file` is `binding.json` in
the attempt control directory: the same object with each node's `gpu_ids` and
`cpus` (the cpulists of its slots) added when known, kept out of the context so
that a large allocation cannot overflow the context's 100000-byte limit (a
binding too large even without them fails the attempt with `protocol_error`
naming its node count). The runner environment carries `HTTK_WORKFLOW_NODELIST` (the hosts,
comma-separated), `HTTK_WORKFLOW_NODEFILE` (a file in the attempt control
directory with exactly one host line per reserved processor slot, the
`PBS_NODEFILE` convention) and `HTTK_WORKFLOW_LAUNCH`
(the launch prefix, shell-quoted). The Python SDK exposes the member as
`Attempt.binding`, and Bash reads it with `httk_workflow_context binding`.

The launch prefix is what a runner puts before its parallel command. It is
`shlex`-quoted, so in Bash run `eval "$HTTK_WORKFLOW_LAUNCH vasp_std"` (or split
it with `read -ra`) rather than relying on word splitting when the prefix may
hold quoted words; the built-in Slurm prefix has none unless the workspace path
needs quoting, so `$HTTK_WORKFLOW_LAUNCH vasp_std` usually works there too. Inside a Slurm allocation it
is

```text
env SLURM_HOSTFILE=NODEFILE srun [--mpi=M] --ntasks=T --distribution=arbitrary
     --exact --cpus-per-task=C
     [--mem=MBM | --mem-per-cpu=MBM] [--gpus=G | --gres=none]
```

with `T` the nodefile's line count, equal to the reserved processor slots,
and `M` the workspace setting `manager.launch_mpi` when it is set.
A share with zero slots adds no task. A reservation with no processor slots
gets no default scheduler launch prefix; a parallel application must request
the processor slots it will use. The default Slurm prefix rejects a mixed
placement with GPUs on a node without processor slots. The prefix sets `SLURM_HOSTFILE` to the
nodefile for its own `srun` only, so the arbitrary distribution places exactly
the reserved tasks on each node while any other `srun` the runner starts is
unaffected. The hostfile alone names the nodes: `srun` refuses `--nodes` with
the arbitrary distribution, and a `--nodelist` would replace the hostfile, so
the prefix passes neither (a custom `manager.launch_template` using
`--distribution=arbitrary` must not either). `--cpus-per-task` repeats the
allocation's normalized CPUs per processor slot, captured by the probe when
the manager starts. A one-node attempt with `mem` gets `--mem` (its share); a multi-node
one gets `--mem-per-cpu`, its `mem` divided over its tasks' CPUs and rounded
up, so a node may be asked slightly more than its share. When the inventory
does not place memory, the attempt's counted `mem` requirement is used the
same way; without either, or with zero (which `srun` reads as all of the
node's memory), there is no memory option.
`--gpus=G` is added when it has GPUs and `--gres=none` when the allocation has
GPUs it does not get; with `--gpus` Slurm chooses the step's devices, which
need not be the binding's `gpu_ids`. This built-in prefix still needs
acceptance on a real Slurm cluster. The workspace setting `manager.launch_template` replaces it for every kind of
allocation; it is split like shell words, and `{procs}`, `{nodes}` (the node
count), `{hosts}`, `{nodefile}`, `{gpus}`, `{mem}` (MB on the first node, or
empty) and `{cpus_per_proc}` (the CPUs in one slot's cpulist, otherwise the
allocation's normalized value) are
substituted in each word; other braces, such as `{}`, are kept as written:

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
code command setting names only the program (`vasp.command = "vasp_std"`,
`qe.command = "pw.x"`) and the process count comes from the job's resources,
not from the command. `launch=False` (`--no-launch`) runs the command as given,
and ORCA gets no prefix by default because it starts its own MPI. A code command
that itself starts with a launcher such as `srun` or `mpirun` is refused when a
prefix applies, with a message to set the bare program and configure
`manager.launch_template`. The generic `run` verb, `Attempt.run` and
`httk_workflow_run` are not code-aware and never prepend the prefix.

In a [confined attempt](#confining-attempts) the rendered prefix would start
ranks outside the sandbox, so `HTTK_WORKFLOW_LAUNCH` is set instead to a launch
client, used the same way (`$HTTK_WORKFLOW_LAUNCH vasp_std`); it stays unset
when no prefix applies. The client asks the trusted manager to start the
launch, and the manager runs `<rendered launch template> <rank helper>`,
rendered from the placement in its memory with its own nodefile for
`{nodefile}` and `SLURM_HOSTFILE`; the nodefile and `binding.json` in the
attempt control directory stay informational and are never read back. On
every node the rank helper starts each rank in its own sandbox: the job
directory writable and the workspace read-only, host networking, a per-launch
shared-memory directory below `confine.shm_root` at `/dev/shm`, the step's PMIx
directory only when it lies below `confine.pmix_roots`, the `confine.devices`
nodes, and the `confine.environment.<NAME>` variables. Rank standard output
and standard error stay separate, and the client exits with the launch's exit
status. One launch runs at a time per attempt; further requests wait. A
launch is stopped when its attempt is cancelled, times out, is drained or
publishes its outcome, and a client stopped with `SIGTERM`, for instance by a
code helper's timeout, returns only after the ranks are gone (a helper that
then kills the client can return about a second before ranks ignoring
`SIGTERM` are reaped). A launch counts as finished when the launcher's local
process group (such as `srun`) is gone; remote tasks of a killed `srun` end
when Slurm cleans up the step. The attempt keeps
its placement until every launch is reaped. The client keeps its liveness lock
in `<confine.shm_root>/httk-launch-<attempt_id>/`, a node-local tmpfs
directory (`confine.shm_root` must be a tmpfs and is checked), so the workspace filesystem
needs no lock support. ORCA,
which starts its own MPI, is supported on one node only under confinement.
Each launch style needs site acceptance, and stale shared-memory directories
after a node failure are a site cleanup item; see
[parallel launches](workspace_daemon.md#parallel-launches).

An attempt placed on one node that is the manager's own host is executed
locally, so the manager also binds it to its devices. The `host` probe's node
and the Slurm batch host (the node named by `SLURMD_NODENAME`) are the
manager's own host; any other node is when its host name equals the manager's,
exactly or up to the first dot.

- When the share has GPUs whose ids are known, the runner environment sets the
  variable the ids came from (`CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES`
  or `ZE_AFFINITY_MASK`) to exactly those ids, comma-separated. This is
  always on. A local attempt that requests no GPUs on a node with known GPUs
  sees none (`CUDA_VISIBLE_DEVICES`/`ROCR_VISIBLE_DEVICES` set empty); an
  empty `ZE_AFFINITY_MASK` does not hide Level Zero devices, so that one is
  inherited. Any other attempt inherits the manager's value unchanged.
- With the workspace setting `manager.bind_cpus` true, the `slurm` (batch host
  only) and `host` probes split the manager's CPU affinity into equal slots,
  one per processor slot, in ascending CPU id order and ignoring core and
  hyperthread topology (leftover CPUs stay unused; with fewer CPUs than slots
  there are none), and an `exec:` envelope's `cpus` are used as given. The
  runner is then pinned to the union of its slots' CPUs before it starts. A
  pin the kernel refuses is logged as `attempt_pin_failed` and the attempt
  runs unpinned. Thread-count variables such as `OMP_NUM_THREADS` are not set.
- The probes discover slots once, when the manager starts, so for `slurm` and
  `host` set `manager.bind_cpus` before starting the manager. The pinning
  decision itself reads the setting at every launch, so turning it off takes
  effect at the next launch.

```console
httk workspace settings set --key manager.bind_cpus --value true WORKSPACE
```

Placement is in memory only and not recorded: a replacement manager places its
own attempts on its own inventory. Accepted limitations:

- The binding is information for well-behaved runners; the manager neither
  confines an attempt to its nodes nor, beyond the local binding above, sets
  CPU affinity or GPU visibility. [Confinement](#confining-attempts) restricts
  what an attempt can write, not which nodes it uses.
- There is no backfill or reservation: a `nodes=N` or other wide attempt can
  wait behind a stream of small attempts that keep every node partly busy.
- Device identity (`gpu_ids` and pinned CPUs) holds only for attempts
  executed locally, on the manager's own host; the Slurm probe knows device
  ids and CPUs for the batch host only, and an `srun` step started through the
  launch prefix chooses its own CPUs and GPUs.

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

Memory is recorded in MB. A trailing `M`, `G` or `K` is accepted; `G`
multiplies by 1024 and `K` divides by 1024. Missing or invalid input omits only
the affected capacity, with a warning for invalid input.

Under SLURM, `procs`, `gpus`, `nodes` and `mem` come from the allocation unless
given on the command line. Each manager owns its own allotment, and a
replacement manager taking over a job brings its own capacity. SLURM adapters
let the allocation variables describe the real allocation.

### Local capacities

One local manager probes the host itself (`--allocation host`). For multiple
local managers, the launcher supplies host `procs` and half the host physical
memory in MB when the caller did not; explicit resource pairs are per-manager
values and stay unchanged, and only injected host capacities are split across
managers, quotient plus remainder.

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
httk manager run --workspace WORKSPACE \
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
- `committing`: that a published outcome is being committed, which its owning
  manager resumes, or another manager serving the executor once the owner is
  evidently gone (see the commit takeover above). If a commit anomaly has repeated for the same
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

A manager claims a request by moving it into
`.httk-workspace/requests/claimed/<manager-id>/` while it applies it, and a
restarted manager returns its own claims to `ready`. Claims of a manager that
never comes back are recovered by the others: at most every 10 seconds a
manager looks at the other managers' claim directories and, once a manager is
evidently gone (the same evidence as a
[commit takeover](#taking-over-another-managers-commit)), moves its claimed
requests back to `ready`, logging `request_recovered`. They are then claimed
and checked against their exact preconditions like any other request, so a
request is never applied twice. If the former owner was only slow and had
already applied the request, the recovered copy is retired as stale, and
`--wait` and `job why` report that retirement although the request took effect.

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
full policy-gated collection at a clean exit. Whatever its `gc_interval`, it
also collects expired transfer receipts and acknowledgements older than
`trash_days` (`transfer_receipts`, `transfer_records`) once an hour. A clean
manager removes its own metadata directory; a crash leaves it for
`journal_days` collection (`manager_directories`). The trusted launch records
of confined launches in it are removed first, once the manager has been silent
for its lease times the takeover grace factor and each recorded process group
is provably gone; a directory still holding a record is kept as takeover
evidence.

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
httk manager run --workspace WORKSPACE --gc-interval 3600
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

`httk manager run` executes the normal `path` runner executor.
Converted `httk-v1` packages use the same path through their packaged v1 runner;
select their `taskset` claim pool with the manager's `--pool` option. See
[*httk* v1 task compatibility](v1_compatibility.md).
