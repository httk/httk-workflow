# Task-manager usage

*For operators: initializing workspaces, submitting jobs, running managers, and inspecting or repairing what they leave behind.*

Every command below is spelled the canonical way, `httk workflow …`. The
`httk-taskmanager` executable installed beside it is an alias of the same tree
(`init`, `submit`, `run`, `status`, and `request`); see
[the project and workflow command line](workflow_cli.md) for the mapping.

## Initialize a workspace

Every `httk workflow` command names a *registered* workspace — a name bound once
to a place — never a bare path. `workspace init` both creates the workspace and
registers the name. Plain names are local and default to `./NAME`; a
`REMOTE:NAME` name creates a workspace on that remote.
`WORKSPACE` below is that registered name.

```console
httk workflow workspace init WORKSPACE --path runs/WORKSPACE
```

A workspace on a cluster names the remote it lives on instead of `local`; it is
created there over the adapter and its name is registered here. `workspace list`
shows every registered name and where it resolves, `workspace forget` deregisters
a name, and `workspace delete --force` destroys the workspace and deregisters it.
A library caller still constructs `Workspace(path)` directly; the registry is the
command-line contract. See {doc}`workflow_cli` for the whole `workspace` group and
{doc}`campaigns` for spreading a very large run across many workspaces.

Protocol publications are synchronized to storage by default. `--no-durable`
turns that off for throwaway workspaces and makes submission and transitions
faster at the price of correctness after a node crash: an unsynchronized
journal frame can be lost while the marker naming it survives, which leaves a
job whose state cannot be read until `workspace fsck --repair` restores it.
`--durable` is still accepted and does nothing, since it is now the default.

## Workspace policy

Four tunables belong to the workspace rather than to any one process, so that
every manager, CLI, and independent implementation attaching it agrees on them.
They live in `.httk-workflow/format.json` and are read and written with:

```console
httk workflow workspace policy show WORKSPACE
httk workflow workspace policy set WORKSPACE visibility_deadline_seconds 60
httk workflow workspace policy set WORKSPACE retention.journal_days 90
```

| Key | Default | Meaning |
| --- | --- | --- |
| `visibility_deadline_seconds` | `5.0` | How long a marker rename or a referenced journal frame may take to become visible before it is called damage. |
| `lease_seconds` | `900.0` | The claim lease of a manager started without `--lease-seconds`. |
| `journal_segment_bytes` | `67108864` | The size at which a journal writer rotates to its next segment. |
| `retention` | `{}` | Optional `attempt_control_days`, `journal_days`, and `trash_days` collection limits. |

Values are given as JSON and validated on write; an unknown key is refused
rather than stored. A change reaches a manager when it attaches, so restart
long-running managers after changing policy. Concurrent policy writers are not
serialized: the write itself is atomic, but the last writer wins.

## Application settings

Separate from that engine policy, a workspace also holds *application settings*:
a flat, dotted-name map of small values a runner resolves at run time — the VASP
command, a pseudopotential library — rather than tunables the scheduler reads.

```console
httk workflow workspace settings set WORKSPACE vasp.command '"srun -n 32 vasp_std"'
httk workflow workspace settings show WORKSPACE
```

A runner reads one through `a.setting("vasp.command")`, resolved in layers — the
job's `inputs`, then the environment (`HTTK_VASP_COMMAND`), then the workspace
setting, then the runner's default — so an operator configures the command once
per workspace instead of exporting it for every job, while a machine's own
environment still wins. The manager snapshots the settings into each attempt's
`context.json` at claim time, so a runner sees the values the workspace held when
its job was claimed. See {doc}`vasp_runners` and {doc}`sdk_parity`.

## Freeing disk on a quota'd filesystem

A manager frees nothing while it runs, by design: it is never required to
execute cleanup code, so it can disappear between any two instructions. Over a
long campaign the workspace therefore accumulates one control directory per
attempt, one journal writer and manager directory per process start, a full
copy of every tree a transaction replaced, and an intact bundle for every
transfer already acknowledged. On a quota'd HPC filesystem that is what fails
first.

Collection is a separate, explicit operation. Configure the retention limits
once, then run it from a maintenance job or by hand:

```console
httk workflow workspace policy set WORKSPACE retention.attempt_control_days 14
httk workflow workspace policy set WORKSPACE retention.trash_days 14
httk workflow workspace policy set WORKSPACE retention.journal_days 90
httk workflow workspace gc WORKSPACE --dry-run
httk workflow workspace gc WORKSPACE
```

It is safe to run against a live workspace: a manager that is still
heartbeating keeps its own directory and every journal segment it wrote, no
marker or payload is touched beyond the aged attempt-control directories of
terminal jobs, and pruning an empty placement mirror that a transition is
recreating underneath is an ordinary outcome rather than an error. A limit left
unset means keep. See
[the command guide](workflow_cli.md#freeing-disk) for the full category table
and for what collecting journal history costs.

A long-lived manager can also do this itself, which is convenient where no
maintenance job exists:

```console
httk workflow manager run WORKSPACE --gc-interval 3600
```

The manager then collects at most once per interval, at the end of a tick and
never between observing a marker and acting on it, obeying exactly the same
`policy.retention` limits. It is off by default, and a failed collection is
logged rather than allowed to disturb scheduling. Keep the interval long: a
collection walks the state tree and the journal directory, which is work the
scheduling passes do not need done often.

## Shared filesystems

A workspace is designed to be attached from several nodes at once, which makes
the metadata behavior of the shared filesystem part of the configuration.

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
httk workflow job submit WORKSPACE PAYLOAD --placement project-a/00/17
```

Submission copies by default. `--move` performs a same-filesystem rename and
consumes the source directory.

## Share one runner between many jobs

A partitioned campaign should not copy its runner into every payload. Publish
the runner once into the workspace runner store instead:

```console
httk workflow runner publish ./relax.py --workspace WORKSPACE --name relax.py
```

The command prints the reference to embed in every `job.json` that uses it:

```json
{"path": "relax.py", "sha256": "…", "source": "workspace"}
```

Publication is content addressed. Publishing identical bytes again changes
nothing, and replacing a stored name whose content differs requires `--replace`,
because live jobs already reference the stored digest. Before each attempt the
manager copies the runner below the attempt control directory, verifies the
pinned digest against that copy, and executes only the copy; a mismatch fails
the job with `runner_mismatch` and an unresolvable runner with
`runner_unavailable`. A detached transfer carries the runners its job
references, and importing installs the missing ones at the destination.

Runners deployed outside any workspace use `"source": "installed"` and resolve
against the ordered `--runner-search-path` roots of the manager.

## Run

```console
httk workflow manager run WORKSPACE --workers 8
```

Without pool configuration, a manager advertises the reserved `default` pool.
Additional routing and capability labels are explicit:

```console
httk workflow manager run WORKSPACE \
  --pool vasp \
  --capability gpu \
  --workers 4
```

A manager claims work under the workspace's `lease_seconds` unless
`--lease-seconds` overrides it for that manager alone.

The default until-idle behavior is useful for batch invocations and tests;
pass `--idle` to keep serving.

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

**Long scans.** A manager heartbeats between its scheduling passes and inside
long ones, and bounds how many markers of one kind it processes per pass,
resuming the rest on the next pass in a stable order. A workspace too large to
scan inside one lease is therefore served round-robin instead of making the
manager look abandoned to its peers. A pass that still consumes half of the
lease is logged as a warning, and nine tenths of it as an error: raise
`lease_seconds`, split the workspace, or reduce what the manager scans.

Every claim, launch, transition, recovery decision, and refused request is
logged. The console reports warnings and errors, while the complete info-level
record is rotated into `.httk-workflow/managers/MANAGER_ID/log`. `--log-level`
raises or lowers both, `--log-file` moves the file, and `--json-logs` emits one
JSON object per line for ingestion.

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

The exhaustive workspace operations — `fsck`, `gc`, `harvest`, `status`, and
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
httk workflow manager run WORKSPACE \
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
the placement of the job being diagnosed.

Laying out placements across a large campaign and assigning their subtrees to
managers by a written recipe rather than by hand is out of scope here; the
Phase 14 campaign recipes add it.

## Inspect and control

```console
httk workflow workspace status WORKSPACE
httk workflow workspace status WORKSPACE --json

httk workflow job request WORKSPACE JOB_UUID pause \
  --operator "$USER" --reason "inspection"

httk workflow job request WORKSPACE JOB_UUID continue \
  --operator "$USER" --reason "inputs repaired"
```

Requests capture the exact current marker generation and record reference.
A delayed request therefore cannot mutate a newer job state. One that can never
apply again — because the job has moved on — is moved to
`.httk-workflow/requests/retired/` with the reason recorded beside it instead
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
httk workflow job request WORKSPACE JOB_UUID cancel \
  --operator "$USER" --reason "wrong inputs"
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
httk workflow workspace fsck WORKSPACE
httk workflow workspace fsck WORKSPACE --json
httk workflow workspace fsck WORKSPACE --repair
httk workflow workspace fsck WORKSPACE --repair --quarantine-unrepairable
```

It reads every marker of every state kind and checks that its record reference
resolves — within the configured visibility deadline, so a merely slow network
filesystem is never mistaken for damage — to a readable frame whose checksum
verifies and whose job, kind, and generation agree with the marker name. Each
problem is reported with a stable code: `missing_segment`, `short_read`,
`checksum_mismatch`, `reference_mismatch`, `identity_mismatch`,
`unparseable_name`, and their siblings. Without `--repair` nothing is written.
The command exits `0` when the workspace is clean or everything found was
repaired, and `1` when something is left for an operator.

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
  `.httk-workflow/quarantine/` with an audit record only if
  `--quarantine-unrepairable` is also given.

Run it when a node crashed while writing, when a filesystem was restored from a
snapshot, or whenever `job show` reports that a state frame is not readable.

## Inspecting jobs

Five commands read one job the way a manager reads it — the authoritative marker,
the journal frame that marker names, and the immutable `job.json` — and none of
them writes protocol state:

```console
httk workflow job list WORKSPACE --kind ready --placement project-a
httk workflow job show WORKSPACE JOB
httk workflow job log WORKSPACE JOB --limit 20
httk workflow job why WORKSPACE JOB
```

`JOB` is a job UUID, a complete `tag--uuid` job key, or any unique prefix of
either; an ambiguous prefix is refused with the jobs it matched. Every command
also accepts `--json` and prints one object: a report, a frame array, a
diagnosis, or a job array.

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
  serving the executor resumes it;
- `waiting`: the join condition, every child with its label and state, which
  children block, and which cannot be resolved in this workspace;
- `failed`: the failure, whether an operator `continue` still fits inside the
  retry budget, and the `error.json` breadcrumb of the last attempt;
- `paused`, `succeeded`, and `cancelled`: the state and how to proceed.

The job side of every precondition comes from `job.json` and cannot drift. The
other side — pools, capabilities, and served executors — is deployment policy of
whichever manager is running and is read from the manifest each manager
publishes, so a manager that is not running is reported as absent rather than
assumed.

Reading *results* rather than status is a harvest: `httk workflow harvest
WORKSPACE`, or `httk.workflow.harvest`, streams one record per finished job for a
data layer to store; see {doc}`harvest`.

## The foreground debug runner

```console
httk workflow job debug WORKSPACE PAYLOAD --step relax
httk workflow job debug WORKSPACE JOB --follow-children
```

`job debug` drives exactly one job to a terminal state in the foreground and
streams the attempt's `stdout.log` and `stderr.log` to the console as they grow.
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
the context named by `HTTK_WORKFLOW_CONTEXT` and publishes
`outcome.tmp.<nonce>/` as `outcome.ready/` beneath
`HTTK_WORKFLOW_CONTROL_DIR`. See the
{doc}`workflow_filesystem_api` for the complete protocol, and
{doc}`runtime_helpers`, {doc}`native_bash_api`, or the {doc}`sdk_parity` table
for the two authoring SDKs that implement it.

The local executor starts runners behind a one-byte launch gate. It records
the process identity and commits the `running` marker before releasing that
gate. If the manager disappears during this narrow launch interval, the gated
process observes end-of-file and exits without executing the runner.

`httk workflow manager run` executes only the normal `path` runner executor. Legacy
`ht_steps` jobs use a distinct executor and are intentionally left untouched;
run those with [*httk* v1 task compatibility](v1_compatibility.md).
