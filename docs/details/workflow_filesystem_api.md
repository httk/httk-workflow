# Workflow filesystem API in detail

*For implementers of this protocol, and for anyone who needs to know what a
workspace on disk means.*

## Status and scope

This is the normative on-disk protocol of the *httk-workflow* engine, not
Python API documentation. The current `httk.workflow` implementation writes and
serves the `core-v3` profile described in
[Conformance profiles](#conformance-profiles). Relocation and cross-workspace
children remain reserved future capabilities; they are rejected rather than
partially executed.

The protocol is language independent: a step may be any program that can read
and write files and atomically rename a file or directory.

The design descends from the `ht.task.*` directories, `ht_steps`,
`ht.nextstep`, subtasks, and `ht.atomic.*` replay of *httk* v1, and keeps its
central property: neither a workflow step nor a task manager is ever required
to run policy-gated cleanup code. A manager runs always-safe cleanup at attach
and the full policy-gated collection at clean exit, and the owning manager
removes a locally reaped successful attempt control tree after commit, best
effort. Either may disappear between any two instructions; a later task
manager must be able to identify the last commit point and continue from it.

The design target is workspaces larger than the measured local snapshot in
{doc}`/details/benchmarks`; that target is not a capacity measurement. Metadata
inode count, directory fan-out, scheduler scan cost, and manual inspection are
therefore correctness-level design concerns, not later optimizations.

Effects outside the workflow filesystem are not transactional. Sending mail,
submitting to a second queue, or changing a remote database must be made
idempotent in that external system, for example with the httk job and
activation IDs as idempotency keys.

## Normative language

**MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** have their usual
specification meaning.

Protocol JSON is UTF-8. Unknown object members MUST be ignored when reading a
compatible major format version.

## Design summary

1. A job has one payload directory containing one required metadata file,
   `job.json`. Its parent path is an arbitrary, user-chosen placement below
   the workspace's `jobs/` directory.
2. A job has exactly one small marker file in the global `state/` tree.
3. The marker's location is the sole authority for the job's current state. It
   is atomically renamed between `submitted`, `ready`, `claimed`, `running`,
   `committing`, `waiting`, and terminal state directories.
4. Transition details and history are packed into shared append-only journal
   segments. There is no per-job state-record file, event directory, failure
   file, or revision directory.
5. An active attempt temporarily adds one small control directory below the
   reserved `attempts/` payload directory. The manager and collector refuse a
   symlinked `attempts/` or control entry; a concurrent replacement by the
   payload's own owner after that check is outside the threat model. The
   application runs in either one persistent `run/` workdir or an isolated
   `run.<attempt-id>/`.
6. `data/` and replayable transactions are optional. Jobs that opt in get
   all-or-none publication at attempt boundaries; jobs such as large VASP runs
   may instead keep all mutable state in persistent `run/`.

### Per-job metadata cost

Steady-state workflow metadata for a job with no retained application data:

| Object | Per job | Lifetime |
| --- | ---: | --- |
| Job payload directory | 1 directory | Job lifetime |
| `job.json` | 1 file | Job lifetime |
| Authoritative state marker | 1 file | Job lifetime |
| Shared journal records | 0 files | Packed into writer segments |
| Runner | 0 files | Shared and referenced by digest, unless it lives in the payload |
| `.httk-job/` runner job state | 0 or 1 directory | Job lifetime, when a runner keeps state across attempts |
| `attempts/` reserved container | 0 or 1 directory | Exists only while an attempt is live, failed/cancelled evidence is retained, or a succeeded leftover awaits collection |
| `attempts/<attempt-id>/` control directories | 0 or more directories | Live-attempt lifetime, failed/cancelled evidence retention, or a succeeded leftover awaiting collection |
| `logs/` | 0 or 1 directory | Created by the manager when it runs an attempt: `stdio.out` and `runlog.jsonl` |
| Persistent/isolated workdir | 0 or 1 directory | Application policy |
| Per-state/per-event/per-failure files | 0 | Not used |

By design arithmetic (not measurement), the permanent floor is **three
inodes**: the payload directory, `job.json`, and the marker. The floor is
reachable because a runner may live outside the payload: `runner.source` names
the workspace runner store or an installed search path, pinned by
`runner.sha256`. A partitioned campaign whose children run the same program
then stores it once, and each child's payload is just its `job.json`. A payload
runner costs whatever its files cost. A runner keeping state across attempts
adds `.httk-job/`; a live or retained attempt adds its attempt-control
directory and workdir.

Shard and journal directories are shared by many jobs, and application files
are not counted. Arbitrary placements can make some placement directories
unique to one job, and old state kinds can temporarily retain empty copies of
those paths. That overhead matters operationally even though it is not a
permanent per-job protocol object.

## Concepts

**Workflow workspace**
: One self-contained filesystem tree with an immutable workspace UUID, its
  jobs, authoritative state markers, and journals.

**Watch root**
: An ordinary directory below which a manager discovers workspaces. A manager
  may also receive explicit workspace paths and may supervise several
  workspaces at once.

**Job**
: The durable unit of scheduling, history, and final success or failure.

**Job key**
: The filesystem component `[<tag>--]<job-uuid>`. The UUID is authoritative;
  the optional tag is for human navigation.

**Placement**
: An arbitrary relative parent path below a workspace, such as
  `project-17/0/03a`, relative to the workspace's `jobs/` directory and possibly
  empty. The payload is at `jobs/<placement>/<job-key>`.

**Step**
: An application-defined name such as `relax` or `collect`, not declared in
  advance.

**Activation**
: One logical request to execute a step. Advancing to any step, including the
  same textual name, creates a new activation.

**Attempt**
: One physical execution of an activation. Retrying after a timeout or
  abandoned allocation creates a new attempt of the same activation.

**Data generation**
: Identifies the committed contents of an optional job `data/` tree. It is
  absent for jobs without transactional data and advances after each
  successful transaction.

**Outcome**
: The step's atomically published request to advance, wait, succeed, fail,
  retry, or pause.

**Child job**
: An ordinary job whose immutable definition names a parent. Children may
  create children.

## Required filesystem semantics

All correctness-critical paths of one workspace MUST reside on one filesystem
on which:

1. renaming a file within that filesystem is atomic;
2. renaming a directory within that filesystem is atomic;
3. a successful rename removes the source and installs the destination as one
   indivisible namespace operation;
4. a failed rename is reported to the caller.

Atomic rename of the exact current marker is the compare-and-swap. The
protocol never depends on `flock`, advisory locks, PID uniqueness, or an exit
trap on the workspace filesystem (see [No file locks](#no-file-locks)). The
only kernel locks are `flock` death notifications between processes of one
node on node-local tmpfs, used by [confined launches](#confined-launches).

The baseline guarantee is **process-interruption safety**. Storage-crash
durability additionally requires synchronizing new file contents and affected
parent directories before publishing their names; implementations claiming it
MUST use `fsync` or an equivalent in the order the filesystem requires.

These require an explicit executor adapter and validation:

- source and destination paths on different mounts;
- object stores that only emulate rename;
- synchronization tools operating on a live root;
- network filesystems without coherent atomic rename;
- filesystems on which a client can indefinitely cache a removed name.

Modification times and wall clocks are evidence for lease expiry, never
fencing. Correctness comes from moving the one current state marker.

## Conformance profiles

A conforming **core** implementation of the current profile, **core-v3**,
supports:

- one workflow workspace per job and all references within that workspace;
- submission, validation, claiming, leases, execution, and outcomes of dynamic
  multi-step jobs, with safe competition between any number of task managers;
- persistent and isolated workdirs;
- retries, dynamic fan-out into child jobs and joins, explicit, unexpected, and
  dependency failures, cancellation, and operator requests for manual
  continuation;
- packed journals with durable history and discovery of failed jobs, verified
  marker renames, startup recovery, and garbage collection;
- transactional data publication and replay;
- sealed detached transfer, and replay and recovery after a stopped manager.

In core-v3:

- every `spawn.json` entry carries a mandatory unique `label`;
- a join summary records typed per-child observations, which the next
  activation also reads as `children` in its attempt context;
- `runner` in `job.json` may name a shared runner outside the payload through
  `source` and `sha256`;
- a runner-declared failure marked `retryable` is retried within the job's
  existing attempt budgets.

`format.json` carries an `extensions` array for additions such as the exchange
extension. A workspace declaring an unknown extension refuses to attach, and a
workspace of an older format version is refused rather than migrated: reset and
recreate it with `httk system reset`.
Unknown state kinds are never treated as failed or orphaned jobs.

Priority is encoded in marker names, not directory levels. Scheduling is
therefore best effort: a cold start or incremental scan may temporarily find
lower-priority work first, and strict global priority is not guaranteed.

## Workspace layout and arbitrary placement

A workspace is an ordinary directory whose top level *httk₂* owns. Protocol
control data live below `WORKSPACE/.httk-workspace/` and every job payload below
`WORKSPACE/jobs/`:

```text
WORKSPACE/
├── .httk-workspace/
│   ├── format.json
│   ├── maintenance.lock                # only while a maintenance operation runs
│   ├── seal.json                       # only while the workspace is sealed
│   ├── tmp/
│   ├── quarantine/<epoch>-<uuid>/      # entry, report.json
│   ├── state/
│   │   ├── submitted/<placement>/
│   │   ├── ready/<placement>/
│   │   ├── claimed/<placement>/
│   │   ├── running/<placement>/
│   │   ├── committing/<placement>/
│   │   ├── cancelling/<placement>/
│   │   ├── relocating/<placement>/
│   │   ├── transferring/<placement>/
│   │   ├── waiting/<placement>/
│   │   ├── paused/<placement>/
│   │   ├── succeeded/<placement>/
│   │   ├── failed/<placement>/
│   │   └── cancelled/<placement>/
│   ├── journal/
│   │   └── <writer-id>/<segment-number>.hwj
│   ├── managers/
│   │   └── <manager-id>/
│   │       ├── manager.json
│   │       ├── heartbeat.json
│   │       └── launches/<attempt-id>.<request-id>/   # confined launches only
│   ├── requests/
│   │   ├── tmp/
│   │   ├── ready/
│   │   ├── claimed/<manager-id>/
│   │   └── retired/
│   ├── runners/                        # the workspace runner store
│   ├── runner-builds/                  # machine-local build registrations
│   └── transfers/                      # see Transfer artifacts
├── jobs/
│   └── project-17/
│       └── 0/
│           └── 03a/
│               └── silicon-relax--01234567-89ab-cdef-0123-456789abcdef/
│                   ├── job.json
│                   ├── data/
│                   ├── files/
│                   ├── run/
│                   ├── attempts/<attempt-id>/
│                   └── logs/           # stdio.out, runlog.jsonl; created when an attempt runs
├── logs/
│   ├── managers/<manager-id>.log
│   └── batch/                          # launcher batch scripts and scheduler output
├── postprocess/                        # default postprocess output root
└── exchange/                           # only with the exchange extension
```

Here the placement is `project-17/0/03a`, and the marker has a parallel path:

```text
.httk-workspace/state/ready/project-17/0/03a/
└── silicon-relax--01234567-89ab-cdef-0123-456789abcdef.p500.g4.<record-ref>
```

The layout includes the core-v3 transfer state directories (see
[Transfer artifacts](#transfer-artifacts)). No empty
state-kind or placement directory is required.

- The top-level names are exactly `.httk-workspace/`, `jobs/`, `logs/`,
  `postprocess/`, and, with the exchange extension, `exchange/`; the exchange
  extension is specified in [Exchange extension](#exchange-extension). *httk₂* never reads or writes any other
  top-level name, and a future top-level name is a format change. A workspace
  root is never a project root.
- `jobs/` is created at initialization and holds every payload. `logs/` and
  `postprocess/` are created on first use.
- `logs/managers/<manager-id>.log` is the diagnostic log of one manager, with a
  single writer. `logs/batch/` is created by a configured manager launcher such
  as the packaged Slurm launcher and holds its batch scripts and scheduler
  output. It is not remote-adapter state and is absent with the built-in
  process launcher.
- `.httk-workspace/tmp/` holds unpublished entries. Managers MUST ignore it for
  scheduling. Garbage collection may remove old entries, but correctness MUST
  NOT depend on cleanup. Its transfer transaction directories are specified in
  [Transfer artifacts](#transfer-artifacts), and the staged copies of a commit
  in [Child publication](#child-publication).
- `.httk-workspace/quarantine/<epoch>-<uuid>/` holds one quarantined entry as
  `entry`, renamed there whole, and a `report.json` with `original_path`,
  `reason` and `quarantined_at`.
- `managers/<manager-id>/` is described in
  [Heartbeats and manager logs](#heartbeats-and-manager-logs), its `launches/`
  in [Confined launches](#confined-launches), and `requests/` in
  [Manual continuation and control requests](#manual-continuation-and-control-requests).
- `runners/` is the workspace runner store of `runner.source: workspace` (see
  [Shared runners](#shared-runners)); `runner-builds/` holds the machine-local
  build registrations of compiled workspace runners ({doc}`workflow_packages`),
  derived state that is rebuilt rather than collected.
- `maintenance.lock` is the maintenance fence, created exclusively by a
  maintenance operation (`workspace seal`, `workspace move`, project manifests)
  with the holder's `pid`, `hostname` and `created` time and removed when it
  ends. It is an ordinary file whose existence is the fence, not a kernel lock:
  a manager launches no claimed attempt while a live one exists and releases
  the claim instead. A lock whose content is malformed or lacks `pid`,
  `hostname` or `created`, whose same-host process is gone, or that is older
  than 24 hours is stale: managers ignore it, and the next maintenance
  operation reclaims it. A lock the reader may not read at all is not stale
  and is honoured (see {doc}`workflow_cli`).
- `seal.json` is the workspace seal and `<payload>/.httk-job/seal.json` a job
  seal; both are specified in {doc}`sealing`.
- `files/`, `data/`, a workdir, and attempt control are created only when
  required. Empty placeholder directories SHOULD NOT be created.

### Placement rules

A placement is a relative path below `jobs/`; the payload is at
`jobs/<placement>/<job-key>`. Placement components have no protocol meaning. They may be projects, users,
dates, hash shards of any depth, or a mixture, and jobs in one workspace may use
different schemes. There is no configured sharding depth and no priority
level: a marker's path below its state kind is its placement and nothing else.

Placement components MUST be normalized relative path components. Empty
components, `.`, `..`, NUL bytes, and `.httk-workspace` are forbidden.
A placement MAY be empty: the payload is then at `jobs/<job-key>` and the marker
at `state/<kind>/<marker>`. The canonical text form of the empty placement is
`""` wherever a placement is written: state frames, manifests, seal records,
spawn entries, the `parent` member, join observations, and cursors. Each
component must fit the filesystem's filename limit. A workspace MAY set policy
limits on depth and total relative path length; these are operational limits,
not a sharding scheme.

Job directories never nest. Because every job directory is
`<placement>/<job-key>`, a placement component that parses as a job key would
put one job inside another's directory, so a new job's placement MUST NOT
contain such a component. Submission, import and adoption (including every
member of a transferred tree) refuse it, and a spawn that names it fails the
parent attempt with `protocol_error`, checked before any spawn record is
written. Markers of jobs placed before this rule keep parsing; a manager
refuses to confine an attempt whose placement violates the rule or whose job
directory contains another job.

Placement directories are operator layout and may be symlinks; job
directories MUST NOT be. A manager follows placement directories as the
workspace does, but opens the job directory and everything below it without
following symlinks.

Empty placement directories have no meaning and may be pruned under the rules
in “State-marker rename.”

### `format.json`

`format.json` identifies the self-contained workspace:

```json
{
  "format": "httk-workflow-filesystem",
  "format_version": 3,
  "core_profile": "core-v3",
  "extensions": [],
  "record_ref_encoding": "hwref-v2",
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "created_at": "2026-07-24T12:00:00Z",
  "policy": {
    "visibility_deadline_seconds": 5.0,
    "lease_seconds": 900.0,
    "journal_segment_bytes": 67108864,
    "retention": {"journal_days": 1.0, "trash_days": 1.0}
  },
  "settings": {},
  "workflow_preludes": {}
}
```

`policy`, `settings`, and `workflow_preludes` are always present. `settings`
holds the workspace's configuration values and `workflow_preludes` its
workflow-prelude map; both are administrative, like `policy`. When set,
`settings.postprocess.directory` MUST be an absolute path outside the
workspace; the default output root is the workspace's `postprocess/` directory.

### Workspace policy

Everything this specification calls *configured* is the `policy` object of
`format.json`, so that every implementation attaching a workspace agrees on
it. It is part of format version 3 and holds exactly these members:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `visibility_deadline_seconds` | number | `5.0` | How long a reader keeps re-probing before it declares a marker rename or a referenced journal frame incoherent. |
| `lease_seconds` | number | `900.0` | The default claim lease of a manager that does not state its own. |
| `journal_segment_bytes` | integer | `67108864` | The size at which a writer rotates to its next journal segment. |
| `retention` | object | `{"journal_days": 1.0, "trash_days": 1.0}` | `journal_days` and `trash_days` default to one day; `attempt_control_days` is unset. An explicit `null` or `"keep"` keeps a category forever. |

An implementation MUST refuse an unknown `policy` member, and MUST read an
absent `policy` object or member as the default above, so older workspaces
attach without migration. An absent `attempt_control_days` means keep; absent
`journal_days` and `trash_days` mean one day. Values are validated on write:
`lease_seconds` ≥ one second, `visibility_deadline_seconds` ≤ one day, and
`journal_segment_bytes` ≥ 4096.

Policy is administrative, not protocol state. A change is a read-modify-write
of `format.json` through an exclusively created temporary file and a rename, so
no reader sees a torn object. Concurrent policy writers are not serialized; the
last writer wins. Managers read policy at attach, so running managers see a
change only after restart.

## Workspace ownership

A workspace is single-user. Multi-user shared workspaces are not supported yet;
the ownership model needs more work before they can be safe.

The ownership oracle is the kernel ownership chain of a job's marker, payload
directory, and `job.json`: all three must be regular, non-symlink entries owned
by the manager's uid, and a manager claims only such jobs. Child jobs belong to
the manager's account; imported jobs belong to the importing account.

## Managing and combining workspaces

A workspace name is a client and user-interface concept. This API is path- and
`workspace_id`-based: callers provide paths, and the protocol identifies a
workspace by its immutable ID.

A core manager may attach several independent workspaces, but jobs and joins
stay within their own workspace. Cross-workspace references and coordinated
movement are reserved future capabilities.

### Discovery and resolution

A task manager accepts any combination of explicit workspace paths and watch
roots, below which it discovers directories containing
`.httk-workspace/format.json`.

Commands without `--workspace` use the closest enclosing workspace found by
walking upward from the current directory (a file path counts as its
directory; the walk stops only at the filesystem root). This takes precedence
over, in order, the project's recorded default, the registry default, and the
auto-created per-user default.

Both common arrangements work:

```text
# One workspace with projects as placement prefixes
WORKSPACE/jobs/project-a/00/17/<job-key>
WORKSPACE/jobs/project-b/hash/x9/<job-key>

# A watch root containing self-contained project workspaces
WATCH/project-a/.httk-workspace/format.json
WATCH/project-a/jobs/00/17/<job-key>
WATCH/project-b/.httk-workspace/format.json
WATCH/project-b/jobs/hash/x9/<job-key>
```

A manager may schedule all attached workspaces from one resource pool. Job and
child references are `(workspace_id, job_id)` pairs; a bare job UUID is
accepted only when unambiguous among attached workspaces.

Discovery stops at a workspace boundary. Attached workspaces MUST NOT overlap
unless an explicit advanced profile defines ownership of every placement
prefix; the default manager rejects nested or overlapping roots.

### Workspace identity and duplicate IDs

A manager identifies a workspace by `workspace_id`, not its path, and attaches a
workspace reached through two path aliases once. Journal references are
workspace-relative and include the workspace ID when used from another
workspace.

If two discovered roots declare the same `workspace_id`, a manager MUST prove
that they are the same directory before treating them as aliases: equal
filesystem object identity from open directory handles, reliable device/inode
identity, or an equivalent executor facility. Equal `format.json` bytes, UUIDs,
or canonical paths are insufficient, because a copied backup has all three.
Without proof, the manager MUST refuse both roots for mutation and report a
loud duplicate-workspace-ID error; it MUST NOT attach an arbitrary winner.

### Dynamic attachment

A complete workspace can be built elsewhere on the same filesystem and
atomically renamed below a watch root; its `format.json`, state tree, journals,
and payloads arrive together. The manager discovers it and starts scheduling
without restarting.

Filesystem notifications are hints. Managers periodically rescan watch roots so
a lost notification cannot hide a workspace.

Renaming an attached workspace within watched paths does not create a new
workspace; managers SHOULD hold an open directory handle and update the path
for the same ID. Copying a live workspace is forbidden, because it duplicates
authoritative markers and journal identity. A discovered copy, including a
backup restored beside the live workspace, hits the duplicate-ID refusal above.

### Dynamic detachment

Detaching one workspace does not stop a manager serving its others. A detach
coordinator obtains acknowledgement from every manager with a live heartbeat in
the workspace. Each manager:

1. stops new claims from that workspace;
2. completes or releases manager-owned claimed jobs;
3. completes committing replays;
4. waits for, pauses, or explicitly hands off running attempts;
5. closes the workspace journal writer;
6. acknowledges that the workspace is quiescent for it.

Once all live managers acknowledge and no marker is claimed, running, or
committing, the workspace may be atomically moved out of the watch root. A
manager MUST NOT treat a vanished workspace as failure of its jobs; it marks
the workspace unavailable until the same ID is reattached.

A raw move of a workspace with active attempts works only if the same managers
follow the rename by workspace ID and keep valid directory handles. The
portable, recommended sequence is controlled detach, move, attach.

## Job UUIDs, names, and path tags

Job IDs are lowercase canonical UUIDs. Human-readable names need not be unique
and stay in `job.json`. An optional **tag** may prefix the UUID in the job key:

```text
silicon-relax--01234567-89ab-cdef-0123-456789abcdef
```

The tag:

- is an immutable convenience label, not identity;
- is 1 through 48 ASCII bytes;
- uses lowercase letters, digits, `.`, `_`, and `-`;
- starts with a letter or digit;
- MUST NOT contain `--`;
- SHOULD be a short slug derived from the job name;
- need not be unique.

Without a tag, the job key is the UUID. Parsers identify the final UUID rather
than trusting the tag. A tag lookup may return several jobs; a UUID lookup
returns at most one per workspace.

The payload is at `<workspace>/jobs/<placement>/<job-key>/`, and the state marker
uses the same placement and job key, so ordinary shell completion or `find`
locates both:

```bash
find jobs -type d -name 'silicon-relax--*'
find .httk-workspace/state -type f -name 'silicon-relax--*'
```

Renaming a tag by hand is forbidden, because payload and state keys must agree.
Mutable descriptive labels belong in application data or an external catalog;
avoiding alias files keeps the per-job inode cost fixed.

## The authoritative state tree

### One marker, one source of truth

Every submitted job has exactly one regular marker file below `state/`. There is
no second marker inside the job and no advisory state index; the marker's
directory is the current scheduler state. The marker is created once at
submission and afterwards only renamed, so a job consumes one marker inode
regardless of its steps, retries, failures, or manual continuations.

The sole exception is an explicitly detached transfer bundle: its markers are
empty files in `.httk-transfer/markers/`, outside every manager's state tree,
and the bundle is not schedulable until import renames them into the state
tree.

### Marker names

```text
.httk-workspace/state/ready/project-17/0/03a/
└── silicon-relax--01234567-89ab-cdef-0123-456789abcdef.p500.g4.<record-ref>
```

The basename grammar is:

```text
<job-key>.p<priority>.g<generation>.<record-ref>
```

- `priority` is always a zero-padded three-digit integer, `000` through `999`.
- `generation` is a monotonically increasing unsigned 64-bit state generation in
  lowercase base 36 without leading zeroes, unrelated to the data generation.
- `record-ref` locates the immutable transition record in a packed journal. The
  initial submitted marker uses `g0.init`, because its initial information is
  in `job.json`.

The complete relative marker path is the authoritative current state:

- its state directory gives the state kind;
- the directories after the state kind give current placement and nothing
  else: no priority, shard, or index level is ever inserted between them;
- `p<priority>` gives current priority;
- the job key gives identity;
- the generation prevents stale operator actions;
- the journal reference gives transition details.

A transition MUST rename the exact old marker path to the exact new one; it MUST
NOT create a second marker and later delete the first. Therefore:

- two managers racing to claim the same ready marker cannot both win;
- a crash cannot leave old and new authoritative states;
- terminal and non-terminal collections are immediately inspectable;
- no index-repair process is part of ordinary correctness.

### Markers without payloads

Outside the recoverable `relocating` and `transferring` states, a marker
without its payload at the mirrored placement is corruption, except for the
quiescent, unowned removable kinds:

- a terminal (`succeeded`, `failed`, `cancelled`) marker without payload means
  an operator removed the finished job;
- a `submitted` marker was never claimed;
- a `ready` marker may come from a released or retried claim and retain attempt
  identifiers, but has no current manager owner.

Garbage collection removes these orphaned markers. A `claimed`, `running`,
`committing`, `cancelling`, `waiting`, or `paused` marker may still carry
scheduling or manager-owned attempt state, so its missing payload remains
corruption and is not collected.

A payload directory without a marker is unsubmitted temporary or orphan data,
not a queued job, unless it is a sealed detached bundle containing
`.httk-transfer/manifest.json` and its markers.

### State-marker rename

Before a transition, the actor:

1. prepares and synchronizes the new journal record;
2. derives the new marker basename from its journal reference;
3. creates the destination placement parents if absent;
4. attempts to rename the exact old marker to the unique destination;
5. resolves the observed filesystem state before deciding whether it won.

The return status of `rename` is not the transition result. `ENOENT` can mean
that the source vanished or that a destination parent was pruned, and a network
filesystem can execute a rename even when the caller sees a failure after
retransmission. Every implementation MUST use this verified-transition
algorithm after a rename error or ambiguous response, and MAY use it always:

1. Reopen or refresh the source and destination parent directories rather than
   relying on one cached negative lookup.
2. If the actor's unique expected destination exists and its basename resolves
   to the prepared journal record, the actor won, even if `rename` reported
   failure.
3. Otherwise, if the exact source still exists, recreate any missing
   destination parents and retry the same rename.
4. Otherwise, locate and validate the job's current marker. A different valid
   marker proves that another transition won.
5. If source, expected destination, and any other valid current marker all
   remain absent after bounded reopen/backoff retries, the workspace is
   unavailable or corrupt. The actor MUST stop mutation and report it; it MUST
   NOT silently classify the result as a lost race.

Record references are unique within a workspace, so the expected destination
MUST initially be absent. An implementation SHOULD use no-replace rename where
available. An existing expected destination is success only if it names the
actor's prepared record; any conflicting entry is corruption.

This algorithm applies to every correctness-critical rename: marker
transitions, submission, outcome publication, transaction operations, child
registration, relocation, and transfer.

### Off-chain journal records

A loser's prepared but unreferenced record stays in the journal. It is
*off-chain* and harmless: history is always reconstructed by starting at the
authoritative marker and walking `previous_record_ref` backwards, and no marker
or frame references the orphan. No implementation may reconstruct history by
scanning the journal forwards. Orphans need no tombstone, supersession record,
or compaction.

Marker repair is the one operation that reads the journal without a chain to
walk, because the frame holding the backward link is the unreadable one. It is
bounded instead: it considers only frames of the same job at a generation
strictly lower than the damaged marker's, never walks forward onto a frame no
marker committed, and records the adopted frame's reference in its repair
frame. An orphan shares its generation with the transition that won, so a
repair reaching back across a lost race MAY adopt the loser's members; the
recorded `fsck_repair.recovered_record_ref` keeps that auditable.

### Pruning empty placement directories

Empty state-placement parents MAY be removed only with operations that fail on
a nonempty directory. A pruner racing a transition either sees the new marker
and fails, or removes the empty directory first and makes the transition
recreate it and retry. Broad recursive deletion is forbidden.

A pruner MUST make at most one removal attempt per candidate directory per scan
and MUST NOT immediately retry a failed removal; it backs off until a later
scan. Transition parent-creation and rename retries are also bounded; if
confirmed prune collisions would exhaust that budget, the manager suppresses
pruning for the affected subtree or workspace and retries the transition before
reporting storage failure. Pruning is optional and MUST yield to state
mutation; implementations MAY disable it while managers are attached.

## Packed transition journal

Transition metadata are frames in shared append-only journal segments rather
than one JSON file per state, event, failure, or revision:

```text
journal/<writer-id>/<segment-number>.hwj
```

### Writers and segments

`writer-id` is a fresh random UUID per task-manager **process incarnation**.
The `manager-id` used in claims and heartbeats is likewise a fresh
per-incarnation UUID; a stable administrative name is a separate
`manager_label`. Writer and manager UUIDs MAY be equal, but a restarted process
MUST NOT reuse either. On startup a process MUST create a new writer directory
and first segment with exclusive creation and MUST NOT reopen any existing
segment for append, even one it believes was its own. A zombie and its
replacement thus write different paths and heartbeats.

Each process is the sole writer of its own segments. Segments rotate at
`policy.journal_segment_bytes`, not per job. Submission writes no journal file,
because the initial marker uses `init`.

### Frame format

A core journal segment begins with the 12 bytes
`48 54 54 4b 2d 48 57 4a 2d 56 31 0a` (`HTTK-HWJ-V1` plus newline). Segment
filenames are lowercase base-36 numbers without leading zeroes. Each frame then
contains:

1. an eight-byte unsigned big-endian payload length;
2. exactly that many bytes of one UTF-8 JSON record;
3. the 32-byte SHA-256 checksum over the eight length bytes and payload;
4. the same eight-byte big-endian length as a trailer.

### Record references

A marker's `record-ref` identifies writer, segment, byte offset, length, and
checksum. Format version 3 mandates the filename-safe `hwref-v2` encoding, with
no alternative, so independent implementations resolve marker names
identically:

```text
w<writer-uuid-hex>-s<segment-base36>-o<offset-base36>-l<length-base36>-h<checksum128-hex>
```

- The writer UUID is 32 lowercase hexadecimal digits without hyphens.
- Segment numbers are unsigned 32-bit integers; offsets and lengths are
  unsigned 64-bit integers. Numeric fields use lowercase base 36 without
  leading zeroes except for zero itself.
- `checksum128` is the first 128 bits of the frame's SHA-256 checksum (over the
  encoded length and payload) as 32 lowercase hexadecimal digits. The full
  checksum stays in the frame.

The worst-case noninitial marker basename is 213 ASCII bytes:

```text
86 job-key + 5 ".p999" + 15 (".g" + 13 generation digits)
+ 1 "." + 106 maximum hwref-v2 = 213
```

The 86-byte job key is a 48-byte tag, `--`, and a 36-byte UUID. The reference
budget is `33` for `w` and the writer UUID, `9` for `-s` and a seven-digit
base-36 segment number, `15` each for `-o`/offset and `-l`/length, and `34` for
`-h` plus the checksum: `33 + 9 + 15 + 15 + 34 = 106`. A conforming workspace
MUST support at least 213 bytes per filename component and MUST validate this at
initialization. These field limits and the tag limit MUST NOT be enlarged
within format version 3.

### Durability

Before a state-marker rename, the writer MUST flush the entire frame and, in
the storage-durable profile, synchronize the segment, so a marker never
legally references a torn tail. An unreferenced partial final frame is ignored
and may be truncated during journal repair.

This implementation runs the storage-durable profile by default, and it covers
the runner-side artifacts that publish work too: a frame claiming a committed
outcome is worthless if the outcome itself was never flushed. Each artifact
below is synchronized (file contents, then the directory entries naming it)
before the rename that makes it authoritative:

| Artifact | Synchronized before |
| --- | --- |
| Journal frame, including the `process` identity in a `running` frame | the state-marker rename that references it |
| State marker / request / submitted payload | it becomes visible in `state/` (its parent directory is flushed) |
| Running marker directories (source and destination) | the launch gate byte is written, after the verified `running` marker rename and both marker-directory entries are flushed |
| Attempt context environment value | the runner is launched with it; the compact UTF-8 JSON must be under 100,000 bytes |
| Outcome bundle (`outcome.json`, `runner_steps`, failure detail, its sealed transaction manifest and staged payload, its child `job.json` bundles) | the `outcome.tmp.<nonce>` → `outcome.ready` rename; the whole draft tree is flushed in one batch, then the attempt-control directory after the rename; cleanup follows the durable destination transition and local process reap |
| Committed transaction data (`data/`) | the manager appends the destination state frame and renames the marker out of `committing`; every replayed destination and each parent directory it touched, including the parents of directories it created, is flushed first; its own trash directories, the evidence that an operation started, are flushed before its first data step |
| Registered child payloads and their submitted markers | the parent's marker leaves `committing` |
| Job state (`.httk-job/state.json`), observed declarations | each atomic replace returns |
| Sealed replayable workdir batch (`.httk-runner/workdir-ready/`) and its replay into the workdir | the batch is published, then retired as applied |
| Process record of a confined launch (`process.json` in its trusted launch directory) | the launch gate line is written; it is always synchronized, whatever the profile |
| Operator request in `requests/tmp/`, retirement record | the request's rename into `requests/ready/`; the record's rename into place |

The append-only run log (`logs/runlog.jsonl`) and the captured
`logs/stdio.out` keep only process-interruption safety even in the durable
profile: per-line synchronization would dominate their cost, and they are
operator evidence, not authoritative state. A power cut can cost their tail,
never an outcome or a half-applied transaction.

In the non-durable profile (`--no-durable`, for throwaway and test workspaces)
every artifact above keeps only process-interruption safety. Torn writes and
interrupted renames are still never observed, but a power loss may lose a
journal frame, a marker, a published outcome, half of a "committed"
transaction, or a registered child, and can leave a marker naming a frame or
outcome its storage never received. That is the damage
[`workspace fsck`](#workspace-check-and-marker-repair) repairs.

### Visibility on network filesystems

On a network filesystem, a marker and the extended journal segment may become
visible to different clients at different times. A reader that sees a
referenced frame as absent, short, or checksum-incomplete MUST refresh the
segment (for example close and reopen it) and retry with bounded backoff. It
declares corruption only after `policy.visibility_deadline_seconds`, and MUST
NOT mutate the marker while the frame is temporarily unreadable. Damage no wait
can repair, such as a present but wrong checksum, is reported at once. The same
deadline bounds the retries of a verified state-marker rename and of any other
metadata-visibility probe.

### State frames

State frames have this shape; `resources` is absent until an outcome sets it:

```json
{
  "format": "httk-workflow-state",
  "format_version": 3,
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "job_key": "silicon-relax--01234567-89ab-cdef-0123-456789abcdef",
  "placement": "project-17/0/03a",
  "state_generation": 4,
  "kind": "ready",
  "previous_record_ref": "...",
  "created_at": "2026-07-24T12:00:00Z",
  "step": "collect",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "activation_ordinal": 3,
  "attempt_ordinal": 1,
  "total_attempts": 4,
  "data_generation": 2,
  "resources": {},
  "priority": 500,
  "reason": "advance"
}
```

- `data_generation` is omitted or `null` when `job.json` declares
  `data.mode: "none"`.
- `resources` is the validated dynamic resource requirement of the current
  activation. It carries across that activation's attempts; a new activation
  replaces it with its own selected requirement.
- `reservation` (claimed and running frames) is the effective requirement the
  owning manager reserved for the attempt, fair share included; it is not
  carried into later attempts or activations.
- `pause_requested` is added by an in-flight operator pause, carried to the
  next attempt boundary, and consumed when the job enters `paused`; a terminal
  outcome supersedes it.

A `running` frame also carries the launched process identity in a `process`
object with exactly these protocol members:

| Member | Type | Meaning |
| --- | --- | --- |
| `pid` | integer | Process ID of the gated launcher. |
| `process_group` | integer | Process-group ID used for signalling. |
| `hostname` | string | Host on which the launcher runs. |
| `launched_at` | string | UTC timestamp at which the launcher was created. |

`process` is retained through `cancelling` and may remain on the terminal
`cancelled` frame. It is not carried into a recovered or relaunched attempt.

State frames form a backwards-linked history across writer segments. Failure,
join, operator, and outcome details are embedded in the applicable state frame
or in a journal frame it references, never in per-job metadata files.

### Retention and derived caches

Segments are append-only and retained according to history policy. They may be
compressed only into a random-access archive format that preserves record
references. A derived SQL database or in-memory map MAY accelerate queries, but
it is a cache; the state tree and referenced frames remain authoritative.

Three rules make such a cache safe; this implementation's job-id-to-marker
index obeys them:

1. **A hit is confirmed before use.** The cached location is a path, and a
   marker another actor moved is detected by checking it.
2. **A miss is never an answer.** Absence is reported only after walking the
   whole ladder in [Waiting and joining](#waiting-and-joining), including one
   complete scan; otherwise a job another manager just published could be
   reported as nonexistent.
3. **It is process-local.** This implementation keeps the index in memory per
   attached workspace, built lazily from one scan and updated by the process's
   own renames. There is no on-disk index: many concurrent writers have no
   protocol-level way to order updates to a shared derived file, so it would
   need the synchronization and repair the state tree already provides. An
   implementation MAY build a shared derived database under the same three
   rules.

## Job definition and submission

A minimal `job.json` is:

```json
{
  "format": "httk-workflow-job",
  "format_version": 2,
  "id": "01234567-89ab-cdef-0123-456789abcdef",
  "tag": "silicon-relax",
  "name": "Silicon relaxation",
  "workflow": "example.vasp-relax",
  "runner": {
    "executor": "path",
    "path": "files/runner",
    "arguments": []
  },
  "workdir": {
    "mode": "persistent",
    "path": "run"
  },
  "data": {
    "mode": "none"
  },
  "initial_step": "prepare",
  "priority": 500,
  "claim": {
    "pool": "default",
    "required_capabilities": []
  },
  "retry_policy": {
    "maximum_attempts_per_activation": 10,
    "maximum_total_attempts": 100,
    "maximum_activations": 50,
    "retry_on": ["lease_lost", "timeout", "process_failure"]
  },
  "resources": {},
  "step_resources": {},
  "parent": null
}
```

`job.json` is the only required metadata file in the payload and is immutable
after submission. The immutable job digest a manager records is the SHA-256 of
the stored `job.json` bytes exactly as submitted; nothing renormalizes them, so
any hash utility reproduces it.

Small workflow-specific parameters and inputs SHOULD be embedded in `job.json`
rather than stored one file each. The optional `files/` directory holds
submitted code, templates, or immutable inputs that need separate files.

### Parameters

The optional top-level `parameters` member is a JSON object with string keys
and application-defined values, opaque to the protocol and covered by the job
digest. An implementation MUST reject a `parameters` object whose serialization
exceeds 262144 bytes (`MAXIMUM_PARAMETERS_BYTES` in the protocol model); bulk
content belongs in the payload or in transactional `data/`. A parent that
synthesizes a child job normally varies only its `initial_step` and
`parameters`.

### Resources

`resources` maps a resource label (validated with `validate_label`) to a
non-negative integer; booleans, negative values, and non-integers are invalid.
`step_resources` optionally maps validated step names to the same kind of
mapping. Units are opaque integers; SLURM-derived `mem` values, for example,
are megabytes.

Two labels are reserved for time requirements, in integer seconds:

- `maxtime` is the time limit of one attempt; it MUST be at least 1.
- `mintime` is the least remaining allocation time a manager needs to start an
  attempt; it MUST NOT exceed `maxtime` in the same mapping.

The protocol stores only integer seconds. Every authoring surface (the workflow
manifest, the SDKs, the Bash bridge's `--resource NAME=VALUE`) spells them as
Slurm `--time` strings (`M`, `M:S`, `H:M:S`, `D-H`, `D-H:M`, `D-H:M:S`, so
`"30"` is 30 minutes and `"2-12"` is 60 hours) and refuses integers.

The manager selects the effective requirement for step `s` from the first
present of: the state frame's dynamic `resources`, `job.step_resources[s]`,
`job.resources`, and `{}`. For manager resources named `procs` or `mem` that a
requirement omits, it assumes the worker's fair share (`capacity // workers`).
A manager never runs a job requiring a resource it does not provide. The time
labels are not consumable and never a manager capacity: they never block a
claim, and each resolves on its own through the same three layers, so a
job-level `maxtime` still applies to a step whose `step_resources` names only
`procs`. A mapping that names only time labels leaves the consumable
selection to the next level, and a resolved `mintime` above the resolved
`maxtime` is lowered to it. How a manager acts on them is described in the
[task-manager guide](taskmanager.md).

A child that a running step synthesizes never inherits `mintime`, and each of
its `maxtime` values is capped at the spawning attempt's effective `maxtime`
(a child without a job-level `maxtime` gets the cap). A prepared payload
directory spawned as a child is registered as written, uncapped.

### Declarations

The optional top-level `declarations` member maps a declaration name to one
declaration document. A declaration states what a workflow is (its inputs,
method, and outputs) without describing a graph, and feeds provenance.

The document is carried **verbatim** and is opaque to this protocol. An
implementation MUST validate that each member is a JSON object and MUST NOT
interpret, wrap, normalize, or version it further; versioning and
self-description live inside the document, as the `$id`-style members of the
property-definition conventions it follows.

- A declaration name MUST match `[A-Za-z0-9_][A-Za-z0-9._-]{0,63}`, because it
  is also a file basename (see [Runner job state](#runner-job-state)).
- An implementation MUST reject a `declarations` object whose serialization
  exceeds 262144 bytes, an allowance separate from that of `parameters`.
- Declarations are immutable after submission and covered by the job digest.
- Nothing is inherited: a synthesized child carries only the declarations its
  parent gave it.

### Runner

`runner.executor` selects an installed execution adapter and defaults to
`path`, which the core manager implements. Managers MUST leave jobs with an
unavailable or disallowed executor unclaimed, so specialized managers can share
a workspace without running each other's jobs. Executor-specific immutable
fields belong in `job.json`, validated at submission by that executor.

`runner.source` selects the root of `runner.path` and defaults to `payload`:

| `runner.source` | Root of `runner.path` |
| --- | --- |
| `payload` | the job's own payload directory |
| `workspace` | `<workspace>/.httk-workspace/runners/` |
| `installed` | one ordered runner search path configured in the manager |

`runner.path` MUST stay beneath its root under every source. `arguments` is an
argument vector, never a shell string. An executor may treat the path as an
executor-specific program under the same containment rules. An `installed`
path may use the reserved form `pkg:<module>/<resource>`, resolved inside an
installed Python package; a manager MUST restrict that form to an explicit
module allowlist, `httk.workflow` by default.

### Shared runners

A `payload` runner is pinned by the job digest, so `runner.sha256` MUST be
absent. Every other source names one file or tree shared across jobs (the
point of these sources, for example in a large partitioned campaign), so
`runner.sha256` is REQUIRED: the digest of a file's bytes, or the canonical
tree digest.

A manager MUST verify the digest against the bytes it will execute (a file
through the open descriptor it keeps until launch, a tree in place), executes
the runner in place with the job workdir as cwd, and MUST NOT modify it. A
mismatch fails the job with `runner_mismatch`; a runner that cannot be resolved
or entered fails it with `runner_unavailable`. Both are ordinary continuable
failures, never silent substitutions.

A tree is entered at its top-level `run` file unless the job carries
`runner.command`. File verification pins the inode through launch; tree
verification accepts the TOCTOU window between digest check and execution,
because the store owner is trusted not to overwrite a runner concurrently. For
a file runner, `argv[0]` and Python's `__file__` are its `/dev/fd/<N>` path;
siblings are found via `HTTK_WORKFLOW_RUNNER_ROOT`.

### Runner commands

A shared tree runner may carry `runner.command`, the unexpanded argument
vector a workflow package declares as `[workflow.runner] command`, such as
`["{artifacts}/relax"]` or `["java", "-cp", "{artifacts}/classes", "Relax"]`.
A package entry other than `run`, such as `run.py`, is recorded as
`["{package}/run.py"]`. It is a nonempty array of strings, covered by the job
digest and forbidden for a `payload` runner.

The only placeholders are `{package}` (the verified tree) and `{artifacts}`
(the build registered for this machine). A placeholder MUST start its element
or directly follow `NAME=` (as in `-Dhome={package}`); the rest of the element
is empty or `/PATH`, a relative POSIX path whose parts are nonempty and not `.`
or `..`. The program starts with a placeholder or is a bare name resolved on
the attempt `PATH`.

The manager:

1. verifies the tree digest;
2. resolves `{artifacts}`, failing with `runner_not_built` if the build is
   unregistered;
3. expands the vector;
4. checks that every placeholder path exists and resolves inside its root and
   that a placeholder program is an executable file, else fails with
   `runner_unavailable`;
5. appends `runner.arguments` and runs the result instead of the tree's `run`
   file, which such a tree lacks.

The attempt's runlog event records the expanded vector as `runner_command`. A
manager predating `runner.command` ignores it, finds no `run` entry, and fails
the job with `runner_unavailable`.

`runner.command` is trusted like the rest of the digest-covered `job.json`:
anyone who can submit a job can already run code through a payload runner. The
placeholder rules keep a package's references inside its own tree and build;
they are not a sandbox, and a bare program name is not restricted to an
allowlist. `{artifacts}` exists only for a `workspace` runner, the only source
with build registrations; an `installed` runner's command can use only
`{package}`.

### Workdir and data modes

`workdir.mode` MUST be explicit; there is no default:

- `persistent` reuses the declared workdir across activations and attempts. The
  workflow program owns recovery and cleanup of partial application files.
- `isolated` creates a new `run.<attempt-id>/` per attempt, optionally
  initialized from submitted files or transactional `data/`.

`data.mode` is `none` or `transactional` and is also explicit. With `none`, the
job may keep all mutable and final data in its persistent workdir and never
creates `data/`, publishes a transaction, or increments a data generation.
With `transactional`, `data/` and the transaction protocol are available in
every core-v3 workspace.

### Claim eligibility

`claim.pool` is required and names the scheduling pool or queue the job may be
claimed from; `claim.required_capabilities` may be empty. Pool and capability
labels use the tag component syntax. A manager advertises its pools and
capabilities and MUST claim a job only when both match. This is eligibility,
not a workflow name; quantitative `resources` are separate and used by a
capable manager when packing attempts.

The pool name `default` is reserved for jobs needing no explicit routing, and a
manager started without pool configuration MUST advertise it. A trivial
deployment thus needs no out-of-band pool agreement; sites opt into other
pools explicitly.

The optional `requires` member is an array of `NAME>=VERSION` strings naming
minimum installed distribution versions (only `>=`, a plain `N(.N)*` release,
each distribution once). A manager MUST leave a ready job unclaimed when its
environment does not meet every entry, as for a missing capability. The member
is omitted when empty; older managers ignore it.

The optional `calls` member maps an alias (label syntax) to the workflow
reference a job may call as a sub-workflow: a workflow name or a commit-pinned
git URI. It is written when the workflow declares `[workflow.calls]` (an empty
object when the table has no entries) and absent otherwise. A manager SHOULD
leave a ready job unclaimed while any reference does not resolve on its machine
or names a compiled package not built in the workspace. A runner SDK MUST
refuse a call to a workflow the member does not name.

### Retry budgets and priority

Retry limits are independent, optional, and unbounded when omitted:

- `maximum_attempts_per_activation` limits retries of one activation;
- `maximum_total_attempts` limits physical executions over the whole job;
- `maximum_activations` limits initial plus advanced activations, including
  advances back to the same textual step.

The counters are durable state-frame values. Before creating an activation or
attempt, the manager checks the limits; a request that would exceed one
transitions to failed with `budget_exhausted` and is not launched.

Priority is 0 through 999, where 0 is highest. It affects scheduling, not
correctness.

### Submission protocol

To submit:

1. Create a complete job below `.httk-workspace/tmp/`, including `job.json` and
   any initial `files/` or `data/`.
2. Choose any placement and atomically rename it to
   `<workspace>/jobs/<placement>/<job-key>/`.
3. Create a temporary zero-length marker named
   `<job-key>.p<priority>.g0.init`.
4. Atomically rename that marker to
   `.httk-workspace/state/submitted/<placement>/<job-key>.p<priority>.g0.init`.

Step 4 is the **submission commit point**. Before it, the placed payload is an
unsubmitted orphan that may be completed, retried, or collected; after it, a
fully populated submitted job exists.

Resubmitting an existing job UUID succeeds only if the existing immutable job
digest is identical and its marker already exists. A different definition
under the same UUID is an error.

### Registration

A manager registers a job by validating `job.json`, appending its first ready
state frame, and renaming the same marker from `submitted` to the mirrored path
below `state/ready/`. A crash before this rename leaves the job submitted, and
another manager repeats validation.

`set_priority` and `pause` also apply to `submitted`, as ordinary verified
transitions that rename the marker to `<job-key>.p<new-priority>.g1.<record-ref>`
in the same directory. Registration MUST therefore accept both forms of a
submitted marker:

- generation 0 referencing `init`, whose priority is the one in `job.json`;
- a later generation referencing a real frame, whose priority is the
  operator's and may differ from `job.json`.

The marker is authoritative for priority; only at generation 0 must the
`job.json` priority agree with it.

If a well-formed submitted marker's `job.json`, immutable files, or their
relationship to the marker fail validation, the manager appends a `failed`
frame with class `protocol_error` and moves the same marker to
`state/failed/<placement>/`. The frame records bounded validation details and,
when the document's identity cannot be trusted, uses the UUID and job key from
the marker. The payload is kept for diagnosis. Implementations MUST NOT leave
invalid jobs indefinitely in `submitted`.

An entry whose marker name or placement cannot be parsed cannot enter the state
machine. A workspace repair tool moves it to the shared
`.httk-workspace/quarantine/` area outside `state/`; managers report it loudly
and never schedule it. Quarantine names MUST preserve or record the original
relative path without permitting collisions or traversal. Quarantine is
exceptional corruption, not a job state.

## State machine

| Kind | Meaning |
| --- | --- |
| `submitted` | Complete job awaiting manager validation and registration. |
| `ready` | Eligible to be claimed when resources permit. |
| `claimed` | A manager won the claim but has not launched the attempt. |
| `running` | The current attempt may have a live process. |
| `committing` | The attempt is fenced; its outcome is being replayed, and the frame may still name the fenced live process. |
| `cancelling` | The attempt is fenced by a cancellation; its process is being stopped and its exit verified. |
| `relocating` | No attempt may run; the placement is changing. |
| `transferring` | A quiescent payload is moving or detaching. |
| `waiting` | Waiting for a declared child join. |
| `paused` | Requires an operator request to continue. |
| `succeeded` | Successful terminal state. |
| `failed` | Failed until explicit operator continuation. |
| `cancelled` | Cancelled terminal state. |

### Terminal and permanent states

- *Terminal for scheduling* means no manager moves the marker on its own:
  `succeeded`, `failed`, and `cancelled`. Join conditions, the completeness of
  `state/failed/`, and "a failure's frame is the job's last automatic one" use
  this sense.
- *Permanent* means nothing moves the marker again: only `succeeded` and
  `cancelled`. A `failed` job can be revived by operator `continue` or
  `override_step`, which is why broken jobs live in the state tree.

Unqualified, "terminal" means terminal for scheduling. `cancelling` is neither;
it is a live state in which a fenced attempt is being stopped.

### Transitions

```text
the cycle
  submitted ─validate─> ready ─claim─> claimed ─launch─> running ─outcome─> committing
                          ▲                                                       │
                          └──────────── advance or retry ────────────────────────┘

  committing ─────> ready | waiting | succeeded | failed | paused

back to ready
  claimed ─release────────────────────────────────────────────────> ready
  running ─retry within budget────────────────────────────────────> ready
  waiting ─join satisfied, or on_impossible advance───────────────> ready
  failed | paused ─operator continue or override_step─────────────> ready

into failed
  submitted ─protocol_error───────────────────────────────────────> failed
  claimed   ─prepare failure──────────────────────────────────────> failed
  running   ─lease loss, process failure, or an exhausted budget──> failed
  waiting   ─dependency_failure, or a join that cannot be read────> failed

cancellation
  running ─operator cancel─> cancelling ─verified exit─> cancelled
                                 ▲   │
                                 └───┘ the exit is not proven yet: stay fenced
  any other nonterminal state ─operator cancel───────────────────> cancelled

operator moves that do not change the kind, or only queue the job
  submitted | ready | waiting ─pause──────────────────────────────> paused
  claimed | running | committing ─pause───────────────────────────> same kind (deferred)
  ready | waiting ─attempt boundary───────────────────────────────> paused (when deferred)
  submitted | ready | waiting | paused | failed ─set_priority────> the same kind
```

Every core-profile transition:

| From | To | Trigger |
| --- | --- | --- |
| `submitted` | `ready` | Validation and registration. |
| `submitted` | `failed` | `protocol_error`: the payload, `job.json`, or its relationship to the marker is invalid. |
| `ready` | `claimed` | A manager won the claim. |
| `claimed` | `running` | The attempt was launched. |
| `claimed` | `ready` | Release: the manager may no longer launch it — a maintenance lock appeared, its budget changed — and it undoes the attempt counters it took. |
| `claimed` | `failed` | The attempt could not be prepared: `protocol_error`, `process_failure`, `runner_unavailable`, or `runner_mismatch`. |
| `running` | `committing` | A valid outcome was published; the rename fences the attempt. |
| `running` | `ready` | Retry within budget after `lease_lost` or `process_failure`. |
| `running` | `failed` | Retry budget exhausted, or a failure the policy does not retry. |
| `running` | `cancelling` | Operator `cancel` of a job that may have a live process. |
| `committing` | `cancelling` | Operator `cancel` while a launch of the attempt is not proven to have ended. |
| `committing` | `ready` | `advance` to a new activation, or `retry` of this one. |
| `committing` | `waiting` | A `wait` outcome naming a child join. |
| `committing` | `succeeded` | A `succeed` outcome. |
| `committing` | `failed` | A `fail` outcome, an unusable one, or an exhausted budget. |
| `committing` | `paused` | A `pause` outcome. |
| `waiting` | `ready` | The join was satisfied, or became impossible and `on_impossible` names a step. |
| `waiting` | `failed` | `dependency_failure`: the join became impossible with no `on_impossible`, or a named child stayed unresolvable past the join grace. |
| `waiting` | `failed` | `protocol_error`: the recorded join itself cannot be read. |
| `failed`, `paused` | `ready` | Operator `continue` (retry the activation) or `override_step` (new activation). |
| `cancelling` | `cancelled` | For a running attempt with a valid same-host identity, the exit was verified. |
| `committing` | `cancelled` | A valid same-host identity was signalled best-effort when available; exit was not verified, and the terminal frame records `no_live_attempt`. |
| any nonterminal | `cancelled` | Operator `cancel` where no live attempt has to be stopped first. |
| `submitted`, `ready`, `waiting` | `paused` | Operator `pause`. |
| `claimed`, `running`, `committing` | the same kind | Operator `pause`, recorded as a sticky deferred request until the next attempt boundary. |
| `submitted`, `ready`, `waiting`, `paused`, `failed` | the same kind | Operator `set_priority`, which renames the marker to a new priority at the next generation. |

Each row is one verified rename of the same marker; no other marker rename is
defined. Quiescent states may also pass through `relocating` and return to the
same logical state at another placement; `relocating` and `transferring` are
described in [Relocating and transferring jobs](#relocating-and-transferring-jobs).

`advance` may name any next step, including the same name, and creates a new
activation. Retry keeps the activation ID and increments the attempt ordinal.

### Cancellation

An authorized operator can cancel any non-terminal state. Cancelling a job that
may have a live process is three ordered steps, and the order is the guarantee:

1. **Fence first.** The manager renames the exact `running` marker to
   `cancelling`. From then on no manager accepts an outcome from that attempt,
   because the state tree no longer names it as current. Nothing has been
   signalled yet, so no race can skip this step.
2. **Then stop the process.** The owning manager, or any manager that can see
   the process if the owner died, sends `SIGTERM` to the recorded process group
   and `SIGKILL` after a configured grace period. The `cancelling` frame names
   the attempt, its attempt-control directory, and its previous owner, which is
   all a recovering manager needs.
3. **Only then finish, against evidence.** For a `running` or `cancelling`
   attempt with a valid same-host identity, the marker moves to `cancelled`
   only once the exit is verified, and every launch recorded for the attempt
   has [launch end evidence](#launch-end-evidence), recorded in the terminal
   frame's `cancellation` member.

`cancelling` is neither terminal nor quiescent. An attempt that publishes an
outcome after being fenced finds its marker in `cancelling`, and the outcome is
ignored like any other outcome from a fenced attempt.

Cancelling a `committing` job signals the recorded process group when a valid
same-host identity is available, and the recorded launches on this host. When
every launch recorded for the attempt has
[launch end evidence](#launch-end-evidence), it moves directly to `cancelled`
with `no_live_attempt` and that evidence (`launch_end_evidence`), without
verifying the attempt process's exit, because the outcome is published and
replay may be partially applied. Known limitation: the attempt process itself
may still be running after this terminal transition. Otherwise the job moves
to `cancelling` and is verified like a `running` attempt.

Apart from that exception, a manager MUST NOT publish `cancelled` for a
`running` attempt it has merely signalled. Acceptable evidence is:

| `cancellation.verified` | Meaning |
| --- | --- |
| `process_exited` | The manager launched the process and reaped it, and every launch it made for the attempt; the exit status is recorded. |
| `process_group_absent` | The process was recorded on this host, its process group no longer exists, and every launch recorded for the attempt has ended; the evidence is recorded as `launch_end_evidence` (see [Launch end evidence](#launch-end-evidence)). |
| `no_live_attempt` | The job was cancelled from a state where no exit verification is performed, including `committing` after outcome publication. |

A cancellation whose process cannot be proven stopped, typically one recorded on
another host, MUST leave the marker in `cancelling`, journal why, and retry. A
missing or malformed `process` member is damage, not evidence that the launch
gate was never released, and MUST take the same path. The attempt stays fenced,
and an operator sees a stalled cancellation instead of a terminal state falsely
asserting that nothing still writes the workdir. While the cancellation waits
for launch end evidence, the launches recorded on the manager's own host are
stopped by the same ladder.

## Claiming, leases, and fencing

### Claiming a ready job

To claim a ready job, manager `M`:

1. selects the exact ready marker;
2. reads the state frame and stable `job.json`;
3. generates attempt and claim IDs;
4. appends and synchronizes a `claimed` state frame;
5. renames that exact ready marker to the claimed state path whose basename
   references the new frame;
6. applies the verified-transition algorithm and proceeds only if its unique
   claimed destination is confirmed.

Competing managers rename the same source to different unique destinations; at
most one can remove the source, and each learns whether it won through
verified-transition resolution, not the rename return status.

The claimed frame names:

- manager, writer-incarnation, and attempt IDs;
- step, activation, and attempt ordinal;
- input data generation when transactional data are enabled;
- lease duration and start time;
- matched pool and capabilities;
- the resource `reservation`, the effective requirement including the
  manager's fair share (the running frame repeats it);
- preceding record reference.

### Launching an attempt

The manager creates `attempts/<attempt-id>/`, prepares the persistent or
isolated workdir, appends a running frame, and renames the claimed marker to
running immediately before launching the application.

A local executor MAY first create a process blocked on a launch gate, durably
record its identity in the running frame, and only then release the gate. If
its manager disappears before release, the gate MUST make the process exit
without executing the application. Because the identity is part of the durable
running frame, a missing or malformed identity after repair is damage; it
cannot serve as `no_live_attempt` or as proof that the application never ran.

### Heartbeats and manager logs

A manager creates `managers/<manager-id>/` exclusively when it attaches, and
updates `heartbeat.json` (`{"manager_id", "updated_at"}`) in it by atomic
replacement. `manager.json` (`httk-workflow-manager` version 2) records the
manager's identity and what it serves: `writer_id`, `hostname`, `pid`, `uid`,
its pools, capabilities, placement prefixes, executors and runner search
paths, its `resources` capacity, `end_time`, the epoch second its allocation
ends, and `drain_start`, the epoch second it stops claiming and drains (both
`null` when unknown; absent from older managers).

The directory holds `manager.json`, `heartbeat.json` and, for a manager that
started [confined launches](#confined-launches), `launches/` with one trusted
launch directory per launch. A manager exiting cleanly removes it once
`launches/` is empty; a directory that still holds a launch record is kept,
because the record is the takeover evidence of a launch that may still run.
After a crash the directory awaits policy-gated `manager_directories`
collection.

Manager diagnostics go to the manager's own log,
`logs/managers/<manager-id>.log`, with the manager id on every record. Each
log has a single writer, which rotates it itself by renaming it to a single
backup, `<manager-id>.log.1`, and reopening it. Readers merge the logs of
several managers by timestamp.

A heartbeat is a statement about the manager, not about its scheduling pass. A
manager MUST NOT let one pass over the state tree hold its heartbeat; it takes
heartbeat opportunities *between* the state kinds it scans and *within* long
scans of one kind. A manager whose workspace is too large to serve within one
lease SHOULD also bound the markers of a kind it processes per pass and resume
the rest next pass, in a stable order so no marker starves. An implementation
SHOULD report a pass that consumes a large fraction of its own lease, because
then a healthy manager starts to look abandoned.

### Manager liveness evidence

Protocol work a manager owns (a commit, a claimed operator request, a transfer
or adoption step) is taken over by another actor only when its owner is
*evidently gone*. That is one rule, checked in this order:

| Evidence | Meaning |
| --- | --- |
| `manager_record_absent` | The work names no manager, `managers/<manager-id>/` does not exist, or its `heartbeat.json` cannot be read (for work whose start time is name-encoded, an unreadable heartbeat is instead aged from that time). |
| `lease_grace_expired` | The heartbeat is at least the lease times the takeover grace factor old (`2.0` by default). |
| `manager_process_dead` | `manager.json` names this host and its `pid` is not alive. |

Beginning or taking over a commit (see [Commit ownership](#commit-ownership))
also accepts `manager_closed`: a manager that closes while attempts it started
still run keeps its record and writes `closed_at` into its `manager.json`, and
it never commits their outcomes. Nothing else accepts it: an attempt takeover
(`lease_lost`) must not, since the closed manager's attempt may still run.
Launch end evidence is required either way.

Evidence decides only *when* work is taken over, never whether the result is
correct: every takeover is itself a rename that fences the previous owner. Two
hosts with one hostname are an accepted limit, and a reused pid only delays a
takeover. A CLI process owner (see [Transfer artifacts](#transfer-artifacts))
is gone 24 hours after it started the work, or at once when it ran on this
host and boot and its process is not alive.

### Recovering abandoned attempts

A recoverer uses the state frame, heartbeat, batch scheduler when available,
and a configured grace period. It MUST NOT steal a job merely because one
delayed metadata read looks stale.

Once policy decides that an attempt is abandoned, a recoverer appends a new
claimed frame and renames the exact old claimed or running marker. That rename
fences the old attempt: if its process wakes later, no manager accepts its
outcome, because the state tree names a different attempt. A manager taking
immediate ownership transitions directly to a new claimed state; one merely
releasing work transitions back to ready. Both keep the activation ID for a
retry.

Fencing cannot stop a partitioned old process from producing external side
effects or modifying a persistent workdir, and a duplicated attempt still costs
a second allocation. Lease expiry alone is therefore never sufficient evidence
to relaunch an attempt, in either workdir mode:

- For a persistent workdir, the default safe policy MUST establish that the
  previous writer can no longer modify the workdir (for example by
  scheduler-confirmed allocation expiry, or cancellation and process-group
  termination) before launching a replacement. A site MAY enable lease-only
  persistent takeover as an explicit unsafe policy.
- For an isolated workdir a replacement corrupts nothing, but a manager MUST
  still have one of: the recorded process is provably gone on this host; a
  scheduler confirmation that the allocation ended; or a heartbeat silent for a
  configured multiple of the lease, the *takeover grace*, by default twice the
  lease.

The recorded process is *provably gone* (evidence `writer_process_dead`) only
when it is gone on this host and every launch recorded for the attempt has
[launch end evidence](#launch-end-evidence): ranks of a confined launch write
the job directory from their own process groups, possibly on other hosts.

A manager SHOULD terminate the old process group or cancel its batch allocation
before relaunching when it can. The grace exists because an expired lease only
says a manager is slow: a manager whose scan of a very large workspace overruns
one lease is alive with running attempts, and taking those over at the first
expired lease doubles the cost.

Every takeover MUST record its evidence in the new attempt frame (the admitting
rule and the observed heartbeat age), and every use of an explicitly unsafe
policy MUST be recorded there too.

## Attempt control, workdirs, and runner contract

Attempt control is separate from the application workdir:

```text
<workspace>/jobs/<placement>/<job-key>/
├── attempts/<attempt-id>/
│   ├── outcome.tmp.<nonce>/     # runner: an outcome being composed
│   ├── outcome.ready/           # runner: the published outcome
│   ├── commit.<generation>/     # manager: the outcome renamed by its commit owner
│   ├── nodefile                 # manager: with a node binding (advice only)
│   ├── binding.json             # manager: with a node binding (advice only)
│   ├── launch/                  # confined launches of this attempt
│   ├── error.json               # runner SDK: crash breadcrumb (diagnostic)
│   └── commit-wedge.json        # manager: a repeating commit error (diagnostic)
├── logs/
│   ├── stdio.out               # append-only stdout/stderr chronicle
│   └── runlog.jsonl             # append-only structured run log
├── .httk-job/                   # optional runner-private job state
│   └── declarations/            # optional observed workflow declarations
│       └── <declaration-name>.json
└── run/                         # persistent mode
```

In isolated mode the application directory is `run.<attempt-id>/` instead of
`run/`. The runner's working directory is the application workdir, never the
attempt-control directory. A late process from an old attempt can therefore
publish only beneath its own attempt-control name and cannot replace or
impersonate a newer attempt's outcome, which matters most in persistent mode.

Attempt-control directories are transient metadata. Persistent workdirs are
application data and MUST NOT be garbage-collected merely because an attempt
ended; isolated workdirs may be collected under their retention policy. No
state transition depends on cleanup.

The manager creates the attempt-control directory exclusively (an existing one
was not made for this attempt and refuses the launch); everything in it is
job-writable. The manager reads back only the published outcome, by the rules
of [Publishing an outcome](#publishing-an-outcome) and
[Commit ownership](#commit-ownership), and the launch requests of
[Confined launches](#confined-launches). `nodefile` and `binding.json` are
written for the runner and never read back. `error.json`
(`httk-workflow-runner-error`) is the breadcrumb a runner SDK leaves when a
step handler raises, and `commit-wedge.json` (`httk-workflow-commit-wedge`
version 2: `error`, `manager_id`, `recorded_at`, and `launch_end_pending`, the
`record`, `rule` and `host` of the launch a waiting takeover is blocked on)
records a commit error, a waiting commit takeover, or a waiting commit of
another manager's published outcome, that keeps repeating. The manager that
begins, takes over or completes the commit removes the file, whichever manager
recorded it. Both are diagnostic operator evidence that nothing reads
as protocol state.

### Run chronicle

`logs/stdio.out` is one append-only chronicle for all attempts. The manager's
marker lines are, verbatim:

```text
=== httk attempt <attempt-id> step <step> ordinal <attempt_ordinal> started <utc-iso>
=== httk attempt <attempt-id> ended <utc-iso> exit <returncode> outcome <action|none>
=== httk attempt <attempt-id> ended <utc-iso> launch-failed <reason>
```

Each marker is one `os.write` whose bytes begin and end with `\n`, so a
preceding empty line is normal. To cut an attempt's block, treat any line
starting with `=== httk attempt` as a marker. A late fenced process may append
after a newer attempt's start marker, so the file is evidence, not an ordering
authority. An attempt whose manager was lost may have a start marker without an
end marker; a takeover writes its own start/end pair.

### Runner job state

`.httk-job/` is runner-private state belonging to the job rather than one
attempt: it survives retries, step advances, and isolated workdirs, and travels
with the payload on transfer. A runner MAY store what it needs there,
atomically. `.httk-job/`, every `attempts/<attempt-id>/`, and `logs/` are
excluded from every payload digest (submission, child registration, and
detached transfer), so publishing outcomes and writing job state never disturb
an immutability check.

Three names inside `.httk-job/` are reserved:

- `declarations/<declaration-name>.json` holds the *observed* declaration of
  that name, the runtime-refined counterpart of what `job.json` declared, for
  example from a campaign that discovers its outputs while running. The last
  write wins. It is carried verbatim and opaque, as in `job.json`, and the
  basename before `.json` MUST satisfy the declaration-name syntax. An
  implementation MUST report the declared and observed documents side by side
  and MUST NOT merge them; an unreadable document is reported as absent, with
  the reading tool's damage flag set.
- `tree/` holds job-tree metadata owned by the manager and operator tools. A
  runner MUST NOT write there.
- `seal.json` is the job seal of {doc}`sealing`, written by the manager or an
  operator tool. A runner MUST NOT write it.

`tree/spawns/<attempt-id>.json` records the children one committed outcome
spawned, as `{"format": "httk-workflow-spawns", "format_version": 1,
"children": [{"job_id", "job_key", "label", "placement", "spawn_id"}]}` copied
from that outcome's `spawn.json`. While the parent is `committing`, the manager
publishes it (temporary file, atomic rename, and in the storage-durable profile
the file and every directory it created synchronized) **before** moving the
first child into place, so no registered child is missing from its parent's
records. A replayed commit MUST find an existing fragment byte-identical and
otherwise stop with a corruption error. A reader takes the union of all
fragments, de-duplicated by `job_id`, and trusts an entry only when the child
is live at the recorded placement and its own `job.json` names this parent's
`job_id` and, if the entry records one (a hand-written `spawn.json` may omit
it), the entry's `spawn_id`.

`tree/detached.json` marks a child an operator made independent of its parent
(`{"format": "httk-workflow-detached", "format_version": 1, "detached_at",
"operator"}`). It is permanent, requires no state transition, and leaves the
`parent` member of `job.json` in place as provenance.

### Persistent workdir

The declared workdir is reused across step advances and retries. The manager
does not clean, copy, snapshot, or transactionally inspect its files. A VASP
workflow can, for example, keep a very large `WAVECAR` in `run/` across several
steps and decide itself whether an interrupted calculation can continue;
`data/` need not exist.

On retry, the manager fences and tries to terminate the old process, then
invokes the same activation in the same directory with a new attempt context.
The workflow code examines the reason and existing files and may:

- continue directly;
- remove known partial outputs and restart;
- repair inputs and request another retry;
- declare the job failed.

A planned advance to another step also reuses the directory but is not a
restart: the new activation has attempt ordinal 1 and `is_restart: false`.

Persistent mode gives up workdir isolation: an old process not proven
incapable of writing may still modify `run/` after being fenced. The safe
default is to wait, force scheduler cancellation, or pause for manual action;
only an explicitly configured unsafe takeover policy accepts concurrent-writer
risk (see [Recovering abandoned attempts](#recovering-abandoned-attempts)).

### Isolated workdir

Every attempt gets a new `run.<attempt-id>/`, which the manager may populate
from submitted files or transactional `data/` by copying, reflinking, or a
filesystem snapshot. Writes through an isolated workdir MUST NOT mutate
committed `data/` before a transaction.

Old isolated workdirs may be kept for diagnosis or collected later. A manual
continuation can import selected files into a new isolated workdir, with the
choice recorded in history.

### Context and restart detection

The manager MUST set:

```text
HTTK_WORKFLOW_CONTEXT=<compact JSON attempt-context document>
HTTK_WORKFLOW_CONTROL_DIR=<absolute attempt-control path>
HTTK_WORKFLOW_WORKSPACE_DIR=<absolute workspace path>
HTTK_WORKFLOW_JOB_DIR=<absolute current payload path>
HTTK_WORKFLOW_WORKDIR=<absolute selected workdir path>
HTTK_WORKFLOW_IS_RESTART=0|1
HTTK_WORKFLOW_UNCLEAN_RESTART=0|1
HTTK_WORKFLOW_DURABLE=0|1
HTTK_WORKFLOW_ATTEMPT_REASON=<reason>
HTTK_WORKFLOW_STEP=<current step>
HTTK_WORKFLOW_PYTHON=<manager Python interpreter>
HTTK_WORKFLOW_BASH_API=<absolute packaged workflow Bash library>
HTTK_WORKFLOW_RUNNER_ROOT=<absolute shared runner file or tree root>
```

`HTTK_WORKFLOW_DATA_DIR` is set only for transactional-data jobs.
`HTTK_WORKFLOW_DEADLINE` is set exactly when the context has a `deadline`
member and carries it. `HTTK_WORKFLOW_NODELIST` (the comma-separated hosts),
`HTTK_WORKFLOW_NODEFILE` (the nodefile path) and, when the binding has a
`launch` prefix, `HTTK_WORKFLOW_LAUNCH` (that prefix, shell-quoted) are set
exactly when the context has a `binding` member. In an attempt confined with
`manager.confine=bwrap`, `HTTK_WORKFLOW_LAUNCH` is instead the shell-quoted
launch client that asks the manager to start the launch,
`HTTK_WORKFLOW_LAUNCH_LOCKS` names the attempt's launch-lock directory, and
`HTTK_WORKFLOW_CONFINED` is `1` (see [Confined launches](#confined-launches));
the binding's `launch` member keeps the rendered prefix as information only,
since it cannot start ranks from inside the sandbox. When the binding is one node
on the manager's own host with GPUs of known ids, the variable those ids came
from (`CUDA_VISIBLE_DEVICES`, `ROCR_VISIBLE_DEVICES` or `ZE_AFFINITY_MASK`) is
set to exactly them, comma-separated. A local attempt that requests no GPUs on
a node with known GPUs sees none (`CUDA_VISIBLE_DEVICES`/`ROCR_VISIBLE_DEVICES`
set empty; `ZE_AFFINITY_MASK` is inherited, since an empty mask hides
nothing). Otherwise the variable is inherited unchanged.
`HTTK_WORKFLOW_RUNNER_ROOT` names a shared runner's file or tree root. The JSON
document is the source of truth; the scalar variables are language-neutral
conveniences.

The attempt-context document exists only as the value of
`HTTK_WORKFLOW_CONTEXT`; no context file is created. Its canonical compact
UTF-8 encoding must be shorter than 100,000 bytes, and a larger context is a
`protocol_error` at launch, so application settings belong in the bounded
settings snapshot, not bulk files or values. The version-2 document includes
`payload`, the absolute payload path, so SDKs can find job-level logs
independently of the workdir.

Workspace settings are non-secret configuration: they are snapshotted into the
attempt context and exported into the runner environment, so credentials MUST
NOT be stored there. Remote credentials already live elsewhere.

`HTTK_WORKFLOW_DURABLE` and the context's `durable` member carry the workspace
durability mode. A runner that publishes an outcome, transaction, or child
bundle synchronizes it before the authoritative rename exactly when this is
`1`. A runner that does not compose outcomes on storage itself may ignore it;
the packaged Python and Bash SDKs honour it automatically. A context written
before the member existed reads as `0`.

A manager MAY export further variables belonging to the runner libraries it
ships. These are SDK conveniences: a manager exporting none is still
conforming, and a runner needing one MUST treat its absence as a missing
dependency of that library, not a protocol violation. This implementation
exports:

```text
HTTK_WORKFLOW_<CODE>_BASH_API=<absolute Bash API of each installed code, e.g. HTTK_WORKFLOW_VASP_BASH_API>
HTTK_WORKFLOW_PERL_API=<absolute packaged Perl SDK directory>
HTTK_WORKFLOW_LANGUAGES_DIR=<absolute installed httk/workflow/languages directory>
HTTK_WORKFLOW_RUNNER_ARTIFACTS=<absolute registered build-artifacts directory>
```

`HTTK_WORKFLOW_RUNNER_ARTIFACTS` is set only when a workspace package has a
registered build; a compiled package's `run` entry must find its binaries there.

An unclean persistent retry context is:

```json
{
  "format": "httk-workflow-attempt-context",
  "format_version": 2,
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "placement": "project-17/0/03a",
  "payload": "/srv/httk/jobs/project-17/0/03a/job-1",
  "step": "relax",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "attempt_id": "a6c2c973-29e1-44e2-9649-ae419e340ac4",
  "attempt_ordinal": 2,
  "is_restart": true,
  "is_unclean_restart": true,
  "attempt_reason": "lease_lost",
  "previous_attempt_id": "31fa431e-e01c-49aa-8fa8-e8af23b73c52",
  "activation_reason": "advance",
  "workdir_mode": "persistent",
  "workdir_reused": true,
  "data_generation": null,
  "durable": true,
  "resources": {},
  "join": null,
  "children": []
}
```

`attempt_ordinal > 1` and `is_restart` mean the same activation is being
retried. `is_unclean_restart` means the preceding attempt did not publish and
complete a valid outcome; reasons include `lease_lost`, `timeout`, and
`process_failure`. `requested_retry` and an orderly `manual_continue` may have
`is_unclean_restart: false`. A step thus never infers restart from leftover
filenames: it reads `HTTK_WORKFLOW_CONTEXT` (or the scalar variables) and then
treats existing files according to application policy.

The context's `resources` member is the validated effective requirement
selected for the launched activation, including the resolved `maxtime` and
`mintime` in seconds, so a runner can use it as the manager's placement
decision without re-resolving the job declaration.

The context's `deadline` member is the integer epoch second by which the
attempt must finish: the earlier of launch time plus the effective `maxtime`
and the launching manager's drain point (its allocation end minus its deadline
margin). It is present exactly when either exists. The manager stops the
attempt at or shortly after it, never before. A runner that wants to checkpoint or
publish a `retry` before it is stopped watches this rather than recomputing it.
A context written before the member existed has no time limit.

The context's `binding` member is present exactly when the launching manager
places attempts on a node inventory (see {doc}`taskmanager`). It is an object
with `nodes`, one entry per node the attempt was given (`host`, `procs`, `gpus`
and, when the inventory places memory, `mem` in MB); `nodefile`, the absolute
path of a file in the attempt control directory holding one host line per
processor slot (one line for a node given no processor slot); `file`, the
absolute path of `binding.json` in the same directory; and, when a launch
prefix applies, `launch`, the argument vector to put before a parallel command
(such as `env SLURM_HOSTFILE=... srun --ntasks=8 ...`). `binding.json` holds the same object without
`file`, with each node's `gpu_ids` and `cpus` (the cpulists of its processor
slots) added when known; they are kept out of the context so that it stays
within its size limit. The built-in Slurm prefix starts with
`env SLURM_HOSTFILE=<nodefile>`, so the hostfile applies to its own `srun`
only; the runner environment does not carry it. The binding is advice to a
well-behaved runner; the manager does not confine the attempt to it, except
that a locally executed attempt sees only its GPUs and, with the
`manager.bind_cpus` setting, is pinned to its CPUs. Under confinement, launches
are rendered from the placement in the manager's memory and a manager-owned
nodefile; the nodefile and `binding.json` in the attempt control directory are
never read back by the manager.

### Confined launches

An attempt confined with `manager.confine=bwrap` cannot reach the scheduler,
so its parallel launches are started by its manager through a file protocol.
Four roles take part:

| Role | Runs | Writes |
| --- | --- | --- |
| launch client (`HTTK_WORKFLOW_LAUNCH`, `python -m httk.workflow._launch_client`) | inside the attempt sandbox | its request, stop marker and lock |
| manager | trusted, unconfined | the launch outputs, the status and the trusted launch directory |
| rank helper (`python -I -m httk.workflow._confine_rank --launch-dir T`) | trusted, on every rank, started by the rendered launch template | the per-launch shared-memory directory |
| inner exec (`python -I -m httk.workflow._confine_exec`) | first program inside each rank sandbox | nothing; it execs the requested command |

The files of one launch, with `ID` its request identifier (32 lowercase
hexadecimal digits the client draws at random):

```text
<payload>/attempts/<attempt-id>/launch/          job-writable, created by the client
├── .ID.request.tmp                              client: the request being written
├── ID.request.json                              client: the published request
├── ID.stop                                      client: asks the manager to stop the launch
├── ID.stdout, ID.stderr                         manager: the launch's output, created exclusively
└── ID.status.json                               manager: the final status, replaced atomically
<confine.shm_root>/httk-launch-<attempt-id>/     node-local tmpfs, manager-created, mode 0700
└── ID.lock                                      client: held with flock(LOCK_EX) while it lives
.httk-workspace/managers/<manager-id>/launches/<attempt-id>.ID/
├── nodefile                                     manager: rendered from the placement in memory
├── launch.json                                  manager: the trusted launch description
└── process.json                                 manager: the started launch's process record
<confine.shm_root>/httk-<token>/                 per-launch shared memory, joined by the ranks
└── .lock                                        ranks: held with flock(LOCK_SH)
```

**Documents.** Three documents share version 1 and one encoding rule: UTF-8
without a byte-order mark, no duplicate keys, no non-finite numbers, exactly
the listed members, and canonical JSON (sorted keys, no insignificant
whitespace, non-ASCII escaped). A reader MUST refuse a document that is not
byte for byte the canonical encoding of the value it decodes to, and reads each
file below a no-follow directory descriptor, refusing a symlink or special
file, within the size bound.

| Document | `format` | Members | Bound |
| --- | --- | --- | --- |
| request | `httk-workflow-launch-request` | `request_id`, `attempt_id` (a canonical UUID), `argv` (1 to 1024 entries of at most 64 KiB, the first nonempty), `cwd` (`.` or a canonical relative path without `..`, relative to the job directory, at most 4096 bytes), `environment` (sorted `[name, value]` pairs: at most 2048, names `[A-Za-z_][A-Za-z0-9_]{0,255}`, values at most 128 KiB) | 1 MiB |
| status | `httk-workflow-launch-status` | `request_id`, `state`, `exit_code`, `error` (at most 4096 bytes or `null`) | 16 KiB |
| trusted launch | `httk-workflow-launch` | `request_id`, `attempt_id`, `workspace_id`, `workspace_root`, `placement`, `job_key`, `request` (`attempts/<attempt-id>/launch/ID.request.json`), `confine` (`bwrap`, `block_userns`, `readonly_paths`, `devices`, `pmix_roots`, `shm_root`, `environment`, `block_mpi_spawn`), `python`, `token` (32 lowercase hexadecimal digits the manager draws) | 1 MiB |

A request MUST NOT carry an environment name starting with `PMI_`, `PMIX_`,
`OMPI_`, `OPAL_`, `SLURM_`, `SLURMD_`, `SRUN_`, `SBATCH_`, `SALLOC_` or
`HTTK_`; the client drops those, and names that are not portable identifiers,
from its own environment. A status `state` is `refused` (never started, no
`exit_code`), `exited` (the launch exited by itself; `exit_code` 0 to 255,
`128+N` for signal `N`), `stopped` (the manager stopped it; `exit_code`
optional) or `uncertain` (not confirmed reaped, no `exit_code`).

**Client.** The client checks that `HTTK_WORKFLOW_CONTROL_DIR` is
`attempts/<attempt_id>` of `HTTK_WORKFLOW_JOB_DIR` for the context's
`attempt_id` and that its working directory lies inside the job directory. It
creates `ID.lock` exclusively in `HTTK_WORKFLOW_LAUNCH_LOCKS` and holds an
exclusive `flock` on it for its whole life: the lock is a death notification
on node-local tmpfs, never mutual exclusion and never on the workspace
filesystem. It then publishes `ID.request.json` by writing
`.ID.request.tmp` exclusively and renaming it, copies `ID.stdout` and
`ID.stderr` to its own output until `ID.status.json` exists and both are
drained, and exits with the launch's status (`2` for a `refused`,
`uncertain` or unreadable status, or when `launch/` is removed first; `143`
for a `stopped` launch without an exit code). `SIGTERM`, `SIGINT` and `SIGHUP` do not end it: it creates
`ID.stop` and keeps waiting for the `stopped` status.

**Admission.** The manager scans each local confined attempt's `launch/` at
most once a second, visits at most 4096 entries and decides at most 32
requests per scan, ignores dot names, and takes the requests without a status
in arrival order (modification time). It answers `refused` unless the attempt
is live (not fenced, cancelling, timed out or drained, still owning its
`running` marker, its process not exited or reaped, no outcome published, and
its admission not closed by an uncertain launch), the request decodes, names
its own file's `ID` and this attempt, has no stop marker, and its client lock
exists and is still held. Only one launch per attempt runs at a time; the
other requests wait. The manager never interprets the requested command.

**Start.** The manager creates `ID.stdout` and `ID.stderr` exclusively (an
existing one refuses the request), creates the trusted launch directory
exclusively with mode 0700, writes `nodefile` and `launch.json`, and starts
the launch template rendered from the placement it holds in memory, around
the rank helper, in a new session behind a gate that reads one line from a
pipe before it execs. It then writes `process.json` (`pid`, which is also the
process group, `hostname`, `attempt_id`, `started_at`) through a temporary,
`fsync`, rename and directory `fsync`, and then checks that the attempt
still owns its `running` marker: the exact marker path it started from, or,
after a same-kind transition renamed it, the job's current `running` marker
naming the attempt. Only then does it release the gate. If the record cannot
be written, or the marker has moved on (the manager may have been frozen while
a successor took the attempt over or it was cancelled), the gate is closed
unopened, the launch exits without starting anything and is stopped.
Accepted residual: a manager frozen exactly between that check and the gate
write releases ranks of a fenced attempt when it wakes; it then stops them at
its next pass, because the attempt is no longer live. A trusted launch directory with
only `launch.json` therefore never hides started ranks. `process.json` also
names the allocation the ranks run in (`allocation`, see
[Launch end evidence](#launch-end-evidence)), so that another manager can
later establish that the launch has ended.

**Ranks.** Jobs can read but not write `.httk-workspace/`, so `launch.json` is
trusted input; the rank helper still requires `--launch-dir` to have the shape
above and to match the description's attempt and request, opens it without
following symlinks, and requires the described workspace root to be the same
directory. It refuses an invalid placement or job key, and a job directory
that is the workspace root or lies below `.httk-workspace`, `exchange`, `logs`
or `postprocess`. It joins `<confine.shm_root>/httk-<token>/` (mode 0700,
owner checked; created by the first rank of a node, removed by the last) and
runs the inner exec in a rank sandbox that binds the workspace and
`confine.readonly_paths` read-only, and read-write the job directory, the
shared-memory directory (at `/dev/shm`), the step's PMIx directory (only when
it lies below `confine.pmix_roots`) and the approved `confine.devices`. The inner exec reads the request inside
the sandbox, changes to `cwd` without following symlinks below the job
directory, adds the request's environment, and execs `argv`. The request is
job-writable throughout: what a rank executes is the job's own choice and
runs confined, so a request changed after admission gains nothing.
`confine.shm_root` MUST be a tmpfs directory owned by root or the manager's
user, and world-writable only as a sticky root-owned one; the manager and every
rank helper check it.

**Supervision and stop.** A launch whose process group is gone after its
leader exited gets `exited`. A stop marker, a client whose lock can be taken
(it died, even by `SIGKILL`), a launch directory that vanished, or an attempt
that stops being live stops it: `SIGTERM` to its process group, `SIGKILL`
after the cancellation grace, `stopped` once the group is gone, and
`uncertain`, with admission closed for the attempt, if it is still not gone a
further grace later. Every status is written by an exclusive temporary and a
rename; a started launch's final status is retried every tick until it is
written. The manager that runs the attempt does not treat it as finished,
and does not commit, seal or eject it, while any launch it tracks for the
attempt is unreaped. Every other manager needs
[launch end evidence](#launch-end-evidence) for every launch recorded for the
attempt in any manager's `launches/` before it takes the commit over, calls
the attempt's writer provably dead, or verifies a cancellation
(`process_group_absent`).

**Cleanup.** Once a launch is reaped and its status written, the manager
removes its trusted launch directory, so the status file alone marks the
request as done. The manager removes the attempt's launch-lock directory when
it stops tracking the attempt; one left by a crashed manager stays on its node
until reboot. `launch/` goes with the attempt-control directory. A manager
deciding the [launch end evidence](#launch-end-evidence) of an attempt also
deletes the process records it found ended of other managers silent for their
lease times its takeover grace factor. Garbage collection removes the trusted
launch directories of a manager silent for the workspace policy's
`lease_seconds` times the default takeover grace factor (`2.0`) that hold no
process record, a malformed one, or one whose process group is provably gone
on this host, but never asks a scheduler: a record naming a queryable
allocation (other than the collector's own) is kept until its `end_time` plus
`LAUNCH_END_GRACE` has passed, and is otherwise left to the evidence ladder,
which asks and prunes it; an I/O error proves nothing (see
[Retention gates and always-safe collection](#retention-gates-and-always-safe-collection)).
The sandbox details and site requirements are in {doc}`workspace_daemon`.

### Launch end evidence

Ranks of a confined launch write the job directory from process groups of
their own, possibly on hosts other than the manager's. No path, rename or
inode can fence a descriptor a rank already holds open on a shared
filesystem, so a manager that did not start a launch MUST have evidence that
it has ended before it takes over the attempt's commit, calls the attempt's
writer provably dead (`writer_process_dead`, see
[Recovering abandoned attempts](#recovering-abandoned-attempts)), or verifies
its cancellation (`process_group_absent`), and before it begins the commit
of an outcome another manager's attempt published (see
[Commit ownership](#commit-ownership)). The manager that started a launch
tracks it itself (see [Confined launches](#confined-launches)). Only the
managers of the evaluating manager's own uid are searched: only they start
attempts of the jobs it serves, and another user's `launches/` is not
readable.

**The record.** Besides `pid`, `hostname`, `attempt_id` and `started_at`,
`process.json` carries `process_start` and `boot_id` where `/proc` provides
them: the launch leader's start time in clock ticks after boot
(`/proc/<pid>/stat` field 22) and `/proc/sys/kernel/random/boot_id`. On this
host, a leader with another start time under the recorded pid, or another
boot id, means the recorded process group is gone; the kernel does not reuse
a pid while a process group of that id exists. A record without them is
judged by its process group alone. `process.json` also carries `allocation`:
`null` for a manager without an allocation, otherwise an object with these
members.

| Member | Meaning |
| --- | --- |
| `probe` | The recording manager's `--allocation` specification, such as `slurm` or `exec:PATH` (`auto` is recorded as the scheduler it found), at most 4096 bytes |
| `kind` | The allocation kind, a label |
| `identity` | The scheduler's identity of the allocation, or `null`: an object of at most 16 entries whose keys are labels and whose values are strings of at most 256 UTF-8 bytes. Slurm records `job_id` (`SLURM_JOB_ID`) and, when set, `cluster` (`SLURM_CLUSTER_NAME`); an `exec:PATH` probe's envelope may carry one |
| `end_time` | The epoch second the allocation ends, or `null` |

A record without `allocation` (written before the member existed) proves only
what its host proves. A reader ignores unknown members and drops an
`identity` or `end_time` it cannot use; an `allocation` without a usable
`probe` and `kind` counts as `null`. Either only removes evidence.

**The ladder.** For each record of the attempt in every manager's
`launches/`, the first rule that applies decides:

| Rule | When | Ended |
| --- | --- | --- |
| `not_started` | Only `launch.json` survived, and the manager that wrote it is gone by [manager liveness evidence](#manager-liveness-evidence) (judged with this manager's lease): the gate let no rank run before `process.json` was durable, and only that manager writes it. | yes |
| `launch_starting` | Only `launch.json` survived, and the manager that wrote it may be live: it may be between the start and the process record. | no |
| `malformed_record` | The record is malformed (it belongs to the attempt its name starts with), and the manager that wrote it is gone; as for garbage collection, it describes no live launch. | yes |
| `malformed_record_live` | The record is malformed, and the manager that wrote it may be live: it may be writing it. | no |
| `launch_running_here` | The record names this host and its process group is alive. When the manager that wrote it is gone, the evaluating manager sends the group `SIGTERM`, and `SIGKILL` once its cancellation grace has passed since the first signal; a live manager stops its own launches, and no other manager signals them. | no |
| `process_group_gone` | The record names this host and its process group is gone, and the launch was not a step of a queryable allocation (a maintained scheduler's, or an `exec:PATH` probe's with an `identity`) other than the evaluating manager's own (such a step falls through to the rules below). | yes |
| `scheduler_confirmed_ended` | The recorded allocation can be asked here (it has an `identity` and names an `exec:PATH` probe that exists on this host, or a maintained scheduler whose client is installed here), and its scheduler or probe confirms that it ended (below). A manager asks about one allocation at most once a minute and reuses the answer meanwhile. | yes |
| `allocation_active` | That scheduler or probe says the allocation is still active, even when its recorded `end_time` has passed: an administrator may have extended the time limit. | no |
| `allocation_end_passed` | The allocation cannot be asked here and its recorded `end_time` lies more than `LAUNCH_END_GRACE` (300 seconds, Slurm's `KillWait` and clock skew) in the past; or it can be asked, its scheduler cannot tell now, and the end lies more than `LAUNCH_END_GRACE` plus `SCHEDULER_UNAVAILABLE_SECONDS` (one hour) in the past. The rule is stateless, so every manager decides alike. | yes |
| `scheduler_unavailable` | The allocation can be asked, its scheduler cannot tell now, and its end lies less than that hour beyond the grace: one failing query is no evidence. | no |
| `launch_end_unprovable` | Nothing above applies. | no |

Managers sharing one multi-node allocation cannot commit a dead sibling's
confined outcome before that allocation ends: Slurm answers that the
allocation is active, as it is. This is conservative and intended; it is the
same allocation whose ranks may still run.

`srun` places its ranks under Slurm's step daemons, outside its own process
group, so a gone `srun` group does not prove a step's ranks gone; the same
holds for a site launcher such as `mpiexec` under PBS. For a step of another
queryable allocation the ladder therefore asks its scheduler or probe. For a
step of the evaluating manager's own allocation, or a launch without a
queryable allocation, the group decides: an accepted residual, since remote tasks of such
a step may linger until Slurm has cleaned the step up, and requiring Slurm's
word would hold a cancellation until the manager's own allocation ends.

Records that cannot be read, or more than 65536 entries, are
`launch_records_unreadable`, which is not ended either. Every decision is
recorded: a commit takeover's frame, or the committing frame of an outcome
another manager's attempt published, lists one object per ended record
(`record`, `<manager-id>/<attempt-id>.<request-id>` below `managers/`;
`rule`; `host` when known) as `launch_end_evidence`, and a verified
cancellation lists the same in its `cancellation` member.

**Asking a scheduler.** A maintained scheduler implements the private
`Scheduler.allocation_ended(identity, run=..., timeout=...)` hook. It returns
`True` only when the scheduler confirms that the allocation ended, which means
that every process it started has ended; `False` while the allocation is
active; and `None` when it cannot tell. Slurm runs, without a shell and
without the `SQUEUE_*` and `SLURM_CLUSTERS` variables (they could filter the
job out of the answer),
`squeue --noheader --jobs=<job_id> [--clusters=<cluster>] --states=all --format=%i|%T`;
`--clusters`, which needs `slurmdbd`, is passed only when the recorded
cluster is not the manager's own `SLURM_CLUSTER_NAME`; a manager outside any
Slurm allocation has none and always passes it, so without `slurmdbd` every
answer it gets cannot tell, and the stateless end-time rule above decides. No
row for the job, or rows only in `COMPLETED`, `CANCELLED`, `FAILED`,
`TIMEOUT`, `NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL`, `DEADLINE` or
`OUT_OF_MEMORY` including the job's own, or a failure reporting `Invalid job
id specified`, is ended; any other state, `COMPLETING` included, is active;
anything else, errors and timeouts included, cannot tell. `%i` prints a job
array element as `<base>_<index>`, not as its `SLURM_JOB_ID`, so a finished
element cannot tell until Slurm forgets it (`MinJobAge`, 300 seconds by
default) and is then ended.

An `exec:PATH` probe, recorded as an absolute path, is asked only for an
allocation with an identity, by running `PATH ended`, without a shell, with
one query on standard input:

```json
{"format": "httk-workflow-allocation-query", "format_version": 1,
 "kind": "pbs", "identity": {"job_id": "4711.pbs01"}, "end_time": 1790000000}
```

It answers with exactly this document on standard output, `ended` a boolean:

```json
{"format": "httk-workflow-allocation-status", "format_version": 1, "ended": true}
```

A non-zero exit, a timeout, or any other output, such as the allocation
envelope an older probe prints whatever its arguments, cannot tell. A `host`
or `none` allocation, or any allocation without an identity, cannot be asked.

**Requirement.** Any launch technology that places ranks on hosts other than
the manager's MUST provide an allocation probe whose `ended` query can
confirm the end of its allocations, or record an allocation end time. A
launch that has neither is never proven ended by another manager: a commit
takeover then waits, reported in `commit-wedge.json` and by `job why`, until
an operator publishes a `launches_ended` request (see
[Request contents and actions](#request-contents-and-actions)). This is an
accepted limitation. An operator who issues that request takes responsibility
that no rank of the attempt still runs.

### Executable workflow-hook wire formats

Directory-package instantiate and collect hooks that are not `.py` use these
UTF-8 JSON subprocess formats. The package manifest and hook trust rules are in
{doc}`workflow_packages`; this is the wire-format catalogue.

An executable instantiate hook receives one document on stdin, with its working
directory set to the staging payload:

```json
{
  "format": "httk-workflow-instantiate",
  "format_version": 3,
  "workflow": "example.relax",
  "tag": "silicon",
  "parameters": {"cutoff": 520},
  "defaults": {"cutoff": 450, "kpoint_density": 30.0},
  "inputs": {
    "structure": {"kind": "file", "path": "files/inputs/structure/POSCAR"},
    "settings": {"kind": "value", "value": {"kpoints": [4, 4, 4]}}
  }
}
```

Its stdout is one JSON object containing `parameters` and, optionally, `tag`.

An executable collect hook receives JSONL whose first line is exactly

```json
{"format": "httk-workflow-collect-stream", "format_version": 2}
```

and whose later lines are each exactly one request envelope:

```json
{"record": {"workspace_id": "workspace", "job_id": "job-1", "state": "succeeded", "job": {}}}
```

The record value is the complete `JobRecord.as_mapping()` mapping; the example
shows only the envelope. The hook writes one ordered response per record,
either:

```json
{"job_id": "job-1", "outputs": {"energy": {"value": 3.14}}}
```

or:

```json
{"job_id": "job-1", "error": "could not read the result"}
```

Python hook fast paths skip this subprocess boundary but keep the same
successful-path hook and assembly semantics. Failure handling differs:
registered Python collector exceptions abort iteration, while executable
responses can degrade jobs independently. `httk.workflow.hookapi` provides
`instantiate_main()` and `collect_main()` for Python executables implementing
these formats.

## Publishing an outcome

Exit codes are not the workflow protocol. A step communicates through the
attempt-control directory named by `HTTK_WORKFLOW_CONTROL_DIR`:

1. Create `outcome.tmp.<nonce>/`.
2. Write `outcome.json` and any transaction or child bundles inside it.
3. Close all files and, in the storage-durable profile, synchronize the whole
   draft tree (outcome, transaction manifest and staged payload, child bundles)
   in one batch.
4. Atomically rename the directory to `outcome.ready/` and, in the durable
   profile, synchronize the attempt-control directory so the name survives a
   crash.
5. Exit.

The directory rename is the **outcome publication point**; temporary outcomes
are ignored. The fixed destination is nonempty, so a second publication MUST
fail rather than replace the first. A crash before step 4 discards the
unreferenced draft whole, and the batched synchronization in step 3 ensures a
published outcome never names staged data the storage did not receive.

### Outcome document

A minimal outcome is:

```json
{
  "format": "httk-workflow-outcome",
  "format_version": 2,
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "attempt_id": "a6c2c973-29e1-44e2-9649-ae419e340ac4",
  "action": "advance",
  "next_step": "collect",
  "message": "relaxation converged"
}
```

`expected_data_generation` is required only when the outcome contains a
transaction, and MUST equal the generation in the attempt context. An outcome
MAY also contain:

- `priority`, an integer from 0 through 999, sets the priority of the marker
  the committed action produces, so a decision and its scheduling preference
  share one atomic transition. Omission keeps the current priority.
- `runner_steps` lists the step names the publishing runner implements. A
  manager copies a valid array verbatim into the state frame and carries it
  forward, and ignores a malformed one with a log entry. It is evidence for
  operators and tools drawing a job's reachable steps, never an input to a
  manager decision. A runner that declares it SHOULD do so in the job's first
  outcome.
- `resources`, allowed only on `advance` and `wait` (other actions MUST NOT
  contain it), is the next activation's requirement, validated like
  `job.json` resources (time labels in seconds).

### Actions

| Action | Required information | Effect after commit |
| --- | --- | --- |
| `advance` | `next_step` | Create a ready activation for that step. |
| `wait` | `next_step`, `join` | Register children and wait for the join. |
| `succeed` | none | Enter successful terminal state. |
| `fail` | `failure` | Record a declared failure and enter failed. |
| `retry` | `retry.reason` | Retry this activation under policy. |
| `pause` | `pause.reason` | Require an operator request. |

`advance` and `retry` first apply the `job.json` budgets; if the next
activation or attempt would exceed one, the committed effect is
`failed/budget_exhausted`.

After observing a valid outcome, the manager appends a committing frame and
renames `running` to `committing` before modifying durable job data or
registering children. This fences the step and makes interrupted replay visible
in the state tree.

A valid current outcome is authoritative over the process exit status. An
outcome from a fenced attempt is kept only for diagnosis.

### Exit without an outcome

If a process exits without an outcome:

- exit status zero is a `protocol_error`, because success is ambiguous;
- nonzero exit is `process_failure`;
- an attempt the manager stopped for exceeding its `maxtime` is `timeout`;
- loss of manager or allocation is `lease_lost`, including an attempt the
  manager stopped while draining (on a stop signal or at its allocation's drain
  point) unless it had already exceeded its `maxtime`; `lease_lost` is an
  unclean restart.

Retry policy decides whether these create another attempt or
`retry_exhausted`. A declared `fail` is permanent by default; a step wanting a
managed retry uses `retry`.

### Commit ownership

The manager the `running` frame names begins the commit of the outcome its
attempt published. Another manager serving the job's executor begins it only
on [manager liveness evidence](#manager-liveness-evidence) for that manager,
judged with the frame's lease, and on
[launch end evidence](#launch-end-evidence) for every launch recorded for the
attempt (or an operator's `launches_ended` attestation), and records
`previous_manager_id`, `takeover_evidence` and `launch_end_evidence` in the
`committing` frame, as a commit takeover does. A manager that exits with an
attempt it owns keeps its record, so its outcome waits for that evidence
(`manager_process_dead` at once on the same host); one that exits with nothing
owned removes its record, which is `manager_record_absent` evidence at once.
The manager that begins the commit validates the outcome, digests every child
bundle and checks the spawn labels while the marker is still `running`, then
appends the `committing` frame and renames the marker, and removes any
`commit-wedge.json` of the attempt. The frame names the
commit's owner and everything a successor needs: `manager_id`, `writer_id`,
`attempt_id`, `attempt_control`, `outcome_action`, `child_digests` (child job
key to the bundle digest taken at acceptance), `child_labels`, the `process`
identity when one was recorded, and `commit_base_generation`, the generation
of this first `committing` marker.

**The commit draft.** The holder of the `committing` marker at generation `g`
renames the published draft to `attempts/<attempt-id>/commit.<g>` before its
first step and reaches it only by that name, opening it afresh below the
attempt-control directory for every step and never creating it. It finds the
current name by fixed names, never by a listing: `commit.<g>`, then
`commit.<g-1>` down to `commit.<commit_base_generation>` (a previous owner's,
or the name before a same-kind operator transition advanced the generation),
then `outcome.ready`; the rename is checked by inode. A draft name that is
absent stops the step: while the marker is still this owner's, that is a
`protocol_error` of the job; otherwise the commit was taken over and the
owner records nothing. A second `outcome.ready` a lingering attempt process
publishes after the first rename is never read.

**Single owner and takeover.** Only the manager the `committing` frame names
processes the commit. Another manager serving the job's executor takes it
over only on [manager liveness evidence](#manager-liveness-evidence) for that
owner, judged with the frame's lease, and on
[launch end evidence](#launch-end-evidence) for every launch recorded for the
attempt, by a `committing` → `committing` transition whose frame repeats
every non-envelope member of the current one and sets `manager_id`,
`writer_id`, `previous_manager_id`, `takeover_evidence`,
`launch_end_evidence` and `reason: "commit_takeover"`. Without launch end
evidence the takeover waits, and only an operator's `launches_ended` request
replaces it: the takeover then records
`[{"rule": "operator_attested", "request_id": ..., "operator": ..., "operator_key": ...}]`
(`operator_key` only for a signed request) as its launch end evidence. The marker rename decides: of two successors
one wins, and the previous owner's own later transition loses. The new owner
then renames the draft to its own `commit.<g>`, which fences the previous
owner at its next draft access, and retires the previous owners' transaction
trash before touching `data/` (see [Transaction bundle](#transaction-bundle)).
Because a takeover advances the generation, an operator request issued
against the earlier generation is retired as stale.

A manager does not process a commit while a [confined launch](#confined-launches)
it tracks for the attempt it ran is unreaped. A commit that cannot proceed yet
(a predecessor's trash that cannot be retired) is deferred and retried, a
takeover that waits for launch end evidence is retried every pass, and one
that keeps failing or waiting is reported once and recorded in
`commit-wedge.json`; none of them is a job failure. A commit failing on a malformed outcome or draft fails the job with
`protocol_error`. It fails with `transaction_corruption` when its replay fails
midway, and also before any replay step when a nontransactional job published
a transaction or the outcome's or manifest's `expected_data_generation` is
stale.

## Optional transactional contributions to `data/`

### Visibility guarantee

This section applies only to jobs declaring `"data": {"mode": "transactional"}`.
Their steps MUST NOT modify committed `data/` directly; they publish a
replayable transaction in the outcome. A job with `data.mode` `none` has no
`data/` or transaction bundle, and a persistent-workdir job may freely update
application files such as `WAVECAR` in `run/`, which are not protocol metadata.

The guarantee is:

> A later attempt starts only after either none of a transaction or all of it
> has been applied to `data/`.

POSIX cannot atomically rename several unrelated paths, so raw observers inside
`data/` during `committing` may see replay in progress. The atomic boundary is
between attempts: no runner is launched while the marker is `committing`.
Applications that need an atomic tree for external readers MAY contribute one
complete version directory and atomically replace a single `current` name, an
application-level use of the protocol rather than a mandatory per-step
revision directory.

### Transaction bundle

```text
attempts/<attempt-id>/outcome.tmp.<nonce>/
├── outcome.json
└── transaction/
    ├── manifest.json
    ├── payload/
    │   ├── results/energy.json
    │   └── restart/CHGCAR
    └── trash/
```

Example manifest:

```json
{
  "format": "httk-workflow-transaction",
  "format_version": 2,
  "id": "6fc3852b-f1df-4edf-a92c-7f81c3e02465",
  "expected_data_generation": 3,
  "operations": [
    {
      "id": "energy",
      "op": "put-file",
      "source": "payload/results/energy.json",
      "path": "results/energy.json",
      "sha256": "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"
    },
    {
      "id": "old-scratch",
      "op": "remove",
      "path": "scratch/obsolete.dat",
      "missing_ok": true
    }
  ]
}
```

Paths are normalized relative POSIX paths; absolute paths, empty components,
`.`, `..`, NUL bytes, and paths into protocol control data are forbidden.
Operations MUST NOT overlap. Operation IDs are unique normalized protocol
components and determine all replay scratch paths.

Required operation types:

- `make-dir`: create an application directory and any explicitly declared
  missing parents;
- `put-file`: atomically install or replace one regular file;
- `remove`: atomically rename an existing path into transaction trash;
- `put-tree`: install a complete directory tree, normally at a previously
  absent path;
- `replace-tree`: move the old tree to transaction trash, then install the new
  tree.

Put operations declare a content digest. Replacement and removal operations
MAY declare an expected old digest or require absence; these preconditions stop
a replayed or stale transaction from overwriting an unexpected workdir.
Symlinks, devices, sockets, and FIFOs are forbidden by default.

A removal goes to the trash entry `removed`, and a `replace-tree` moves the
old tree to the trash entry `old` before installing its deterministic payload
source. These destinations MUST NOT contain preexisting unrelated data, and no
random name may affect the paths.

A runner replaying a batch into its own workdir uses
`transaction/trash/<operation-id>/`, created idempotently, and keeps what it
moved there.

The manager replaying a published transaction uses a trash directory per owner:
the holder of committing generation `g` replays from its draft
`attempts/<attempt-id>/commit.<g>/` (see [Commit ownership](#commit-ownership))
and uses `transaction/trash/<operation-id>.<g>/` in it, holding the entries
`removed`, `old` and the fresh directories `new<n>` it renames into `data/`.
Operation IDs may contain dots; the
generation is the integer after the last one, and the name is always built,
never parsed from a listing. The manager:

- creates `transaction/trash/` once before its first operation, below a
  non-creating open of `transaction/`;
- for each operation step that moves something aside or creates a directory,
  opens the draft by name (the fence), creates its own directory (an existing
  one is fine), then opens the draft by name again and opens its own directory
  without creating it; every move of the step goes through that descriptor, and
  nothing after the second fence creates `transaction/`, `trash/` or any
  `trash/<operation-id>.*`;
- creates `make-dir` targets and every missing parent of a destination as an
  empty directory inside its own trash directory and renames it into `data/`,
  outermost level first; a level that is then the created directory or an
  existing one counts as created, and only then is the next level's parent
  opened. Nothing is created through the `data/` descriptor;
- deletes what it removed or set aside right after the operation; what it
  cannot delete is left for `transaction_trash` collection.

Before its first data step the manager runs one retirement pass over the
manifest. For each operation in order it decides whether the operation applies
(`remove`: target present; `make-dir`: target absent; `put-file`, `put-tree`,
`replace-tree`: source present or an old tree already set aside), probes
`trash/<operation-id>.<k>` by fixed-name `lstat` for every generation `k` from
the attempt's `commit_base_generation` to `g` (every generation that could
have held this commit, its own included, so its own directory from an earlier
run at the same generation counts as evidence; never a listing), and creates
its own directory when the operation applies or a directory of any generation
exists. Only then does it retire the predecessor directories it found, those of
generations `commit_base_generation` to `g - 1`: it deletes their content and
removes them. In the durable profile its own directory is flushed (its parent
`trash/` synchronized) as soon as it is created, so the evidence is on storage
before any predecessor directory is removed. Anything other than a directory at a predecessor name is a protocol
error of the job and is never followed. If a predecessor directory cannot be
removed in a few passes (for example an NFS `.nfsXXXX` file another process
keeps open), the commit is deferred: no data step this tick, a retry next
tick, and a WARNING anomaly (`commit_deferred`) naming the path. Waiting
decides only when; the replay never proceeds while such a directory exists.

A removed directory accepts no new entry even through a descriptor still open
to it, so a fenced owner frozen at any point, holding descriptors to its trash
or to `data/`, can no longer move anything into its trash or any created
directory into `data/` once a successor has started replaying. This is a
filesystem assumption: after `rmdir`, `rename`, `mkdir` and creating opens
through a descriptor to the removed directory fail (`ENOENT` on local
filesystems, `ESTALE` on NFS v3/v4, Lustre and GPFS). An NFS export whose
filesystem reuses inode numbers without generation numbers could re-bind a
stale handle; supported filesystems carry generation numbers.

### Idempotent replay

While the marker is `committing`, a manager applies operations in manifest
order with atomic renames and the verified-transition algorithm:

- source present, destination not yet new: validate and rename source to
  destination;
- declared directory absent: create it; an already matching directory means
  `make-dir` already happened;
- source absent, destination has the declared new digest: the put already
  happened;
- removal target absent, matching trash entry present: removal already
  happened. For the manager the evidence is a trash directory of the operation
  of any generation, observed in the retirement pass before any was retired;
  target absent and no such directory means the target was missing from the
  start, and `missing_ok` decides;
- both old tree and new source present during `replace-tree`: continue its
  defined two-rename sequence;
- any other combination: stop with transaction corruption rather than guess.

Only the manager a `committing` frame names replays it; a successor takes an
abandoned commit over by a marker transition and renames the draft, which
fences the previous owner at its next access (see
[Commit ownership](#commit-ownership)). The retirement pass then ends
everything a frozen previous owner could still do in `data/`; a `put-file` or
`put-tree` rename it had in flight before that is recognised by re-observing
the destination, so replays converge. A replayer MUST perform the bounded
visibility retries of the verified-transition algorithm before declaring an
impossible combination. The manifest, the operation IDs and the generations
carry all replay information; no per-operation progress files are needed.

After all operations validate as applied, the manager:

1. appends the destination state frame, with incremented data generation if the
   transaction changed data;
2. renames the exact committing marker to ready, waiting, succeeded, failed, or
   paused;
3. only then allows transaction trash and, in isolated mode, an old isolated
   workdir to be collected.

If interrupted between the data changes and the final marker rename, the owner
(or, once it is evidently gone, a successor that took the commit over) sees
`committing`, reads the draft of the attempt the committing frame names, and
idempotently completes replay without rerunning the step. This keeps the
*httk* v1 `ht.atomic.*` principle without one permanent revision and manifest
hierarchy per step.

## Relocating and transferring jobs

This section specifies relocation and detached transfer in core-v3. The current
implementation performs detached transfer, which is lock-free (see
[No file locks](#no-file-locks)); relocation within one workspace is a
reserved capability that it rejects rather than partially executes, and its
`relocating` state is specified here so that a conforming implementation can
add it without changing the profile.

A job may move after submission, but a raw `mv` of an authoritative payload is
not a state transition, because the marker would still name the old placement.
Relocation is therefore a short replayable protocol.

### Relocation within one workspace

Only a quiescent job may relocate: `ready`, `waiting`, `paused`, `failed`,
`succeeded`, or `cancelled`. A claimed or running job must first be released or
fenced; a committing job must finish replay.

To move from placement `A` to `B`, a manager:

1. validates that `B/<job-key>` does not exist;
2. appends a `relocating` frame containing source `A`, destination `B`, the
   prior logical state, priority, and exact expected record;
3. renames the exact marker from `state/<old-kind>/A/` to
   `state/relocating/A/`, fencing scheduling;
4. atomically renames payload directory `A/<job-key>` to `B/<job-key>`;
5. appends a destination frame with placement `B` and the prior logical state;
6. renames the same marker from `state/relocating/A/` to
   `state/<old-kind>/B/`.

The marker remains the sole authority throughout. Recovery from `relocating`:

| Source payload | Destination payload | Recovery |
| --- | --- | --- |
| Present | Absent | Perform the payload rename and finish. |
| Absent | Present with matching job ID/digest | Payload rename happened; finish the marker transition. |
| Present | Present | Stop: destination collision or non-atomic copy. |
| Absent | Absent | Stop: payload loss. |

Creating and later removing empty placement parents are not state changes.

A batch relocation MAY move a common project or shard prefix with one directory
rename. It first moves every affected marker to `relocating` and journals one
batch ID and the complete member set; only when all members are fenced does it
rename the common payload prefix and return each marker to its mirrored
destination placement. Recovery uses the batch record; no per-job coordinator
files are needed.

### Moving new jobs into a running workspace

Files may be copied or generated under `.httk-workspace/tmp/` while managers
work. The complete payload is renamed to `jobs/<placement>/<job-key>` and becomes schedulable
only when its `submitted` marker is published; a partial copy has no marker and
is invisible.

A whole project tree of complete, unsubmitted payloads may be renamed into a
placement prefix at once, and their markers then published from a validated
batch manifest, one job at a time. A batch needing an all-at-once scheduling
barrier should publish its members paused and release them through an explicit
batch request.

From another filesystem, the copy must finish and be validated inside the
destination workspace before marker publication; atomic rename is relied on
only for the final same-filesystem publication.

### Moving jobs between workspaces

A job moves between workspaces only as a **detached transfer bundle**, a sealed
directory that is moved by rename on one filesystem and by copy and
acknowledgement across filesystems. There is no coordinator that attaches to
two workspaces, no ledger file, and no lock. Every step of every transaction below
is a single-source rename, a no-replace creation, or an idempotent write, so
any actor may stop at any point and any other actor may finish or undo its
work.

If the job may be named by an unresolved cross-workspace join, the source
workspace MUST retain a packed forwarding record keyed by source workspace ID,
job ID, job key, and old placement, naming the destination workspace and
placement, for at least the maximum join-history period. A transfer
implementation without such lookup MUST reject transfer of a job in an
unresolved join.

### Transfer artifacts

Transfer state lives below `.httk-workspace/tmp/` and
`.httk-workspace/transfers/`. Transaction directories are named by transaction
and owner, and no generic cleanup removes them (they may hold the only copy of
a job):

```text
.httk-workspace/
├── tmp/
│   ├── birth.<rand>/                 a sealing transaction directory being built
│   ├── eject.<T>/                    live sealing transaction T (an export: with export/copy-to.json)
│   ├── abort.<T>/                    T decided aborted (renamed from eject.<T>)
│   ├── import.<owner>.<ns>.<L>/      adoption lineage L (with verified.json from V8 on)
│   ├── export.<owner>.<ns>.<T>/      wrapper of held export T while its copy-out runs
│   └── trash.<rand>/                 garbage: collected, or quarantined if it holds a job payload
└── transfers/
    ├── adopting/<job_id>             per-job claim, a hard link of <lineage>/claims/<job_id>
    ├── received/<T>                  receipt of an addressed import
    ├── acks/<T>.json                 signed acknowledgement (destination side)
    ├── incoming/<T>/                 addressed bundle delivered by the transfer CLI
    ├── outgoing/<T>/                 sealed addressed bundle waiting for its acknowledgement
    ├── retired/<T>/                  acknowledged addressed bundle
    ├── exports/<T>/                  wrapper of a held ejected bundle: <job_key>/ and copy-to.json
    └── in-doubt/<T>/                 wrapper a copy-out may have delivered: <job_key>/, copy-to.json,
                                      publishing (never copied out again)
```

`T` is a transfer ID (a fresh UUID), `L` a lineage ID (16 random hex digits),
`<ns>` integer UTC nanoseconds, and `<owner>` an owner token: `m<manager-id>`
(the UUID's 32 hexadecimal digits) for a manager, or
`c<host-hash8><pid><boot-hash8>` for a CLI process, made from the hostname,
process ID and boot ID. Whether an owner is gone follows
[Manager liveness evidence](#manager-liveness-evidence), with an unreadable
heartbeat aged from the name-encoded start time; a CLI owner is gone after 24
hours, or at once when it is on this host and boot and its process is not
alive. All transfer times are integer UTC nanoseconds, parsed strictly.
`transfers/acks`, `incoming`, `outgoing` and `retired` are created with the
workspace; the other transfer directories are created on first use.

### No file locks

The protocol takes no lock of any kind on any configured filesystem; the
`flock` death notifications of [confined launches](#confined-launches) live on
node-local tmpfs and order nothing. It relies on exactly these primitives:

- **single-source rename**: every move has one source, so of two actors only
  one succeeds; a directory rename onto a non-empty directory fails;
- **no-replace creation**: `link(2)` of a fully written temporary, `O_EXCL`
  and `mkdir`, never an overwrite;
- **verify after error**: after any error other than `EXDEV`, a rename or link
  is decided by looking at its source and destination (a retransmitted NFS
  request can report an error for an operation that happened); `EXDEV` is
  never turned into a copy;
- **name lookups for negatives**: "X is absent" is decided by a lookup of a
  fixed name, never by a directory listing, before any destructive action. A
  listing only finds work; missing an entry only delays it.

Liveness evidence (heartbeats, name-encoded times) decides only *when* an
actor takes over work that belongs to somebody else, never whether the result
is correct: a takeover renames the owner's transaction directory, which fences
every later step of the previous owner. A device number is never compared
across processes; identity across processes uses the inode number as a
secondary check whose mismatch stops and reports.

### Detached transfer bundles

A bundle is a job payload directory whose `.httk-transfer/` directory carries
everything needed to verify and import it:

```text
<payload>/.httk-transfer/
├── manifest.json
├── markers/<job_id>                  one empty regular file per job in the bundle
├── runners/<runner_path>
└── tree/<placement>/<job_key>/       member payloads; with the empty placement, tree/<job_key>/
```

`.httk-transfer/` is excluded from payload digests and job seals, so a sealed
job verifies while detached and after adoption. The bundle is not schedulable:
its markers are empty files inside `markers/`, outside every state tree, and
become markers only when an import renames them into place.

The manifest has `format` `httk-workflow-detached-transfer`, `format_version`
3 and `core_profile` `core-v3`, and these members: `transfer_id`,
`source_workspace_id`, `destination_workspace_id` (null for an ejection),
`destination_remote`, `destination_placement`, `sealed_at`, and for the root
job `job_id`, `job_key`, `source_placement`, `payload_sha256`, `seal_sha256`,
`prior_kind`, `prior_state`, `priority`, `source_generation`; `runners`; and
`members`, a top-down list of the tree's other jobs, each with `job_id`,
`job_key`, `placement`, `parent_job_id`, `payload_sha256`, `seal_sha256`,
`prior_kind`, `prior_state`, `priority` and `source_generation`. A bundle has
one transfer ID: every member's provenance names it.

### Bundle verification

Every import verifies the bundle before moving anything out of the claimed
envelope, and verification is the only trust gate:

1. the root is a real directory;
2. a walk of the whole bundle, including `.httk-transfer`, without following
   links refuses special files, symlinks that leave the bundle, regular files
   with more than one hard link, more than 1,000,000 entries, and nesting
   deeper than 256; nothing is opened before the walk;
3. the manifest is read with a 1 MiB bound and a strict schema: canonical
   UUIDs; each job key's UUID equals its job ID; placements normalize and
   satisfy the placement rules; member placements and keys are unique; every
   member's parent is the root or an earlier member; every `prior_kind` is
   quiescent;
4. `markers/` holds exactly one empty regular file per job, named by job ID,
   and `tree/` holds exactly the members' payload directories;
5. root and member payload digests, runner digests and seal digests match;
6. the destination matches: an addressed bundle names this workspace, an
   ejected bundle names none.

Only content errors refuse a bundle. An I/O error leaves it in place to be
retried. A bundle from an exchange inbox (an untrusted client) is verified
further: `prior_state` is filtered to an allowlist of members that a manager
itself writes for quiescent states (and never `transfer`, `origin`,
`outgoing`, `prior_kind` or `prior_state`), a waiting root whose join names
jobs outside the bundle is refused, and so is a root whose `job.json` parent
names a job present in this workspace.

### Sealing transaction

Ejection, export and addressed transfer all seal a job (and, for an ejection,
its bound tree) with one transaction. `T` is a fresh UUID, `E` is
`tmp/eject.<T>`, `A` is `tmp/abort.<T>`. The root is `R`; the other members
`M1..Mn` are empty for an addressed transfer and a single job. Fencing order
is by job ID; envelope order is top-down. The steps are:

| # | Step | Mechanism |
| --- | --- | --- |
| S0 | Pre-check | job kinds (ejection tree: terminal or paused; addressed: quiescent); no unresolved joins; tree boundary rules; no member `transferring` under another transfer; no `transfers/adopting/<job_id>` for any job of the tree; no non-empty `.httk-transfer` in the root payload; every payload on the same device as `tmp/`; target absent. For every job that arrived by an addressed transfer still inside its freshness window, create `received/<Tin>` from its carried provenance |
| S1 | Birth | build `E` with an `envelope.partial` skeleton (an export also with its wrapper `export/copy-to.json`) in a `tmp/birth.<rand>` directory and rename it to `E` |
| S2 | Fence members | each member `Mi` in job-ID order: transition to `transferring`, recording `outgoing` (transfer ID, role `member`, root, owner, start time) and the prior kind and state |
| S3 | Fence root | root transition to `transferring`, recording `outgoing` (role `root`, members, destination, target, owner, times, payload inode) and the prior kind and state |
| S4 | Prepare | create the manifest and runners in `E/envelope.partial` with no-replace links (a content mismatch aborts); rename each member payload to `E/envelope.partial/tree/<p>/<k>`; rename `envelope.partial` to `envelope` once every member is inside |
| S5 | Detach root | rename the root payload to `E/payload`, compare its inode with the recorded one (a mismatch stops and reports), and rename `E/envelope` to `E/payload/.httk-transfer` |
| S6 | Commit | rename `E/payload` to its destination: the target path or exchange outbox (ejection) or `transfers/outgoing/<T>` (addressed); an export to another filesystem takes two renames, `E/payload` to `E/export/<job_key>` (prepare), then the whole wrapper `E/export` to `transfers/exports/<T>` (commit) |
| S7 | Clean up | ejection and export: remove member markers, then the root marker, then trash `E`; addressed: after acknowledgement or `retire` |

Every rename whose source or destination lies inside `E` or `A` names that
directory by path (or by the `tmp/` descriptor and a relative path), never by
a descriptor opened inside it, so a fencing rename of the directory cannot be
followed.

**Abort.** A failed fence (lost transition, sealed job, wrong kind), a failed
commit (`EXDEV`, non-empty target) or an S4 mismatch makes the actor abort by
renaming `E` to `A`. That rename is the single abort decision: after it, every
forward move names a path inside `E` and fails without effect, and `E` never
exists again. Anyone who finds `A` and a `transferring` marker under `T`, once
the owner is gone, runs the **abort steps**: (1) for an addressed reclaim
authorized by `A/reclaim`, rename `transfers/outgoing/<T>` to `A/payload`, and
rename an export's prepared `A/export/<key>` to `A/payload`; (2) rename
`A/payload/.httk-transfer` to `A/envelope`; (3) move every member payload in
`A/envelope.partial/tree` or `A/envelope/tree` back to its placement; (4)
rename `A/payload` to the root's placement; (5) unfence each member and then
the root, restoring `prior_kind` and `prior_state`; (6) trash `A`.

**Phase reader.** Every actor that finds a marker transferring under `T`, and
every actor after an error, locates the payloads by the existence of fixed
names, never by guessing from a listing:

1. `E` present: the **forward phase**. The root payload is, earliest first,
   at its placement, in `E/payload`, in `E/export/<key>` (an export), or at a
   committed location (`exports/<T>/<key>`, `outgoing/<T>`, `retired/<T>`). An
   export is committed when `exports/<T>/<key>` holds a bundle whose manifest
   names `T` (positive observation; another `T` there stops and reports). If
   none holds it and `E` is still present the commit happened (an exchange
   ejection's bundle is in client territory, which is never inspected; an
   export's wrapper may have moved on into its copy-out, since `E/export`
   leaves `E` only by the commit rename). The envelope is, earliest
   first, `E/envelope.partial`, `E/envelope`, `E/payload/.httk-transfer`.
2. Else `A` present: the **abort phase**. The root payload is, earliest first,
   in `outgoing/<T>` (addressed only), `A/export/<key>` (an export),
   `A/payload`, its placement, or `retired/<T>`. If it is in none of the first
   four, the commit or the
   acknowledgement happened before the abort rename: run clean-up, never
   unfence.
3. Else **neither**: no payload of `T` can move again. A marker transferring
   under `T` whose payload is at its placement is unfenced; an addressed root
   whose bundle is in `retired/<T>` is removed; anything else is reported.

Three invariants make this safe. Payloads move only along `jobs -> E -> committed`
(forward), `outgoing -> retired` (acknowledgement) or `outgoing -> A -> jobs`
(reclaim and abort). A marker of `T` is unfenced only in the abort phase after
every payload of `T` is back, or in the neither phase, and is removed only
after the commit or acknowledgement was positively observed. `E` is trashed
only after the markers are removed, and only through a trash step that moves
any job payload it finds to `quarantine/` instead of removing it.

Tree membership is computed from a listing of the state tree; a member that
moves between kinds at that instant can be missed and left behind, and the
next pass re-checks it.

### Adoption

Every import is the same chain, whatever the source: an exchange inbox entry
(claimed by descriptor), a directory on the same filesystem, a directory on
another filesystem (copied), `transfers/incoming/<T>` (addressed receive) or a
held export `transfers/exports/<T>/<key>`. `S` is the lineage directory
`tmp/import.<owner>.<claim_ns>.<L>`.

| # | Step | Mechanism |
| --- | --- | --- |
| V1 | Staging | `mkdir S`; write `intent.json` (source kind and name, whether to remove the source) with a no-replace link |
| V2 | Claim | same filesystem: rename the source to `S/bundle`; otherwise copy it without following symlinks into `S/bundle.partial`, fsync, and rename to `S/bundle`. Losing the rename ends the chain |
| V3 | Writable | make each real directory below `S/bundle` writable through a no-follow descriptor |
| V4 | Verify | bundle verification; an I/O error leaves `S` for a retry, a content error refuses (V10) |
| V5 | Presence | for every job: `received/<T>` present, or a marker (any kind) whose current frame carries provenance `T`: **replay**, without publishing; a `transferring` marker: wait; any other marker: refuse (job already present); `acks/<T>.json` present: for an addressed bundle replay, for an ejected one refuse as a stale copy; for an addressed bundle, the freshness check with a fresh `now` |
| V6 | Claims | for each job in job-ID order, write `S/claims/<job_id>` and hard-link it to `transfers/adopting/<job_id>`. On `EEXIST` the claim is ours when both names are the same inode; another lineage's claim means wait: release this lineage's claims and leave `S` |
| V7 | Recheck | once every claim is held, repeat V5 |
| V8 | Envelope | rename `S/bundle/.httk-transfer` to `S/envelope` |
| V9 | Publish | store the runners; rename each member's `S/envelope/tree/<p>/<k>` to `jobs/<p>/<k>`, then the root `S/bundle` to its placement; for each member and then the root, append the import frame and rename `S/envelope/markers/<job_id>` to the state tree under the prior kind |
| V10 | Refuse | release the claims; an exchange entry is moved into a fresh unique directory below `outbox/rejected` with a `reason.json`; another source is renamed back when its place is free, else left and reported; trash `S` |
| V11 | Finish | when every job's current frame carries provenance `T` (or on replay): create `received/<T>` (addressed), remove a cross-filesystem source that still holds `T`, create `acks/<T>.json`, release the claims, trash `S` |

The import frame carries `transfer` (`transfer_id`, `source_workspace_id`,
`payload_sha256`, `sealed_at`) and, for the exchange inbox, `origin`. A claim
is released by renaming `transfers/adopting/<job_id>` into `S/released/`; the
destination lies inside `S`, so only the current holder of `S`'s name can
release or create a claim.

A job's claim is held from V6 until V10 or V11. A marker of an adopted job is
created only by renaming a marker from the envelope of a lineage that holds
its claim. Return passes, ejection, `detach_job`, removal and gc wait while
`transfers/adopting/<job_id>` exists for any job of a tree.

**Takeover.** An actor that finds `S` whose owner is gone renames it to
`tmp/import.<self>.<now_ns>.<L>`; every later step of the previous owner has a
source or destination inside the old name and fails, including its claim and
release operations. The new owner reads the state earliest first:
`bundle.partial` (an unfinished copy: trash `S`, the source still holds the
bundle), `bundle/.httk-transfer` (before V8: redo V4 to V7), `envelope` (after
V8: continue V9; a payload absent from `S` and present at its placement was
moved there by this lineage, because `S` is private), all markers published
(V11).

**Never imported twice.** An import publishes only after V7, which holds every
per-job claim and found neither a receipt, an acknowledgement nor a marker
with provenance `T`. Two copies of one bundle serialize on the claims: the
loser releases and waits, and after the winner finishes its V7 finds the
provenance and replays. A job that arrived by an addressed transfer and later
leaves creates `received/<T>` first, so a copy of the bundle arriving later is
a replay.

### Provenance carried across transitions

An imported job's frame carries `transfer` and `origin`. `Workspace.transition`
carries both forward from the job's previous frame unless the update sets
them, so provenance survives every later transition. It reads the previous
frame itself and fails when that frame is unreadable; it never silently drops
provenance. A caller that supplies a complete recovered frame (marker repair)
uses `repoint_marker`, which does not carry anything forward.

### Addressed transfers and receipts

An addressed transfer names a destination workspace. The source runs the
sealing transaction with commit into `transfers/outgoing/<T>`; the transfer
CLI moves the bundle to the destination's `transfers/incoming/<T>` and the
destination imports it with the adoption chain, returning one result per
bundle: `imported`, `replay`, `expired` (discarded) or `refused` (kept for the
operator), none of which aborts the batch. The destination signs an
acknowledgement, `acks/<T>.json`, kept `trash_days` for the stale-copy check.

**Acknowledgement.** The source validates the acknowledgement against
`outgoing/<T>/.httk-transfer/manifest.json`, renames `outgoing/<T>` to
`retired/<T>`, and only after that rename (or after seeing `retired/<T>`)
removes the root marker and trashes `E`. With `outgoing/<T>` and `retired/<T>`
both absent the acknowledgement is ignored and retried; a payload found at its
placement means the transfer was reclaimed and is reported as in doubt.

**Freshness and receipts.** An addressed bundle is accepted only when
`sealed_at <= now + S` and `now <= sealed_at + W`, with `W` = 7 days (the
freshness window) and `S` = 130 minutes (the clock-skew bound), evaluated in
V5 and V7 with a fresh `now` and never after V8. `received/<T>` is a small
document (`sealed_at`, `source_workspace_id`, `job_id`, `payload_sha256`)
created by a no-replace link in V11 and in S0 of any later sealing of the job;
it is the replay fence while a copy of the bundle could still be accepted. It
is deleted once `now > sealed_at + W + S`, by which time every importer
refuses the bundle as expired; an unparsable receipt is never deleted. The
replay check (receipt, marker provenance, acknowledgement) always comes before
the freshness check, so a replayed bundle is acknowledged even after its
window. The source's transfer CLI re-sends every pending outgoing bundle on
each run.

**In doubt.** An outgoing bundle not acknowledged by `sealed_at + W` is in
doubt: it may have been delivered, or may still be delivered until
`sealed_at + W + S`. Two operator verbs decide it:

- `httk transfer retire JOB_ID` records that the destination holds the
  job: it behaves as an acknowledgement without a document;
- `httk transfer reclaim JOB_ID` takes the job back, and is allowed
  only after `sealed_at + W + S`: it renames `E` to `A` (or, when neither
  exists any more, creates an empty `A`), records the authorization
  `abort.<T>/reclaim`, and runs the abort steps. A later transfer of the job
  mints a new transfer ID.

An in-doubt transfer is never resolved automatically. The end of the
freshness window proves only that no destination may accept the bundle any
more, not that none did, so the abort steps take a committed bundle back from
`outgoing/<T>` only when the operator's authorization `abort.<T>/reclaim`
exists; recovery continues such an authorized reclaim after a crash. An abort
decided by anything else after the commit (the orphan sweep missing a marker,
say) leaves `A`, the fenced root and `outgoing/<T>` in place: the bundle stays
deliverable and retirable, and `httk transfer status` reports it once
its window has passed. A copy-out is in doubt the same way when it may have
published: before its publishing rename the owner links a `publishing` witness
into the export wrapper in its staging directory (fenced by that directory's
name; an owner whose link fails stops), and an actor that later finds the
witness while the destination does not hold the transfer never copies again:
it first renames the temporary the witness names (the original owner's, carried
along by every takeover of the wrapper) to a unique name and removes it, so a
stale owner can no longer publish it, and then renames the whole wrapper to
`transfers/in-doubt/<T>`, a path that no copy-out ever claims. Its
`copy-to.json` and `publishing` witness are the in-doubt record; the bundle is
at `transfers/in-doubt/<T>/<key>`. `httk job eject --resume` and `httk workflow transfer
status` report it (exit status 1), and the operator either removes the held
copy or takes it back with `httk job adopt <held path>`.

### Ejection, export and adoption of free-standing directories

An *ejected* job is a bundle addressed to no workspace (a null destination).
`httk job eject` runs the sealing transaction and commits with one rename to
the target path. When the target is on another filesystem the ejection never
copies: it commits the wrapper `transfers/exports/<T>` (the bundle at
`<job_key>` beside `copy-to.json`, which records where the copy goes) on the
workspace filesystem, which completes the ejection (the job has left the
workspace), and a separate **copy-out** carries that one wrapper by renames:

1. read `copy-to.json` of `exports/<T>`;
2. claim the whole wrapper by renaming `exports/<T>` to
   `tmp/export.<owner>.<ns>.<T>` (another resume loses cleanly; a wrapper a
   concurrent `job adopt` already emptied is discarded);
3. copy the bundle to `<dest dir>/.httk-export.<token>` (named independently of `<name>`);
4. verify the copy's manifest `transfer_id` and digests;
5. link the `publishing` witness into the wrapper, then rename the copy to
   `<dest dir>/<name>`; when the target is taken, remove the copy, unlink the
   witness, rename the wrapper back to `exports/<T>` and report;
6. trash the wrapper only after step 5 succeeded.

`httk job eject --resume` re-runs every pending copy-out (managers never do),
and `httk job adopt` of a held export takes it back through the adoption
chain. Takeover follows the owner rule, and a takeover that finds the target
holding `T` trashes the staging directory.

`httk job adopt DIR` runs the adoption chain on a free-standing directory. A
second directory with the same transfer ID whose job has since left is
refused as stale (`acks/<T>.json`); a directory whose job is present is a
replay. A tree root ejects with its bound descendants, which must all be paused
or terminal; the descendants travel in its envelope under `tree/`, a tree keeps
its placements (each child records its parent's placement immutably), and a
member directory is never adopted on its own.

### Recovery

One recovery function serves manager attach, the exchange pass, `job adopt`,
`job eject`, transfer import and offer, and the CLI. It takes over adoption
lineages whose owner is gone; runs the phase reader for every `transferring`
marker group whose recorded owner is gone; and sweeps orphans: an `eject.<T>`
with no marker under `T` and older than 24 hours is renamed to `abort.<T>` (an
abort is always safe), an `abort.<T>` with no marker is trashed, and a
`tmp/birth.*` older than 24 hours is trashed. Generic gc never removes
`import.*`, `eject.*`, `abort.*`, `export.*` or `birth.*`.

### Job trees move together

A spawned child is **bound** to its parent while it is not detached (see
`.httk-job/tree/detached.json`) and its parent has a live, non-`transferring`
marker at the placement its `job.json` records. A transfer implementation MUST
NOT move a bound child alone, and MUST NOT move a parent with bound children
unless they leave with it as one tree:

- the tree of a selected job is the job plus, recursively, every bound child its
  spawn records confirm, selected whole regardless of any state or placement
  filter that selected the root;
- every member except the root MUST be `paused` or terminal and none may be
  referenced by an unresolved join, so no manager can claim a member between
  the eligibility check and its fence; otherwise the whole tree stays;
- the root is fenced first, then the members top-down. A member whose parent
  failed to fence stays behind with it. Each member's parent is already
  `transferring` when the member is fenced, so the member is no longer bound
  then, and a member left behind by a partial failure may follow later;
- a multi-member tree's destination placement MUST equal its source placement,
  because every child records its parent's placement immutably.

A parent without spawn records (from before they existed) cannot list its
children; they are still bound while it is live, but it can leave without them.
Tree metadata is excluded from digests and seals like the rest of
`.httk-job/`, so tampering can only separate a tree, never corrupt one. A peer
implementing an older profile moves the files intact but does not enforce the
rule.

For whole projects, a self-contained workspace is preferable: controlled detach
and attach carry its state tree, journals, and all placements together.

## Exchange extension

The exchange extension lets a client that has no workspace access of its own
(typically a service or a daemon's remote side) hand jobs to a workspace and
receive results through plain directories. The writer of the exchange must be
the workspace owner's account (see [Adoption from the inbox](#adoption-from-the-inbox)). It is
declared by `"exchange"` in the `extensions` array of `format.json`; a
workspace declaring an extension the implementation does not know refuses to
attach.

### Enabling and layout

`httk workspace exchange enable [WORKSPACE]` births `exchange/` and its
directories, writes `exchange.json`, adds `"exchange"` to `extensions`, and
re-reads `format.json` to verify. Before it enables, it proves with a rename
probe that `exchange/` and `tmp/` are on one filesystem (`EXDEV` refuses),
because bundles move between the two by rename. `httk workspace daemon init`
enables the extension when absent and says so.

```text
WORKSPACE/exchange/
├── inbox/<name>/           bundles submitted by the client
├── outbox/<job-key>/       finished bundles returned to the client
│   └── rejected/<unique>/  refused inbox entries: <name>/ and reason.json
├── requests/<id>.json      daemon mailbox: signed requests, written by the client
├── responses/<id>.json     daemon mailbox: signed responses, written by the daemon
├── managers/<handle>.log   manager logs, written by the daemon
├── exchange.json           extension marker, written by enable
├── status.json             every job of the workspace with its state, written by managers
├── daemon.json             the daemon's public menu, written by the daemon
└── managers.json           the daemon's manager starts, written by the daemon
```

`requests/`, `responses/` and `managers/` are created by `enable`; they and
the root's `daemon.json` and `managers.json` belong to the workspace daemon
(see [Daemon mailbox](#daemon-mailbox)). There is no sibling exchange
directory. The trusted side considers only inbox entries whose names match
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and are not one of the exchange's own names
(`daemon.json`, `exchange.json`, `status.json`, `managers.json`, `managers`,
`rejected`, `requests`, `responses`, `inbox`, `outbox`); dot names are the
client's own temporaries.

`exchange.json` is `{"format": "httk-workspace-exchange", "format_version": 1,
"workspace_id": "<uuid>"}`. `status.json` is `{"format":
"httk-workspace-exchange-status", "format_version": 1, "workspace_id", "updated_at",
"jobs": [{"job_id", "job_key", "state"}], "truncated"}`: it lists every job of
the workspace, not only the ones adopted from the inbox, in job-key order and
cut to 1 MiB (`truncated`), so the client can see the whole queue it shares. A rejection's
`reason.json` is `{"format": "httk-workspace-exchange-rejection",
"format_version": 1, "name": "<inbox name>", "reason", "rejected_at"}`. The
daemon's own `daemon.json` is a different file, specified in the daemon guide,
not part of this extension.

### Who serves the exchange

Every manager serves the exchange as the first part of each tick, whatever
launched it, when it has no placement-prefix restriction and serves the default
pool (`accept_any_pool`, or pools that include `default`), and never a `job
debug`. It serves only while its attempts are confined (below) and it is not
draining. A manager rereads `format.json` at every claim pass, so an extension
enabled later is noticed, and an unreadable `format.json` stops claiming and
serving. There is no flag that selects exchange serving.

### Confinement requirement

The inbox is untrusted input: a job from it runs code chosen by the client. A
manager on a workspace with the extension therefore MUST run its attempts
under the `bwrap` confinement (`manager.confine = bwrap`); it refuses to start
otherwise, and stops claiming when the extension appears under a manager that
is not confined.

### Descriptor-anchored access

The client may replace directories with symlinks. The manager opens `exchange/`
from the workspace descriptor without following links, then `inbox`, `outbox`
and `outbox/rejected` relative to it with no-follow directory opens, and does
every move relative to those descriptors. It never reads or follows anything
inside client-owned directories other than the entries it claims, never trusts
a name taken from a bundle, and verifies each entry as an untrusted bundle.

### Adoption from the inbox

At most one eligible inbox entry is adopted per pass (names starting with a dot
are ignored and the scan is bounded) with the adoption chain: the entry is
claimed by renaming it out of the inbox, verified as an exchange bundle (see
[Bundle verification](#bundle-verification)), and published with
`origin: "exchange"` in its frame. A refused entry is moved into a unique new
directory below `outbox/rejected`, which is opened without following links and
checked for the owner and device of its parent (another is minted otherwise),
together with a `reason.json` written by a no-replace link through the same
descriptor; `reason.json` is written only once the bundle has arrived there.
A claimed bundle is verified in private staging, but the verification is not a
guarantee against a writer that held a file open before the claim: a write
handle the client opened on a file before the rename (for example an open SFTP
handle) can still change that file's bytes after verification. What such a
write can still reach is limited to the job's own payload files: right after
the verification and the presence checks, and before the envelope moves (V8),
the adopter writes the validated plan (the canonical manifest, with the
filtered `prior_state` of every job, the root's placement and the origin) into
a fresh private `S/verified.json`, and every later step and every recovery after
V8 uses only that snapshot, never the bundle's `manifest.json`; a lineage past
V8 without it is reported and kept. A bundled runner is copied into a private
directory, and that copy is checked against the snapshot's digest and is what
is installed. A marker file's contents are never read (only its name), and a
payload's `job.json` and other files are the job's own content. This is an
accepted limitation for the client's own confined job, and it is why the
exchange writer must be the workspace owner's account.

The bundle's spawn records may not name a job of this workspace outside the
bundle, nor its root a parent present here. An entry the workspace cannot read or
search (a mode-0 file or directory) refuses the bundle rather than leaving it
to be retried. Recovery runs at most once per 10
seconds, and only a manager with the exchange directories open continues an
interrupted inbox adoption. A manager with an until-idle stop counts eligible
inbox entries, exchange-origin trees in their grace period or ready to return,
adoption staging directories and `transferring` markers as outstanding work,
except the markers of addressed transfers (`outgoing`) waiting for a remote
acknowledgement. The finished trees come from a scan made at most every 10
seconds and again after every change, so a quiet census does not re-read every
finished job.

### Return to the outbox

A top-level tree whose root has `origin: "exchange"` returns when it has been
terminal for 60 seconds (its terminal members too), has no unfinished child
work, has no per-job claim, no member `transferring`, every member of exchange
origin, and a parent that is not `transferring`. A job's first frame
here, however it is written (registered, failed at registration, cancelled or
paused while submitted), inherits `origin` from its `job.json` parent, so a
tree's own children leave with it and a local job never does. This includes a job
submitted by hand whose `job.json` names an exchange-origin job as its
`parent`: it inherits exchange origin and is returned to the client with that
tree once the tree is free to leave. The manager runs one sealing transaction
(an ejection) whose commit is a rename into `outbox/<job-key>` by descriptor;
an occupied name is skipped until the client fetches the earlier copy. The
client owns the bundle from then on and may remove or adopt it elsewhere.
`status.json` is rewritten at most every 10 seconds.

Outside the pass, `httk job eject JOB DIR` accepts any local workspace's
`exchange/inbox` as a destination (committed by descriptor from that
workspace's root) and this workspace's `exchange/outbox`. `httk job adopt`
accepts this workspace's `exchange/outbox/<key>` (claimed by descriptor,
verified as client content, a refused bundle quarantined rather than put back)
and `transfers/exports/<T>/<key>`; it refuses this workspace's
`exchange/inbox`, whose entries only the exchange pass adopts.
`transfers/exports/` is an adoption source only, never an ejection destination.

### Daemon mailbox

The optional workspace daemon ({doc}`workspace_daemon`) is a broker that
starts, queries and cancels managers on Slurm. It moves no bundle and reads no
job content; its only workspace state is in the exchange, and its ledger,
keys and configuration lie outside the workspace. Managers never read the
mailbox. The authoritative description of the request operations, signatures,
time window, ledger and outcomes is the daemon guide; the filesystem rules
are these:

- **Names and bounds.** A mailbox publication is a regular file named
  `[0-9a-f]{32}.json`, at most 16 KiB of UTF-8 JSON without a byte-order mark
  or duplicate keys. A request MUST be named `<request_id>.json` after its own
  `request_id`; other names are not publications. Every writer installs a
  publication by writing an exclusive dot-named temporary
  (`.<random>.tmp`) in the same directory, `fsync`, a rename onto the
  publication name and a directory `fsync`; the rename replaces an existing
  file, so one active caller per request ID is the client's obligation.
- **Requests.** The client publishes `requests/<request_id>.json`
  (`httk-workspace-command` version 4, signed with an authorized *httk*
  identity). It does not replace a request that is still present, and before
  republishing it checks once more for a response, because the daemon
  publishes the response before it removes the request.
- **Consumption.** The daemon opens `requests/` and `responses/` afresh for
  every poll, each component without following symlinks, and works relative
  to those descriptors. One poll takes the first 4096 publications in
  directory order, examining at most 1,000,000 entries, and processes that
  batch sorted by name; temporaries and dot names are skipped. A publication that is not a regular file, is oversized, does not
  decode, is named after another request ID or is not signed by an authorized
  key is discarded without a response: removed, or, when it cannot be removed,
  renamed to `.invalid-<name>-<random>` so later polls skip it. A transient
  read error leaves the request for the next poll.
- **Responses.** For every other request the daemon, while the request is
  still published, installs `responses/<request_id>.json`
  (`httk-workspace-response` version 4, signed with the daemon's response key
  and binding the request identities and the canonical request digest), and
  only then removes the request. A request the daemon admits into its ledger
  (`ready`, `submitted`, `uncertain`, `status`, `cancel_requested`, and
  `refused` for an expired request, an unknown manager, an unavailable
  scheduler or an invalid or stale configuration) has its response recorded
  there first, and a repeated request ID with identical content gets that
  recorded response again. The refusals decided before admission
  (`wrong_workspace`, `wrong_enrollment`, `request_conflict`) and `busy` for
  exhausted capacity are published without a ledger record and decided afresh
  on a retry. A response name the client blocked (a directory there) discards
  the request.
- **Retirement.** The client consumes a response by verifying it against its
  request and the pinned daemon key, then removing it. The daemon removes any
  response older than the request lifetime plus the clock-skew allowance
  (3600 + 7800 seconds by default), which no client can still be waiting for.
- **Published state.** `daemon.json` (the daemon's identities, approved
  configuration digests and request lifetime), `managers.json`
  (`httk-workspace-daemon-managers` version 3) and `managers/<handle>.log`
  (the last 1 MiB of a finished manager's Slurm output) are installed by an
  exclusive temporary and a rename relative to an exchange descriptor, so a
  client's symlink at the name is replaced, never followed. They are
  informational: no trusted side reads them back, and a client acts only on
  signed responses.

A manager the daemon submits is an ordinary manager: it runs the workflow
manager with `--by-path --workspace WORKSPACE --idle` (as
`<python> -I -m httk.core.cli workflow manager run`, or the launcher's
`manager.command` after an `environment.prelude`), and its own state is
the `managers/<manager-id>/` directory of the workspace, not the exchange.

## Dynamic branching and joins

### Child publication

A step places complete child bundles in its outcome:

```text
attempts/<attempt-id>/outcome.tmp.<nonce>/
├── outcome.json
└── children/
    ├── spawn.json
    ├── jobs/
    │   ├── branch-a--<child-uuid>/
    │   │   ├── job.json
    │   │   └── ...
    │   └── branch-b--<child-uuid>/
    │       ├── job.json
    │       └── ...
    └── runners/<store-path>      # optional: workspace runners the children pin
```

Each child `job.json` names the parent workspace, job, activation, spawn ID,
and (a clean pre-release requirement) the parent's `placement`. Child UUIDs and
tags are chosen before publication. `spawn.json` chooses each child's target
workspace and placement; in the core profile the target workspace MUST be the
parent's, since cross-workspace children are a reserved future capability.

The parent placement lets the
[revival guard](#reviving-a-child-a-decided-join-consumed) probe the parent's
exact state set at that placement, never falling back to a whole-workspace scan
inside a scheduling tick. The guard is advisory, so a child written before
spawns carried the parent placement makes it probe nothing rather than rescan;
a fresh child MUST carry it.

Every `spawn.json` entry MUST carry a `label`: a nonempty tag-syntax name unique
within the spawn set. A gathering step selects inputs by label, so a missing or
ambiguous label makes the parent's join unusable, and the manager rejects the
outcome with `protocol_error` without registering any child of the set.

While the parent is `committing`, the manager:

1. validates every spawn entry (job key, placement rule, workspace) and
   publishes the parent's spawn record (see [Runner job state](#runner-job-state))
   before anything else is written;
2. copies each complete child bundle out of the draft into its own staging,
   `.httk-workspace/tmp/child.<attempt-id>.<job-key>`, never renaming it out:
   every staged file is a fresh inode, so nothing the attempt still holds open
   or linked in its draft is published. A copy an interrupted commit left
   behind is removed and made again. It verifies the copy (its `job.json` names
   the key, and its digest is the `child_digests` entry recorded when the
   outcome was accepted);
3. copies each workspace runner a verified child pins and the draft stages
   under `children/runners/<store-path>` (as `Attempt.call` does) into
   `tmp/runner.<attempt-id>.<hash>`, checks it against the child's
   `runner.sha256`, and publishes it into the runner store, a no-op when the
   store already holds that digest; staged runners no child pins are ignored;
4. only then moves each verified copy to its chosen
   `<workspace>/jobs/<placement>/<job-key>` path and publishes its one
   `g0.init` marker (a zero-length temporary `tmp/child-marker.<uuid>`) at the
   mirrored path below `state/submitted`;
5. treats a child whose payload directory and a marker of any kind already
   exist at the target as registered, without further checks;
6. for a payload directory already at the target without a marker (an
   interrupted commit published it), checks it against the recorded digest
   and its `job.json` key before publishing the marker, and fails the commit
   with `protocol_error` when either differs, so a different definition under
   the same key is never registered.

The staging names are deterministic, so garbage collection keeps a staged
`child.*` or `runner.*` entry while a commit of that attempt is unfinished.

Children are registered before the parent leaves committing. A crash may expose
only some of them, and replay registers the deterministic missing set;
registration is never rolled back. Children may safely start before the parent
completes its transition.

For a target workspace on another filesystem, the child is first copied into
that workspace's temporary area and validated; its placement rename and
submitted-marker publication then happen on the target filesystem while the
parent is still committing.

Each child is an ordinary independently schedulable job with one job file, one
marker, its own attempts, and its own children. Because its `job.json` names
the parent's `job_key` and `placement`, a running child can find its parent's
payload at `<workspace>/jobs/<placement>/<job_key>` without a scan, for example to
read a large shared file in place; the SDKs expose this as the `parent` read.
The location is meaningful only while parent and child share a workspace.

An advance or succeed outcome may also publish detached children without a
join.

### Waiting and joining

A wait outcome names its child set explicitly:

```json
{
  "action": "wait",
  "next_step": "aggregate",
  "join": {
    "children": [
      {
        "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
        "job_id": "411d9e6e-c050-451d-a851-e20f2570d7c5",
        "job_key": "branch-a--411d9e6e-c050-451d-a851-e20f2570d7c5",
        "placement_hint": "project-17/0/041"
      },
      {
        "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
        "job_id": "7ead0705-e1bb-4290-9ebd-fc1b24df9005",
        "job_key": "branch-b--7ead0705-e1bb-4290-9ebd-fc1b24df9005",
        "placement_hint": "project-17/0/042"
      }
    ],
    "condition": "all_succeeded",
    "on_impossible": {
      "action": "advance",
      "next_step": "handle_child_failure"
    }
  }
}
```

Supported conditions are `all_succeeded`, `all_terminal`, `any_succeeded`,
`any_terminal`, and `at_least` with a successful-child count.

The waiting journal frame records each exact child identity, its spawn
placement as `placement_hint` (a lookup hint, not identity), and the condition.
No join file or child marker is added to the parent directory, and new
unrelated descendants cannot affect the join.

### Resolving join children

A manager resolves each named child through this ladder, in order:

1. the finite set of state kinds at the child's `placement_hint`, when the
   reference has one. Ordinary transitions never change placement, so this is
   a bounded number of lookups and resolves the normal case;
2. its in-memory job-id-to-marker index, confirmed against the filesystem
   before use;
3. any packed relocation or transfer forwarding record;
4. a complete marker scan of the named workspace.

A cache is never the only recovery path, and a cache miss is never the answer.

In the core profile, a clean pre-release break makes step 1 mandatory: every
join child reference MUST carry both `job_key` and `placement`, and join
evaluation probes only the state set at that exact placement, never falling
back to a whole-workspace scan inside a scheduling tick. A reference without a
placement is a protocol error of its publisher, and the manager rejects the
outcome rather than rescanning per child. The step 4 scan survives only for the
forwarding record of step 3 and for interactive resolution (`job show`,
`job why`, `job log`, and collect resolve), where one exhaustive scan for an
arbitrary, possibly finished job is acceptable; it never runs on the scheduling
hot path. A waiting parent's per-tick cost is therefore independent of how many
children its join names: one hint lookup or confirmed index hit per child.

A child not found after bounded visibility retries is an unavailable or corrupt
dependency, not evidence that the join is impossible. The join still may not
wait forever. Children are registered before their parent leaves committing,
so a child unresolvable past a bounded grace is corrupt evidence, and the
manager MUST fail the parent with `dependency_failure` and a message naming the
missing child. This implementation records the first unresolved observation in
memory and fails the parent after `join_grace_seconds`, one hour by default; a
restarted manager restarts the grace, which is safe because it only absorbs
transient nonvisibility.

In the core profile a child named by an unresolved join MUST NOT relocate, so
its placement hint stays valid. Future implementations MAY relocate it only
with the forwarding and full-scan fallback above.

### Join observations

When the join is satisfied, the manager appends a ready frame for a new
activation and renames waiting to ready. Its attempt context summarizes each
child's exact state generation, terminal data generation, and failure
information as one observation object carrying the child's:

- `workspace_id`, `label`, `job_id`, `job_key`, and `placement`;
- `kind`, `state_generation`, and `record_ref`;
- published `failure` object, when it ended `failed` or `cancelled`;
- `data_generation`;
- workspace-relative `payload_path` and `workdir_path`, which include the
  leading `jobs/` and through which its results are read.

The observations appear in the context both as the `join` summary and as the
`children` array, which is empty for an activation that follows no join. Child
committed data may be exposed through read-only paths named in the context. A
transactional parent imports selected child data through its own transaction;
a non-transactional workflow may instead inspect or copy child data into its
persistent workdir by its own conventions.

Join evaluation records an observation vector of every child's exact marker
generation, state-frame reference, and state kind. It need not be a
simultaneous snapshot, but it is the complete evidence for the parent's
transition, and each entry must have been the child's current authoritative
marker when observed. Once the parent's transition commits, later manual
continuation of a child does not change or retract that decision.

### Impossible joins

Success is **currently impossible** when the observation vector holds enough
terminal nonsuccess states to make the condition false:

- `all_succeeded`: any child is `failed` or `cancelled`;
- `any_succeeded`: every child is terminal and none succeeded;
- `at_least N`: succeeded children plus nonterminal children is less than `N`;
- `all_terminal`: never impossible merely because a child failed;
- `any_terminal`: never impossible.

Cancellation is terminal but not successful, and a manually continuable
`failed` child counts as terminal nonsuccess. When success is impossible,
`on_impossible` selects an error-handling step, or the parent fails with
`dependency_failure`. A later revival of the child does not retract the
committed `on_impossible` transition.

`on_impossible` is optional and has exactly one defined form:

```json
{"action": "advance", "next_step": "handle_child_failure"}
```

`advance` is the only legal action, and `next_step` MUST be a valid step name,
including the step the parent just ran. It behaves like an `advance` outcome: a
new activation of the parent at that step, with the observation vector as join
summary, so the error-handling step sees what the satisfied step would have
seen. An absent `on_impossible`, or one with any other `action`, fails the
parent with `dependency_failure`; a manager MUST NOT invent another action or
treat an unrecognized one as a reason to keep waiting.

### Reviving a child a decided join consumed

A join decision is final, but its children are still ordinary jobs. A
`continue` or `override_step` on one would write its workdir and payload again,
possibly while the consuming parent activation reads them, and nothing can
order those two writers.

A manager MUST therefore refuse `continue` or `override_step` for a job that a
decided join has observed, unless the request carries `"force": true`. The
check is exact and needs no index: the child's `job.json` names its `parent`,
and the parent's current state frame carries the `join_summary` of its current
activation, so the refusal applies precisely while the consuming activation is
current. A refused request is retired with a reason naming the parent, so the
operator can continue the parent instead. With `"force": true` the manager
applies the request and MUST record the accepted hazard in the resulting state
frame as a `revival_hazard` object naming the parent, its state, and the
observation that consumed this child.

## Failures and tracking broken jobs

A step declares itself broken with a `fail` outcome:

```json
{
  "action": "fail",
  "failure": {
    "code": "vasp.nonconvergent",
    "message": "electronic minimization did not converge",
    "details": {"iterations": 61, "log": "vasp/convergence.log"},
    "retryable": false
  }
}
```

### Failure object

Application runners, every language binding, and the manager itself use one
failure object shape:

| Member | Type | Required | Meaning |
| --- | --- | --- | --- |
| `code` | string | yes | Stable machine identity, one token without whitespace, at most 128 bytes. Matched against `retry_policy.retry_on`. |
| `message` | string | yes | One nonempty human sentence. |
| `details` | object | no | Structured evidence, such as `exit_status` or application counters. |
| `retryable` | boolean | no, default `false` | Whether repeating the attempt could help. A runner-declared failure with `retryable: true` is retry-eligible regardless of `retry_on` while the job's retry budget remains; `retry_on` governs manager-detected failures. |

No other member is permitted. A manager MUST validate a runner-published
failure before recording it and MUST NOT store an unvalidated object. A
malformed failure is a protocol violation: the job fails with the manager's own
`protocol_error` code and a message naming the defect, so a broken runner
cannot hide a failure from consumers.

The manager embeds the failure in the failed state frame and moves the marker to
`state/failed/<placement>/`. No `failure.json`, `ht.reason`, per-job event
directory, or second failure index marker is created. The failure frame
contains job, step, activation, and attempt IDs; the failure object (code,
message, and details such as exit status or signal); retry history; manager ID;
relevant retained log paths; and data generation (`null` when `data.mode` is
`none`). Manager-generated failure details retain the payload-relative path
`log_paths: ["logs/stdio.out"]`, which `job why` uses when present.

### Reserved failure codes

Codes emitted by the manager itself are reserved. Those currently in use:

| Code | Meaning |
| --- | --- |
| `protocol_error` | Invalid submission, an outcome the protocol forbids, a malformed published failure, an unusable join, or a runner that exited successfully without publishing an outcome. |
| `process_failure` | A runner that could not be launched, or that exited nonzero without publishing an outcome. |
| `lease_lost` | The owning manager's heartbeat expired, or the manager drained and stopped an attempt that published no outcome. |
| `timeout` | The manager stopped an attempt that ran longer than its `maxtime`, and it published no outcome. |
| `retry_exhausted` | `maximum_attempts_per_activation` reached during retry. |
| `budget_exhausted` | An attempt or activation budget exceeded. |
| `dependency_failure` | A join became impossible, or a named join child stayed unresolvable past the manager's bounded grace. |
| `transaction_corruption` | The replay of a published transaction failed midway. A transaction manifest or outcome the manager cannot parse is a `protocol_error`, not this. |
| `runner_unavailable` | A runner outside the payload could not be resolved, opened, or entered at all. |
| `runner_mismatch` | The bytes of such a runner did not match the `runner.sha256` the job pinned. |
| `code_support_unavailable` | The Bash API of an installed simulation-code support package could not be found, so the attempt environment could not be built. |

A runner library that dispatches steps on a runner's behalf publishes ordinary
runner failures, so its codes are reserved too. The shipped runner libraries
use:

| Code | Meaning |
| --- | --- |
| `no_outcome` | The step handler returned without publishing an outcome. |
| `unknown_step` | The job asked for a step this runner does not implement. |
| `declared_failure` | A legacy `ht_steps` task declared itself broken; published by the *httk* v1 compatibility runner only. |

The *httk* v1 compatibility runner also publishes `timeout` for a legacy task
that exceeded its own timeout. `resource_unsatisfiable`, `manager_error`, and
`cancelled` are reserved for manager use but not currently emitted. Application codes SHOULD be namespaced, as in
`vasp.nonconvergent`, to stay distinct from the reserved set.

### Failure history

Current broken jobs are exactly the markers below `state/failed/`, which is
authoritative and needs no reconciliation. A manually continued job's marker
moves elsewhere, but its failed frame stays in the backwards-linked history.
"Ever failed" queries are answered from a compact journal scan, an optional
derived database, or a periodic report, never another per-job marker inode.

Logs are evidence, not state; missing or truncated logs cannot prevent
recovery.

## Manual continuation and control requests

Operators MUST NOT edit state markers, journal segments, or `job.json`. They
write one complete request file in `requests/tmp/` and atomically rename it,
under the same name, into `requests/ready/`, so a manager never reads a partial
request and publication is an ordinary verified rename.

### Request contents and actions

A request is a JSON object named `<uuid>.json`. A writer MUST emit exactly
these members:

| Member | Meaning |
| --- | --- |
| `format`, `format_version` | `httk-workflow-request`, `2` |
| `request_id` | a fresh UUID |
| `job_id`, `job_key`, `placement` | the target job; the job key's UUID MUST equal `job_id` |
| `expected_generation`, `expected_record_ref` | the exact current marker generation and record reference |
| `action` | the requested action |
| `operator`, `reason` | operator identity (a label such as `Name <email>`) and explanation |
| `created_at` | UTC timestamp of publication |
| `priority` | optional; the new priority of `set_priority` |
| `step` | optional; the step of `override_step` |
| `force` | optional `true`: the operator's explicit acceptance of a hazard the manager would otherwise refuse |
| `operator_key`, `signature` | optional, together: the *httk* identity signature (below) |

Actions and the states they apply to:

| Action | Effect | Applies to |
| --- | --- | --- |
| `continue` | Retry the current activation. | `failed`, `paused` |
| `override_step` | Create a new activation at a named step. | `failed`, `paused` |
| `cancel` | Cancel; a live attempt uses the fencing and process-termination procedure of [Cancellation](#cancellation). | Any nonterminal state. A second `cancel` of a job in `cancelling` is not an error and changes nothing. |
| `set_priority` | Rename the marker to a new priority. | Only `submitted`, `ready`, `waiting`, `paused`, `failed` |
| `pause` | Pause. | Immediately from `submitted`, `ready`, `waiting`; deferred from `claimed`, `running`, `committing` to the next attempt boundary; a handled no-op on `paused`. Terminal outcomes supersede a pending deferred pause. |
| `launches_ended` | The operator takes responsibility that every launch of the current attempt has ended, on every host; once the attempt's manager is gone, a manager takes the commit over, or begins the commit of the attempt's published outcome, with that attestation as its [launch end evidence](#launch-end-evidence). | `committing`, and `running` with a published outcome. Retired as not actionable by the manager that owns the attempt (it tracks its own launches) and for a running attempt without an outcome; left actionable and retried while the owner may be alive or a launch of the attempt runs on the applying manager's host, which that manager stops first. |

A manager checks only `format` and `format_version` of the envelope and the
members an action uses; it ignores unknown members. The actions `relocate` and
`transfer`, with the destination workspace, placement and operation ID they
would carry, and the import of selected retained files are reserved: a core-v3
manager rejects a request with any action outside the table as invalid, and
ignores the reserved members. Relocation is the reserved capability of
[Relocation within one workspace](#relocation-within-one-workspace), and jobs
move between workspaces only by the transfer verbs. The exact member set is
enforced only by the publisher of remote requests (`httk job publish-requests`).

`set_priority` MUST NOT rename a `claimed`, `running`, or `committing` marker
behind its owning manager. An in-flight `pause` instead records a sticky
`pause_requested` member in a same-kind frame and pauses at the next attempt
boundary; a manager older than this additive member quarantines such a request
as invalid.

`continue` and `override_step` remain subject to the job budgets of the
immutable `job.json`; no request member changes them (an auditable budget
change under site policy is reserved). Without `force`,
both are refused for a job a decided join already consumed (see
[Reviving a child a decided join consumed](#reviving-a-child-a-decided-join-consumed)).

The signature is the *httk* identity signature of *httk-core*: `operator_key`
is `ed25519:` and the base64 public key, and `signature` the base64 Ed25519
signature of `SHA-256(b"httk-identity-v2\0" + J)`, where `J` is the canonical
JSON (UTF-8, keys sorted, separators `,` and `:` without whitespace, non-ASCII
not escaped) of every member except `signature`, `operator_key` included. A
signature is attribution, not authorization: a request without one is
applied, one whose signature does not verify is invalid, and a verified one
records its `operator_key` in the resulting state frame beside `operator` and
`reason` (see {doc}`workflow_cli`). For a remote workspace the owning machine
builds the unsigned request documents (`httk job request-envelopes`, answered
as `httk-workflow-request-envelopes` version 1), the client signs them, and
the owning machine publishes the signed documents verbatim
(`httk job publish-requests`); on disk they are ordinary requests.

### Applying requests

A manager lists `requests/ready/` and, for each request:

1. reads it, binding the bytes read to the file's identity (an entry replaced
   while it was opened is invalid);
2. resolves the target by probing the state set at the request's exact
   `placement`, never by scanning the workspace from a scheduling tick (the
   same clean pre-release break as the join ladder);
3. checks ownership: a request whose file owner is not the owner of the job's
   marker is retired, a job of another user than the manager's is left for
   that user's manager, and a request for a runner executor this manager does not serve is left in
   `ready/` for a manager that does, and a manager SHOULD remember such
   decisions rather than reread the file every pass;
4. claims it by renaming it into `requests/claimed/<manager-id>/` under the
   same name, deciding a failed rename by looking at the destination, and
   checks that the claimed file is the inode it read;
5. verifies the exact expected marker generation and record reference,
   appends the new journal frame, and renames the marker.

A request without a usable placement, naming a nonexistent job, carrying an
invalid signature or otherwise violating the protocol is claimed and moved to
the quarantine as malformed rather than allowed to trigger a global lookup. A
delayed request cannot apply to a newer state, because its expected
generation no longer matches. All request-induced marker moves are verified
transitions. A manager whose live transition loses to cancellation rereads the
current marker and stops the fenced attempt; it does not infer ownership from
an errno. The result is written to the shared journal; there is no per-job
request directory.

A request stays in `requests/claimed/<manager-id>/` only while its manager
decides it: an applied request is removed, and an I/O error whose outcome is
unknown leaves it claimed for a retry. A manager returns its own claims to
`ready/` at the start of every request pass. Claims of a manager that never
comes back, including the previous incarnation of a restarted manager (every
incarnation has a fresh manager ID), are recovered by the others: at most every
10 seconds a manager lists the other managers' claim directories and, for an owner with
[manager liveness evidence](#manager-liveness-evidence) (judged with its own
lease, since a claim records none), renames each claimed request back to
`ready/`. It is then claimed and checked against its exact preconditions like
any other, so a request is never applied twice: if the former owner was only
slow and had applied it, the recovered copy no longer matches and is retired.

### Retiring requests

A claimed request that can never become actionable MUST be retired rather
than left to be read again. Examples are an expected generation or record
reference that no longer matches, a job that moved while the request was
applied, or a request the protocol refuses, such as reviving a job a decided
join consumed.

Retiring moves the claimed file to `requests/retired/`, which is never
rescanned, and then writes a retirement record beside it, under the request's
name plus `.retirement`. The move comes first, so a claim another manager
recovered meanwhile leaves no record describing a retirement that never
happened:

```json
{
  "format": "httk-workflow-retired-request",
  "format_version": 2,
  "request": "3f2b0a1c-6c2e-4a2a-9a94-1b7c3e5f0d21.json",
  "manager_id": "b0d1e2f3-4455-6677-8899-aabbccddeeff",
  "reason": "the job is at generation 7, not the expected one",
  "retired_at": "2026-07-26T09:12:44.512314Z"
}
```

Both are transient operator evidence, not protocol state; tools such as
`job why` show the latest reason, and both are collected after a month (see
[Retention gates and always-safe collection](#retention-gates-and-always-safe-collection)).

These cases MUST NOT be retired:

- a request whose job uses a *runner executor this manager does not serve*, or
  belongs to another user, is left in `requests/ready/`;
- a request that cannot be decided *right now* (its bytes, ownership or
  target cannot be observed, applying it hit an I/O error of unknown
  outcome, or it is a `launches_ended` request whose commit owner may still
  be alive) stays in `ready/` or in this manager's claim and is retried; the
  maintenance lock does not hold requests back;
- a request that violates the protocol, including one naming a nonexistent
  job, is quarantined as malformed input.

### Continuation and workdirs

Manual continuation preserves failure history. In persistent mode it reuses the
workdir in place, and the new context records `is_restart: true`, its
cleanliness, and `attempt_reason: "manual_continue"`; no import or transaction
is needed. In isolated mode, selected retained files may be imported into the
new workdir; if they also become committed `data/`, that takes a transaction.

## Required outcome-processing order

For a valid current outcome, a manager:

1. validates job, activation, and attempt, plus expected data generation when a
   transaction is present;
2. appends a committing frame naming itself as the commit owner;
3. renames the exact running marker to committing, fencing the attempt;
4. renames the published draft to `commit.<generation>` (see
   [Commit ownership](#commit-ownership)), then applies or verifies the
   transaction, if present, and in the durable profile
   synchronizes every replayed destination and the directories it touched;
5. registers or verifies every child, synchronizing each published payload and
   marker's directory in the durable profile;
6. computes failure or join information;
7. appends and synchronizes the destination state frame;
8. renames the exact committing marker to the destination;
9. performs optional cleanup later.

Because steps 4 and 5 synchronize before steps 7 and 8, a power cut can never
leave a marker out of `committing` that names data or children its storage only
half received.

After the destination transition succeeds, the manager that owned the attempt
removes its control directory once it has reaped the attempt's process and
learned its return code, when the destination is `ready`, `waiting`, `paused`,
or `succeeded`; `failed` and `cancelled` keep their evidence. A manager that
inherited the `committing` marker did not reap that process and leaves the
directory for `attempt_control` garbage collection.

Before recovering a stale claimed or running job, a manager MUST check its
attempt-control directory for a valid published outcome, so a committed
decision is not mistaken for an abandoned attempt.

Recovery from each interruption point:

| Interruption point | Recovery |
| --- | --- |
| Before marker submission | No job was submitted; placed orphan may be retried or collected. |
| In `submitted` | Revalidate; move the same marker to ready or to failed with `protocol_error`. |
| Before `outcome.ready` | Ignore temporary outcome; retry under policy. |
| After outcome publication, before committing | Apply that outcome; do not rerun the step. |
| While a transaction is replayed | State remains committing; infer completed operations from source, destination, the trash directories observed before predecessors' are retired, and digests. |
| While children are registered | Verify existing children and register the missing set. |
| After destination frame, before marker rename | Frame is prepared but not current; replay and rename. |
| After marker rename | New state is already authoritative; the owning manager cleans up after it reaps the process when the destination is `ready`, `waiting`, `paused`, or `succeeded`. |
| Old fenced process publishes late | Never apply it. |

## Task-manager startup and recovery

A manager:

1. discovers explicit and watched workspaces and validates each `format.json`;
2. rejects duplicate workspace IDs and unknown future capabilities before
   mutation;
3. creates a manager record, fresh writer-incarnation journal, and heartbeat in
   each attached workspace, recovers transfer and adoption transactions whose
   owners are gone, and runs the always-safe collection;
4. resumes markers in `committing` it owns, taking over those whose owner is
   evidently gone, and, when enabled, `relocating` and `transferring`;
5. resumes markers in `cancelling`, each naming a fenced attempt still to be
   stopped and verified, whether this manager fenced it or inherited it from
   one that died mid-cancellation;
6. examines possibly abandoned claimed and running markers;
7. evaluates waiting joins, including cross-workspace references when enabled;
8. handles submitted jobs and operator requests, recovering the claimed
   requests of departed managers;
9. claims eligible ready work in pool, capability, priority, and resource
   order, skipping requirements that cannot fit its advertised capacities and
   packing fitting attempts against the reservations of its running attempts;
10. keeps watching for workspaces being attached, renamed, or detached.

In every tick it also supervises the [confined launches](#confined-launches)
of its attempts. A manager without placement prefixes that serves the default
pool, on a workspace with the exchange extension, first runs the exchange pass
unless it is draining (see [Who serves the exchange](#who-serves-the-exchange)).

There is no separate state index to reconcile: listing `state/` is listing the
authoritative scheduler state.

### Scaling scans

A high-scale implementation SHOULD:

- scan only non-terminal state placement prefixes relevant to its configured
  projects or job set;
- keep an in-memory job-key-to-marker map while running;
- process placement subtrees incrementally rather than repeatedly scanning
  every attached workspace;
- use filesystem notifications only as hints, since they may be lost;
- keep task-manager query caches outside per-job directories.

Finding the globally highest-priority ready job requires enumerating the ready
tree, so incremental scans MAY cause long-lived priority inversion, especially
after cold start. This is not a gap to close with directory levels, and no scan
strategy affects claim correctness.

A manager MAY restrict every scan to a set of placement prefixes, a deployment
policy like its advertised pools and capabilities, so disjoint managers divide
a large workspace without walking each other's trees. This is a
self-restriction, not a protocol change: placement values stay project-owned
semantics that the engine only validates and filters on, overlapping
assignments stay safe because the marker rename still arbitrates claims, and a
manager records its prefixes in its manifest so a diagnosis can report when a
live manager's prefixes exclude a job's placement. A manager with no assignment
scans the whole workspace.

## Workspace check and marker repair

A marker that no longer resolves to its journal frame is the one damage a
manager cannot route around: the marker is authoritative, and everything beyond
its kind, priority, and generation lives in the frame.

### Workspace check

A workspace check walks every marker of every state kind and verifies that its
record reference resolves, within the visibility deadline, to a readable frame
whose checksum verifies and whose `workspace_id`, `job_id`, `job_key`, `kind`,
and `state_generation` agree with the marker name. A submitted marker's `init`
reference resolves to nothing by definition and is verified as such. Each
unresolved marker is reported with a stable problem code: `missing_segment`,
`short_read`, `checksum_mismatch`, `length_mismatch`, `trailer_mismatch`,
`reference_mismatch`, `invalid_header`, `undecodable_frame`,
`invalid_record_ref`, `identity_mismatch`, or `unparseable_name` for a
marker-shaped entry whose basename cannot be interpreted.

### Marker repair

Repair is optional, separate, and conservative. Since the frame holding
`previous_record_ref` is the unreadable one, repair cannot follow the chain. It
scans the journal segments for readable frames naming the job and adopts the
newest whose `state_generation` is *strictly older* than the marker's. Adopting
a frame at or beyond the marker's generation is forbidden: it is either the
damaged frame or a transition never committed by a rename, and publishing it
would invent state no marker carried.

The repair appends one ordinary state frame with `reason: "fsck_repair"`, whose
`previous_record_ref` is the recovered frame and whose members are carried
forward from it, keeping the marker's kind, placement, and priority. It renames
the marker onto that frame at the next generation. Repair thus adds history
rather than rewriting it, and the job becomes loadable and schedulable again.

A repair MUST NOT touch a `claimed`, `running`, `committing`, or `cancelling`
marker whose owning manager (identified from the job's readable frames) is
still heartbeating within its lease; such a marker is only reported, and its
manager owns the next transition. For `cancelling`, the marker *is* the fence
the live manager is acting on, and re-pointing it at an older frame would
remove the attempt identity it needs to finish stopping and verifying the
process.

A marker with no readable older frame is unrepairable and is reported. It is
moved into `.httk-workspace/quarantine/` only when an operator explicitly asks.

## Garbage collection and compaction

Under the retention policy in `policy.retention`, a collector may remove:

- unpublished workspace temporary entries;
- placed payload directories that never reached submitted state, except sealed
  transfer bundles;
- journal frames not referenced by any marker or history chain;
- incomplete outcome directories;
- obsolete attempt-control directories;
- abandoned and completed isolated workdirs;
- transaction trash leftovers (what a replay could not delete, and emptied
  trash directories) after the destination marker transition, normally removed
  with the attempt-control tree for `ready`, `waiting`, `paused`, and
  `succeeded` destinations;
- retained diagnostic application files.

That list bounds what a conforming collector *may* touch, not what it must.
`httk workspace gc` collects the subset in
[Retention gates and always-safe collection](#retention-gates-and-always-safe-collection),
plus the retired transfer bundles, acknowledgements and receipts core-v3
accumulates. It leaves isolated workdirs, incomplete outcome directories, and
payloads that never reached `submitted` alone, because each may be the only
remaining evidence of a job that went wrong.

### What collection must preserve

- Journal compaction operates on shared segments, not per-job files, and must
  preserve every record reference reachable from a current marker plus the
  configured history.
- Terminal `job.json`, committed application data, the state marker, and
  required journal history are retained according to site policy. Age alone
  never permits deleting a non-terminal job.
- A persistent workdir is application data, not attempt scratch. It is kept
  until an explicit job or site retention rule permits removal, including after
  failure or manual continuation.
- A payload containing `.httk-transfer/manifest.json` and its markers is
  not an orphan, though it has no marker in a state tree, and the transaction
  directories `tmp/{birth,eject,abort,import,export}.*` may hold a job's only
  copy. Generic temporary/orphan GC MUST NOT collect, alter, or unseal them;
  only an explicit transfer import, abort, recovery step, or
  transfer-specific retention action may.
- Entries in `.httk-workspace/quarantine/` are likewise outside generic orphan
  GC. Only an explicit repair decision or a separately configured
  quarantine-retention policy that keeps an audit record removes them.
- A collector MUST NOT prune the runner store or `runner-builds/`. Runners are referenced by digest
  from `job.json` and transfer manifests, an attached workspace can gain a job
  referring to one at any time, and the store is small. A future version may add
  an explicit runner-retention rule; generic collection never applies one.

Empty placement-directory pruning follows the nonrecursive race rules in
“State-marker rename.” Deep or job-unique placements can temporarily leave one
empty hierarchy under several state kinds, although the marker inode count
stays one per job.

No recovery operation starts by broadly deleting `tmp`, run, or unknown files.
Cleanup is separate from correctness.

### Retention gates and always-safe collection

An explicit `null` or `"keep"` in `policy.retention` means **keep**, and a
collector MUST NOT prune a category whose limit is unlimited (by default
`attempt_control_days`; see [Workspace policy](#workspace-policy)).

These always-safe categories are collected regardless of `policy.retention`,
because their entries carry no information, or (transfer receipts) carry it
only until an expiry that is a pure function of the receipt and the clock (the
last is the one conditional case). Every manager collects them after
attaching, collects transfer receipts and import acknowledgements
(`transfer_receipts`, `transfer_records`) once an hour, and runs the full
policy-gated collection at clean exit and every `--gc-interval` when one is
configured; `workspace gc` also collects them.

- An empty placement mirror below a state kind, pruned by `rmdir` alone.
- An entry in `.httk-workspace/tmp/` or `.httk-workspace/requests/tmp/` more
  than 24 hours old, since every publication renames its staging entry away
  within one operation. The transfer transaction directories
  (`import.*`, `eject.*`, `abort.*`, `export.*`, `birth.*`) are never collected
  here; a staged `child.*` or `runner.*` copy is kept while a commit of its
  attempt is unfinished; and a `trash.*` directory that holds a job payload is
  moved to the quarantine instead of being removed.
- A request in `.httk-workspace/requests/claimed/<manager-id>/` more than 30
  days old whose manager no longer heartbeats (a running manager normally
  recovers such claims long before).
- A request in `.httk-workspace/requests/retired/`, with its `.retirement`
  record, more than 30 days old.
- A transfer receipt below `transfers/received/` once `now > sealed_at + W +
  S`, when no importer can still accept a copy of the bundle. An unparsable
  receipt is kept.
- A removable marker (`succeeded`, `failed`, `cancelled`, `submitted`, or
  `ready`) whose complete payload directory is absent, removed as an
  operator-requested job removal. It is kept when a non-terminal parent's
  current `state.join.children[*].job_id` references the job, and the category
  is skipped conservatively when a non-terminal state frame is unreadable.

That final parent/`committing` check leaves a TOCTOU window, in principle
unbounded, because GC may be descheduled before unlinking. A parent publishing a
join on the removed child in that window sees a missing child and may fail or
stall; this affects scheduling correctness, not payload data. Operators must
remove children only when their parent is terminal; the GC guard is best effort,
not a lock.

The remaining categories are gated as follows:

| Category | Gate | Additional condition |
| --- | --- | --- |
| Attempt-control directory | `attempt_control_days` | Failed and cancelled jobs retain their newest; other quiescent jobs' leftovers (including succeeded) must be older than both this limit and one workspace `lease_seconds` grace. |
| Transaction trash | `trash_days` | The job's marker has reached a quiescent kind, so the destination transition has happened and no replay consults the trash again. The manager's replay deletes removed and set-aside content at once, so this collects only leftovers. |
| Retired transfer bundle | `trash_days` | Below `transfers/retired/`; kept `trash_days` after its acknowledgement. |
| Import acknowledgement | `trash_days` | Below `transfers/acks/`; the stale-copy check of an ejected bundle uses it until then. |
| Journal segment | `journal_days` | No current terminal marker, nor `transferring` marker of a bundle awaiting handover, references it; no frame chain of a current non-terminal marker contains it; and its writer belongs to no manager heartbeating within its lease. |
| Manager directory | `journal_days` | The manager's heartbeat is expired and none of its writer's segments were retained. Its trusted launch directories are removed first when the manager has been silent for the policy `lease_seconds` times the default takeover grace factor (`2.0`) and each holds no process record, a malformed one, or one naming a process group provably gone on this host; a directory still holding a launch record is kept. |
| Manager log | `trash_days` | `logs/managers/<manager-id>.log` and its `.log.1` backup, once the manager's directory is gone. `logs/batch/` is never collected. |

Failed and cancelled jobs keep their newest attempt-control directory
regardless of age, because it holds the outcome and failure breadcrumb of the
deciding attempt and the metadata identifying it. A succeeded job's directory
is normally removed by the committing manager (see
[Required outcome-processing order](#required-outcome-processing-order)); if
that manager dies first, or another manager inherits the committing marker, the
leftover falls under `attempt_control_days`, even when it is the job's only
directory.

### The cost of collecting journal history

Every segment in the frame chain of a current non-terminal marker is protected;
a terminal marker protects only its current segment. Collecting a terminal
job's older segments of the same writer is what `journal_days` buys, and that
job's deep history goes with them: `collect` and `job log` then report its
timeline from the remaining frames and set `gaps`. A non-terminal job keeps its
complete reachable chain.

### Crash safety and journaling of a collection

A collection MUST be interruptible at any instruction. It removes entries
bottom-up, renames nothing, and rewrites no protocol state, so an interrupted
removal leaves scratch the next collection removes. Empty-placement pruning
treats `ENOTEMPTY` and `ENOENT` as ordinary outcomes, because a concurrent
transition may recreate the path.

A collection that removed anything appends one summarizing frame:

```json
{
  "format": "httk-workflow-gc",
  "format_version": 2,
  "workspace_id": "…",
  "collected_at": "2026-07-26T09:12:44.512314Z",
  "retention": {"journal_days": 30},
  "removed": 41,
  "bytes_reclaimed": 918273,
  "removed_jobs": ["finished--…"],
  "categories": {"attempt_control": {"candidates": 12, "removed": 12, "bytes_reclaimed": 40960}}
}
```

It is not a state frame, so history readers ignore it. A collection that
removed nothing writes no frame, because opening a journal writer creates a
writer directory, and an empty collection must not create garbage.

## Manual filesystem inspection

The layout is legible without a database:

- `state/ready/<arbitrary-placement>/` is the runnable queue, with `p000`
  through `p999` in marker names and no priority directory level;
- `state/running/` is everything believed to be executing;
- `state/committing/` is the replay backlog;
- `state/cancelling/` is every attempt fenced by a cancellation whose exit is
  not yet verified;
- `state/waiting/` is the join backlog;
- `state/failed/` is the current broken-job collection;
- `state/succeeded/` is the finalized-success collection;
- `.httk-workspace/quarantine/` holds malformed entries needing workspace
  repair rather than scheduling;
- every marker begins with the optional tag plus UUID job key;
- the matching payload is at the same placement below `jobs/`;
- a first placement component such as `project-17` groups a project without a
  protocol-specific hierarchy.

Operators may read all of these paths but must change state with a workflow
control command, not `mv`, because a correct transition appends the matching
journal record and validates the expected old generation.

An inspection tool should accept a UUID, workspace UUID, exact job key, marker
path, current payload path, placement-prefix query, or tag query (returning all
matches). It follows the marker's journal reference and backward links to show
state, step, retries, manager ownership, failure, children, and manual
intervention history.

## Security and hostile input

Task code is arbitrary code and should run inside an appropriate OS, container,
or batch-scheduler boundary. Managers additionally MUST:

- open paths relative to already opened job/run directories where possible;
- reject path traversal and forbidden file types;
- avoid following symlinks during protocol validation;
- limit JSON size, file count, nesting, journal-frame size, and payload size;
- treat all runner IDs and paths as untrusted;
- never interpolate a runner string through a shell;
- verify that outcome job, activation, and attempt IDs match the current frame;
- prevent a runner from writing immutable job metadata, the state tree, or the
  journal directly;
- for transactional jobs, additionally prevent direct writes to committed
  `data/`. Writes to the declared application workdir are allowed.

The maintained manager provides such a boundary with `manager.confine=bwrap`:
each attempt, and each rank it launches through `HTTK_WORKFLOW_LAUNCH`, runs in
a Bubblewrap sandbox that can write only its own job directory, while the
manager stays trusted and treats everything a job leaves in its directory as
hostile input. Everything the manager reads back from an attempt (the outcome
draft, launch requests) is opened below no-follow descriptors, its JSON
documents within size bounds, and what it needs to act on is either copied out
first (child bundles and staged runners, whose digests and copies are not
size-bounded) or kept in its own memory and manager-owned files (launch
placement, trusted launch descriptions). See {doc}`taskmanager` for the sandbox and its
settings, and [Confined launches](#confined-launches) for the launch files.

## Persistent VASP restart example

A VASP workflow wanting traditional in-place execution declares:

```json
{
  "workdir": {"mode": "persistent", "path": "run"},
  "data": {"mode": "none"}
}
```

Then:

1. The first activation runs in `run/` with `is_restart: false` and
   `HTTK_WORKFLOW_UNCLEAN_RESTART=0`.
2. VASP writes `WAVECAR`, `CHGCAR`, `OUTCAR`, and other files directly in
   `run/`; the manager does not copy, rename, or interpret them.
3. If execution disappears without an outcome, the manager fences the attempt
   and, once the old writer provably cannot modify the workdir, starts a new
   attempt in the same `run/`.
4. The new context has `is_restart: true`, `is_unclean_restart: true`, and, for
   example, `attempt_reason: "lease_lost"`. The step may validate or remove
   partial files, adjust `INCAR`, and resume from `WAVECAR`.
5. After an `advance`, the next step may use the same `run/` but gets
   `is_restart: false`: it is a first attempt, not a restart because files
   exist.
6. A later operator `continue` reuses `run/` and is identified as a restart. No
   transactional `data/` output is involved.

For a POSIX shell step the essential test can be:

```sh
if [ "$HTTK_WORKFLOW_UNCLEAN_RESTART" = 1 ]; then
    repair_or_validate_vasp_restart_files
fi
exec vasp_std
```

The context JSON is the full, versioned interface; the environment variables
are convenient projections for simple runners.

## Worked example

Job `silicon-relax--J`, using isolated workdirs in a core-v3 workspace, starts
at `prepare`, creates two calculations, joins them, and finalizes:

1. Submission publishes its one marker as
   `state/submitted/project-17/0/03a/<job-key>.p500.g0.init`.
2. A manager validates `job.json`, journals ready record `R1`, and renames the
   same marker to `state/ready/project-17/0/03a/...p500.g1.R1`.
3. Manager `M1` journals claim `R2` and wins the rename to claimed.
4. `M1` creates `run.A1`, journals running `R3`, renames the marker, and
   launches `prepare`.
5. `prepare` publishes one outcome containing transaction `T1`, children
   `branch-a--C1` and `branch-b--C2`, and an `all_succeeded` join.
6. `M1` journals `R4`, renames running to committing, replays `T1`, and
   publishes both child job directories and their one markers.
7. `M1` journals waiting record `R5` and renames committing to waiting.
8. Children run independently and may restart without changing the parent
   marker.
9. Once both succeed, a manager journals `R6` and moves the parent marker to
   ready for `aggregate`.
10. `aggregate` publishes an advance outcome and transaction `T2`. The manager
    moves through committing, applies all files, increments data generation,
    and queues `finalize`.
11. `finalize` publishes succeed. The same marker that existed at submission is
    renamed to `state/succeeded/...`.
12. Completed isolated workdirs are later collected. The payload retains one
    job file and application data; its one external marker mirrors its
    placement, and its history is packed with many other jobs in journal
    segments.

At every interruption point, the marker location selects exactly one recovery
rule.

## Relationship to *httk* v1

The packaged `httk.workflow.compat.v1.v1_runner` is an ordinary installed `path`
runner used by converted packages. The normal manager maps instantiated
*httk* v1 task templates as follows:

| *httk* v1 | This protocol |
| --- | --- |
| `ht.task.<set>.<id>.<step>...<status>` | One global state marker plus a packed state frame |
| Task-manager `-s <set>` eligibility | `claim.pool` and manager pool membership |
| Rename to `*.running` | Exact marker rename to claimed/running |
| Stale directory `ctime` | Lease evidence followed by marker fencing |
| `ht.run.current` | Persistent `run/` or isolated `run.<attempt-id>/` |
| `ht.nextstep` plus exit code 2 | `advance` outcome |
| Exit code 3 / `waitsubtasks` | `wait` outcome with explicit child set |
| Exit code 4 / broken | `fail` outcome and failed journal frame |
| `ht.reason` | Structured failure in packed history plus retained log |
| `ht.tmp.task.*` to `ht.task.*` | Child bundle to placed payload plus one marker |
| `ht.tmp.atomic.*` / `ht.atomic.*` | Idempotent adapter preflight replay before an *httk₂* outcome |
| Restart count in pathname | Activation ID and attempt ordinal in state frame |
| `ht.run.resume` | Audited manual continuation, reusing a persistent workdir or explicitly importing into an isolated one |

The adapter keeps the *httk* v1 rule that a published `ht_finished`/broken
decision or pending atomic transaction is completed without rerunning
`ht_steps`.

The *httk* v1 restart counter carried across steps but grew only when a stale
`running` task was adopted, so it did not bound an endlessly advancing clean
workflow. *httk₂* keeps the per-activation retry limit and adds optional total
attempt and activation budgets for that concern.

### Joins and subtasks

Join migration is not a pathname-for-pathname emulation. *httk* v1
`waitsubtasks` searched the whole nested task subtree and could notice
descendants that appeared later; an *httk₂* join waits only for its immutable,
explicit child set. A child that publishes detached grandchildren may complete
without them, so a migrated workflow needing subtree completion must make the
child join its own descendants before succeeding, or name the extra jobs in the
ancestor's join.

The shipped runner makes each discovered direct subtask an explicit native
child, and each child applies the same rule recursively, so ordinary nested
*httk* v1 task trees keep subtree completion. It uses `all_terminal` rather than
`all_succeeded`, because *httk* v1 resumed a `waitsubtasks` parent once no
descendant remained active; broken descendants did not keep it waiting. The
legacy task directories stay in place and are not mirrored with symlinks.
Native child payloads, markers, and journal state are authoritative, and
state-based deduplication prevents rediscovering a child.

### Improvements over *httk* v1

- one authoritative, atomically moving state entry rather than a state plus a
  potentially stale index;
- a constant one-marker state inode cost per job;
- transition, failure, and event metadata packed into shared journal segments;
- optional human-readable path tags without weakening UUID identity;
- arbitrary project/shard placement and crash-safe relocation;
- dynamic multi-workspace attachment for combining projects;
- explicit attempt fencing and restart detection;
- explicit child sets and join policies;
- transactional replay with no permanent revision-metadata tree per step;
- structured, durable manual and failure history.

The filesystem remains the interoperability layer. Its steady-state inode cost
follows from the arithmetic above; capacity beyond the measured snapshot is an
operational question for the target filesystem and should be evaluated with
{doc}`/details/benchmarks` before sizing a campaign.
