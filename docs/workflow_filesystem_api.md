# Workflow filesystem API

## Status and scope

This document specifies the filesystem protocol around which the
*httk-workflow* engine is to be built. It is a design specification, not
documentation of an existing Python API.

The protocol is language independent. A workflow step may be a shell script, a
Python program, a compiled executable, or any other program that can read and
write files and atomically rename a file or directory.

The design descends from the `ht.task.*` directories, `ht_steps`,
`ht.nextstep`, subtasks, and `ht.atomic.*` replay mechanism in httk v1. It keeps
the central property of that design: neither a workflow step nor a task manager
is ever required to run cleanup code. Either may disappear between any two
instructions. A later task manager must be able to identify the last commit
point and continue from it.

The design is intended for stores containing many millions of jobs. Metadata
inode count, directory fan-out, scheduler scan cost, and manual filesystem
inspection are therefore correctness-level design concerns, not later
optimizations.

The protocol covers:

- submission and execution of dynamic, multi-step jobs;
- safe competition between any number of task managers;
- automatic and manual continuation;
- dynamic fan-out into child jobs and later joins;
- transactional contributions to a durable job workspace;
- explicit, unexpected, and dependency failures;
- durable history and discovery of failed jobs.

It does not promise transactional semantics for effects outside the workflow
filesystem. Sending mail, submitting to a second queue, or changing a remote
database must be made idempotent in that external system, for example by using
the httk job and activation IDs as idempotency keys.

## Normative language

The words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are used
in their usual specification sense.

Protocol JSON is UTF-8. Unknown object members MUST be ignored when reading a
compatible major format version.

## Design summary

The core representation is deliberately small:

1. A job has one payload directory containing one required metadata file,
   `job.json`. Its parent path is an arbitrary, user-chosen placement path.
2. A job has exactly one small marker file in the global `state/` tree.
3. The marker's location is the sole authority for the job's current state. It
   is atomically renamed between `submitted`, `ready`, `claimed`, `running`,
   `committing`, `waiting`, and terminal state directories.
4. Transition details and history are packed into shared append-only journal
   segments. There is no state-record file, event directory, failure file, or
   revision directory per job.
5. An active attempt temporarily adds one small control directory. Application
   execution may use either one persistent `run/` workspace or an isolated
   `run.<attempt-id>/`.
6. `data/` and replayable transactions are optional. Jobs that opt in receive
   all-or-none publication at attempt boundaries; jobs such as large VASP runs
   may instead keep all mutable state directly in persistent `run/`.

The expected steady-state metadata cost for a job with no retained application
data is:

| Object | Per job | Lifetime |
| --- | ---: | --- |
| Job payload directory | 1 directory | Job lifetime |
| `job.json` | 1 file | Job lifetime |
| Authoritative state marker | 1 file | Job lifetime |
| Shared journal records | 0 files | Packed into writer segments |
| Attempt control directory | 0 or 1 directory | Attempt/retention lifetime |
| Persistent/isolated workspace | 0 or 1 directory | Application policy |
| Per-state/per-event/per-failure files | 0 | Not used |

Shard and journal directories are shared by many jobs. Application inputs,
outputs, code, and logs naturally add their own files; the table only counts
workflow metadata.

## Concepts

A **workflow store** is one self-contained filesystem tree with an immutable
store UUID, its jobs, authoritative state markers, and journals.

A **watch root** is an ordinary directory below which a manager discovers
workflow stores. A manager may also receive explicit store paths and may
supervise several stores at once.

A **job** is the durable unit of scheduling, history, and final success or
failure.

A **job key** is the filesystem component
`[<tag>--]<job-uuid>`. The UUID is authoritative; the optional tag is for human
navigation.

A **placement** is an arbitrary relative parent path below a store, for example
`project-17/0/03a`. A payload at that placement has the path
`<placement>/<job-key>`.

A **step** is an application-defined name such as `relax` or `collect`. The set
of possible step names need not be declared in advance.

An **activation** is one logical request to execute a step. Advancing from one
step to another creates a new activation. Deliberately advancing to the same
textual step name also creates a new activation.

An **attempt** is one physical execution of an activation. Retrying after a
timeout or abandoned allocation creates a new attempt of the same activation.

A **data generation** identifies the committed contents of an optional job
`data/` tree. It is absent for jobs that do not use transactional data and
advances after a successful transaction for jobs that do.

An **outcome** is the step's atomically published request to advance, wait,
succeed, fail, retry, or pause.

A **child job** is an ordinary job whose immutable definition names a parent.
Children may themselves create children.

## Required filesystem semantics

All correctness-critical paths within one workflow store MUST reside on one
filesystem on which:

1. renaming a file within that filesystem is atomic;
2. renaming a directory within that filesystem is atomic;
3. a successful rename removes the source and installs the destination as one
   indivisible namespace operation;
4. a failed rename is reported to the caller.

Atomic rename of the exact current marker is the compare-and-swap operation.
The protocol never depends on `flock`, advisory locks, PID uniqueness, or an
exit trap.

The baseline guarantee is **process-interruption safety**. Storage-crash
durability additionally requires an implementation to synchronize new file
contents and affected parent directories before publishing their names.
Implementations claiming storage-crash durability MUST use `fsync` or an
equivalent operation in the order required by the filesystem.

The following require an explicit backend adapter and validation:

- source and destination paths on different mounts;
- object stores that only emulate rename;
- synchronization tools operating on a live root;
- network filesystems without coherent atomic rename;
- filesystems on which a client can indefinitely cache a removed name.

Modification times and wall clocks are evidence for lease expiry, but never
provide fencing. Correctness comes from moving the one current state marker.

## Store layout and arbitrary placement

A store is an ordinary directory. Its protocol control data are below
`STORE/.httk-workflow/`; job payload directories may be placed at any valid
relative path outside that reserved directory:

```text
STORE/
├── .httk-workflow/
│   ├── format.json
│   ├── tmp/
│   ├── state/
│   │   ├── submitted/<placement>/
│   │   ├── ready/<placement>/
│   │   ├── claimed/<placement>/
│   │   ├── running/<placement>/
│   │   ├── committing/<placement>/
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
│   │       └── heartbeat.json
│   └── requests/
│       ├── tmp/
│       ├── ready/
│       └── claimed/
└── project-17/
    └── 0/
        └── 03a/
            └── silicon-relax--01234567-89ab-cdef-0123-456789abcdef/
                ├── job.json
                ├── data/
                ├── files/
                ├── run/
                └── .httk-attempt.<attempt-id>/
```

Here the job placement is `project-17/0/03a`. Its authoritative marker has a
parallel path such as:

```text
.httk-workflow/state/ready/project-17/0/03a/
└── silicon-relax--01234567-89ab-cdef-0123-456789abcdef.p500.g4.<record-ref>
```

The protocol assigns no meaning to the placement components. They may represent
projects, user names, dates, hash shards of any depth, or a mixture. Different
jobs in one store may use completely different placement schemes.

`files/`, `data/`, a workspace, and attempt control are created only when required.
Empty placeholder directories SHOULD NOT be created. Empty placement
directories have no protocol meaning and may be removed.

`format.json` identifies the self-contained store:

```json
{
  "format": "httk-workflow-filesystem",
  "format_version": 1,
  "store_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "created_at": "2026-07-24T12:00:00Z"
}
```

There is no configured sharding depth. Priority is encoded in marker names
rather than represented by another directory level.

`.httk-workflow/tmp/` contains unpublished entries. Task managers MUST ignore
it for scheduling. Garbage collection may remove old temporary entries, but
correctness MUST NOT depend on cleanup.

Placement components MUST be normalized relative path components. Empty
components, `.`, `..`, NUL bytes, and `.httk-workflow` are forbidden. Each
component must fit the underlying filesystem's filename limit. A store MAY set
policy limits on depth and total relative path length, but these are operational
limits rather than a protocol sharding scheme.

## Managing and combining stores

A task manager accepts any combination of:

- explicit store paths;
- watch roots below which it discovers directories containing
  `.httk-workflow/format.json`.

The manager identifies a store by `store_id`, not its current absolute path.
The same store discovered through two paths is attached once. Journal
references are store-relative and include the store ID when used from another
store.

This permits both common arrangements:

```text
# One store with projects as placement prefixes
STORE/project-a/00/17/<job-key>
STORE/project-b/hash/x9/<job-key>

# A watch root containing self-contained project stores
WATCH/project-a/.httk-workflow/format.json
WATCH/project-a/00/17/<job-key>
WATCH/project-b/.httk-workflow/format.json
WATCH/project-b/hash/x9/<job-key>
```

A manager may schedule from all attached stores in one resource pool. Job and
child references are `(store_id, job_id)` pairs; the job UUID alone is accepted
only when unambiguous among attached stores.

Discovery stops at a store boundary. Attached stores MUST NOT overlap unless an
explicit advanced profile defines ownership of every placement prefix; the
default manager rejects nested/overlapping store roots.

### Dynamic attachment

A complete store can be built elsewhere on the same filesystem and atomically
renamed below a watch root. Its `format.json`, state tree, journals, and
payloads arrive together. The manager discovers the immutable store ID and
begins scheduling it without restarting.

Filesystem notifications are hints. Managers periodically rescan watch roots so
a lost notification cannot hide an attached store.

Renaming an attached store within watched paths does not create a new store.
Managers SHOULD hold an open directory handle and update the path associated
with the same store ID. Copying a live store is forbidden: it duplicates
authoritative markers and journal identity.

### Dynamic detachment

Detaching one store does not stop a manager from serving its other stores. A
detach coordinator obtains acknowledgement from every manager with a live
heartbeat in that store. Each manager:

1. stops new claims from that store;
2. completes or releases manager-owned claimed jobs;
3. completes committing replays;
4. waits for, pauses, or explicitly hands off running attempts;
5. closes the store journal writer;
6. acknowledges that the store is quiescent for it.

Once all live managers acknowledge and no marker is claimed, running, or
committing, the store directory may be atomically moved out of the watch root.
A manager MUST NOT interpret disappearance of a store as failure of every job
in it. It marks that store unavailable until the same store ID is reattached.

A raw move of a store containing active attempts is supported only when the
same supervising managers follow the atomic rename by store ID and retain valid
directory handles. The portable and recommended operation is controlled
detach, move, then attach.

## Job UUIDs, names, and path tags

Job IDs are lowercase canonical UUIDs. Human-readable names do not have to be
unique and remain in `job.json`.

An optional **tag** may be included in the job key:

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

Without a tag, the job key is just the UUID. Parsers identify the final UUID
rather than trusting the tag. A lookup by tag may return several jobs; UUID
lookup returns at most one job within a store.

The current payload path is:

```text
<store>/<placement>/<job-key>/
```

The authoritative state marker uses the same placement and job key.
Consequently, an operator can use ordinary shell completion or `find` to find
both payload and state:

```bash
find . -path './.httk-workflow' -prune -o -type d -name 'silicon-relax--*' -print
find .httk-workflow/state -type f -name 'silicon-relax--*'
```

Renaming a tag by hand is forbidden because the payload and state key must
agree. A workflow may instead put mutable descriptive labels in application
data or an external catalog. Avoiding alias files keeps the per-job inode cost
fixed.

## The authoritative state tree

### One marker, one source of truth

Every submitted job has exactly one regular marker file somewhere below
`state/`. There is no second marker inside the job and no advisory state index.
The marker's directory is the current scheduler state.

The sole exception is an explicitly detached transfer bundle: its same marker
inode is sealed inside `.httk-transfer/`, outside every manager's state tree,
and the bundle is not schedulable until import republishes that marker.

For example:

```text
.httk-workflow/state/ready/project-17/0/03a/
└── silicon-relax--01234567-89ab-cdef-0123-456789abcdef.p500.g4.<record-ref>
```

The basename has this logical grammar:

```text
<job-key>.p<priority>.g<generation>.<record-ref>
```

`generation` is a monotonically increasing base-36 state generation. It is
unrelated to the data generation. `record-ref` locates the immutable transition
record in a packed journal. The initial submitted marker uses `g0.init`,
because its initial information is in `job.json`. Priority is always a
zero-padded three-digit integer from `000` through `999`.

The complete relative marker path is the authoritative current state:

- its state directory gives the state kind;
- the directories after the state kind give current placement;
- its `p<priority>` component gives current priority;
- its job key gives identity;
- its generation prevents stale operator actions;
- its journal reference gives transition details.

A transition MUST rename the exact old marker path to the exact new marker
path. It MUST NOT create a second marker and later delete the first. Therefore:

- two managers racing to claim the same ready marker cannot both win;
- a crash cannot leave old and new authoritative states;
- terminal and non-terminal collections are immediately inspectable;
- no index-repair process is part of ordinary correctness.

Outside the explicitly recoverable `relocating` state, a marker without its
payload at the mirrored placement is corruption. A payload directory without a
marker is unsubmitted temporary/orphan data, not a queued job.

### State-marker rename

Before a transition, the actor:

1. prepares and synchronizes the new journal record;
2. derives the new marker basename from its journal reference;
3. renames the exact old marker to the destination state directory;
4. regards the transition as committed only if that rename succeeds.

If the source no longer exists, another actor won. The loser rereads state and
does not assume its transition occurred. Its unreferenced journal record is
harmless.

Record references are unique, so the destination MUST be absent. An
implementation SHOULD use no-replace rename where available. Encountering an
existing destination is corruption, not permission to overwrite it.

The marker file is created once at submission and then only renamed. A normal
job lifetime therefore consumes one state-marker inode regardless of its number
of steps, retries, failures, or manual continuations.

## Packed transition journal

Creating one JSON file for every state, event, failure, and output revision
would be prohibitive at this scale. Transition metadata are instead frames in
shared append-only journal segments:

```text
journal/<writer-id>/<segment-number>.hwj
```

Each task manager is the sole writer of its own current segment. It never
appends to another manager's segment. Segments rotate at a configured size, not
per job. Submission needs no journal file per invocation because the initial
marker uses `init`.

A segment starts with a format/version header. Each frame contains:

1. a fixed-width payload length;
2. one UTF-8 JSON record;
3. a checksum over the length and payload;
4. a repeated fixed-width length trailer.

The marker's compact `record-ref` identifies writer, segment, byte offset,
length, and checksum. It must fit with the job key in a 255-byte filename. An
implementation MAY use a binary encoding for the reference, but its decoding is
part of the store format.

Before a state-marker rename, the writer MUST flush the entire frame and, in
the storage-durable profile, synchronize the segment. A marker can therefore
never legally reference a torn tail. An unreferenced partial final frame is
ignored and may be truncated during journal repair.

Every state frame includes:

```json
{
  "format": "httk-workflow-state",
  "format_version": 1,
  "store_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "job_key": "silicon-relax--01234567-89ab-cdef-0123-456789abcdef",
  "placement": "project-17/0/03a",
  "state_generation": 4,
  "kind": "ready",
  "previous_record_ref": "...",
  "created_at": "2026-07-24T12:00:00Z",
  "step": "collect",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "attempt_ordinal": 1,
  "data_generation": 2,
  "priority": 500,
  "reason": "advance"
}
```

`data_generation` is omitted or `null` when `job.json` declares
`data.mode: "none"`.

State frames form a backwards-linked history across writer segments. Failure,
join, operator, and outcome details are embedded in the applicable state frame
or in another journal frame referenced by it. They do not create per-job
metadata files.

Journal segments are append-only and retained according to history policy.
They may be compressed only into a random-access archive format that preserves
record references. A derived SQL database or in-memory map MAY accelerate
queries, but it is a cache: the state tree and referenced journal frames remain
authoritative.

## Job definition and submission

A minimal `job.json` is:

```json
{
  "format": "httk-workflow-job",
  "format_version": 1,
  "id": "01234567-89ab-cdef-0123-456789abcdef",
  "tag": "silicon-relax",
  "name": "Silicon relaxation",
  "workflow": "example.vasp-relax",
  "runner": {
    "path": "files/runner",
    "arguments": []
  },
  "workspace": {
    "mode": "persistent",
    "path": "run"
  },
  "data": {
    "mode": "none"
  },
  "initial_step": "prepare",
  "priority": 500,
  "retry_policy": {
    "maximum_attempts_per_activation": 10,
    "retry_on": ["lease_lost", "timeout", "process_failure"]
  },
  "resources": {},
  "parent": null
}
```

`job.json` is the only required metadata file in the payload directory and is
immutable after submission. Small workflow-specific parameters SHOULD be stored
directly in it instead of one file per parameter.

`runner.path` is resolved relative to the current payload directory. It MUST remain
beneath that directory unless an administrator has enabled external runners.
`arguments` is an argument vector, never a shell command string.

`files/` is optional and contains submitted code, templates, or immutable input
objects that require separate files. Small inputs SHOULD be embedded in
`job.json`.

`workspace.mode` is `persistent` or `isolated`:

- `persistent` reuses the declared workspace directory across step activations
  and attempts. The workflow program owns recovery and cleanup of partial
  application files.
- `isolated` creates a new `run.<attempt-id>/` for every attempt. It may be
  initialized from submitted files or transactional `data/`.

The mode MUST be explicit in `job.json`; the protocol has no implicit default.

`data.mode` is `none` or `transactional` and is also explicit. With `none`, the
job may keep all mutable and final application data in its persistent workspace.
It never needs to create `data/`, publish a transaction, or increment a data
generation. With `transactional`, `data/` and the transaction protocol below
are available.

Priority is 0 through 999, where 0 is highest. It affects scheduling, not
correctness.

### Submission protocol

To submit:

1. Create a complete job below `.httk-workflow/tmp/`, including `job.json` and
   any initial `files/` or `data/`.
2. Choose any placement and atomically rename it to
   `<store>/<placement>/<job-key>/`.
3. Create a temporary zero-length marker named
   `<job-key>.p<priority>.g0.init`.
4. Atomically rename that marker to
   `.httk-workflow/state/submitted/<placement>/<job-key>.p<priority>.g0.init`.

Step 4 is the **submission commit point**. Before it, the placed payload is an
unsubmitted orphan and may be completed, retried, or eventually collected.
After it, a fully populated submitted job exists.

A task manager registers the job by validating `job.json`, appending its first
ready state frame, and renaming the same marker from `submitted` to the
mirrored path below `state/ready/`. A crash before this rename leaves the job
visibly submitted; another manager repeats validation.

Retrying submission with an existing job UUID succeeds only when the existing
immutable job digest is identical and its marker already exists. A different
definition under the same UUID is an error.

## State machine

The state kinds are:

| Kind | Meaning |
| --- | --- |
| `submitted` | Complete job awaiting manager validation and registration. |
| `ready` | Eligible to be claimed when resources permit. |
| `claimed` | A manager won the claim but has not launched the attempt. |
| `running` | The current attempt may have a live process. |
| `committing` | The attempt is fenced; its outcome is being replayed. |
| `relocating` | No attempt may run; payload placement is being changed. |
| `transferring` | A quiescent payload is moving between stores or detaching. |
| `waiting` | Waiting for a declared child join. |
| `paused` | Requires an operator request to continue. |
| `succeeded` | Successful terminal state. |
| `failed` | Failed until explicit operator continuation. |
| `cancelled` | Cancelled terminal state. |

Normal transitions are:

```text
submitted ──validate──> ready ──claim──> claimed ──launch──> running
                           ▲                  │                 │
                           │                  └────recovery─────┤
                           │                                    ▼
                           │                               committing
                           │                              /    |    \
                           ├────────advance/retry────────┘     |     \
                           │                                  |      ├─> succeeded
                           ├────────join satisfied──── waiting <      ├─> failed
                           │                                         └─> paused
                           └────operator continuation──── failed/paused
```

`advance` may name any next step, including the same textual name. It creates a
new activation. Retry retains the activation ID and increments the attempt
ordinal.

Any non-terminal state can be cancelled by an authorized operator. Cancellation
of `running` first fences the attempt by moving its marker; a late outcome from
that attempt can no longer commit.

Quiescent states may also pass through `relocating` and return to the same
logical state at a different placement.

## Claiming, leases, and fencing

To claim a ready job, manager `M`:

1. Selects the exact ready marker.
2. Reads the state frame and stable `job.json`.
3. Generates attempt and claim IDs.
4. Appends and synchronizes a `claimed` state frame.
5. Renames that exact ready marker to the claimed state path whose basename
   references the new frame.
6. Proceeds only if the rename succeeds.

All competing managers rename the same source path to different unique
destinations. Exactly one source rename succeeds.

The claimed frame names:

- manager and attempt IDs;
- step, activation, and attempt ordinal;
- input data generation when transactional data are enabled;
- lease duration and start time;
- resource allocation;
- preceding record reference.

The manager creates `.httk-attempt.<attempt-id>/`, prepares the selected
persistent or isolated workspace, appends a running frame, and renames the
claimed marker to running immediately before launching the process.

Managers update `managers/<manager-id>/heartbeat.json` by atomic replacement.
A recoverer uses the state frame, heartbeat, batch scheduler when available,
and a configured grace period. It MUST NOT steal a job merely because one
delayed metadata read appears stale.

Once policy determines that an attempt is abandoned, a recoverer appends a new
claimed frame and renames the exact old claimed or running marker. That rename
fences the old attempt. Even if its process later wakes, no task manager accepts
its outcome because the state tree names a different attempt.

A manager taking immediate ownership transitions directly to a new claimed
state. A manager merely releasing work transitions back to ready. Both retain
the activation ID for a retry.

Filesystem fencing cannot prevent a partitioned old process from producing
external side effects. Managers SHOULD terminate its process group or cancel
its batch allocation before recovery when possible.

## Attempt control, workspaces, and runner contract

Attempt control is separate from application workspace:

```text
<store>/<placement>/<job-key>/
├── .httk-attempt.<attempt-id>/
│   ├── context.json
│   ├── outcome.tmp.<nonce>/
│   └── outcome.ready/
└── run/                         # persistent mode
```

In isolated mode the application directory is
`run.<attempt-id>/` instead of `run/`. The runner's current working directory is
the selected application workspace, not the attempt control directory.

Separating them matters in persistent mode: a late process from an old attempt
can only publish beneath its own attempt-control name. Its outcome cannot
replace or impersonate the new attempt's outcome.

### Persistent workspace

The same declared workspace is used across normal step advances and retries.
The manager does not clean, copy, snapshot, or transactionally inspect its
application files.

This mode supports, for example, a VASP workflow that retains a very large
`WAVECAR` in `run/`, modifies it over several steps, and decides for itself
whether an interrupted calculation can continue. `data/` need not exist.

On retry, the manager first fences and attempts to terminate the old process,
then invokes the same step activation in the same directory with a new attempt
context. The workflow code examines the reason and the existing files and may:

- continue directly;
- remove known partial outputs and restart;
- repair inputs and request another retry;
- declare the job failed.

A planned advance to another step also reuses the directory, but is not a
restart: the new activation has attempt ordinal 1 and `is_restart: false`.

Persistent mode deliberately gives up workspace isolation. If an old process
cannot be proven dead, it may still modify `run/` after being fenced from
publishing an outcome. Site policy must then wait, force scheduler cancellation,
pause for manual action, or explicitly accept that risk before launching the
replacement attempt.

### Isolated workspace

Every attempt receives a new `run.<attempt-id>/`. The manager may populate it
from submitted files or transactional `data/` by copying, reflinking, or a
filesystem snapshot. Writes through an isolated workspace MUST NOT mutate
committed `data/` before a transaction.

Old isolated workspaces may be retained for diagnosis or collected later. A
manual continuation can import selected files into a new isolated workspace,
with the choice recorded in history.

### Context and restart detection

The manager MUST set:

```text
HTTK_WORKFLOW_CONTEXT=<absolute path to attempt context.json>
HTTK_WORKFLOW_CONTROL_DIR=<absolute attempt-control path>
HTTK_WORKFLOW_STORE_DIR=<absolute store path>
HTTK_WORKFLOW_JOB_DIR=<absolute current payload path>
HTTK_WORKFLOW_RUN_DIR=<absolute selected workspace path>
HTTK_WORKFLOW_IS_RESTART=0|1
HTTK_WORKFLOW_UNCLEAN_RESTART=0|1
HTTK_WORKFLOW_ATTEMPT_REASON=<reason>
```

`HTTK_WORKFLOW_DATA_DIR` is additionally set only for transactional-data jobs.
The JSON file is the source of truth; scalar environment variables are
language-neutral conveniences.

An unclean persistent retry context is:

```json
{
  "format": "httk-workflow-attempt-context",
  "format_version": 1,
  "store_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "placement": "project-17/0/03a",
  "step": "relax",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "attempt_id": "a6c2c973-29e1-44e2-9649-ae419e340ac4",
  "attempt_ordinal": 2,
  "is_restart": true,
  "is_unclean_restart": true,
  "attempt_reason": "lease_lost",
  "previous_attempt_id": "31fa431e-e01c-49aa-8fa8-e8af23b73c52",
  "activation_reason": "advance",
  "workspace_mode": "persistent",
  "workspace_reused": true,
  "data_generation": null,
  "join": null
}
```

`attempt_ordinal > 1` and `is_restart` mean that the same activation is being
retried. `is_unclean_restart` specifically means that the preceding attempt did
not publish and complete a valid outcome. Reasons include `lease_lost`,
`timeout`, and `process_failure`. `requested_retry` and an orderly
`manual_continue` may have `is_unclean_restart: false`.

Thus a shell or Python step does not infer restart from leftover filenames. It
reads `HTTK_WORKFLOW_CONTEXT` (or the scalar variables), then uses the existing
files according to application policy.

Attempt-control directories are transient metadata. Persistent workspaces are
application data and MUST NOT be garbage-collected merely because an attempt
ended. Isolated workspaces may be collected under their retention policy. No
state transition depends on cleanup.

## Publishing an outcome

Exit codes are not the workflow protocol. A step communicates in the
attempt-control directory named by `HTTK_WORKFLOW_CONTROL_DIR`:

1. Create `outcome.tmp.<nonce>/`.
2. Write `outcome.json` and any transaction or child bundles inside it.
3. Close all files.
4. Atomically rename the directory to `outcome.ready/`.
5. Exit.

The directory rename is the **outcome publication point**. Temporary outcomes
are ignored. The fixed destination is nonempty, so a second publication MUST
fail rather than replace the first.

A minimal outcome is:

```json
{
  "format": "httk-workflow-outcome",
  "format_version": 1,
  "job_id": "01234567-89ab-cdef-0123-456789abcdef",
  "activation_id": "e7f86a0e-34d6-45a7-b92d-3f4b2dc98c54",
  "attempt_id": "a6c2c973-29e1-44e2-9649-ae419e340ac4",
  "action": "advance",
  "next_step": "collect",
  "message": "relaxation converged"
}
```

`expected_data_generation` is required only when the outcome contains a
transaction. It MUST equal the generation supplied in the attempt context.

Actions are:

| Action | Required information | Effect after commit |
| --- | --- | --- |
| `advance` | `next_step` | Create a ready activation for that step. |
| `wait` | `next_step`, `join` | Register children and wait for the join. |
| `succeed` | none | Enter successful terminal state. |
| `fail` | `failure` | Record a declared failure and enter failed. |
| `retry` | `retry.reason` | Retry this activation under policy. |
| `pause` | `pause.reason` | Require an operator request. |

After observing a valid outcome, the manager appends a committing frame and
renames `running` to `committing` before modifying durable job data or
registering children. This fences the step and makes interrupted outcome replay
explicit in the state tree.

If a process exits without an outcome:

- exit status zero is a `protocol_error`, because success is ambiguous;
- nonzero exit is `process_failure`;
- manager timeout is `timeout`;
- loss of manager/allocation is `lease_lost`.

Retry policy decides whether these create another attempt or
`retry_exhausted`. A declared `fail` is permanent by default. A step requesting
a managed retry uses `retry`.

A valid current outcome is authoritative over the process exit status. An
outcome from a fenced attempt is retained only for diagnosis.

## Optional transactional contributions to `data/`

### Visibility guarantee

This section applies only when `job.json` declares
`"data": {"mode": "transactional"}`. Such a step MUST NOT modify committed
`data/` directly. It publishes a replayable transaction in its outcome.

A job with `data.mode` equal to `none` omits `data/` and the transaction bundle.
In particular, a persistent-workspace job may freely update application files
such as `WAVECAR` in `run/`; those files are not workflow-protocol metadata.

The required guarantee is:

> A later attempt starts only after either none of a transaction or all of it
> has been applied to `data/`.

POSIX cannot atomically rename several unrelated paths. Raw observers looking
inside `data/` during `committing` may therefore see replay in progress. The
atomic boundary is between workflow attempts: no runner is launched while the
marker is `committing`.

Applications that require a simultaneously atomic tree for external readers
MAY contribute one complete version directory and atomically replace a single
`current` name. That is an application-level use of the same protocol, not a
mandatory per-step revision directory.

### Transaction bundle

```text
.httk-attempt.<attempt-id>/outcome.tmp.<nonce>/
├── outcome.json
└── transaction/
    ├── manifest.json
    ├── payload/
    │   ├── results/energy.json
    │   └── restart/CHGCAR
    └── trash/
```

Example:

```json
{
  "format": "httk-workflow-transaction",
  "format_version": 1,
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

Paths are normalized relative POSIX paths. Absolute paths, empty components,
`.`, `..`, NUL bytes, and paths into protocol control data are forbidden.
Operations MUST NOT overlap.

Required operation types are:

- `make-dir`: create an application directory and any explicitly declared
  missing parents;
- `put-file`: atomically install or replace one regular file;
- `remove`: atomically rename an existing path into transaction trash;
- `put-tree`: install a complete directory tree, normally at a previously
  absent path;
- `replace-tree`: move the old tree to transaction trash, then install the new
  tree.

Put operations declare a content digest. Replacement and removal operations
MAY declare an expected old digest or require absence. Preconditions prevent a
replayed or stale transaction from overwriting an unexpected workspace.
Symlinks, devices, sockets, and FIFOs are forbidden by default.

### Idempotent replay

While the marker is `committing`, a manager applies operations in manifest
order using atomic renames:

- source present, destination not yet new: validate and rename source to
  destination;
- declared directory absent: create it; an already matching directory means
  `make-dir` already happened;
- source absent, destination has the declared new digest: the put already
  happened;
- removal target absent, matching trash entry present: removal already
  happened;
- both old tree and new source present during `replace-tree`: continue its
  defined two-rename sequence;
- any other combination: stop with transaction corruption rather than guess.

The manifest and deterministic operation IDs supply all replay information. No
per-operation progress marker files are required.

After all operations validate as applied, the manager:

1. appends the destination state frame with incremented data generation when the
   transaction changed data;
2. renames the exact committing marker to ready, waiting, succeeded, failed, or
   paused;
3. only then permits transaction trash and, in isolated mode, an old isolated
   workspace to be collected.

If interrupted after the data changes but before the final marker rename, a new
manager sees `committing`, reads the outcome named by the committing frame, and
idempotently completes replay. It does not rerun the step.

This retains the useful httk v1 `ht.atomic.*` principle while avoiding one
permanent revision and manifest hierarchy per step.

## Relocating and transferring jobs

Arbitrary placement is dynamic. A job may move after submission, but a raw
`mv` of an authoritative payload is not a state transition: the marker would
still name the old placement. Relocation is therefore a short replayable
protocol.

### Relocation within one store

Only a quiescent job may relocate. `ready`, `waiting`, `paused`, `failed`,
`succeeded`, and `cancelled` are quiescent. A claimed or running job must first
be released or fenced; a committing job must finish replay.

To move from placement `A` to placement `B`, a manager:

1. validates that `B/<job-key>` does not exist;
2. appends a `relocating` frame containing source `A`, destination `B`, the
   prior logical state, priority, and exact expected record;
3. renames the exact marker from `state/<old-kind>/A/` to
   `state/relocating/A/`, fencing scheduling;
4. atomically renames payload directory `A/<job-key>` to `B/<job-key>`;
5. appends a destination frame with placement `B` and the prior logical state;
6. renames the same marker from `state/relocating/A/` to
   `state/<old-kind>/B/`.

The marker remains the sole authority throughout. Recovery from `relocating`
uses these cases:

| Source payload | Destination payload | Recovery |
| --- | --- | --- |
| Present | Absent | Perform the payload rename and finish. |
| Absent | Present with matching job ID/digest | Payload rename happened; finish the marker transition. |
| Present | Present | Stop: destination collision or non-atomic copy. |
| Absent | Absent | Stop: payload loss. |

Creation and later removal of empty placement parents are not state changes.

A batch relocation MAY move a common project/shard prefix with one directory
rename. It first moves every affected job marker to `relocating` and journals
one batch ID and the complete member set. Only after all members are fenced does
it rename the common payload prefix. It then returns each marker to its mirrored
destination placement. Recovery uses the batch record; no per-job coordinator
files are required.

### Moving new jobs into a running store

Files may be copied or generated under `.httk-workflow/tmp/` while managers
continue working. The complete payload is renamed to any chosen placement and
becomes schedulable only when its `submitted` marker is published. A partial
copy has no marker and is invisible.

A whole project tree containing many complete, unsubmitted payloads may be
renamed into a placement prefix at once. Their markers are then published from
a validated batch manifest. Marker publication is intentionally one job at a
time; a batch requiring an all-at-once scheduling barrier should initially
publish its members paused and release them through an explicit batch request.

If the source is on another filesystem, the copy must finish and be validated
inside the destination store before marker publication. Atomic rename is relied
on only for the final same-filesystem publication.

### Moving jobs between stores

An individual job can transfer atomically between two stores only when both
control trees and the payload source/destination are on the same filesystem. A
coordinator attached to both stores:

1. moves the quiescent source marker to `transferring`, recording source and
   destination store IDs, placements, transfer ID, and prior logical state;
2. renames the payload into the destination store;
3. appends the import frame to a destination-store journal;
4. renames the same marker inode from the source `transferring` tree to the
   mirrored destination state tree.

Until step 4, the source transferring marker remains authoritative and points
to both possible payload locations for recovery. The destination cannot run the
job early.

Moving across filesystems is necessarily copy-and-acknowledge rather than one
atomic filesystem transaction. The source job must first be sealed in
`transferring`; the destination publishes a marker only after a complete copy
and transfer-token validation; the source is retired only after explicit
acknowledgement. A backend implementing this profile must document its
duplicate-suppression and failure policy.

To detach one job without an immediately attached destination store, the
manager moves it to `transferring`, writes one compact
`.httk-transfer/manifest.json` inside the payload, and renames the authoritative
marker into `.httk-transfer/` inside that payload. The directory is then a
sealed, nonschedulable bundle that can be moved out. Import places and validates
the complete bundle first, appends an import frame, then renames the embedded
marker into the target store's state tree. The extra transfer metadata exists
only while the job is detached or retained for transfer provenance.

For moving whole projects, a self-contained store is preferable: controlled
detach and attach carry its state tree, journals, and all arbitrary placements
together.

## Dynamic branching and joins

### Child publication

A step places complete child bundles in its outcome:

```text
.httk-attempt.<attempt-id>/outcome.tmp.<nonce>/
├── outcome.json
└── children/
    ├── spawn.json
    └── jobs/
        ├── branch-a--<child-uuid>/
        │   ├── job.json
        │   └── ...
        └── branch-b--<child-uuid>/
            ├── job.json
            └── ...
```

Each child `job.json` names parent store, parent job, parent activation, and
spawn ID. `spawn.json` chooses the target store and arbitrary placement for
each child. Child UUIDs and tags are chosen before outcome publication.

While the parent is `committing`, the manager:

1. moves each complete child bundle to its chosen
   `<store>/<placement>/<job-key>` path;
2. creates its one `g0.init` marker at the mirrored target-store path below
   `state/submitted`;
3. treats an identical existing child plus marker as already registered;
4. fails on the same UUID with different immutable content.

Children are registered before the parent leaves committing. A crash may expose
only some children, but replay registers the deterministic missing set.
Registration is not rolled back. Published children may start before the parent
has completed its transition; that is safe.

When a target store is on another filesystem, the child is first copied into
that store's temporary area and validated there. Its placement rename and
submitted-marker publication then occur within the target filesystem while the
parent remains committing.

Each child is an ordinary independently schedulable job with one job file, one
state marker, its own attempts, and the ability to create more children.

### Waiting and joining

A wait outcome explicitly names its child set:

```json
{
  "action": "wait",
  "next_step": "aggregate",
  "join": {
    "children": [
      {
        "store_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
        "job_id": "411d9e6e-c050-451d-a851-e20f2570d7c5",
        "job_key": "branch-a--411d9e6e-c050-451d-a851-e20f2570d7c5"
      },
      {
        "store_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
        "job_id": "7ead0705-e1bb-4290-9ebd-fc1b24df9005",
        "job_key": "branch-b--7ead0705-e1bb-4290-9ebd-fc1b24df9005"
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

Supported conditions are:

- `all_succeeded`;
- `all_terminal`;
- `any_succeeded`;
- `at_least`, with a successful-child count.

The waiting journal frame contains the exact child keys and condition. No join
file or child marker is added to the parent directory. New unrelated descendants
cannot affect the join.

A manager finds each child's one authoritative marker in its specified store,
using the job key. Managers will normally cache the job-key-to-marker mapping
while recursively scanning arbitrary placements, but verify decisions against
the marker. A controlled store transfer leaves a forwarding frame at the source
store; jobs in an unresolved active join SHOULD NOT be transferred without such
a forwarding record.

When satisfied, the manager appends a ready frame for a new activation and
renames waiting to ready. Its attempt context summarizes the exact child state
generation, terminal data generation, and failure information. Child committed
data may be exposed through read-only paths named in the context. A
transactional parent imports selected child data through its own transaction.
A non-transactional workflow may instead let application code inspect or copy
child data into its persistent workspace according to its own conventions.

The join summary is a snapshot. Later manual continuation of a child does not
change what the joined parent saw.

If success becomes impossible, `on_impossible` selects an error-handling step or
the parent fails with `dependency_failure`. Cancellation is terminal but not
successful.

An advance or succeed outcome may also publish detached children without a
join.

## Failures and tracking broken jobs

A step declares itself broken with a `fail` outcome:

```json
{
  "action": "fail",
  "failure": {
    "code": "vasp.nonconvergent",
    "summary": "electronic minimization did not converge",
    "details_path": "vasp/convergence.log",
    "retryable": false
  }
}
```

The manager embeds the structured failure in the failed state frame and moves
the one marker to `state/failed/<placement>/`. No `failure.json`,
`ht.reason`, per-job event directory, or second failure index marker is
created.

Manager failure classes include:

- `protocol_error`;
- `process_failure`;
- `timeout`;
- `lease_lost`;
- `retry_exhausted`;
- `dependency_failure`;
- `resource_unsatisfiable`;
- `transaction_corruption`;
- `manager_error`;
- `cancelled`.

The failure frame contains job, step, activation, and attempt IDs; class;
application code and summary; exit status or signal; retry history; manager ID;
relevant retained log paths; and data generation. For a job with `data.mode`
equal to `none`, data generation is `null`.

Current broken jobs are exactly the markers below `state/failed/`. This
directory tree is authoritative and requires no reconciliation.

If a failed job is manually continued, its marker moves elsewhere, but its
failed frame remains in the backwards-linked journal history. “Ever failed”
queries are answered from a compact journal scan, an optional derived database,
or a periodically generated report. They intentionally do not consume another
per-job marker inode.

Logs are evidence, not state. Missing or truncated logs cannot prevent
recovery.

## Manual continuation and control requests

Operators MUST NOT edit state markers, journal segments, or `job.json`.
They publish one temporary request file under `requests/ready/` by sibling
rename. A request names:

- job UUID and job key;
- exact expected marker generation and record reference;
- requested action;
- operator identity and reason;
- any selected retained files for manual import.

Actions include:

- `continue`: retry the current activation;
- `override_step`: create a new activation at a named step;
- `cancel`;
- `set_priority`;
- `pause`.

A manager claims the request by rename, verifies the exact expected current
marker, appends the new journal frame, and renames that marker. A delayed
request cannot apply to a newer state because its expected generation no longer
matches.

The result is written to the shared journal. The transient request file may be
removed after retention policy permits; there is no per-job request directory.

Manual continuation preserves failure history. In persistent mode it reuses the
declared workspace in place; the new attempt context records
`is_restart: true`, its cleanliness, and `attempt_reason: "manual_continue"`.
No file import or transaction is required. In isolated mode, selected retained
files may instead be imported into the new workspace; if they also become
committed `data/`, that contribution uses a transaction.

## Required outcome-processing order

For a valid current outcome, a manager:

1. validates job, activation, and attempt, plus expected data generation when a
   transaction is present;
2. appends a committing frame;
3. renames the exact running marker to committing, fencing the attempt;
4. applies or verifies the transaction, if present;
5. registers or verifies every child;
6. computes failure or join information;
7. appends and synchronizes the destination state frame;
8. renames the exact committing marker to the destination;
9. performs optional cleanup later.

Interruption recovery follows directly:

| Interruption point | Recovery |
| --- | --- |
| Before marker submission | No job was submitted; placed orphan may be retried or collected. |
| In `submitted` | Revalidate and move the same marker to ready. |
| Before `outcome.ready` | Ignore temporary outcome; retry under policy. |
| After outcome publication, before committing | Apply that outcome; do not rerun the step. |
| While a transaction is replayed | State remains committing; infer completed operations from source, destination, trash, and digests. |
| While children are registered | Verify existing children and register the missing set. |
| After destination frame, before marker rename | Frame is prepared but not current; replay and rename. |
| After marker rename | New state is already authoritative; cleanup is optional. |
| Old fenced process publishes late | Never apply it. |

Before recovering a stale claimed or running job, a manager MUST inspect its
named attempt-control directory for a valid published outcome. A committed
decision must not be mistaken for an abandoned attempt.

## Task-manager startup and recovery

A manager:

1. discovers explicit and watched stores and validates each `format.json`;
2. creates a manager record, journal writer, and heartbeat in each attached
   store;
3. resumes markers in `committing`, `relocating`, and `transferring`;
4. examines possibly abandoned claimed and running markers;
5. evaluates waiting joins, including references to other attached stores;
6. handles submitted jobs and operator requests;
7. claims ready work in priority and resource order;
8. continues watching for stores being attached, renamed, or detached.

It does not need to reconcile a separate state index. Listing `state/` is
listing the authoritative scheduler state.

A high-scale implementation SHOULD:

- scan only non-terminal state placement prefixes relevant to its configured
  projects or job set;
- keep an in-memory job-key-to-marker map while running;
- process arbitrary placement subtrees incrementally rather than repeatedly
  scanning every attached store;
- use filesystem notifications only as hints, since notifications may be lost;
- store task-manager query caches outside per-job directories.

## Garbage collection and compaction

The following may be collected under explicit retention policy:

- unpublished store temporary entries;
- placed payload directories that never reached submitted state;
- journal frames not referenced by any marker or history chain;
- incomplete outcome directories;
- obsolete attempt-control directories;
- abandoned and completed isolated workspaces;
- transaction trash after the destination marker transition;
- retained diagnostic application files.

Journal compaction operates on shared segments, not by creating files per job.
It must preserve every record reference reachable from a current marker and the
configured amount of history.

Terminal `job.json`, committed application data, the one state marker, and
required journal history are retained according to site policy. Age alone is
not permission to delete a non-terminal job.

A persistent workspace is application data, not attempt scratch. It is retained
until an explicit job or site retention rule permits its removal, including
after failure or manual continuation.

No recovery operation begins by broadly deleting `tmp`, run, or unknown files.
Cleanup is separate from correctness.

## Manual filesystem inspection

The layout is intentionally legible without a database:

- `state/ready/<arbitrary-placement>/` is the runnable queue, with `p000`
  through `p999` encoded in marker names;
- `state/running/` is everything presently believed to execute;
- `state/committing/` is exactly the replay backlog;
- `state/waiting/` is the join backlog;
- `state/failed/` is the current broken-job collection;
- `state/succeeded/` is the finalized-success collection;
- every marker begins with the optional human tag plus UUID job key;
- the matching payload is at the same relative placement outside
  `.httk-workflow`;
- a first placement component such as `project-17` groups a project without
  imposing a protocol-specific hierarchy.

Operators may read all of these paths. They must use a workflow control command
rather than `mv` for state changes, because a correct transition must append
the matching journal record and validate the expected old generation.

An inspection tool should accept any of:

- UUID;
- store UUID;
- exact job key;
- marker path;
- current payload path;
- placement-prefix query;
- tag query, returning all matches.

It follows the marker's journal reference and backwards links to render state,
step, retries, manager ownership, failure, children, and manual intervention
history.

## Security and hostile input

Task code is arbitrary code and should run under an appropriate OS, container,
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
  `data/`. Writes to the declared application workspace are allowed.

## Persistent VASP restart example

A VASP workflow that wants traditional in-place execution can declare:

```json
{
  "workspace": {"mode": "persistent", "path": "run"},
  "data": {"mode": "none"}
}
```

The behavior is then:

1. The first activation runs with `run/` as its working directory,
   `is_restart: false`, and `HTTK_WORKFLOW_UNCLEAN_RESTART=0`.
2. VASP writes `WAVECAR`, `CHGCAR`, `OUTCAR`, and any other application files
   directly in `run/`. The manager does not copy, rename, or interpret them.
3. If execution disappears without publishing an outcome, the manager fences
   that attempt. After establishing that the old writer cannot still modify the
   workspace, it starts a new attempt in the same `run/`.
4. The replacement context says `is_restart: true`,
   `is_unclean_restart: true`, and, for example,
   `attempt_reason: "lease_lost"`. The step may validate or remove partial
   files, adjust `INCAR`, and resume from `WAVECAR` using domain-specific logic.
5. If the step publishes `advance`, the next workflow step may also use the
   same `run/`, but it receives `is_restart: false`: this is its first attempt,
   not a restart merely because the workspace already contains files.
6. A later operator `continue` again reuses `run/` and is explicitly identified
   as a restart. No transactional `data/` output is involved.

For a POSIX shell step, the essential test can be as small as:

```sh
if [ "$HTTK_WORKFLOW_UNCLEAN_RESTART" = 1 ]; then
    repair_or_validate_vasp_restart_files
fi
exec vasp_std
```

The context JSON remains the full, versioned interface; the environment
variables are convenient projections for simple runners.

## Worked example

This example deliberately uses isolated workspaces and transactional data.
Job `silicon-relax--J` starts at `prepare`, creates two calculations, joins
them, and finalizes:

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
12. Completed isolated workspaces are later collected. The payload retains one
    job file and application data; its one external state marker mirrors its
    arbitrary placement, and history is packed with many other jobs in journal
    segments.

At every interruption point, the marker location selects exactly one recovery
rule.

## Relationship to httk v1

The future compatibility layer can map:

| httk v1 | This protocol |
| --- | --- |
| `ht.task.<set>.<id>.<step>...<status>` | One global state marker plus a packed state frame |
| Rename to `*.running` | Exact marker rename to claimed/running |
| Stale directory `ctime` | Lease evidence followed by marker fencing |
| `ht.run.current` | Persistent `run/` or isolated `run.<attempt-id>/` |
| `ht.nextstep` plus exit code 2 | `advance` outcome |
| Exit code 3 / `waitsubtasks` | `wait` outcome with explicit child set |
| Exit code 4 / broken | `fail` outcome and failed journal frame |
| `ht.reason` | Structured failure in packed history plus retained log |
| `ht.tmp.task.*` to `ht.task.*` | Child bundle to placed payload plus one marker |
| `ht.tmp.atomic.*` / `ht.atomic.*` | Published transaction plus committing replay |
| Restart count in pathname | Activation ID and attempt ordinal in state frame |
| `ht.run.resume` | Audited manual continuation, reusing a persistent workspace or explicitly importing into an isolated one |

The adapter must preserve the v1 ordering rule that a published
`ht_finished`/broken decision or pending atomic transaction is completed without
rerunning `ht_steps`.

The principal improvements are:

- one authoritative, atomically moving state entry rather than a state plus a
  potentially stale index;
- a constant one-marker state inode cost per job;
- transition, failure, and event metadata packed into shared journal segments;
- optional human-readable path tags without weakening UUID identity;
- arbitrary project/shard placement and crash-safe relocation;
- dynamic multi-store attachment for combining projects;
- explicit attempt fencing and restart detection;
- explicit child sets and join policies;
- transactional replay with no permanent revision-metadata tree per step;
- structured, durable manual and failure history.

The filesystem remains the interoperability layer, but its steady-state inode
cost is small enough for multi-million-job stores and its state remains directly
understandable to a human with ordinary filesystem tools.
