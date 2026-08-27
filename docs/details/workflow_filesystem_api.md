# Workflow filesystem API in detail

*For implementers of this protocol, and for anyone who needs to know exactly what a
workspace on disk means.*

## Status and scope

This document specifies the filesystem protocol around which the
*httk-workflow* engine is built. It is the normative on-disk protocol rather
than Python API documentation. The current `httk.workflow` implementation
writes and serves the `core-v2` profile. Core-v2 includes transactional data,
detached transfer, and replay-after-stop semantics; relocation and
cross-workspace children remain reserved future capabilities and are rejected
rather than partially executed.

`core-v2` defines the current on-disk protocol: every `spawn.json` entry
carries a mandatory unique `label`; a join summary records typed per-child
observations which the next activation also reads as `children` in its attempt
context; `runner` in `job.json` may name a shared runner outside the payload
through `source` and `sha256`; and a runner-declared failure marked
`retryable` is retried within the job's existing attempt budgets. Transaction
publication, sealed job transfer, and recovery after a stopped manager are also
core semantics.

The protocol is language independent. A workflow step may be a shell script, a
Python program, a compiled executable, or any other program that can read and
write files and atomically rename a file or directory.

The design descends from the `ht.task.*` directories, `ht_steps`,
`ht.nextstep`, subtasks, and `ht.atomic.*` replay mechanism in *httk* v1. It keeps
the central property of that design: neither a workflow step nor a task manager
is ever required to run cleanup code. Either may disappear between any two
instructions. A later task manager must be able to identify the last commit
point and continue from it.

The design target is workspaces larger than the measured local snapshot in
{doc}`/benchmarks`; that target is not a capacity measurement. Metadata inode
count, directory fan-out, scheduler scan cost, and manual filesystem inspection
are therefore correctness-level design concerns, not later optimizations.

The protocol covers:

- submission and execution of dynamic, multi-step jobs;
- safe competition between any number of task managers;
- automatic and manual continuation;
- dynamic fan-out into child jobs and later joins;
- transactional contributions to durable job data;
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
   execution may use either one persistent `run/` workdir or an isolated
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
| Runner | 0 files | Shared and referenced by digest, unless it lives in the payload |
| `.httk-job/` runner job state | 0 or 1 directory | Job lifetime, when a runner keeps state across attempts |
| Attempt control directory | 0 or 1 directory | Attempt/retention lifetime |
| Persistent/isolated workdir | 0 or 1 directory | Application policy |
| Per-state/per-event/per-failure files | 0 | Not used |

**Design arithmetic, not a measurement:** the permanent floor is therefore
**three inodes**: the payload directory, its
`job.json`, and the one marker. The floor is reachable in practice and not only
in theory, because a runner may live outside the payload — `runner.source`
naming the workspace runner store or an installed search path, pinned by
`runner.sha256` — so a partitioned campaign whose children all execute the same
program stores that program once and each child's payload is exactly its
`job.json`. A payload runner is the alternative, not the requirement, and costs
whatever its own files cost. A runner that keeps state across the attempts and
steps of one job adds `.httk-job/`, and a live or retained attempt adds its
attempt-control directory and workdir.

Shard and journal directories are shared by many jobs. Application inputs,
outputs, code, and logs naturally add their own files; the table only counts
workflow metadata. Arbitrary placements can nevertheless make some placement
directories unique to one job, and old state kinds can temporarily retain
empty copies of those paths. That directory overhead is operationally relevant
even though it is not a permanent per-job protocol object.

## Concepts

A **workflow workspace** is one self-contained filesystem tree with an immutable
workspace UUID, its jobs, authoritative state markers, and journals.

A **watch root** is an ordinary directory below which a manager discovers
workflow workspaces. A manager may also receive explicit workspace paths and may
supervise several workspaces at once.

A **job** is the durable unit of scheduling, history, and final success or
failure.

A **job key** is the filesystem component
`[<tag>--]<job-uuid>`. The UUID is authoritative; the optional tag is for human
navigation.

A **placement** is an arbitrary relative parent path below a workspace, for example
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

All correctness-critical paths within one workflow workspace MUST reside on one
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

The following require an explicit executor adapter and validation:

- source and destination paths on different mounts;
- object stores that only emulate rename;
- synchronization tools operating on a live root;
- network filesystems without coherent atomic rename;
- filesystems on which a client can indefinitely cache a removed name.

Modification times and wall clocks are evidence for lease expiry, but never
provide fencing. Correctness comes from moving the one current state marker.

## Conformance profiles

The protocol is deliberately phased. A conforming **core** implementation of the
current profile, **core-v2**, supports:

- one workflow workspace per job and all references within that workspace;
- submission, validation, claiming, leases, execution, and outcomes;
- persistent and isolated workdirs;
- retries, joins, failures, cancellation, and operator requests;
- packed journals, verified marker renames, startup recovery, and garbage
  collection;
- transactional data publication and replay;
- sealed detached transfer and replay after a stopped manager.

`format.json` carries an `extensions` array for future protocol additions; it is
empty in this release, and a workspace declaring an unknown extension refuses
to attach. Unknown state kinds are never treated as failed or orphaned jobs.

Priority is encoded in marker names rather than directory levels, and scheduling
is best effort rather than a strict global ordering guarantee.

## Workspace layout and arbitrary placement

A workspace is an ordinary directory. Its protocol control data are below
`WORKSPACE/.httk-workflow/`; job payload directories may be placed at any valid
relative path outside that reserved directory:

```text
WORKSPACE/
├── .httk-workflow/
│   ├── format.json
│   ├── tmp/
│   ├── quarantine/
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
│   │       └── heartbeat.json
│   └── requests/
│       ├── tmp/
│       ├── ready/
│       ├── claimed/
│       └── retired/
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
jobs in one workspace may use completely different placement schemes.

The layout includes the transfer state directories used by core-v2, and no
empty state-kind or placement directories are required.

`files/`, `data/`, a workdir, and attempt control are created only when
required. Empty placeholder directories SHOULD NOT be created. Empty placement
directories have no protocol meaning and may be pruned, subject to the
rename/prune rules below.

`format.json` identifies the self-contained workspace:

```json
{
  "format": "httk-workflow-filesystem",
  "format_version": 2,
  "core_profile": "core-v2",
  "extensions": [],
  "record_ref_encoding": "hwref-v2",
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
  "created_at": "2026-07-24T12:00:00Z",
  "policy": {
    "visibility_deadline_seconds": 5.0,
    "lease_seconds": 900.0,
    "journal_segment_bytes": 67108864,
    "retention": {}
  }
}
```

### Workspace policy

Everything this specification calls *configured* is one object in
`format.json`, because a tunable that lives in each process cannot be agreed
on by two implementations attaching the same workspace. The `policy` object is
part of format version 2 and holds exactly these members:

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `visibility_deadline_seconds` | number | `5.0` | How long a reader keeps re-probing before it declares a marker rename or a referenced journal frame incoherent. |
| `lease_seconds` | number | `900.0` | The default claim lease of a manager that does not state its own. |
| `journal_segment_bytes` | integer | `67108864` | The size at which a writer rotates to its next journal segment. |
| `retention` | object | `{}` | Optional `attempt_control_days`, `journal_days`, and `trash_days` limits for collection. |

An implementation MUST refuse a `policy` member it does not know rather than
ignore it, and MUST read an absent `policy` object, or an absent member of one,
as the default above; a workspace written before this section existed is
therefore attached without migration. Values are validated on write:
`lease_seconds` is at least one second, `visibility_deadline_seconds` is at
most one day, and `journal_segment_bytes` is at least 4096.

Policy is administrative rather than protocol state. A change is an ordinary
read-modify-write of `format.json` through an exclusively created temporary
file and a rename, so no reader ever sees a torn object, but concurrent policy
writers are not serialized against each other and the last writer wins. A
manager reads policy when it attaches; a change reaches already-running
managers only when they restart.

There is no configured sharding depth and no priority directory level at all:
priority is encoded in marker names. A marker's path below its state kind is its
placement and nothing else.

Managers therefore provide best-effort priority scheduling: a cold start and an
incremental scan may temporarily discover lower-priority work first. Strict
global priority is not a guarantee of this protocol.

`.httk-workflow/tmp/` contains unpublished entries. Task managers MUST ignore
it for scheduling. Garbage collection may remove old temporary entries, but
correctness MUST NOT depend on cleanup.

Placement components MUST be normalized relative path components. Empty
components, `.`, `..`, NUL bytes, and `.httk-workflow` are forbidden. Each
component must fit the underlying filesystem's filename limit. A workspace MAY set
policy limits on depth and total relative path length, but these are operational
limits rather than a protocol sharding scheme.

## Workspace ownership

A workspace is single-user. Multi-user shared workspaces are not supported yet;
the ownership model needs more work before they can be safe.

The kernel ownership chain of a job marker, its payload directory, and that
payload's `job.json` is the ownership oracle: all three entries must be regular,
non-symlink entries owned by the manager's uid. A manager claims only such jobs.
Child jobs belong to the manager's account, and imported jobs belong to the
account that imports them.

## Managing and combining workspaces

A workspace name is a client and user-interface concept. This filesystem API
remains path- and `workspace_id`-based: callers provide paths, and the protocol
identifies the workspace by its immutable ID.

A core manager may attach several independent workspaces for operational
convenience, but jobs and joins remain within their own workspaces. Cross-workspace
references and coordinated movement are reserved future capabilities.

A task manager accepts any combination of:

- explicit workspace paths;
- watch roots below which it discovers directories containing
  `.httk-workflow/format.json`.

The manager identifies a workspace by `workspace_id`, not its current absolute path.
The same underlying workspace discovered through two path aliases is attached
once. Journal references are workspace-relative and include the workspace ID when used
from another workspace.

If two discovered roots declare the same `workspace_id`, a manager MUST prove that
they identify the same underlying directory before treating them as aliases.
Suitable evidence is an equal filesystem object identity obtained from open
directory handles, device/inode identity where reliable, or an equivalent
executor facility. Equal `format.json` bytes, UUIDs, or path canonicalization
alone are insufficient because a copied backup has all three. If equivalence
cannot be proved, the manager MUST refuse both roots for mutation and report a
loud duplicate-workspace-ID error; it MUST NOT attach an arbitrary winner.

This permits both common arrangements:

```text
# One workspace with projects as placement prefixes
WORKSPACE/project-a/00/17/<job-key>
WORKSPACE/project-b/hash/x9/<job-key>

# A watch root containing self-contained project workspaces
WATCH/project-a/.httk-workflow/format.json
WATCH/project-a/00/17/<job-key>
WATCH/project-b/.httk-workflow/format.json
WATCH/project-b/hash/x9/<job-key>
```

A manager may schedule from all attached workspaces in one resource pool. Job and
child references are `(workspace_id, job_id)` pairs; the job UUID alone is accepted
only when unambiguous among attached workspaces.

Discovery stops at a workspace boundary. Attached workspaces MUST NOT overlap unless an
explicit advanced profile defines ownership of every placement prefix; the
default manager rejects nested/overlapping workspace roots.

### Dynamic attachment

A complete workspace can be built elsewhere on the same filesystem and atomically
renamed below a watch root. Its `format.json`, state tree, journals, and
payloads arrive together. The manager discovers the immutable workspace ID and
begins scheduling it without restarting.

Filesystem notifications are hints. Managers periodically rescan watch roots so
a lost notification cannot hide an attached workspace.

Renaming an attached workspace within watched paths does not create a new workspace.
Managers SHOULD hold an open directory handle and update the path associated
with the same workspace ID. Copying a live workspace is forbidden: it duplicates
authoritative markers and journal identity. Discovery of such a copy is handled
by the duplicate-ID refusal above, including when an operator restores a backup
beside the live workspace.

### Dynamic detachment

Detaching one workspace does not stop a manager from serving its other workspaces. A
detach coordinator obtains acknowledgement from every manager with a live
heartbeat in that workspace. Each manager:

1. stops new claims from that workspace;
2. completes or releases manager-owned claimed jobs;
3. completes committing replays;
4. waits for, pauses, or explicitly hands off running attempts;
5. closes the workspace journal writer;
6. acknowledges that the workspace is quiescent for it.

Once all live managers acknowledge and no marker is claimed, running, or
committing, the workspace directory may be atomically moved out of the watch root.
A manager MUST NOT interpret disappearance of a workspace as failure of every job
in it. It marks that workspace unavailable until the same workspace ID is reattached.

A raw move of a workspace containing active attempts is supported only when the
same supervising managers follow the atomic rename by workspace ID and retain valid
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
lookup returns at most one job within a workspace.

The current payload path is:

```text
<workspace>/<placement>/<job-key>/
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

`generation` is a monotonically increasing unsigned 64-bit state generation,
encoded in lowercase base 36 without leading zeroes. It is unrelated to the
data generation. `record-ref` locates the immutable transition record in a
packed journal. The initial submitted marker uses `g0.init`, because its
initial information is in `job.json`. Priority is always a zero-padded
three-digit integer from `000` through `999`.

The complete relative marker path is the authoritative current state:

- its state directory gives the state kind;
- the directories after the state kind give current placement, and nothing
  else: no priority, shard, or index level is ever inserted between them;
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

Outside the explicitly recoverable `relocating` and `transferring` states, a
marker without its payload at the mirrored placement is corruption. A payload
directory without a marker is unsubmitted temporary/orphan data, not a queued
job, unless it is a sealed detached bundle containing
`.httk-transfer/manifest.json` and its marker.

### State-marker rename

Before a transition, the actor:

1. prepares and synchronizes the new journal record;
2. derives the new marker basename from its journal reference;
3. creates the destination placement parents if absent;
4. attempts to rename the exact old marker to the unique destination;
5. resolves the observed filesystem state before deciding whether it won.

The return status of `rename` is not itself a transition result. In particular,
`ENOENT` can mean that the source vanished or that a destination parent was
pruned, and a network filesystem can execute a rename even when the caller
receives a failure after retransmission. Every implementation MUST use this
verified-transition algorithm after a rename error or ambiguous response, and
MAY use it unconditionally:

1. Reopen or refresh the source and destination parent directories rather than
   relying on one cached negative lookup.
2. If the actor's unique expected destination exists and its basename resolves
   to the prepared journal record, the actor won. It proceeds even if
   `rename` reported failure.
3. Otherwise, if the exact source still exists, the actor recreates any missing
   destination parents and retries the same rename.
4. Otherwise, the actor locates and validates the job's current marker. A
   different valid marker proves that another transition won.
5. If source, expected destination, and another valid current marker all remain
   absent after bounded reopen/backoff retries, the workspace is unavailable or
   corrupt. The actor MUST stop mutation and report that condition; it MUST NOT
   silently classify the result as a lost race.

Record references are globally unique within a workspace, so the expected
destination MUST initially be absent. An implementation SHOULD use no-replace
rename where available. An already present expected destination is success
only when it names the actor's prepared record; any conflicting entry is
corruption.

This algorithm applies to every correctness-critical rename in this
specification: marker transitions, submission, outcome publication, transaction
operations, child registration, relocation, and transfer.

When a loser prepared a journal record but another transition won, its
unreferenced record stays in the journal. That record is *off-chain* and is
harmless by construction, because every reconstruction of a job's history
starts at the authoritative marker and walks `previous_record_ref` backwards.
An orphan is on nobody's chain — no marker ever referenced it and no frame ever
named it as its predecessor — so it cannot be observed as a transition, and no
implementation may reconstruct history by scanning the journal forwards.
Readers therefore need no special handling of orphan records, and none is
specified: no tombstone, no supersession record, no compaction requirement.

The one operation that legitimately reads the journal *without* a chain to walk
is marker repair, because there the frame holding the backward link is exactly
the unreadable one. Repair is bounded instead: it considers only frames of the
same job at a generation strictly lower than the damaged marker's own, never
walks forward onto a frame no marker committed, and records the reference of
the frame it adopted in the repair frame it writes. An orphan always shares its
generation with the transition that actually won, so a repair that reaches back
across a lost race MAY adopt the loser's members; the recorded
`fsck_repair.recovered_record_ref` is what makes that visible and auditable
afterwards.

Empty state-placement parents MAY be removed only with operations that fail
when a directory is nonempty. A pruner racing a transition either observes the
new marker and fails to remove the directory, or removes the empty directory
first and causes the transition to recreate it and retry. Broad recursive
deletion is forbidden.

A pruner MUST make at most one removal attempt per candidate directory in one
scan and MUST NOT immediately retry a failed removal. It backs off until a
later scan. Transition parent-creation/rename retries are also bounded; if
repeated confirmed prune collisions would exhaust that budget, the manager
suppresses pruning for the affected subtree or workspace and retries the transition
before reporting storage failure. Pruning is optional and MUST yield to state
mutation. Implementations MAY simply disable placement-directory pruning while
managers are attached.

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

`writer-id` is a fresh random UUID generated for every task-manager **process
incarnation**. The `manager-id` used in claims and heartbeats is likewise a
fresh process-incarnation UUID; any stable administrative name is a separate
`manager_label`. The writer and manager UUIDs MAY be equal but neither may be
reused by a restarted process. On startup a process MUST create a new writer
directory and its first segment with exclusive creation. It MUST NOT reopen any
existing segment for append, even if it believes that segment belonged to a
previous incarnation of the same manager. A zombie and its replacement
therefore write different paths and heartbeats.

Each process is the sole writer of its own segments and never appends to
another writer's segment. Segments rotate at the size
`policy.journal_segment_bytes` configures, not per job.
Submission needs no journal file per invocation because the initial marker
uses `init`.

A core journal segment begins with the 12 bytes
`48 54 54 4b 2d 48 57 4a 2d 56 31 0a` (`HTTK-HWJ-V1` plus newline).
Segment filenames are lowercase base-36 numbers without leading zeroes. Each
frame then contains:

1. an eight-byte unsigned big-endian payload length;
2. exactly that many bytes of one UTF-8 JSON record;
3. the 32-byte SHA-256 checksum over the eight length bytes and payload;
4. the same eight-byte big-endian length as a trailer.

The marker's compact `record-ref` identifies writer, segment, byte offset,
length, and checksum. Format version 2 mandates the canonical filename-safe
`hwref-v2` encoding:

```text
w<writer-uuid-hex>-s<segment-base36>-o<offset-base36>-l<length-base36>-h<checksum128-hex>
```

The writer UUID is 32 lowercase hexadecimal digits without hyphens. Segment
numbers are unsigned 32-bit integers; byte offsets and frame lengths are
unsigned 64-bit integers. Numeric fields use lowercase base 36 without leading
zeroes except for zero itself.
`checksum128` is the first 128 bits of the SHA-256 checksum over the encoded
frame length and payload, rendered as 32 lowercase hexadecimal digits. The
complete checksum remains in the frame. No alternate encoding is permitted in
a core workspace, so independent implementations resolve marker names
identically.

The worst-case noninitial marker basename is 213 ASCII bytes:

```text
86 job-key + 5 ".p999" + 15 (".g" + 13 generation digits)
+ 1 "." + 106 maximum hwref-v2 = 213
```

The 86-byte job key is a 48-byte tag, `--`, and a 36-byte UUID. The reference
budget is `33` for `w` and the writer UUID, `9` for `-s` and a seven-digit
base-36 segment number, `15` each for `-o`/offset and `-l`/length, and `34` for
`-h` plus the checksum: `33 + 9 + 15 + 15 + 34 = 106`. A conforming workspace MUST
support at least 213 bytes per filename component and MUST validate this at
initialization. These field limits and the tag limit MUST NOT be enlarged
within format version 2.

Before a state-marker rename, the writer MUST flush the entire frame and, in
the storage-durable profile, synchronize the segment. A marker can therefore
never legally reference a torn tail. An unreferenced partial final frame is
ignored and may be truncated during journal repair.

This implementation runs the storage-durable profile by default. Durability is
not only a manager-side property of journals and markers: the runner-side
artifacts that publish work are synchronized to the same standard, because a
marker or journal frame that claims a committed outcome is worthless if the
outcome its storage should hold was never flushed. In the durable profile, each
of the following is synchronized — its file contents, then the directory entries
that name it — before the rename that makes it authoritative:

| Artifact | Synchronized before |
| --- | --- |
| Journal frame | the state-marker rename that references it |
| State marker / request / submitted payload | it becomes visible in `state/` (its parent directory is flushed) |
| Attempt `context.json`, `process.json` | the runner is launched against it |
| Outcome bundle (`outcome.json`, `runner_steps`, failure detail, its sealed transaction manifest and staged payload, its child `job.json` bundles) | the `outcome.tmp.<nonce>` → `outcome.ready` rename; the whole draft tree is flushed in one batch, then the attempt-control directory after the rename |
| Committed transaction data (`data/`) | the manager appends the destination state frame and renames the marker out of `committing`; every replayed destination and each parent directory it touched, including the trash a removal moved into, is flushed first |
| Registered child payloads and their submitted markers | the parent's marker leaves `committing` |
| Job state (`.httk-job/state.json`), observed declarations, `runner-steps.json` | each atomic replace returns |
| Sealed replayable workdir batch (`.httk-runner/workdir-ready/`) and its replay into the workdir | the batch is published, then retired as applied |

Two runner-side artifacts keep only process-interruption safety even in the
durable profile, because per-line synchronization would dominate their cost and
neither is authoritative workflow state: the append-only run log
(`.httk-runner/runlog.jsonl`) and the runner's captured `stdout.log` and
`stderr.log`. They are evidence for an operator, not markers or committed data;
losing their tail to a power cut costs a diagnostic line, never a lost outcome
or a half-applied transaction.

In the non-durable profile every one of these keeps process-interruption safety
only: a torn write or an interrupted rename is still never observed, but a node
that loses power may lose any of the above — a journal frame, a marker, a
published outcome, half of a "committed" transaction, or a registered child. The
opt-out (`--no-durable`) exists for throwaway and test workspaces where that
trade buys speed. Without it, a node that loses power can leave a marker naming
a frame or an outcome its storage never received, which is exactly the damage
`workspace fsck` below has to repair.

On a network filesystem, visibility of a marker and visibility of the newly
extended journal segment may reach different clients at different times. A
reader that sees a referenced frame as absent, short, or checksum-incomplete
MUST close and reopen or otherwise refresh the segment and retry with bounded
backoff. It declares corruption only after the workspace's configured
visibility deadline — `policy.visibility_deadline_seconds` in `format.json` —
and it MUST NOT mutate the marker while the referenced frame is temporarily
unreadable. Damage that no amount of waiting can repair, such as a frame whose
checksum is present and wrong, is reported at once rather than waited out. The
same deadline bounds the retries of a verified state-marker rename and of any
other metadata-visibility probe.

State frames have the following shape; the `resources` member is optional and is absent until an outcome sets it:

```json
{
  "format": "httk-workflow-state",
  "format_version": 2,
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

`data_generation` is omitted or `null` when `job.json` declares
`data.mode: "none"`.

An in-flight operator pause adds the optional `pause_requested` member to the
state frame; it is carried to the next attempt boundary, then consumed when the
job enters `paused`, while a terminal outcome supersedes it.

The optional state-frame `resources` member is the validated dynamic resource
requirement of the current activation. It is carried across attempts of that
activation; a new activation replaces it with the requirement selected for the
new activation.

State frames form a backwards-linked history across writer segments. Failure,
join, operator, and outcome details are embedded in the applicable state frame
or in another journal frame referenced by it. They do not create per-job
metadata files.

Journal segments are append-only and retained according to history policy.
They may be compressed only into a random-access archive format that preserves
record references. A derived SQL database or in-memory map MAY accelerate
queries, but it is a cache: the state tree and referenced journal frames remain
authoritative.

Three rules make such a cache safe, and they are what this implementation's
job-id-to-marker index obeys:

1. **A hit is confirmed before it is used.** The cached location is a path; the
   marker either is there or is not, and a marker another actor moved is
   detected by the check rather than reported as current state.
2. **A miss is never an answer.** Absence is reported only after the ladder in
   [Waiting and joining](#waiting-and-joining) has been walked to its end,
   including one complete scan. Any other rule would let a job that another
   manager has just published be reported as nonexistent.
3. **It is process-local.** This implementation keeps the index in memory, per
   attached workspace, built lazily from one scan and updated by every rename
   the same process performs. There is deliberately no on-disk index file: a
   workspace has many concurrent writers and no protocol-level way to order
   their updates to a shared derived file, so a durable cache would need a
   synchronization and repair story that the authoritative state tree already
   provides for nothing. An implementation that wants a shared derived
   database MAY build one, under exactly the same three rules.

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

`job.json` is the only required metadata file in the payload directory and is
immutable after submission. Small workflow-specific parameters SHOULD be stored
directly in it instead of one file per parameter.

The optional top-level `parameters` member is the place for them: a JSON object with
string keys and application-defined values, opaque to the protocol and covered by
the immutable job digest like every other member. An implementation MUST reject a
`parameters` object whose serialization exceeds 262144 bytes; bulk content belongs in
the payload or in transactional `data/`. A parent that synthesizes a child job
varies normally only the child's `initial_step` and its `parameters`.
The limit is exposed as `MAXIMUM_PARAMETERS_BYTES` by the protocol model.

`resources` is a mapping from a resource label to a non-negative integer.
Labels use `validate_label`; booleans, negative values, and all non-integers are
invalid. `step_resources` is an optional mapping from validated step names to
the same kind of resource mapping. Units are opaque integers; for example,
SLURM-derived `mem` values are megabytes. A ready job's effective requirement
for step `s` is selected, by the manager, from the state frame's dynamic
`resources`, then `job.step_resources[s]`, then `job.resources`, then `{}`.
For manager resources named `procs` or `mem` that a requirement omits, the
manager assumes the worker's fair share (`capacity // workers`). A manager that
does not provide a resource a job requires never runs that job.

The optional top-level `declarations` member carries workflow declarations: a
JSON object mapping a declaration name to one declaration document. A
declaration document states what a workflow is — its inputs, its method, its
outputs — without describing a graph, and is intended to feed provenance.

The document is carried **verbatim** and is opaque to this protocol. An
implementation MUST validate that each member is a JSON object and MUST NOT
interpret, wrap, normalize, or version it any further: versioning and
self-description live inside the document itself, as the `$id`-style members of
the property-definition conventions it follows. A declaration name MUST match
`[A-Za-z0-9_][A-Za-z0-9._-]{0,63}`, because it is also one file basename in the
payload area below. An implementation MUST reject a `declarations` object whose
serialization exceeds 262144 bytes, which is the `parameters` allowance again and
separate from it. Being members of `job.json`, declarations are immutable after
submission and covered by the immutable job digest like every other member.
Nothing is inherited: a synthesized child job carries the declarations its parent
gave it and no others.

`runner.executor` selects an installed execution adapter and defaults to
`path` when omitted. The core task manager implements `path`; managers MUST
leave jobs using an unavailable or disallowed executor unclaimed. This permits
specialized managers to share a workspace without either one accidentally running
the other's job profile. Executor-specific immutable fields belong in
`job.json`, and their submission validation is owned by that executor.

`runner.source` selects the root `runner.path` is resolved against and defaults
to `payload`:

| `runner.source` | Root of `runner.path` |
| --- | --- |
| `payload` | the job's own payload directory |
| `workspace` | `<workspace>/.httk-workflow/runners/` |
| `installed` | one ordered runner search path configured in the manager |

`runner.path` MUST remain beneath its root under every source. `arguments` is an
argument vector, never a shell command string. A executor may treat the path as a
executor-specific program while retaining these path-containment rules.

A `payload` runner is already pinned by the immutable job digest, so
`runner.sha256` MUST be absent for it. Every other source names one file, or one
tree, shared across jobs by design, so `runner.sha256` is REQUIRED and pins it:
the digest of a file is over its bytes, and the digest of a tree is the
canonical tree digest. Sharing one runner file across a large partitioned
campaign is the purpose of the non-payload sources.

A manager MUST NOT execute a shared runner in place. It resolves the runner,
copies it below the attempt control directory as `runner`, verifies
`runner.sha256` against that staged copy, and executes only the copy. A digest
disagreement fails the job with `runner_mismatch`; a runner that cannot be
resolved fails it with `runner_unavailable`. Both are ordinary continuable
failures, never silent substitutions. A staged tree is entered at its top-level
`run` file.

An `installed` path may also use the reserved form `pkg:<module>/<resource>`,
which resolves inside an installed Python package. A manager MUST restrict that
form to an explicit module allowlist, `httk.workflow` by default.

The immutable job digest a manager records is the SHA-256 over the stored
`job.json` file bytes exactly as submitted. Nothing renormalizes those bytes, so
any implementation with a hash utility reproduces the digest.

`files/` is optional and contains submitted code, templates, or immutable input
objects that require separate files. Small inputs SHOULD be embedded in
`job.json`.

`workdir.mode` is `persistent` or `isolated`:

- `persistent` reuses the declared workdir directory across step activations
  and attempts. The workflow program owns recovery and cleanup of partial
  application files.
- `isolated` creates a new `run.<attempt-id>/` for every attempt. It may be
  initialized from submitted files or transactional `data/`.

The mode MUST be explicit in `job.json`; the protocol has no implicit default.

`data.mode` is `none` or `transactional` and is also explicit. With `none`, the
job may keep all mutable and final application data in its persistent workdir.
It never needs to create `data/`, publish a transaction, or increment a data
generation. With `transactional`, `data/` and the transaction protocol below
are available in every core-v2 workspace.

`claim` and its `pool` are required; `required_capabilities` may be empty.
`claim.pool` is the scheduling pool or queue from which the job may be claimed.
Pool and capability labels use the same conservative component syntax as tags.
A manager advertises its pools and capabilities and MUST claim a job only when
both match. This is claim eligibility, not a workflow name; quantitative
`resources` are separate declarations used by a capable manager when packing
attempts.

The literal pool name `default` is reserved for jobs requiring no explicit
routing. A manager started without pool configuration MUST advertise
`default`. Thus a trivial deployment uses the value shown above without any
out-of-band pool agreement; sites opt into other pool names deliberately.

Retry limits are independent optional safeguards:

- `maximum_attempts_per_activation` limits retries of one logical activation;
- `maximum_total_attempts` limits physical executions over the whole job;
- `maximum_activations` limits initial plus advanced activations, including
  advances back to the same textual step.

The counters are durable state-frame values. Before creating an activation or
attempt, the manager checks the applicable limits. A request that would exceed
one transitions to failed with `budget_exhausted`; it is not launched. An
omitted limit is unbounded.

Priority is 0 through 999, where 0 is highest. It affects scheduling, not
correctness.

### Submission protocol

To submit:

1. Create a complete job below `.httk-workflow/tmp/`, including `job.json` and
   any initial `files/` or `data/`.
2. Choose any placement and atomically rename it to
   `<workspace>/<placement>/<job-key>/`.
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

`g0.init` describes the marker submission creates, not every marker that may be
found in `submitted`. A job can be acted on before any manager registers it —
`set_priority` and `pause` both apply to `submitted` — and such a request is an
ordinary verified transition, so it appends a frame and renames the marker to
`<job-key>.p<new-priority>.g1.<record-ref>` in the same state directory.
Registration MUST therefore accept both forms of a submitted marker: generation
0 referencing `init`, whose priority is the one in `job.json`, and a later
generation referencing a real frame, whose priority is the operator's and may
differ from `job.json`. The marker is authoritative for priority in both cases;
only at generation 0 does the `job.json` priority have to agree with it.

If the submitted marker is well formed but `job.json`, its immutable files, or
their relationship to the marker fails validation, the manager appends a
`failed` frame with class `protocol_error` and moves that same marker to
`state/failed/<placement>/`. The frame uses the UUID and job key from the
validated submitted marker when the document's identity cannot be trusted and
records bounded validation details. The payload is retained for diagnosis.
Implementations MUST NOT leave invalid jobs indefinitely in `submitted`.

An entry whose marker name or placement is itself not parseable cannot enter
the normal job state machine. A workspace repair tool moves it to a shared
`.httk-workflow/quarantine/` area outside `state/`; managers report it loudly
and never schedule it. Names in quarantine MUST preserve or record the original
relative path without permitting collisions or traversal. Quarantine is
exceptional workspace corruption, not another job state.

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
| `cancelling` | The attempt is fenced by a cancellation; its process is being stopped and its exit verified. |
| `relocating` | No attempt may run; the placement is changing. |
| `transferring` | A quiescent payload is moving or detaching. |
| `waiting` | Waiting for a declared child join. |
| `paused` | Requires an operator request to continue. |
| `succeeded` | Successful terminal state. |
| `failed` | Failed until explicit operator continuation. |
| `cancelled` | Cancelled terminal state. |

**“Terminal” is used in two senses in this document, and they are not the same
thing.** *Terminal for scheduling* means that no manager will move the marker on
its own: the three kinds `succeeded`, `failed`, and `cancelled`. That is the
sense a join condition uses, the sense `state/failed/` is a complete collection
in, and the sense in which a failure's own frame is the job's last automatic
one. *Permanent* means that nothing moves the marker again at all, and only
`succeeded` and `cancelled` are permanent: a `failed` job is terminal for
scheduling but explicitly revivable by an operator `continue` or
`override_step`, which is the whole point of tracking broken jobs in the state
tree. Where this document says “terminal” without qualification it means
terminal for scheduling. `cancelling` is neither: it is a live state in which a
fenced attempt is being stopped.

The scheduling cycle is:

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

Every transition of the core profile, exhaustively:

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
| `committing` | `ready` | `advance` to a new activation, or `retry` of this one. |
| `committing` | `waiting` | A `wait` outcome naming a child join. |
| `committing` | `succeeded` | A `succeed` outcome. |
| `committing` | `failed` | A `fail` outcome, an unusable one, or an exhausted budget. |
| `committing` | `paused` | A `pause` outcome. |
| `waiting` | `ready` | The join was satisfied, or became impossible and `on_impossible` names a step. |
| `waiting` | `failed` | `dependency_failure`: the join became impossible with no `on_impossible`, or a named child stayed unresolvable past the join grace. |
| `waiting` | `failed` | `protocol_error`: the recorded join itself cannot be read. |
| `failed`, `paused` | `ready` | Operator `continue` (retry the activation) or `override_step` (new activation). |
| `cancelling` | `cancelled` | The exit of the fenced attempt was verified. |
| any nonterminal | `cancelled` | Operator `cancel` where no live attempt has to be stopped first. |
| `submitted`, `ready`, `waiting` | `paused` | Operator `pause`. |
| `claimed`, `running`, `committing` | the same kind | Operator `pause`, recorded as a sticky deferred request until the next attempt boundary. |
| `submitted`, `ready`, `waiting`, `paused`, `failed` | the same kind | Operator `set_priority`, which renames the marker to a new priority at the next generation. |

Each row is one verified rename of the same marker; no other rename of a marker
is defined, and the `relocating` and `transferring` states are
described in their own sections.

`advance` may name any next step, including the same textual name. It creates a
new activation. Retry retains the activation ID and increments the attempt
ordinal.

Any non-terminal state can be cancelled by an authorized operator. Cancellation
of `running` first fences the attempt by moving its marker to `cancelling`; a
late outcome from that attempt can no longer commit. `cancelling` is not a
terminal state and not a quiescent one: it is the interval in which the fenced
process is stopped and its exit is verified. See
[Cancellation](#cancellation).

Quiescent states may also pass through `relocating` and return to the same
logical state at a different placement.

### Cancellation

Cancelling a job that may have a live process is three ordered steps, and the
order is the guarantee:

1. **Fence first.** The manager renames the exact `running` marker to
   `cancelling`. From that instant no manager accepts an outcome from that
   attempt, because the state tree no longer names it as current. Nothing has
   been signalled yet, so this step cannot be skipped by a race.
2. **Then stop the process.** The owning manager — or, if it has died, any
   manager that can see the process — sends `SIGTERM` to the recorded process
   group, and `SIGKILL` after a configured grace period. The `cancelling` frame
   names the attempt, its attempt-control directory, and its previous owner, so
   a recovering manager has everything it needs.
3. **Only then finish, against evidence.** The marker moves to `cancelled` only
   once the exit has actually been verified, and the verification is recorded
   in the `cancellation` member of the terminal frame.

A manager MUST NOT publish `cancelled` for an attempt it has merely signalled.
Acceptable evidence is:

| `cancellation.verified` | Meaning |
| --- | --- |
| `process_exited` | The manager launched the process and reaped it; the exit status is recorded. |
| `process_group_absent` | The process was recorded on this host and its process group no longer exists. |
| `no_launched_process` | No process identity was ever durably recorded, so the launch gate guarantees the runner never executed. |
| `no_live_attempt` | The job was cancelled from a state that has no live attempt at all. |

A cancellation whose process cannot be proven stopped — typically one recorded
on a different host — MUST leave the marker in `cancelling`, journal why, and
retry. Staying in `cancelling` is the safe outcome: the attempt remains fenced,
so it can produce no state, and an operator sees a stalled cancellation rather
than a terminal state that falsely asserts that nothing is still writing the
workdir. A site whose batch system can confirm that an allocation has ended MAY
treat that confirmation as evidence and record it in the same member.

Because the fence is a marker rename, cancellation composes with everything
else by construction: an attempt that publishes an outcome after being fenced
finds its marker in `cancelling` rather than `running`, and the outcome is
ignored exactly like any other outcome from a fenced attempt.

## Claiming, leases, and fencing

To claim a ready job, manager `M`:

1. Selects the exact ready marker.
2. Reads the state frame and stable `job.json`.
3. Generates attempt and claim IDs.
4. Appends and synchronizes a `claimed` state frame.
5. Renames that exact ready marker to the claimed state path whose basename
   references the new frame.
6. Applies the verified-transition algorithm and proceeds only if its unique
   claimed destination is confirmed.

All competing managers rename the same source path to different unique
destinations. At most one transition can remove that source; each caller uses
verified-transition resolution rather than the rename return status to learn
whether it won.

The claimed frame names:

- manager, writer-incarnation, and attempt IDs;
- step, activation, and attempt ordinal;
- input data generation when transactional data are enabled;
- lease duration and start time;
- matched pool and capabilities;
- resource allocation;
- preceding record reference.

The manager creates `.httk-attempt.<attempt-id>/`, prepares the selected
persistent or isolated workdir, appends a running frame, and renames the
claimed marker to running immediately before launching the application. A
local executor MAY first create a process blocked on a launch gate, durably
record that process identity, commit the running marker, and only then release
the gate to execute the application. If its manager disappears before release,
the gate MUST cause that process to exit without executing the application.
This closes the otherwise ambiguous crash window between a running transition
and recording the new process identity.

Managers update `managers/<manager-id>/heartbeat.json` by atomic replacement.
A recoverer uses the state frame, heartbeat, batch scheduler when available,
and a configured grace period. It MUST NOT steal a job merely because one
delayed metadata read appears stale.

A heartbeat is a statement about the manager, not about its scheduling pass, so
a manager MUST NOT let one pass over the state tree hold its heartbeat: it
takes heartbeat opportunities *between* the state kinds it scans and *within*
long scans of one kind. A manager whose workspace is too large to serve inside
one lease SHOULD also bound how many markers of a kind it processes per pass
and resume the rest on the next pass, in a stable order so that no marker
starves. An implementation SHOULD report a pass that consumes a large fraction
of its own lease, because that is the condition under which a healthy manager
begins to look abandoned.

Once policy determines that an attempt is abandoned, a recoverer appends a new
claimed frame and renames the exact old claimed or running marker. That rename
fences the old attempt. Even if its process later wakes, no task manager accepts
its outcome because the state tree names a different attempt.

A manager taking immediate ownership transitions directly to a new claimed
state. A manager merely releasing work transitions back to ready. Both retain
the activation ID for a retry.

Filesystem fencing cannot prevent a partitioned old process from producing
external side effects or modifying a persistent workdir, and it does not make
a duplicated attempt free: two allocations burning for one activation is a real
cost whichever workdir mode the job uses. Lease expiry alone is therefore never
sufficient evidence to relaunch an attempt, in either mode.

For a persistent workdir, the default safe policy MUST establish that the
previous writer can no longer modify the workdir—for example, by
scheduler-confirmed allocation expiry or cancellation and process-group
termination—before launching a replacement. A site MAY enable lease-only
persistent takeover as an explicit unsafe policy.

For an isolated workdir, a replacement corrupts nothing, so the requirement is
weaker but not absent: a manager MUST have one of

- the recorded process is provably gone on this host,
- a scheduler confirmation that the allocation has ended, or
- a heartbeat that has been silent for a configured multiple of the lease —
  the *takeover grace*, by default twice the lease.

A manager SHOULD terminate the old process group or cancel its batch allocation
before relaunching when it can. The grace exists because an expired lease says
only that a manager is slow: a manager whose scan of a very large workspace
overruns one lease is alive and its attempts are running, and taking those over
at the first expired lease is how one slow node becomes twice the bill.

Every takeover MUST record its evidence in the new attempt frame — which rule
admitted it, and the observed heartbeat age — and every use of an explicitly
unsafe policy MUST be recorded there too.

## Attempt control, workdirs, and runner contract

Attempt control is separate from application workdir:

```text
<workspace>/<placement>/<job-key>/
├── .httk-attempt.<attempt-id>/
│   ├── context.json
│   ├── runner                   # the verified staged copy of a shared runner
│   ├── outcome.tmp.<nonce>/
│   └── outcome.ready/
├── .httk-job/                   # optional runner-private job state
│   └── declarations/            # optional observed workflow declarations
│       └── <declaration-name>.json
└── run/                         # persistent mode
```

In isolated mode the application directory is
`run.<attempt-id>/` instead of `run/`. The runner's current working directory is
the selected application workdir, not the attempt control directory.

`.httk-job/` is runner-private state that belongs to the job rather than to one
attempt: it survives retries, step advances, and isolated workdirs, and it travels
with the payload when the job is transferred. A runner MAY store what it needs
there, atomically. Both `.httk-job/` and every `.httk-attempt.<attempt-id>/` are
excluded from every payload digest — submission, child registration, and detached
transfer alike — so publishing an outcome and writing job state can never disturb
an immutability check of the payload.

`.httk-job/declarations/<declaration-name>.json` is the one reserved name inside
that area: the *observed* workflow declaration of that name, the runtime-refined
counterpart of what `job.json` declared. A campaign that discovers its outputs
only while running writes what it observed there, and the last write of a name
wins. The document is carried verbatim and is opaque to the protocol exactly as
in `job.json`, the basename before `.json` is the declaration name and MUST
therefore satisfy the same syntax, and being below `.httk-job/` the file is
excluded from every payload digest. An implementation MUST report the declared
and the observed document side by side and MUST NOT merge them; a document that
cannot be read is reported as absent, with the reading tool's damage flag set.

Separating them matters in persistent mode: a late process from an old attempt
can only publish beneath its own attempt-control name. Its outcome cannot
replace or impersonate the new attempt's outcome.

### Persistent workdir

The same declared workdir is used across normal step advances and retries.
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

Persistent mode deliberately gives up workdir isolation. If an old process
cannot be established incapable of further writes, it may still modify `run/`
after being fenced from publishing an outcome. The safe default is therefore
to wait, force scheduler cancellation, or pause for manual action. Only an
explicitly configured unsafe takeover policy may accept concurrent-writer risk,
as specified under fencing above.

### Isolated workdir

Every attempt receives a new `run.<attempt-id>/`. The manager may populate it
from submitted files or transactional `data/` by copying, reflinking, or a
filesystem snapshot. Writes through an isolated workdir MUST NOT mutate
committed `data/` before a transaction.

Old isolated workdirs may be retained for diagnosis or collected later. A
manual continuation can import selected files into a new isolated workdir,
with the choice recorded in history.

### Context and restart detection

The manager MUST set:

```text
HTTK_WORKFLOW_CONTEXT=<absolute path to attempt context.json>
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
HTTK_WORKFLOW_BASH_API=<absolute native workflow Bash library>
```

`HTTK_WORKFLOW_DATA_DIR` is additionally set only for transactional-data jobs.
The JSON file is the source of truth; scalar environment variables are
language-neutral conveniences.

`HTTK_WORKFLOW_DURABLE`, and the `durable` member of the attempt context, carry
the workspace's durability mode to the runner. A runner that publishes an
outcome, a transaction, or a child bundle synchronizes it before the rename that
makes it authoritative exactly when this is `1`. A runner that does not compose
outcomes on storage itself may ignore it; the packaged Python and Bash SDKs
honour it automatically, and a context written before the member existed reads
as `0`.

A manager MAY export further variables that belong to the runner libraries it
ships rather than to this protocol. They are SDK or application conveniences: a
conforming manager that exports none of them is still conforming, and a runner
that needs one MUST treat its absence as a missing dependency of that library
rather than as a protocol violation. The ones this implementation exports are:

```text
HTTK_WORKFLOW_VASP_BASH_API=<absolute native VASP Bash library>
```

### Executable workflow-hook wire formats

Directory-package instantiate and collect hooks that are not `.py` use these
UTF-8 JSON subprocess formats. The package manifest and hook trust rules are
specified in {doc}`workflow_packages`; these are the wire-format catalogue.

An executable instantiate hook receives one document on stdin, with its current
working directory set to the staging payload:

```json
{
  "format": "httk-workflow-instantiate",
  "format_version": 2,
  "workflow": "example.relax",
  "tag": "silicon",
  "parameters": {"cutoff": 520},
  "inputs": {
    "structure": {"kind": "file", "path": "files/inputs/structure/POSCAR"},
    "settings": {"kind": "value", "value": {"kpoints": [4, 4, 4]}}
  }
}
```

Its stdout is one JSON object containing `parameters` and, optionally, `tag`.
An executable collect hook receives JSONL: the first line is exactly

```json
{"format": "httk-workflow-collect-stream", "format_version": 2}
```

and each later line is exactly one request envelope:

```json
{"record": {"workspace_id": "workspace", "job_id": "job-1", "state": "succeeded", "job": {}}}
```

The record value is the complete `JobRecord.as_mapping()` mapping; the example
shows only the envelope shape. The hook writes one ordered response per record,
using either:

```json
{"job_id": "job-1", "outputs": {"energy": {"value": 3.14}}}
```

or:

```json
{"job_id": "job-1", "error": "could not read the result"}
```

The Python hook fast paths do not cross this subprocess boundary, but preserve
the same successful-path hook and assembly semantics. Collector failure handling
is intentionally different: registered Python collector exceptions abort
iteration, while executable responses can degrade jobs independently.
`httk.workflow.hookapi` provides `instantiate_main()` and `collect_main()` for
Python executables implementing these formats.

An unclean persistent retry context is:

```json
{
  "format": "httk-workflow-attempt-context",
  "format_version": 2,
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1",
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
  "workdir_mode": "persistent",
  "workdir_reused": true,
  "data_generation": null,
  "durable": true,
  "resources": {},
  "join": null,
  "children": []
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

The attempt context's `resources` member is the effective requirement selected
for the activation that was launched. It is a validated resource mapping, so a
runner can use it as the manager's placement decision without re-resolving the
job declaration.

Attempt-control directories are transient metadata. Persistent workdirs are
application data and MUST NOT be garbage-collected merely because an attempt
ended. Isolated workdirs may be collected under their retention policy. No
state transition depends on cleanup.

## Publishing an outcome

Exit codes are not the workflow protocol. A step communicates in the
attempt-control directory named by `HTTK_WORKFLOW_CONTROL_DIR`:

1. Create `outcome.tmp.<nonce>/`.
2. Write `outcome.json` and any transaction or child bundles inside it.
3. Close all files, and in the storage-durable profile synchronize the whole
   draft tree — the outcome, its transaction manifest and staged payload, and
   its child bundles — in one batch.
4. Atomically rename the directory to `outcome.ready/`, and in the durable
   profile synchronize the attempt-control directory so the new name survives a
   crash.
5. Exit.

The directory rename is the **outcome publication point**. Temporary outcomes
are ignored. The fixed destination is nonempty, so a second publication MUST
fail rather than replace the first. The draft is unreferenced until step 4, so a
crash before it discards a partial outcome whole; the single batched
synchronization at step 3 is why a published outcome can never name staged data
the storage never received.

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
transaction. It MUST equal the generation supplied in the attempt context.
An outcome MAY contain `priority`, an integer from 0 through 999. The manager
uses it for the marker produced by the committed action; omission preserves
the current priority. This allows a workflow decision and its scheduling
preference to share one atomic marker transition.

An outcome MAY also contain `runner_steps`, the array of step names the publishing
runner implements. A manager copies a valid array verbatim into the state frame it
writes and carries it forward, and ignores a malformed one with a log entry: the
member is evidence for an operator or a tool drawing the reachable steps of a job,
never an input of a manager decision. A runner that declares it SHOULD do so in the
first outcome the job publishes.

An `advance` or `wait` outcome MAY contain `resources`, a resource mapping using
the same validation rules as `job.json`. It is the requirement of the next
activation. Other actions MUST NOT contain it.

Actions are:

| Action | Required information | Effect after commit |
| --- | --- | --- |
| `advance` | `next_step` | Create a ready activation for that step. |
| `wait` | `next_step`, `join` | Register children and wait for the join. |
| `succeed` | none | Enter successful terminal state. |
| `fail` | `failure` | Record a declared failure and enter failed. |
| `retry` | `retry.reason` | Retry this activation under policy. |
| `pause` | `pause.reason` | Require an operator request. |

`advance` and `retry` first apply the total and per-activation budgets from
`job.json`. If the requested next activation or attempt would exceed a limit,
the committed effect is `failed/budget_exhausted` rather than another ready
activation.

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
In particular, a persistent-workdir job may freely update application files
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

Paths are normalized relative POSIX paths. Absolute paths, empty components,
`.`, `..`, NUL bytes, and paths into protocol control data are forbidden.
Operations MUST NOT overlap. Operation IDs are unique normalized protocol
components and determine all replay scratch paths.

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
replayed or stale transaction from overwriting an unexpected workdir.
Symlinks, devices, sockets, and FIFOs are forbidden by default.

Every removal uses the deterministic destination
`transaction/trash/<operation-id>/removed`. Every `replace-tree` moves the old
tree to `transaction/trash/<operation-id>/old` before installing its
deterministic payload source. These destinations MUST NOT contain preexisting
unrelated data. Replayers create the operation directory idempotently; no
random trash name or replayer identity may affect the paths.

### Idempotent replay

While the marker is `committing`, a manager applies operations in manifest
order using atomic renames and the verified-transition algorithm:

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

More than one manager may inspect an abandoned `committing` marker. The
deterministic source, destination, and trash paths make concurrent replay
converge: one rename wins and every other replayer verifies the same resulting
path state. A replayer MUST perform the bounded visibility retries from the
verified-transition algorithm before declaring an impossible combination.

The manifest and deterministic operation IDs supply all replay information. No
per-operation progress marker files are required.

After all operations validate as applied, the manager:

1. appends the destination state frame with incremented data generation when the
   transaction changed data;
2. renames the exact committing marker to ready, waiting, succeeded, failed, or
   paused;
3. only then permits transaction trash and, in isolated mode, an old isolated
   workdir to be collected.

If interrupted after the data changes but before the final marker rename, a new
manager sees `committing`, reads the outcome named by the committing frame, and
idempotently completes replay. It does not rerun the step.

This retains the useful *httk* v1 `ht.atomic.*` principle while avoiding one
permanent revision and manifest hierarchy per step.

## Relocating and transferring jobs

This section describes relocation and detached transfer in core-v2. A core
implementation creates `relocating` and `transferring` states as needed.

Arbitrary placement is dynamic. A job may move after submission, but a raw
`mv` of an authoritative payload is not a state transition: the marker would
still name the old placement. Relocation is therefore a short replayable
protocol.

### Relocation within one workspace

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

### Moving new jobs into a running workspace

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
inside the destination workspace before marker publication. Atomic rename is relied
on only for the final same-filesystem publication.

### Moving jobs between workspaces

An individual job can transfer atomically between two workspaces only when both
control trees and the payload source/destination are on the same filesystem. A
coordinator attached to both workspaces:

1. moves the quiescent source marker to `transferring`, recording source and
   destination workspace IDs, placements, transfer ID, and prior logical state;
2. renames the payload into the destination workspace;
3. appends the import frame to a destination-workspace journal;
4. renames the same marker inode from the source `transferring` tree to the
   mirrored destination state tree.

Until step 4, the source transferring marker remains authoritative and points
to both possible payload locations for recovery. The destination cannot run the
job early.

Moving across filesystems is necessarily copy-and-acknowledge rather than one
atomic filesystem transaction. The source job must first be sealed in
`transferring`; the destination publishes a marker only after a complete copy
and transfer-token validation; the source is retired only after explicit
acknowledgement. A executor implementing this profile must document its
duplicate-suppression and failure policy.

If the job may be named by an unresolved cross-workspace join, the source workspace
MUST retain a packed forwarding record keyed by source workspace ID, job ID, job
key, and old placement, naming the destination workspace and placement. It is
retained for at least the maximum join-history period. A transfer implementation
without such lookup support MUST reject transfer of a job participating in an
unresolved join.

To detach one job without an immediately attached destination workspace, the
manager moves it to `transferring`, writes one compact
`.httk-transfer/manifest.json` inside the payload, and renames the authoritative
marker into `.httk-transfer/` inside that payload. The directory is then a
sealed, nonschedulable bundle that can be moved out. Import places and validates
the complete bundle first, appends an import frame, then renames the embedded
marker into the target workspace's state tree. The extra transfer metadata exists
only while the job is detached or retained for transfer provenance.

For moving whole projects, a self-contained workspace is preferable: controlled
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

Each child `job.json` names parent workspace, parent job, parent activation, and
spawn ID, and — a clean pre-release requirement — the parent's `placement`.
`spawn.json` chooses the target workspace and arbitrary placement for each
child. The parent placement lets the decided-join revival guard resolve the
parent by probing its exact state set rather than scanning the workspace: a
manager deciding whether a `continue` would revive a child a join already
consumed reads the parent at that placement alone and never falls back to a
whole-workspace scan inside a scheduling tick. The guard is advisory, so a child
written before spawns carried the parent placement makes it probe nothing rather
than rescan; a fresh child MUST carry it. Child UUIDs and tags are chosen before outcome publication. In the
core profile, the target workspace MUST be the parent's workspace;
cross-workspace children remain a reserved future capability.

Every `spawn.json` entry MUST also carry a `label`: a nonempty tag-syntax name
that is unique within that spawn set. A gathering step selects its inputs by
label, so a missing or ambiguous label makes the parent's own join unusable and
the manager rejects the published outcome with `protocol_error` instead of
registering any child of the set.

While the parent is `committing`, the manager:

1. moves each complete child bundle to its chosen
   `<workspace>/<placement>/<job-key>` path;
2. creates its one `g0.init` marker at the mirrored target-workspace path below
   `state/submitted`;
3. treats an identical existing child plus marker as already registered;
4. fails on the same UUID with different immutable content.

Children are registered before the parent leaves committing. A crash may expose
only some children, but replay registers the deterministic missing set.
Registration is not rolled back. Published children may start before the parent
has completed its transition; that is safe.

When a target workspace is on another filesystem, the child is first copied into
that workspace's temporary area and validated there. Its placement rename and
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

Supported conditions are:

- `all_succeeded`;
- `all_terminal`;
- `any_succeeded`;
- `any_terminal`;
- `at_least`, with a successful-child count.

The waiting journal frame contains each exact child identity, its spawn
placement as `placement_hint`, and the condition. The placement is explicitly a
lookup hint rather than identity. No join file or child marker is added to the
parent directory. New unrelated descendants cannot affect the join.

A manager resolves each named child through this ladder, in order:

1. the finite set of state kinds at the child's `placement_hint`, when the
   reference carries one — ordinary state transitions never change placement,
   so this is a bounded number of directory lookups and resolves the normal
   case;
2. its in-memory job-id-to-marker index, confirmed against the filesystem
   before it is used;
3. any packed relocation or transfer forwarding record;
4. a complete marker scan of the named workspace.

A cache is never the only recovery path, and a cache miss is never the answer.
In the core profile a clean pre-release break makes step 1 mandatory: every join
child reference MUST carry both `job_key` and `placement`, join evaluation
resolves each child by probing the finite state set at that exact placement
alone, and it never falls back to a whole-workspace scan from inside a
scheduling tick. A reference that lacks a placement is a protocol error of
whatever published it, and the manager rejects the outcome rather than rescanning
the workspace per child. The whole-workspace scan of step 4 survives for the
forwarding record of step 3 and for interactive resolution — `job show`, `job
why`, `job log`, and collect resolve — where locating an arbitrary and possibly
finished job by one exhaustive scan is acceptable; it is never entered on the
scheduling hot path. The
consequence that matters at scale is that a waiting parent's per-tick cost is
independent of how many children its join names — each child is one hint lookup
or one confirmed index hit — rather than one complete scan per child.
Failure to find the child after bounded workspace visibility retries is an
unavailable/corrupt dependency, not evidence that a join condition is
impossible. Such a join is nevertheless never allowed to wait forever: because
children are registered before their parent leaves committing, a child that
stays unresolvable past a bounded grace is corrupt evidence, and the manager
MUST fail the parent with `dependency_failure` and a message naming the missing
child. This implementation records the first unresolved observation in memory
and fails the parent after `join_grace_seconds`, one hour by default; a
restarted manager restarts that grace, which is safe because the grace exists
only to absorb transient nonvisibility.

In the core profile, a child named by an unresolved join MUST NOT relocate, so
its placement hint remains valid. Future implementations MAY relocate it only
when they provide the forwarding and full-scan fallback above.

When satisfied, the manager appends a ready frame for a new activation and
renames waiting to ready. Its attempt context summarizes the exact child state
generation, terminal data generation, and failure information. Each observation
is one object carrying the child's `workspace_id`, `label`, `job_id`, `job_key`,
`placement`, `kind`, `state_generation`, `record_ref`, its published `failure`
object when it ended `failed` or `cancelled`, its `data_generation`, and the
workspace-relative `payload_path` and `workdir_path` through which its results
are read. The
observations appear in the attempt context both as the `join` summary and as the
`children` array, which is empty for an activation that follows no join. Child
committed
data may be exposed through read-only paths named in the context. A
transactional parent imports selected child data through its own transaction.
A non-transactional workflow may instead let application code inspect or copy
child data into its persistent workdir according to its own conventions.

Join evaluation records an observation vector containing every child's exact
marker generation, state-frame reference, and state kind. The vector need not
be a simultaneous filesystem snapshot; it is the complete evidence on which
the parent transition was based, and each entry must have been the child's
current authoritative marker when observed. Once the parent marker transition
commits, later manual continuation of a child does not change or retract that
decision.

Success is **currently impossible** when the recorded observation vector
contains enough terminal nonsuccess states to make the condition false:

- `all_succeeded`: any child is `failed` or `cancelled`;
- `any_succeeded`: every child is terminal and none succeeded;
- `at_least N`: succeeded children plus nonterminal children is less than `N`;
- `all_terminal`: never impossible merely because a child failed.
- `any_terminal`: never impossible.

A manually continuable `failed` child still counts as terminal nonsuccess in
the current vector. If success is currently impossible, `on_impossible`
selects an error-handling step or the parent fails with
`dependency_failure`. A later revival of that child does not retract the
committed `on_impossible` transition. Cancellation is terminal but not
successful.

`on_impossible` is optional and has exactly one defined form:

```json
{"action": "advance", "next_step": "handle_child_failure"}
```

`advance` is the only legal action, and `next_step` MUST be a valid step name —
any application-defined name, including the step the parent just ran. The
member behaves exactly like an `advance` outcome: it starts a new activation of
the parent at that step, carrying the observation vector as the join summary, so
the error-handling step sees precisely what the satisfied step would have seen.
An absent `on_impossible`, or one whose `action` is anything else, means the
parent fails with `dependency_failure`; a manager MUST NOT invent another
action, and MUST NOT treat an unrecognized one as a reason to keep waiting.

#### Reviving a child a decided join consumed

A join decision is final, but a child of a decided join is still an ordinary
job, and `continue` or `override_step` on it would start writing its workdir and
payload again — possibly while the parent activation that consumed it is reading
exactly those files. Nothing in the protocol can order those two writers.

A manager MUST therefore refuse a `continue` or `override_step` request for a
job that a decided join has already observed, unless the request explicitly
carries `"force": true`. The check is a cheap, exact one and needs no index: a
child's `job.json` names its `parent`, and the parent's current state frame
carries the `join_summary` of the activation it is in, so the refusal applies
precisely while the consuming activation is the parent's current one. A refused
request is retired with the reason, which names the parent, so an operator can
continue the parent instead. With `"force": true` the manager applies the
request and MUST record the hazard it accepted in the resulting state frame, as
a `revival_hazard` object naming the parent, its state, and the observation that
consumed this child.

An advance or succeed outcome may also publish detached children without a
join.

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

There is exactly one failure object shape, used identically by application
runners, by every language binding, and by the manager itself:

| Member | Type | Required | Meaning |
| --- | --- | --- | --- |
| `code` | string | yes | Stable machine identity, one token without whitespace, at most 128 bytes. Matched against `retry_policy.retry_on`. |
| `message` | string | yes | One nonempty human sentence. |
| `details` | object | no | Structured evidence, such as `exit_status` or application counters. |
| `retryable` | boolean | no, default `false` | Advisory operator evidence. It never overrides `retry_on`. |

No other member is permitted. A manager MUST validate a runner-published
failure before recording it and MUST NOT store an unvalidated object. A
malformed failure is itself a protocol violation: the job enters failed with
the manager's own `protocol_error` code and a message naming the defect, so a
broken runner can never make a failure invisible to a consumer.

The manager embeds the structured failure in the failed state frame and moves
the one marker to `state/failed/<placement>/`. No `failure.json`,
`ht.reason`, per-job event directory, or second failure index marker is
created.

Codes emitted by this manager itself are reserved. Those currently in use are:

- `protocol_error` — invalid submission, an outcome the protocol forbids, a
  malformed published failure, an unusable join, or a runner that exited
  successfully without publishing an outcome;
- `process_failure` — a runner that could not be launched, or that exited
  nonzero without publishing an outcome;
- `lease_lost` — the owning manager's heartbeat expired;
- `retry_exhausted` — `maximum_attempts_per_activation` reached during retry;
- `budget_exhausted` — an attempt or activation budget exceeded;
- `dependency_failure` — a join became impossible, or a named join child stayed
  unresolvable past the manager's bounded grace;
- `transaction_corruption` — the replay of a published transaction failed
  midway; a transaction manifest or outcome the manager cannot parse is a
  `protocol_error`, not this;
- `runner_unavailable` — a runner outside the payload could not be resolved,
  staged, or entered at all;
- `runner_mismatch` — the staged copy of such a runner did not match the
  `runner.sha256` the job pinned.

A runner library that dispatches steps on a runner's behalf publishes ordinary
runner failures, so its codes are reserved too. Those of the runner libraries
shipped with this implementation are:

- `no_outcome` — the step handler returned without publishing an outcome;
- `unknown_step` — the job asked for a step this runner does not implement;
- `declared_failure` — a legacy `ht_steps` task declared itself broken, published
  by the *httk* v1 compatibility runner only.

`resource_unsatisfiable`, `manager_error`, and `cancelled` remain reserved for
manager use but are not currently emitted by this implementation. `timeout` is
likewise reserved for a manager-enforced attempt timeout, which this manager does
not yet apply; the *httk* v1 compatibility runner does publish it for a legacy
task that exceeded its own timeout. Application codes SHOULD be namespaced, as in
`vasp.nonconvergent`, to keep them distinct from the reserved set.

The failure frame contains job, step, activation, and attempt IDs; the failure
object with its code, message, and details such as exit status or signal; retry
history; manager ID; relevant retained log paths; and data generation. For a job
with `data.mode` equal to `none`, data generation is `null`.

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
They write one complete request file in `requests/tmp/` and atomically rename it
into `requests/ready/` under the same name, so a manager never reads a partially
written request and the publication is the same verified rename as every other
one in this protocol. A request names:

- job UUID, job key, and the job's `placement`;
- exact expected marker generation and record reference;
- requested action;
- operator identity and reason;
- optionally `force`, the operator's explicit acceptance of a hazard the
  manager would otherwise refuse;
- any selected retained files for manual import;
- for relocation or transfer, the exact destination workspace and placement plus a
  unique operation ID.

Actions include:

- `continue`: retry the current activation;
- `override_step`: create a new activation at a named step;
- `cancel`;
- `set_priority`;
- `pause`;
- `relocate`, when a placement changes;
- `transfer`, when a job moves between workspaces.

Action validity is deliberately narrow:

- `continue` and `override_step` apply to `failed` or `paused`;
- `set_priority` applies only to `submitted`, `ready`, `waiting`, `paused`, or
  `failed`;
- an operator `pause` applies immediately to `submitted`, `ready`, or `waiting`, is deferred from `claimed`, `running`, or `committing` until the next attempt boundary, and is a handled no-op against a job already `paused`; terminal outcomes supersede a pending deferred pause;
- `relocate` and `transfer` apply only to their permitted quiescent states;
- `cancel` may target any nonterminal state and uses the explicit fencing and
  process-termination procedure of [Cancellation](#cancellation) for a live
  attempt. A second `cancel` of a job already in `cancelling` is not an error
  and changes nothing: the first one is still being carried out.

`continue` and `override_step` remain subject to job budgets unless the request
contains an authorized, auditable budget change under site policy. Because
`job.json` is immutable, such an override lives in the operator journal frame,
not by editing the job definition. They are additionally refused, without
`force`, for a job that a decided join already consumed; see
[Reviving a child a decided join consumed](#reviving-a-child-a-decided-join-consumed).

In particular, `set_priority` MUST NOT rename a `claimed`, `running`, or
`committing` marker behind its owning manager; an in-flight operator `pause`
records a sticky `pause_requested` member in a same-kind frame and pauses at
the next attempt boundary. A manager older than this additive state member
quarantines such an in-flight pause request as invalid.

A manager claims the request by rename, verifies the exact expected current
marker, appends the new journal frame, and renames that marker. It resolves the
target by probing the finite state set at the request's exact `placement`, the
same clean pre-release break the join ladder makes, and never by scanning the
workspace from a scheduling tick; a request that carries no placement is a
protocol error, claimed and quarantined as malformed input rather than allowed
to trigger a global lookup. A delayed request cannot apply to a newer state
because its expected generation no longer matches. All request-induced marker moves use verified transitions. A manager
whose live transition loses to cancellation rereads the current marker and
stops the fenced attempt; it does not infer ownership merely from an errno.

The result is written to the shared journal. The transient request file may be
removed after retention policy permits; there is no per-job request directory.

A claimed request lives in `requests/claimed/<manager-id>/` until the manager
that claimed it has decided about it: a request that was applied is removed, and
one that can never become actionable is retired.

A request that has been claimed and can never become actionable — its expected
generation or record reference no longer matches, the job moved while the
request was being applied, or it asks for something the protocol refuses such as
reviving a job a decided join already consumed — MUST be retired rather than
left where it will be read again. Retiring means writing a retirement record and
then moving the claimed file to `requests/retired/`; a retired request is never
rescanned. The record sits beside the request under the request's own name plus
`.retirement` and is one object:

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

Both the retired request and its record are transient evidence for an operator,
not protocol state: nothing reads them back, and “Retention gates and
always-safe collection” below collects them after a month. This is distinct
from two neighbouring cases that MUST NOT be retired:

- a request whose job is served by a *runner executor this manager does not
  serve* is left in `requests/ready/` for a manager that does serve it. A
  manager that has decided this about a request SHOULD remember the decision
  rather than reread the file on every pass;
- a request that cannot be applied *right now* — a held maintenance lock, an
  unavailable workspace — stays actionable and is simply retried.

A request that violates the protocol, including one naming a job that does not
exist, is quarantined as malformed input rather than retired.

Manual continuation preserves failure history. In persistent mode it reuses the
declared workdir in place; the new attempt context records
`is_restart: true`, its cleanliness, and `attempt_reason: "manual_continue"`.
No file import or transaction is required. In isolated mode, selected retained
files may instead be imported into the new workdir; if they also become
committed `data/`, that contribution uses a transaction.

## Required outcome-processing order

For a valid current outcome, a manager:

1. validates job, activation, and attempt, plus expected data generation when a
   transaction is present;
2. appends a committing frame;
3. renames the exact running marker to committing, fencing the attempt;
4. applies or verifies the transaction, if present, and in the durable profile
   synchronizes every replayed destination and the directories it touched;
5. registers or verifies every child, synchronizing each published payload and
   marker's directory in the durable profile;
6. computes failure or join information;
7. appends and synchronizes the destination state frame;
8. renames the exact committing marker to the destination;
9. performs optional cleanup later.

Steps 4 and 5 synchronize before steps 7 and 8, so a durable workspace has the
committed data and the registered children on storage before the marker rename
that claims them: a power cut can never leave a marker out of `committing` that
names a transaction its storage only half received.

Interruption recovery follows directly:

| Interruption point | Recovery |
| --- | --- |
| Before marker submission | No job was submitted; placed orphan may be retried or collected. |
| In `submitted` | Revalidate; move the same marker to ready or to failed with `protocol_error`. |
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

1. discovers explicit and watched workspaces and validates each `format.json`;
2. rejects duplicate workspace IDs and unknown future capabilities before mutation;
3. creates a manager record, fresh writer-incarnation journal, and heartbeat in
   each attached workspace;
4. resumes markers in `committing` and, when enabled, `relocating` and
   `transferring`;
5. resumes markers in `cancelling`: every one of them names a fenced attempt
   that still has to be stopped and verified, whether this manager fenced it or
   inherited it from one that died mid-cancellation;
6. examines possibly abandoned claimed and running markers;
7. evaluates waiting joins, including cross-workspace references when enabled;
8. handles submitted jobs and operator requests;
9. claims eligible ready work in pool, capability, priority, and resource
   order, skipping requirements that cannot fit its advertised capacities and
   packing fitting attempts against the reservations of its running attempts;
10. continues watching for workspaces being attached, renamed, or detached.

It does not need to reconcile a separate state index. Listing `state/` is
listing the authoritative scheduler state.

A high-scale implementation SHOULD:

- scan only non-terminal state placement prefixes relevant to its configured
  projects or job set;
- keep an in-memory job-key-to-marker map while running;
- process arbitrary placement subtrees incrementally rather than repeatedly
  scanning every attached workspace;
- use filesystem notifications only as hints, since notifications may be lost;
- workspace task-manager query caches outside per-job directories.

Discovering the globally highest-priority ready job requires enumerating the
ready tree. Incremental scans MAY therefore cause long-lived operational
priority inversion, especially after cold start; priority is a preference
rather than a strict global ordering guarantee. This is not a gap to close by
adding directory levels, and no scan strategy changes claim correctness.

A manager MAY restrict every scan to a set of placement prefixes, the same kind
of deployment policy by which it advertises pools and capabilities, so that
disjoint managers divide a large workspace without walking each other's trees.
This is a restriction a manager places on itself and not a protocol change:
placement values remain project-owned semantics that the engine only validates
and filters on, overlapping assignments stay safe because the marker rename
still arbitrates a claim, and a manager records its assigned prefixes in its
manifest so a diagnosis can report when a live manager's prefixes exclude a
job's placement. A manager with no assignment scans the whole workspace.

## Workspace check and marker repair

A marker that no longer resolves to its journal frame is the one form of damage
a manager cannot route around: the marker is authoritative, and everything
beyond its kind, priority, and generation lives in the frame. A workspace check
walks every marker of every state kind and verifies that its record reference
resolves, within the configured visibility deadline, to a readable frame whose
checksum verifies and whose `workspace_id`, `job_id`, `job_key`, `kind`, and
`state_generation` agree with the marker name. The `init` reference of a
submitted marker resolves to nothing by definition and is verified as such.
Each unresolved marker is reported with a stable problem code: `missing_segment`,
`short_read`, `checksum_mismatch`, `length_mismatch`, `trailer_mismatch`,
`reference_mismatch`, `invalid_header`, `undecodable_frame`,
`invalid_record_ref`, `identity_mismatch`, or `unparseable_name` for a
marker-shaped entry whose basename cannot be interpreted at all.

Repair is optional, separate, and conservative. Because the frame that holds
`previous_record_ref` is precisely the unreadable one, a repair cannot follow
the chain of the job; it walks the journal segments instead, collects the
readable frames naming that job, and adopts the newest one whose
`state_generation` is *strictly older* than the marker's. Adopting a frame at
or beyond the marker's own generation is forbidden: such a frame is either the
damaged one or a transition that was appended and never committed by a rename,
and publishing it would invent state no marker ever carried. The repair then
appends one ordinary state frame with `reason: "fsck_repair"`, whose
`previous_record_ref` is the recovered frame and whose members are carried
forward from it, keeping the marker's own kind, placement, and priority, and
renames the marker onto that frame at the next generation. A repair therefore
adds history rather than rewriting it, and the job becomes loadable and
schedulable again.

A repair MUST NOT touch a `claimed`, `running`, `committing`, or `cancelling`
marker whose owning manager — identified from the readable frames of that job —
is still heartbeating within its lease. Such a marker is reported only; its
manager owns the transition that follows. `cancelling` belongs in that set for
a reason of its own: the marker *is* the fence a live manager put in place and
is still acting on, and re-pointing it at an older frame would take away the
attempt identity that manager needs to finish stopping and verifying the
process. A marker with no readable older frame is
unrepairable and is likewise reported, and may be moved into
`.httk-workflow/quarantine/` only when an operator asks for that explicitly.

## Garbage collection and compaction

The following may be collected under the explicit retention policy the
workspace publishes in `policy.retention`:

- unpublished workspace temporary entries;
- placed payload directories that never reached submitted state, except sealed
  transfer bundles;
- journal frames not referenced by any marker or history chain;
- incomplete outcome directories;
- obsolete attempt-control directories;
- abandoned and completed isolated workdirs;
- transaction trash after the destination marker transition;
- retained diagnostic application files.

That list is permissive: it bounds what a conforming collector *may* touch, not
what one must. The `httk workflow workspace gc` implementation collects the
subset tabulated in “Retention gates and always-safe collection” below, and
adds the retired transfer bundles and per-transfer receipts core-v2 accumulates.
It deliberately leaves isolated
workdirs, incomplete outcome directories, and payloads that never reached
`submitted` alone, because each of those is the only remaining evidence of a
job that went wrong.

Journal compaction operates on shared segments, not by creating files per job.
It must preserve every record reference reachable from a current marker and the
configured amount of history.

Terminal `job.json`, committed application data, the one state marker, and
required journal history are retained according to site policy. Age alone is
not permission to delete a non-terminal job.

A persistent workdir is application data, not attempt scratch. It is retained
until an explicit job or site retention rule permits its removal, including
after failure or manual continuation.

A payload containing `.httk-transfer/manifest.json` and its sealed marker is
not an orphan even though it has no marker in a workspace state tree. Generic
temporary/orphan GC MUST NOT collect, alter, or unseal it. Only an explicit
transfer import, abort, or transfer-specific retention action may do so.

Entries in `.httk-workflow/quarantine/` are likewise outside generic orphan
GC. They are removed only by an explicit repair decision or a separately
configured quarantine-retention policy that preserves an audit record.

Empty placement-directory pruning follows the nonrecursive race rules in
“State-marker rename.” Implementations should account for these directories:
deep or job-unique placements can temporarily leave one empty hierarchy under
several state kinds even though the marker inode count remains one per job.

No recovery operation begins by broadly deleting `tmp`, run, or unknown files.
Cleanup is separate from correctness.

### Retention gates and always-safe collection

An absent member of `policy.retention` means **keep**. A collector MUST NOT
prune a category whose limit is unconfigured; a workspace whose operator has
said nothing therefore loses nothing.

Three categories are exempt because the entries in them cannot carry
information, and are collected whatever `policy.retention` says:

- an empty placement mirror below a state kind, pruned by `rmdir` alone;
- an entry still sitting in `.httk-workflow/tmp/` or
  `.httk-workflow/requests/tmp/` more than 24 hours after it was written, since
  every publication renames its staging entry away within one operation;
- a request in `.httk-workflow/requests/claimed/<manager-id>/` more than 30
  days old whose manager is no longer heartbeating;
- a request in `.httk-workflow/requests/retired/`, and its `.retirement`
  record, more than 30 days old. A retired request was already decided about
  and is never rescanned, so it is evidence for an operator and nothing else.

The remaining categories are gated as follows.

| Category | Gate | Additional condition |
| --- | --- | --- |
| Attempt-control directory | `attempt_control_days` | The job is in `succeeded`, `failed`, or `cancelled`, and this is not its newest attempt-control directory. |
| Transaction trash | `trash_days` | The job's marker has reached a quiescent kind, so the destination transition has happened and no replay consults the trash again. |
| Retired transfer bundle | `trash_days` | Below `transfers/retired/`; the ledger describing it is kept. |
| Import acknowledgement and import record | `trash_days` | Below `transfers/acks/` and `transfers/imported/`. |
| Journal segment | `journal_days` | No current marker — nor the sealed marker of a bundle awaiting handover — references it, and its writer belongs to no manager heartbeating within its lease. |
| Manager directory | `journal_days` | The manager's heartbeat is expired and none of its writer's segments were retained. |

The newest attempt-control directory of a terminal job is retained regardless
of age: it holds the outcome, the failure breadcrumb, and the runner logs of
the attempt that decided the job.

A collector MUST NOT prune the runner store. A runner is referenced by digest
from `job.json` and from transfer manifests, an attached workspace can gain a
job referring to one at any time, and the store is small; a future version may
add an explicit runner-retention rule, but generic collection never applies
one.

### The cost of collecting journal history

Only the segment a marker's `record_ref` points *into* is protected, not the
whole chain behind it. Collecting older segments of the same writer is exactly
what `journal_days` buys, and the consequence must be stated plainly: the deep
history of an old job goes with those segments. `collect` and `job log` then
report that job's timeline from whatever frames remain and set `gaps` — the
job's current state, marker, payload, and terminal outcome are unaffected,
because they never depended on the older frames.

### Crash safety and journaling of a collection

A collection MUST be interruptible at any instruction. It therefore removes
entries bottom-up, renames nothing, and rewrites no protocol state: the remains
of an interrupted removal are scratch that the next collection removes again.
Empty-placement pruning tolerates `ENOTEMPTY` and `ENOENT` as ordinary
outcomes, never as faults, because a concurrent transition recreating exactly
that path is expected.

A collection that removed anything appends one summarizing frame to the
journal:

```json
{
  "format": "httk-workflow-gc",
  "format_version": 2,
  "workspace_id": "…",
  "collected_at": "2026-07-26T09:12:44.512314Z",
  "retention": {"journal_days": 30},
  "removed": 41,
  "bytes_reclaimed": 918273,
  "categories": {"attempt_control": {"candidates": 12, "removed": 12, "bytes_reclaimed": 40960}}
}
```

This is not a state frame, so every reader that walks the journal for job
history ignores it. A collection that removed nothing writes no frame at all,
since opening a journal writer creates a writer directory and an empty
collection must not create the garbage it came to collect.

## Manual filesystem inspection

The layout is intentionally legible without a database:

- `state/ready/<arbitrary-placement>/` is the runnable queue, with `p000`
  through `p999` encoded in marker names and no priority directory level;
- `state/running/` is everything presently believed to execute;
- `state/committing/` is exactly the replay backlog;
- `state/cancelling/` is every attempt already fenced by a cancellation whose
  exit has not been verified yet;
- `state/waiting/` is the join backlog;
- `state/failed/` is the current broken-job collection;
- `state/succeeded/` is the finalized-success collection;
- `.httk-workflow/quarantine/` contains malformed entries requiring workspace
  repair rather than workflow scheduling;
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
- workspace UUID;
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
  `data/`. Writes to the declared application workdir are allowed.

## Persistent VASP restart example

A VASP workflow that wants traditional in-place execution can declare:

```json
{
  "workdir": {"mode": "persistent", "path": "run"},
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
   workdir, it starts a new attempt in the same `run/`.
4. The replacement context says `is_restart: true`,
   `is_unclean_restart: true`, and, for example,
   `attempt_reason: "lease_lost"`. The step may validate or remove partial
   files, adjust `INCAR`, and resume from `WAVECAR` using domain-specific logic.
5. If the step publishes `advance`, the next workflow step may also use the
   same `run/`, but it receives `is_restart: false`: this is its first attempt,
   not a restart merely because the workdir already contains files.
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

This example deliberately uses isolated workdirs in a core-v2 workspace. Job
`silicon-relax--J` starts at `prepare`,
creates two calculations, joins them, and finalizes:

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
    job file and application data; its one external state marker mirrors its
    arbitrary placement, and history is packed with many other jobs in journal
    segments.

At every interruption point, the marker location selects exactly one recovery
rule.

## Relationship to *httk* v1

The packaged `httk.workflow.languages.httk_v1.v1_runner` is an ordinary
installed `path` runner used by converted packages. The normal manager applies
the following mapping for instantiated *httk* v1 task templates:

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

The adapter preserves the *httk* v1 ordering rule that a published
`ht_finished`/broken decision or pending atomic transaction is completed without
rerunning `ht_steps`.

*httk* v1's restart counter was carried across steps but incremented only when a stale
`running` task was adopted; it did not bound an indefinitely advancing clean
workflow. *httk₂* keeps the per-activation retry limit and adds optional total
attempt and activation budgets for that separate concern.

Join migration is intentionally not a pathname-for-pathname emulation. *httk* v1
`waitsubtasks` searched the whole nested task subtree and could notice
descendants that appeared later. An *httk₂* join waits only for its immutable,
explicit child set. A child that publishes detached grandchildren may complete
without those grandchildren, so a migrated workflow that requires subtree
completion must make the child join its own descendants before succeeding or
name the additional jobs explicitly in the ancestor's join.

The shipped runner makes each discovered direct subtask an explicit native
child. Each such child applies the same rule recursively, so ordinary nested
*httk* v1 task trees retain subtree completion. It uses `all_terminal`, rather
than `all_succeeded`, because *httk* v1 resumed a `waitsubtasks` parent once no
descendant remained in an active state; broken descendants did not keep it
waiting. The original legacy task directories remain in place; they are not
mirrored with symlinks. Native child payloads, markers, and journal state are
authoritative, and state-based deduplication prevents rediscovering a child.

The principal improvements are:

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
is a design property described by the arithmetic above, while capacity beyond
the measured snapshot is an operational question for the target filesystem and
should be evaluated with {doc}`/benchmarks` before a campaign is sized.
