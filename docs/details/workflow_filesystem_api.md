# Workflow filesystem API in detail

*For implementers of this protocol, and for anyone who needs to know what a
workspace on disk means.*

This page is the normative on-disk protocol of *httk-workflow*: format 3,
profile `core-v3`, layout `jobs-v3`. It is not Python API documentation. The
Python expression of the protocol is {py:mod}`httk.workflow.protocol`, which
re-exports the document models (`JobDefinition`, `StateDoc`), the name grammar
(`format_job_name`, `parse_job_name`, `JobRef`) and the validators named here.
An independent inspection or verification tool needs only this page and that
module. Runner authors need neither: the SDKs ({doc}`/runtime_helpers`,
{doc}`/sdks/index`) speak the attempt side of the protocol.

## Scope and conformance

### Normative language

**MUST**, **MUST NOT**, **SHOULD** and **MAY** have their usual meaning.
Protocol JSON is UTF-8; writers emit compact JSON with sorted keys, and readers
depend on exact bytes only for the `bundle.json` token and the `job.json`
digest. A *strict* document is invalid with an unknown or a missing member.
Timestamps are UTC ISO 8601, except epoch seconds in signed exchange requests.

### Actors and trust

| Actor | Trust | Writes |
| --- | --- | --- |
| Manager (`TaskManager`, owner kind `manager`) | trusted | everything below the workspace, but inside a job only while it owns it |
| CLI owner (owner kind `cli`) | trusted | as a manager; `job new`, `job submit`, `job delete`, `job seal`, `job unseal`, `job eject`, `job adopt`, `job debug`, `workflow install`, `workflow build`, `workflow uninstall`, `workspace gc` and `workspace fsck --repair` register one for their duration |
| Operator | trusted | request files, tombstones by attestation, the workflow store |
| Workspace daemon | trusted | its private state in `.httk-workspace/daemon/`, and its own documents in `exchange/`; owner kind `daemon` is reserved for it |
| Attempt and its launch ranks | **untrusted**, confined when `manager.confine=bwrap` | its own job directory, minus the trusted entries |
| Exchange client | **untrusted** | `exchange/` only |

Supervision of the processes a manager starts on its own host (fork, wait,
signals, the gate pipe) is not communication between actors.

### Required filesystem semantics

- **One filesystem and mount** for `.httk-workspace/`, `jobs/`, `workflows/`
  and `exchange/`, so that every protocol move is a `rename` (enabling the
  exchange checks it). Bundles may cross filesystems by explicit copies.
- **Atomic rename** of files and directories. A non-empty directory renamed
  onto a non-empty directory fails, leaving both; a renamed directory keeps its
  inode.
- **Basic POSIX:** `mkdir`, `rmdir` that fails on a non-empty directory,
  `unlink`, `open` with `O_CREAT|O_EXCL|O_NOFOLLOW`, `O_DIRECTORY` and
  `O_NONBLOCK`, the `*at` calls with directory descriptors, and `fsync`.
  `NAME_MAX` is at least 213.
- **Not used:** file locks of any kind, hard links, the creation of symlinks,
  `renameat2`/`RENAME_NOREPLACE`, and sockets or any other IPC between
  actors. All coordination goes through control files.
- **Symlinks avoided.** The protocol creates none, refuses them in protocol
  positions (at or below `jobs/<state>/`, in control directories, in bundle
  structure) and moves those inside job content as opaque leaves, never
  following them. The workspace root and `jobs/` may be reached through
  symlinks.
- **Network filesystems.** NFS mounts MUST be hard mounts: a soft mount can
  apply a rename after reporting a failure. A metadata change becomes visible
  on every node within `policy.visibility_deadline_seconds`.
- **Durable mode** (the default) adds the `fsync` calls each primitive lists.

### Conformance

A conforming writer MUST change protocol state only through operations with
the semantics of §Filesystem primitives, write inside a job only while it owns
it (R2), hand work over only on a death proof or an attestation (R1), treat
everything a job or a client wrote as untrusted (R5), and refuse a workspace
whose `format.json` does not match. A read-only tool needs §Workspace layout,
§Names as tokens, §Job documents and the reading rules of R5.

## Workspace layout

```text
WORKSPACE/
├── .httk-workspace/
│   ├── format.json                         identity, format, policy, settings, preludes
│   ├── owners/<owner-id>/                  §Owners and the death proof
│   │   ├── owner.json, heartbeat.json, dead.json
│   │   └── launches/<attempt-id>.<n>/      {process.json, launch.json, nodefile}
│   ├── requests/<job-uuid>.<request-uuid>.json
│   ├── tmp/<owner-id>.<purpose>.<token>/   owner scratch
│   ├── tmp/trash.<token>                   an entry being removed
│   ├── tmp/push.<hex>/                     a remote install's or transfer's pushed copy
│   ├── quarantine/<epoch>-<token>/         {entry, reason.json}
│   ├── exchange-jobs/<exchange-name>/      the exchange index
│   ├── transfers/outgoing/<transfer-id>/   held bundles
│   ├── transfers/incoming/<transfer-id>.<token>/   pushed bundles awaiting adoption
│   └── seal.json                           the workspace seal, when sealed
├── jobs/
│   ├── ready/<placement>/<job-key>~p<NNN>~<t>/
│   ├── waiting/, paused/, failed/, succeeded/, cancelled/   likewise
│   └── owned/<owner-id>/<job-key>~p<NNN>~<t>~<from>/        flat
├── workflows/<slug>--<h16>/                installed workflows
├── logs/managers/<owner-id>.log            one log per manager
├── logs/launchers/<launch id>.*            launcher output: .sbatch script, .out merged output
├── postprocess/                            default postprocess output root
└── exchange/                               the exchange extension, when enabled
```

Initialization creates the control directories `owners`, `requests`, `tmp`,
`quarantine` and `exchange-jobs`, the seven state directories and `workflows/`;
everything else appears on demand.

### `format.json`

```json
{
  "format": "httk-workflow-filesystem", "format_version": 3, "layout": "jobs-v3",
  "core_profile": "core-v3", "extensions": [], "record_ref_encoding": "hwref-v2",
  "workspace_id": "b588833b-87ea-4da2-b860-1c9e768cfbc1", "created_at": "2026-10-10T12:00:00+00:00",
  "policy": {"visibility_deadline_seconds": 5.0, "retention": {"trash_days": 1.0, "owner_tombstone_days": 30.0, "daemon_history_days": 30.0}},
  "settings": {}, "workflow_preludes": {}
}
```

A reader MUST refuse the workspace when `format`, `format_version`,
`core_profile` or `layout` differ from the values above, when `policy`,
`settings` or `workflow_preludes` is missing, when `extensions` names anything
but `"exchange"`, when `record_ref_encoding` is not `"hwref-v2"` (a fixed
constant of format 3), or when `workspace_id` is not a canonical lowercase
UUID. The refusals are listed in §Changes from the previous format.

| Policy member | Default | Meaning |
| --- | --- | --- |
| `visibility_deadline_seconds` | `5.0` | The visibility deadline of the settle rule (§Filesystem primitives). |
| `retention.attempt_control_days` | absent (keep) | Age after which `gc` removes old attempt directories. |
| `retention.trash_days` | `1.0` | Age after which `gc` removes the logs of departed managers. |
| `retention.owner_tombstone_days` | `30.0` | Age after which `gc` removes the tombstone of a recovered owner. |
| `retention.daemon_history_days` | `30.0` | Age after which the confined workspace daemon retires settled requests and ended manager starts from its ledger and `managers.json` (not a `gc` category); never before a request's signed lifetime plus clock skew has passed. |

A retention value of `null` or `"keep"` keeps everything in its category. The
retired members `lease_seconds`, `journal_segment_bytes` and
`retention.journal_days` are ignored on read and refused on write, like every
unknown member. Policy, settings, preludes and extensions change by
read-modify-write of `format.json` through `write_file`, verified by
re-reading it and redone (a bounded number of times) when a concurrent writer
replaced it in between; writers are not serialized, so last writer wins. Managers read the policy
at attach and re-read settings and extensions while they run. `settings` maps
dotted names to JSON scalars; the protocol reads `seal.succeeded`, `seal.keys`,
`manager.confine`, `confine.*` and
`postprocess.directory`.

### Job states

| State | Meaning |
| --- | --- |
| `ready` | Waiting to be claimed by an eligible manager. |
| `owned` | Held by exactly one owner; what it is doing is `state.json` `phase`. |
| `waiting` | A parent waiting for its join to be decided. |
| `paused` | Stopped until an operator request (`continue`, `restart`, `override_step`, `delete`, ...). |
| `failed` | Ended with a failure; `continue`, `restart` and `override_step` revive it; a parent failed at its wait keeps `join` and `continue` reopens it. |
| `succeeded` | Ended successfully; never changed by the protocol until `job unseal`, or unless the job is unlocked (§Unlocked jobs). |
| `cancelled` | Cancelled by a request. |

`succeeded`, `failed` and `cancelled` are terminal (`TERMINAL_STATES`); the six
states other than `owned` are the unowned states (`UNOWNED_STATES`).

### Placement rules

- A placement is a relative POSIX path. Its components MUST NOT be empty,
  `.`, `..` or `.httk-workspace`, MUST NOT contain NUL, and are at most 255
  bytes. The empty placement is legal; its canonical text is `""`.
- Wherever a placement is written it is in canonical text
  (`placement_text(normalize_placement(text)) == text`) and contains no `~`,
  because listings tell job directories from placement directories by `~`.
- No component may parse as a job key, so job directories never nest
  (`check_job_placement`).
- Placement directories below `jobs/<state>/` are real directories made by the
  kernel. A symlink or non-directory at or below `jobs/<state>/` is refused
  (`check_placement`); symlinked placement directories are not supported.
- The `job.json` placement is authoritative and immutable. An unowned job is
  always at `jobs/<state>/<placement>/`; an owned one is flat below
  `jobs/owned/<owner-id>/`, and its placement is read from `job.json` when it
  is released or recovered.
- Empty placement directories mean nothing. Any actor MAY remove one with
  `rmdir`; a move that finds its parent pruned recreates it and retries.
- Managers MAY restrict their scans to placement prefixes. Join children and
  requests carry the placement, so a lookup lists one placement per state.
- Where an SDK places a child that names no placement (by default
  `<parent placement>/<parent job key>.children`, or the parent's placement
  with the workspace setting `children.placement=flat`) is a convention of the
  SDK, not protocol.

## Names as tokens

```text
unowned job:  <job-key>~p<NNN>~<t>            below jobs/<state>/<placement>/
owned job:    <job-key>~p<NNN>~<t>~<from>     below jobs/owned/<owner-id>/
job key:      [<tag>--]<uuid>
```

| Component | Grammar | Meaning |
| --- | --- | --- |
| `uuid` | lowercase canonical UUID | The job id. |
| `tag` | `[a-z0-9][a-z0-9._-]{0,47}`, no `--` | An optional immutable label, not identity, not unique. |
| `NNN` | three digits `000`–`999` | The scheduling priority; `0` is the highest. |
| `t` | 16 characters `[a-z2-7]` | Lowercase base32 of 80 random bits, drawn fresh for every move. |
| `from` | an unowned state | The state an owned job was claimed from. |
| `owner-id` | 32 lowercase hex digits | A UUID4 in hex. |

**Parsing.** Split the name on `~` into three or four parts: the job key
(parsed from the right: the last 36 characters are the UUID, a tag is followed
by `--`), `p` and three digits, a token, and optionally an unowned state;
anything else is not a job name. A three-part name is valid only below a
placement directory of an unowned state, a four-part one only directly below
`jobs/owned/<owner-id>/`. An entry without `~` below a state directory is a
placement directory; one with `~` that does not parse is skipped by listings
and reported by `fsck` (`unparsable_name`).

**The directory is the token (R3).** A job's location is its scheduling
state, its priority is in its name, and the fresh token makes every location
unique. A won rename therefore proves possession, and a listing finds work. A
job is found by UUID by listing its placement in each state (`locate`), never
by constructing a name. The name priority is the current scheduling priority:
`set_priority` and outcome priorities change only the name, while the
`job.json` priority is the submitted one.

**Other protocol names.** Scratch directories are
`<owner-id>.<purpose>.<t>` with a purpose `[a-z][a-z0-9-]{0,31}`; launch
directories `<attempt-uuid>.<n>` with `n` either `0` or 32 hex digits; request
files `<job-uuid>.<request-uuid>.json`; the temporaries of `write_file`
`.<name>.<t>.tmp`.

## The five rules and the invariants

**R1. Death is proven, never inferred from time.** Work changes hands only
when the death proof shows that an owner's whole execution scope has ended, or
when an operator attests it. Heartbeats are informational.

**R2. Only the owner writes; recovery requeues.** Only the owner named by
`jobs/owned/<owner-id>/` writes inside that job. A recoverer never continues
another owner's work: it returns the job, unchanged, to the state it was
claimed from, and the next claimant finishes the work from `state.json`.

**R3. The job directory is the token** (§Names as tokens).

**R4. No shared journal.** Each job carries its own owner-written
`state.json` and owner-appended `logs/runlog.jsonl`. Nothing indexes jobs
across the workspace, except the exchange index of exchange jobs.

**R5. Untrusted content is processed only when quiescent.** Trusted code acts
on what a job wrote (outcomes, transaction staging, child payloads, the
payload it seals) only after the attempt's execution scope has ended: reaped
by its own owner, or proven dead. No process can then hold a descriptor into
the job, so validation is a plain `lstat`-and-check walk followed by plain
path operations, refusing symlinks and special files where a path is
traversed. Read while the attempt lives, always anchored, no-follow,
non-blocking and bounded, are only the confined launch requests and
`progress.json`; written into a live job are only `logs/stdio.out` and the
launch output files (`O_CREAT|O_EXCL|O_NOFOLLOW`). The exchange inbox, whose
writer is live, is taken by an anchored move and validated as hostile input.

**Invariants.**

- **I1.** A job directory exists in exactly one place. The exceptions are the
  cross-filesystem copies of moving (§Moving jobs) and the duplicate residuals
  listed in §Accepted limitations.
- **I2.** No job directory name is ever reused: every move draws a fresh `t`.
- **I3.** Only the owner named by `jobs/owned/<owner-id>/` writes inside that
  job. A job leaves `owned/<O>/` only by O's own release, extraction into a
  bundle or deletion, or by recovery after O was tombstoned.
- **I4.** Before the move or write that makes a decision visible, everything
  needed to finish it is recorded: the `commit` intent and `release_to` in
  `state.json`, `bundle.json` and `eject.json` in an eject scratch, `source.json`,
  `rekey.json` and `plan.json` in an adopt scratch. Every later step is
  idempotent: "source gone" and "destination carries my token" both mean done.
- **I5.** No decision that hands over ownership or removes data depends on a
  clock. Time paces retries, schedules (deadlines, drain), dates records and
  drives retention and policy failures (the join grace, quarantine of stale
  requests); none of those lets a second writer in.
- **I6.** Absence is never decided from one observation where the decision is
  destructive or transfers ownership (§The settle rule). Elsewhere a missed
  entry only delays work.

## Filesystem primitives

These are the only operations that rename, replace, unlink or exclusively
create protocol entries (`_fs`). Every outcome is decided by observation
(`lstat` of the source and the destination), never by a return code: NFS can
report an error for a rename that happened, and `ENOENT` can come from a
pruned parent. `EXDEV` always raises; a primitive never turns a rename into a
copy. Retries are bounded to 8 rounds.

### Locations and anchors

A *plain* location is an absolute path and addresses a trusted directory. An
*anchored* location is one plain name (not empty, `.`, `..`, without `/` or
NUL) below a directory descriptor opened
`O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`. Anchored locations address
directories an untrusted party can write: the exchange directories, an
attempt's `launch/` directory, and a job's `logs/` while the job may be live.
`open_dir_under(root, relative)` opens each component of `relative` with
`O_NOFOLLOW` below a trusted root, optionally creating it.

### The settle rule

Another node's change may take up to the visibility deadline
(`policy.visibility_deadline_seconds`) to become visible. A destructive or
ownership decision that rests on absence MUST repeat its observation until the
deadline has passed: `locate`/`locate_many` with `settle` re-list every 0.1 s
across one deadline (tree members, presumed duplicates, vanished join children,
jobs of stale requests); recovery waits one deadline and re-lists before it
removes a dead owner's directories; the death proof waits one deadline before
it lists a dead owner's launches; the shared-memory sweep lists launch records
twice, one deadline apart. Where a missed observation heals itself, callers
settle for zero seconds (`move_once` never re-observes): a
claim misjudged as lost is found by self-healing, a lost recovery move means
another recoverer moved the job, and `take` recognizes a won move by a
non-empty scratch.

### The primitives

**`make_dirs(path)`** creates `path` and its missing ancestors as real
directories: a concurrent `mkdir` of a directory is success, a symlink or
non-directory at the deepest existing ancestor or at a component it creates
raises `UnsafePath`, and an ancestor pruned meanwhile restarts the walk. A
symlink further up is followed, so `path` MUST lie below directories only
trusted parties write; below a client-writable directory, `open_dir_under`
creates instead. A crash leaves ancestors, which mean nothing.

Plain locations (`lstat`, `exists`, and the destinations below) follow
symlinks in their intermediate components; only the final name is observed
unfollowed.

**`move_once(src, dst)`** is the contested move.

- *Precondition:* `dst` does not exist (otherwise `ValueError`), is fresh (it
  contains a fresh token or the caller's owner id) and, once there, only the
  caller can move it.
- *Steps:* create `dst`'s parents (unless forbidden), `rename`, then observe:
  `dst` exists → `WON`; `src` exists → retry; both absent → `LOST` (a win
  misjudged as lost is found where the next point says).
- *Post and crash:* `WON` means the caller possesses `dst` (durable mode
  fsyncs both parents). After a crash `dst` sits below a name only the caller
  reaches (`owned/<self>/` or its scratch), where self-healing or the scratch
  reconciler finds it.

**`move_owned(src, dst)`** moves an entry only the caller can move: its owned
job, its scratch, a staged transaction entry of its quiescent job.

- *Steps:* create parents, `rename`, then decide by the source alone: `src`
  absent → done, even if `rename` reported an error and even if a successor
  has already moved `dst` on; `src` present → retry, then `MoveFailed` with
  the last `errno`.
- It MAY replace a file or symlink at `dst`, which is why `dst` proves
  nothing. A caller MUST first check that `src` exists: an absent source would
  read as done (the kernel raises `OwnerLost` instead). A `dst` whose parent an
  untrusted party writes MUST be anchored.
- *Crash:* the entry is at either name, both of which the caller owns.

**`deliver(src, dst, token_name, token)`** moves a caller-owned directory to a
name another party may occupy (`<DEST>/<name>`, see eject below).

- *Precondition:* `src` holds the regular file `token_name` with exactly
  `token`; `dst`'s parent is never pruned. A `dst` whose parent an untrusted
  party writes (the exchange outbox) MUST be anchored at its parent, opened
  with `open_dir_under` from the workspace root.
- *Steps:* create `dst`'s parent, `rename`, then: `src` still present with
  `errno` in {`EEXIST`, `ENOTEMPTY`, `ENOTDIR`, `EISDIR`} → `OCCUPIED`; `src`
  present with any other error → re-raise it; `src` gone and `dst` carries
  the token → `DONE`; `src` gone without the token at `dst` →
  `DeliveryUncertain`, which callers report for operator inspection.
- "`dst` exists" alone never proves a delivery. `carries(dst, name, token)`
  opens `dst` with `O_NOFOLLOW` and reads the token anchored at it, bounded
  and non-blocking, so a planted symlink cannot pass.
- *Residual:* an *empty* directory created at `dst` concurrently is replaced
  by the rename, as POSIX allows.

**`publish_dir(staging, dst, nonce)`** creates the name `dst` at most once.

- *Precondition:* `staging` is caller-owned and holds the regular file
  `.nonce` with `nonce` (non-empty, because a rename replaces an empty
  destination directory); `dst`'s parent is never pruned.
- *Steps:* create the parent, `rename`; the result is whether `dst/.nonce`
  equals `nonce`, whatever `rename` reported. A leftover `staging` is removed.
  Used for exchange index entries and the daemon's ledger records.

**`write_file(dst, data)`** replaces a file atomically.

- *Steps:* create `.<name>.<t>.tmp` beside `dst`, anchored like `dst`, with
  `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC`; write all of `data`; fsync
  in durable mode; `rename` it over `dst`; fsync the directory in durable
  mode. If the replace reports an error, the write still counts as landed when
  `dst` now has the temporary's device and inode (an NFS retransmission); a
  missing temporary alone proves nothing. On any failure the temporary is
  unlinked.
- *Contract:* one writer per path, or every writer writes equivalent content
  (tombstones, `status.json`, deterministic requests). Readers never see torn
  content.
- *Crash:* a leftover temporary, removed by the owner's next reconcile, by
  recovery (in `owners/<id>/`) or by `gc` after a day.

**`create_exclusive(dst, data)`** creates a new file that must not exist, not
even as a symlink, writes `data` and returns the descriptor (the launch output
files in a live attempt's `launch/`).

**`open_append(target)`** opens a regular file for appending, creating it when
absent, `O_APPEND|O_NOFOLLOW|O_NONBLOCK`; anything but a regular file is
refused, so a planted FIFO cannot block it. One appender per file. The owner
logs use it (`OwnedJob.open_log`), after removing, unfollowed, anything but a
real `logs/` directory and regular log files.

**`read_bounded(src, limit, nonblock=False)`** reads a regular file of at most
`limit` bytes with `O_RDONLY|O_NOFOLLOW|O_CLOEXEC`, plus `O_NONBLOCK` for
anything a job or client may have written. It returns `None` for an absent
file and raises `UnsafePath` for a symlink or non-regular file and `TooLarge`
past the limit. `UnsafePath.kind` says which: `symlink`, `not_regular`, or
`not_directory` for a directory open (Linux does not tell a symlink from a
non-directory there). Every reader of job-written or client-written files uses
`nonblock`.

**`discard(target, trash_dir)`** removes a caller-owned tree: `move_owned` to
`<trash_dir>/trash.<t>`, then a bottom-up removal through descriptors opened
`O_NOFOLLOW`, which removes symlinks without following them and repeats a pass
over a directory that refilled (up to 8 passes). Owners pass their own `trash`
scratch as `trash_dir`, so a crash mid-removal leaves an owner-named scratch;
`gc` collects stray `tmp/trash.<t>` entries after a day.

**`remove_file(target)`** unlinks a file or symlink; an absent one is done.
**`remove_empty_dir(target)`** is `rmdir`, returning `False` for a non-empty
or absent directory. These are the only single-entry removals.

**`walk_untrusted(root, limits)`** validates and lists a tree written by an
untrusted party, without following symlinks. It refuses a root that is a
symlink or not a directory, special files, regular files with `st_nlink > 1`
(no inode is shared between jobs), names with NUL, more than 1,000,000 entries,
depths beyond 256 and more than 1 TiB of regular files in all. Symlinks below
the root are returned as opaque leaves.

**`copy_tree(src, dst, limits=None)`** copies a tree to a fresh name, possibly
on another filesystem: regular files and directories, special files refused;
durable mode fsyncs every file, directory and `dst`'s parent. A trusted copy
recreates symlinks as symlinks (the only symlinks the protocol creates). With
`limits` (an untrusted source) it refuses symlinks and hard-linked files and
enforces the entry, depth and byte bounds of `walk_untrusted`, counting bytes
as they are read, so a file the source keeps growing ends the copy. It is not
atomic: callers copy to a private or partial name and move or deliver it
afterwards.

**`rename_probe(a, b)`** checks the rename semantics above between two
directories: a non-empty directory moves keeping its inode, and a rename onto a
non-empty directory fails leaving both. `EXDEV` or a failed check raises
`FilesystemUnsupported`.

## Owners and the death proof

### Owners

Any actor that holds jobs or scratch is an owner. It registers before it
claims anything: `owners/<owner-id>/` is created and `owner.json` written
once.

```json
{
  "format": "httk-workflow-owner", "format_version": 1, "owner_id": "<32 hex>", "kind": "manager",
  "label": "manager on node17", "hostname": "node17", "boot_id": "<boot id>", "pid": 4711,
  "process_start_ticks": 123456, "started_at": "<utc>", "launch_id": "<32 hex>",
  "allocation": {"probe": "slurm", "kind": "slurm", "identity": {"job_id": "991"}, "end_time": 1760000000.0},
  "pools": ["default"], "capabilities": [], "prefixes": [], "resources": {"procs": 8},
  "end_time": 1760000000.0, "drain_start": 1759999880.0
}
```

`kind` is `manager`, `cli` or `daemon`; `launch_id` is the id the owner's launcher
gave it (`HTTK_LAUNCH_ID`), naming `logs/launchers/<launch id>.*`, or `null`; `boot_id` and `process_start_ticks`
may be `null`. `allocation` is `null` or the allocation the death proof may ask
about (`probe` is the `--allocation` specification, such as `slurm` or
`exec:PATH`). The advertised members from `pools` on are optional and
informational, and so is `heartbeat.json` (`{owner_id, updated_at}`).

### Launch records

An owner's *execution scope* is its own process and every process it started
for a job. Before the gate of any launch opens, the owner creates
`owners/<owner-id>/launches/<attempt-id>.<n>/` (`n = 0` for the attempt
runner, the launch request id for a confined launch) and writes its
`process.json`; it removes the directory once the launch is reaped.

```json
{"attempt_id": "<uuid>", "n": "0", "pid": 5120, "pgid": 5120, "hostname": "node17", "boot_id": "<boot id>",
 "process_start_ticks": 998877, "started_at": "<utc>", "allocation": null, "ranks_local_only": true}
```

`pid` leads the process group `pgid`; `ranks_local_only` says every process of
the launch is in that group on that host. A confined launch's directory also
holds `nodefile` and the trusted `launch.json` (format `httk-workflow-launch`,
carrying the launch token). Records scale with running launches only.

### The death proof

`probe(owner)` (`_death`) reads `owner.json`, `dead.json` and the launch
records and answers `ALIVE`, `DEAD` or `UNKNOWN` with evidence
(`{subject, rule, detail}` items). It writes nothing. Proof comes only from
the process table of the host it runs on, or from the batch scheduler.

1. `dead.json` exists → `DEAD`. Neither file, or a malformed `owner.json` →
   `UNKNOWN`.
2. The owner's process is *gone* when, on its recorded host, the boot id
   differs, the pid does not exist, or a process with other start ticks holds
   the pid. On another host it cannot be observed.
3. The owner is dead when its process is provably gone, or when it cannot be
   observed here and its recorded allocation has provably ended. A process
   running here is `ALIVE` whatever the scheduler says; an active allocation
   never makes a gone process alive. Otherwise `UNKNOWN`.
4. For a dead owner, wait one visibility deadline and list `launches/`. A dead
   owner adds no records, so the listing is complete.
5. The owner is `DEAD` only when every launch is dead (§Launch end evidence);
   otherwise `UNKNOWN`.

No clock rule exists: neither a stale heartbeat nor a recorded allocation end
time that has passed is evidence of death. An allocation that nobody can be
asked about stays `UNKNOWN` until it can be asked or an operator attests.
`probe_owner` (the kernel) refuses to probe an owner whose `owner.json` names
the calling process itself, and writes the tombstone when the proof says
`DEAD`.

### Launch end evidence

A launch is dead when:

- its directory was removed (reaped);
- it has no `process.json` (its gate never opened: no process of a launch runs
  before its record is durable);
- its `ranks_local_only` process group is observable on this host and gone
  (an observable group that exists is not dead, whatever the scheduler says);
- otherwise, its recorded allocation has provably ended.

A malformed or unreadable record is not dead. Allocation questions go to an
`exec:PATH` probe or to a maintained scheduler interface (Slurm), are paced by
a per-process cache, and answer "ended", "active" or "unknown". Managers that
share one allocation therefore cannot recover each other's jobs before the
allocation (or the sibling's processes, on this host) ends.

### Tombstones and operator attestation

`dead.json` is the tombstone:

```json
{"format": "httk-workflow-tombstone", "format_version": 1, "owner_id": "<32 hex>", "declared_at": "<utc>",
 "by": "operator", "evidence": [{"subject": "owner <id>", "rule": "operator", "detail": "node rebooted"}],
 "operator": "alice", "reason": "node rebooted"}
```

`by` is `probe` or `operator`; `operator` and `reason` appear only when given.
Dead stays dead, so any number of writers MAY write it and the last one wins.
After recovery `owners/<id>/` keeps only `dead.json`: it is what makes
fail-stop enforceable on an owner that wakes up after a false verdict. `gc`
removes it after `retention.owner_tombstone_days`, once nothing else is left.

`httk workspace attest-dead OWNER --reason TEXT [--operator NAME]` writes a
tombstone with `by: operator`. It runs the read-only proof first: an owner
proven `ALIVE` is refused, and one the proof cannot decide needs `--force`.
Attesting an owner that is in fact still running can apply a request twice and
run work twice (§Accepted limitations); the operator attests only after
confirming that the owner and all its launches are gone.

### Recovery

`recover(dead_owner_id)` runs only for a tombstoned owner, by any number of
recoverers at once; every step is a contested move or an idempotent removal.
Recovery never writes job content and reads only `job.json` (for the
placement).

1. **Jobs.** Each `jobs/owned/<dead>/<key>~p<NNN>~<t>~<from>` goes back with
   `move_once` to `jobs/<from>/<placement>/<key>~p<NNN>~<t'>`, priority
   unchanged. `LOST` means another recoverer returned it. A job whose name,
   `job.json` or placement cannot be read or used goes to
   `quarantine/<epoch>-<t>/entry` by `move_once`.
2. **Scratch.** Each `tmp/<dead>.<purpose>.<u>` is renamed with `move_once`
   to the recoverer's own `tmp/<self>.<purpose>.<t>`, so exactly one recoverer
   reconciles it, as its own work (§Scratch).
3. **Launches.** Each launch directory is discarded.
4. **Settle and remove.** After one visibility deadline the recoverer
   re-lists. When `owned/<dead>/`, `launches/` and the dead owner's scratch
   are empty, it removes `owned/<dead>/` and `launches/` with `rmdir`, then
   `owner.json`, `heartbeat.json` and leftover write temporaries. `dead.json`
   stays. Otherwise it repeats from step 1, up to 8 rounds.

`recoverable_owners` lists every owner with a readable `owner.json`, plus
every tombstoned owner that again holds jobs, launches or scratch (a falsely
attested owner that kept working). Managers probe up to three of them, at
random, per tick, and recover those proven dead; `httk workspace gc` does the
same for all of them (category `dead_owners`). A CLI owner killed by a signal
is provable only on its own host, so `gc` there recovers it.

### Scratch

Every temporary directory belongs to an owner and is named after it:
`tmp/<owner-id>.<purpose>.<t>/`, created `0700`. A scratch is reconciled by
its owner's clean exit or, after its owner's death, by the recoverer that won
its rename. An empty scratch and one of a purpose in `DISCARDABLE_PURPOSES`
are discarded without a reconciler; any other purpose needs its registered
reconciler, and a scratch whose reconciler is not registered in the process,
or cannot finish, is kept (it may hold the only copy of a job). A purpose whose
reconciler must still decide after a step that empties the scratch records
what it needs first (an eject's `eject.json`).

| Purpose | Holds | Reconciler |
| --- | --- | --- |
| `trash`, `build`, `landing`, `release`, `claim` | entries being removed, store installations, pulled landings, released holds, exchange-index staging | discard (`DISCARDABLE_PURPOSES`) |
| `submit` | a payload being submitted, complete once named `job/` | submit `job/` to `ready`; without it, discard |
| `eject`, `hold` | a bundle being built and delivered, and `eject.json` once delivery starts | roll forward or back (§Moving jobs) |
| `adopt`, `adopt-untrusted` | a bundle being adopted; the trust is in the purpose | resume adoption (§Moving jobs) |
| `quarantine` | an entry won for quarantine | finish the move into `quarantine/` |

There is no age-based collection of scratch: ownership decides.

### Self-healing, clean exit and fail-stop

- **Self-healing.** Every tick a manager reconciles each job in
  `owned/<self>/` that this process does not hold and releases it back, so a
  claim misjudged as lost is never stranded.
- **Clean exit.** An owner gives back the jobs it does not hold, reconciles its
  scratch and, when nothing was kept and no launch record remains, removes
  `owned/<self>/`, `owner.json`, `heartbeat.json` and its directory. On
  `SIGINT` or `SIGTERM` it gives back its quiescent jobs first; on an error it
  touches nothing and waits for recovery.
- **Fail-stop.** An owner that finds its tombstone, a missing or foreign
  `owner.json`, or a job it did not release gone from `owned/<self>/` was
  declared dead falsely (`OwnerLost`). It kills every attempt and launch it
  started and stops without touching any job. Managers check this at every tick
  and before every launch.

## Job documents

### `job.json`

`job.json` (format `httk-workflow-job`, version 3) is the immutable
declaration of a job: strict, at most 1 MiB, written as compact sorted JSON
with a newline. The protocol never rewrites it, except that the adopter of an
exchange bundle rewrites the client's ids and `claim.pool` before publication
(§Adopt), and the commit of an exchange job sets its children's `claim.pool`
(§Children).

| Member | Type | Meaning |
| --- | --- | --- |
| `format`, `format_version` | `"httk-workflow-job"`, `3` | |
| `id` | UUID | The job id. |
| `tag` | label or `null` | The job key tag. |
| `name` | string | Display name. |
| `placement` | canonical placement text | Authoritative and immutable (§Placement rules). |
| `workflow` | `{id, name}` | The installed workflow's id (§The workflow store) and its name; for a job with an embedded workflow (§Embedded workflows) the id `embedded:<name>@<sha12>`. |
| `initial_step` | step name | The step of the first activation. |
| `priority` | `0`–`999` | The submitted priority. |
| `claim` | `{pool, required_capabilities}` | Eligibility labels; pool `default` needs no routing. A pool `exchange-<16 hex>` is an exchange pool (§Adopt). |
| `retry_policy` | `{maximum_attempts_per_activation?, maximum_total_attempts?, maximum_activations?, retry_on?}` | Budgets (absent is unbounded) and the manager-detected failure codes that are retried. |
| `resources`, `step_resources` | objects | Requirements per job and per step; `maxtime`/`mintime` are seconds. |
| `parameters`, `declarations`, `declared`, `environment` | objects | Workflow inputs and metadata, opaque to the protocol. |
| `parent` | `{workspace_id, job_id, job_key, placement, activation_id, spawn_id}` or `null` | The spawning job. |
| `seal_succeeded` | boolean or `null` | Whether a succeeded job is sealed; `null` defers to the `seal.succeeded` setting. |
| `staged_files` (optional) | array of `{path, size, sha256}`, sorted by `path` | The files `job new` staged (`--file`/`--files` and declared-input destinations), each with its payload-relative path and the size and lowercase hex SHA-256 computed when it was staged; `collect` records each as the run's file input exactly as staged. A runner-prepared payload may give only `{path}`, omitting `size` and `sha256` together. Absent when there are none. |
| `calls` (optional) | object: call slot alias → exactly `{workflow, parameters}` | The binding of every call slot of the workflow: the installed id (non-empty, no whitespace) the slot calls and the parameters it is called with (finite JSON); at most 256 KiB serialized. Present exactly when the workflow has call slots; a manager claims the job only when its alias set is the installed workflow's slot set and each bound workflow and its fixed calls are installed (and built). |
| `embedded` (optional) | `{tree_sha256, runner, steps, initial_step, calls, slots, requires, source}` | Present exactly when `workflow.id` starts with `embedded:` (§Embedded workflows). `tree_sha256` is the embedded digest of `<payload>/workflow`; `runner` is exactly `{entry}` (one member name) or `{command}` (a string list without `{artifacts}`); `steps` are strings including `initial_step`; `calls` values are installed workflow ids, never `embedded:`; `slots` and `requires` are validated as in `install.json`; `source` is a commit-pinned git URI or `null`. The trusted subset of an installation, frozen by the creator from the manifest read at creation. |
| `persist_outputs` (optional) | object: interface output name → output role | Set on a job called through a call slot: on `succeed`, the job runs its own workflow's collect hook and records each role's property value in `.httk-job/outputs.json` as `{format: "httk-workflow-outputs", format_version: 1, outputs: {name: {role, definition, value}}}`, or fails with `workflow.outputs_unavailable`. The file is runner-private (outside the payload digest and the seal) and travels with a trusted bundle. |

The **job digest** is the SHA-256 of the stored `job.json` bytes, exactly as
read. The first reconcile of a job (its first activation) pins it in
`state.json` `job_digest`. From then on a different digest fails the job with
`protocol_error`, and an attempt that changed `job.json` has its outcome voided
(§Exit without an outcome). An unlocked job (§Unlocked jobs) is the exception:
a different digest is pinned again, with a `job_digest_repinned` history entry carrying `{from, to}`,
and a `job.json` that cannot be read still fails the job.

### `state.json`

`state.json` (format `httk-workflow-state`, version 1) is written only by the
job's owner, through `write_file`, and is absent until an owner first writes it
(a published child and an adopted exchange job carry one without an
activation). It is strict except that an absent `job_digest`, `budget_override`,
`workflow_override` or `unlocked` reads as `null`, and at most 100 MiB.

| Member | Value | Meaning |
| --- | --- | --- |
| `format`, `format_version` | `"httk-workflow-state"`, `1` | |
| `job_id` | UUID | Must equal the job's id. |
| `updated_at` | timestamp | |
| `owner_id` | owner id or `null` | The last writer. |
| `phase` | `{kind, attempt_id}` | `kind` is `idle`, `launching` or `running`; `attempt_id` is `null` exactly when `idle`. |
| `activation` | `{id, step, ordinal, reason}` or `null` | `reason`: `initial`, `advance`, `retry`, `manual_continue`, `override_step` or `join`. |
| `attempt` | `{id, ordinal, reason, previous_attempt_id, started_at, unclean}` or `null` | The current attempt; `started_at` is `null` until a launch enters `launching`. |
| `counters` | `{activations, attempts_total}` | Budget counters. |
| `resources` | object or `null` | The dynamic requirement of the activation (from an outcome). |
| `runner_steps` | list or `null` | Steps an outcome declared; evidence only. |
| `failure` | failure object or `null` | The current failure (§Outcomes). |
| `failure_history` | list, at most 16 | The last failures. |
| `join` | object or `null` | The pending join (§Waiting and joins). Kept when the parent fails at its wait with `dependency_failure` (impossible without `on_impossible`, or an unresolvable child) or `budget_exhausted` (the advance would pass the effective `maximum_activations`), so the wait can be reopened. |
| `observations` | list | The children as the decided join saw them. |
| `children` | list | Published children: `{job_id, job_key, label, placement, spawn_id, attempt_id, activation_id}`; for an adopted exchange tree `{job_id, job_key, placement}`. |
| `commit` | object or `null` | The commit intent (§The commit). |
| `release_to` | `{state, priority}` or `null` | The release intent (§The release rule). |
| `applied_requests` | list of request ids | Requests applied by this job's owners whose files may still exist. |
| `origin` | `"local"` or `"exchange"` | Exchange jobs and their descendants are `exchange`. |
| `exchange_name` | UUID or `null` | The client's own id of an exchange job. |
| `detached` | `{at, operator}` or `null` | Set by `detach`. |
| `seal` | `null`, `{sha256, signed}`, `{disabled: true}` or `{released: true, at, request_id}` | The authoritative seal record (§Seals). |
| `workflow_pin` | `{id, tree_sha256, source, workflow_present}` or `null` | The installation the last launch used. `source` is `"installed"` (the installation in the store), `"embedded"` (a locked embedded-workflow job's `workflow/`, verified against `job.json` before the launch) or `"copy"` (the job's own `workflow/` runner copy, for an unlocked job); `workflow_present` is a boolean: whether `workflow/` existed at launch. For a copy, `tree_sha256` is the digest of that copy at launch, or `null` when it cannot be digested. |
| `history_tail` | list, at most 32 | `{at, event, ...}` entries: `released`, `request_applied`, `request_dropped`, `join_decided`, `paused`, `unsealed`, `job_digest_repinned`. |
| `job_digest` | 64 hex digits or `null` | The pinned job digest. |
| `budget_override` | object or `null` | Optional. Set by `set_budget`: keys are a non-empty subset of `maximum_attempts_per_activation`, `maximum_total_attempts` and `maximum_activations`, each an integer of at least 1 or `null` (that limit lifted). Absent keys fall back to `job.json`. The effective policy is the `retry_policy` of `job.json` with these keys replaced. |
| `workflow_override` | `{id, name}` or `null` | Optional. Set by `set_workflow`: the installed workflow the job runs instead of `job.json`'s `workflow` (both members nonempty strings). Every run path (precheck, the manager's assessment and launch, the attempt context) resolves the **effective** workflow, this override or else `job.json`'s; collection of a job with an override uses `workflow_pin`, the installation that produced the outputs. A damaged `state.json` is not salvaged for it. |
| `unlocked` | `{at, operator, reason?, inherited_from?}` or `null` | Optional. Set by `unlock`, or seeded into the first `state.json` of a child published by an unlocked parent (`inherited_from`: that parent's job key). One-way: no request clears it, and damage handling keeps it. It marks the job as debugged (§Unlocked jobs). |

An unowned job's `state.json` is stable, because nobody owns the job; readers
read it non-blocking and bounded and tell an absent file from a damaged one.

### Run logs

`logs/runlog.jsonl` is the job's timeline, appended only by its owner
(`OwnedJob.append_log`), one JSON object per line:

```json
{"at": "<utc>", "event": "committed", "owner_id": "<32 hex>", "attempt_id": "<uuid>", "to": "ready", "detail": "advance"}
```

`at`, `event` and `owner_id` are always present; `attempt_id`,
`activation_id`, `step`, `from`, `to`, `detail` and event-specific members
appear as relevant. The events are `claimed`, `recovered` (`detail`: the
previous owner or `self-healed`), `attempt_started` (`detail.command`),
`launched`, `attempt_ended` (`detail.exit_status`), `outcome`, `committed`,
`sealed`, `request_applied`, `failed` (`detail`: the code), `released`
(`detail.priority`), `ejected` (`destination`) and `collected` (`gc`). Readers
skip a torn last line; a commit replay appends only events missing from the
tail. `logs/stdio.out` captures the attempt's output between marker lines
`=== httk attempt <A> step <step> ordinal <n> started <utc>` and
`=== httk attempt <A> ended <utc> exit <status> outcome <action>`. Both files
are trusted and read-only to the job.

`.httk-job/runlog.jsonl` is the runner's own evidence log, job-writable and
untrusted (format `httk-workflow-runlog-event` version 2: `timestamp`, `kind`,
`message`, `files`). `job log` and the collectors read both logs; the owner
events form the timeline and the runner records annotate it.

`workflow/` at the top of the payload exists only in an unlocked job made with
a runner copy: the copy of the installed `package/` the manager launches the
runner from (§Unlocked jobs). `workflow` is a protocol-reserved top-level
payload name (with `job.json`, `state.json`, `attempts/`, `logs/` and
`.httk-job/`): input destinations, `staged_files` and `--input-move-from-job`
paths may not target it, and an httk-v1 template package with a top-level
`workflow` member is refused at job preparation. Execution from `workflow/` is
not automatic: only an unlocked job (`state.json` `unlocked`) that has the
directory runs from it, and the `workflow/` of a locked job is inert. Being a
top-level payload member, it is covered by the job seal if one is written;
unlocked jobs are never sealed.

## Jobs

### Submission

A creator (`job new`, `job submit`, campaigns) validates a complete payload in
a `submit` scratch, renames it to `<scratch>/job/`, and publishes it with
`submit`: `move_owned` to `jobs/ready/<placement>/<key>~p<NNN>~<t>`, with key,
placement and priority from `job.json`. A fresh job carries no `state.json` and
no `logs/`. There is no `submitted` state; the `submit` reconciler finishes an
interrupted submission.

### Claiming and eligibility

`claim(ref)` is `move_once` of an unowned job to
`jobs/owned/<self>/<key>~p<NNN>~<t'>~<from>`, keeping the priority. `LOST` means
another actor moved it. The claimant then reads `job.json`; one that cannot be
read leaves the job owned, and self-healing quarantines it.

A manager reads `job.json` and `state.json` *before* claiming, to check
eligibility; the reads are hints, cached by directory name, which changes with
every move. The checks run in this order, and a job that fails one stays in
`ready`, unclaimed and not failed:

1. `claim.pool` is one of the manager's pools (or it accepts any pool);
2. `claim.required_capabilities` are all advertised;
3. the job's workflow and the transitive closure of its calls are installed,
   and each package with a build section is built for this platform;
4. the installed manifest's `requires` is met by the manager's environment;
5. the effective resource requirement fits the manager's capacity;
6. the effective `mintime` fits before the manager's drain point.

A manager acts only on jobs whose directory and `job.json` its own uid owns. It
claims eligible jobs in priority order within a bounded, resumed window.

### The release rule

Every claim, modify and release path ends with `OwnedJob.release(doc,
Release(state, priority, applied_requests))`:

1. write `state.json` with the modification, `release_to: {state, priority}`,
   the applied request ids added to `applied_requests`, and `commit: null`, in
   one write;
2. delete those request files;
3. `move_owned` the job to `jobs/<state>/<placement>/<key>~p<priority>~<t>`,
   the placement taken from `job.json`.

A `release_to` is *pending* exactly when `(state, priority)` differs from the
state and priority in the owned name (`~<from>`, `p<NNN>`). If the job was
recovered before the move, it returns to `<from>`, which differs, so the next
claimant finishes the release. If the move happened, the job was claimed from
that very state and nothing is pending. When the target equals `<from>`,
pending and done are the same place.

**Give-back** (`give_back`) returns an owned, quiescent job exactly as recovery
would: a pending release is applied, otherwise the job moves to `<from>` with
its claimed priority and no state write, so an unfinished `commit` intent
stays for the next claimant. A manager gives a job back after five consecutive
failed reconcile or commit passes.

### Reconcile

Every claim of a job, and every self-healing adoption, runs `reconcile` before
anything else:

1. Remove write temporaries beside `state.json` and in `logs/`; read
   `job.json` and `state.json` (unreadable: §Damage handling).
2. **Pending `release_to`:** finish the release (the applied request files are
   deleted on sight, never re-applied). Nothing else runs.
3. A job without an activation gets its initial activation; a job without
   `job_digest` gets it pinned.
4. **`commit` intent:** resume the commit from its transactions (§The commit).
5. **Digest mismatch:** fail with `protocol_error`; pending requests still
   apply, from `failed`.
6. **`phase: launching(A)`:** the gate opens only after `running` is written,
   so A never ran. Its attempt directory is removed and A is launched again
   under its own id, without counting an attempt.
7. **`phase: running(A)`:** A is provably dead. A valid
   `attempts/A/outcome.ready` is committed (the step is not rerun); otherwise
   a pending `cancel` request cancels the job; otherwise the attempt failed
   with `owner_lost` (unclean; `process_failure` if this very owner ran it)
   under the retry policy.
8. Discard uncommitted transaction staging (`attempts/*/txn/*.tmp`).
9. Apply the pending requests (§Requests); the first that releases or deletes
   the job ends the reconcile.

### Launching an attempt

A launch runs only on an owned, reconciled, eligible job, in this order:

1. Choose the attempt: reuse `state.attempt` while its `started_at` is `null`,
   otherwise record the next attempt. A budget already exceeded (the effective
   budgets: `retry_policy` with `budget_override`) fails the job with
   `budget_exhausted`.
2. Write `state.json` with `workflow_pin` and `phase: launching(A)`, which
   stamps `started_at`.
3. Create `attempts/A/` (and `run/`) anchored below the job, refusing a
   symlink the job left.
4. Create the launch directory `owners/<self>/launches/A.0/`.
5. Append `attempt_started`, then start the runner behind a closed gate (in
   the sandbox when confined).
6. Write `process.json`, then `state.json` with `phase: running(A)`. This is
   the last trusted write into the payload root before any job process runs.
7. Mark the job live (`begin_attempt`), open the gate, append `launched`.

A failure before the gate opens removes the launch directory and commits a
failure. The attempt is live until its process group and every launch it made
are reaped (`end_attempt`); only then is the job quiescent.

The attempt directory `attempts/<A>/` is the job's control area for A:

```text
attempts/<A>/
├── outcome.tmp.<uuid>/ → outcome.ready/      the outcome draft and its publication
│   └── children/{spawn.json, jobs/<key>/}    proposed children
├── txn/<seq>.tmp/ → txn/<seq>/               transactions, seq six digits
├── launch/                                   confined launch requests and their answers
├── error.json, prelude.sh, binding.json, nodefile
```

**The sandbox.** With `manager.confine=bwrap` the attempt runs in Bubblewrap
(user, PID, IPC and UTS namespaces, the network one unless configured
otherwise, no capabilities) with the workspace bound read-only and the job
directory writable, both at their real paths, and read-only overlays of the
trusted entries present at launch: `job.json`, `state.json`,
`.httk-job/seal.json`, `.httk-job/releases/`, `logs/` and `workflow/`, for the
attempt and for its ranks (the rank keeper sandbox of `HTTK_WORKFLOW_LAUNCH`) alike (job-local
code runs only inside the job's own sandbox, and nothing an attempt writes there
is trusted afterwards). A symlinked trusted entry, `workflow` included, fails the
launch. Each overlay
binds the inode opened when the sandbox is built, so the `state.json` replaced before the gate opens stays
read-only to the job. Installed workflows, the parent and the children are
visible read-only through the workspace bind. The attempt context
(`HTTK_WORKFLOW_CONTEXT`) and the `HTTK_WORKFLOW_*` variables are described in
{doc}`/runtime_helpers`. The context (format `httk-workflow-attempt-context`,
version 3) carries `workflow: {id, name}`, the installation the manager
launched: the job's effective workflow. A child the attempt spawns names that
workflow in its `job.json`, and `a.call` resolves aliases in that
installation's `[workflow.calls]`.

### Confined launches

Under confinement `HTTK_WORKFLOW_LAUNCH` is a launch client that publishes
`ID.request.json` (format `httk-workflow-launch-request`) in
`attempts/<A>/launch/`; the manager reads it while the attempt lives,
anchored, non-blocking and bounded. It admits one launch at a time per live,
unpublished attempt: it creates `owners/<self>/launches/A.ID/` (`nodefile`,
`launch.json`), creates `ID.stdout` and `ID.stderr` with `create_exclusive`,
starts the launch behind a gate, writes `process.json`, and answers in
`ID.status.json` (`refused`, `exited`, `stopped` or `uncertain`). A client's
`ID.stop` or the attempt ending stops the launch. Ranks share a per-launch
directory `httk-<token>/` below the node-local `confine.shm_root`; a rank helper
removes every such directory whose token no launch record names, from two
listings one visibility deadline apart. In its `ns/`, the rank helpers of the
launch on the node elect the one that starts the node's namespace keeper
supervisor by renaming a prepared generation directory into place, and count
the ranks still running through a FIFO whose read end the supervisor holds. No
lock is involved.


### Outcomes

A step publishes its outcome by renaming a complete draft
`attempts/<A>/outcome.tmp.<uuid>/` onto `attempts/<A>/outcome.ready/`, after
synchronizing the draft in durable mode. The rename is the publication point; a
draft is ignored, and a second publication fails because the destination
exists.

```json
{"format": "httk-workflow-outcome", "format_version": 2, "job_id": "<uuid>", "activation_id": "<uuid>",
 "attempt_id": "<uuid>", "action": "advance", "next_step": "collect", "message": "relaxation converged"}
```

`job_id`, `activation_id` and `attempt_id` MUST name the current activation
and attempt. Optional members: `priority` (0–999, the priority of the release
the commit makes), `runner_steps`, `resources` (only on `advance` and `wait`),
`message`, and the action's own members:

| Action | Required | Committed target |
| --- | --- | --- |
| `advance` | `next_step` | `ready`, new activation (`failed`/`budget_exhausted` past the effective `maximum_activations`) |
| `wait` | `next_step`, `join` | `waiting` (§Waiting and joins) |
| `succeed` | none | `succeeded` |
| `fail` | `failure` | `failed`; `ready` (retry) when `retryable` and the budget allows |
| `retry` | `retry.reason` | `ready`, next attempt; `failed`/`retry_exhausted` past the effective budget |
| `pause` | `pause.reason` | `paused` |

**Failure object:** `{code, message, details?, retryable?}`, nothing else.
`code` is one token of at most 128 bytes, `message` a non-empty sentence,
`details` an object, `retryable` a boolean (default `false`). A malformed
failure fails the job with `protocol_error`. Manager failures carry
`details.log_paths: ["logs/stdio.out"]` and `details.exit_status` when known.

### Exit without an outcome

An attempt that ends without a valid outcome fails with a reserved code below.
A cancelled attempt commits as `cancelled` whatever it published. An attempt
that changed or broke `job.json`, or that left a `workflow/` that did not exist at its launch (`workflow_pin`
`workflow_present` false), has its outcome voided and that `workflow/` deleted; the
same check runs when the manager recovers a crashed `running` attempt. Manager-detected
failures are retried only when their code is in `retry_on` and the effective
budgets (`retry_policy` with `budget_override`) allow.

| Code | Meaning |
| --- | --- |
| `protocol_error` | Exit status 0 without an outcome, an unusable outcome, or an outcome, child, transaction, seal or `job.json` the protocol forbids; an attempt that created `workflow/` fails with the message `the attempt created workflow/; it was removed`. |
| `process_failure` | A runner that could not be started, or exited nonzero without an outcome. |
| `owner_lost` | Unclean: the attempt's owner died, or drained it, before it published. |
| `timeout` | The manager stopped the attempt at its `maxtime`. |
| `retry_exhausted` | A retry past the effective attempt budgets. |
| `budget_exhausted` | An effective attempt or activation budget exceeded. |
| `dependency_failure` | A join became impossible or a child vanished. |
| `data_conflict` | A committed transaction met the wrong kind of entry. |
| `runner_unavailable`, `runner_build_failed`, `runner_not_built` | The installed workflow vanished between eligibility and launch, declares no usable runner, or its platform probe failed. |
| `code_support_unavailable` | A simulation-code package's Bash API is missing. |

The shipped runner libraries also publish `no_outcome`, `unknown_step` and
(the *httk* v1 compatibility runner) `declared_failure`. Application codes
SHOULD be namespaced, as in `vasp.nonconvergent`.

### The commit

A commit runs only when the attempt is quiescent (R5) and is decided once:

1. **Validate** the outcome and its children (§Children).
2. **Decide.** Write `state.json` with the `commit` intent.
3. **Transactions.** Discard uncommitted staging and apply every committed
   transaction (§Transactions). A `DataConflict` or unsafe staging rewrites
   the intent to `failed` (`data_conflict` or `protocol_error`) with
   `transactions_failed: true`, no children and no seal, and the remaining
   staging is discarded; a replay that sees the flag skips this step.
4. **Children.** Publish them from the intent alone.
5. **Seal.** For a `succeed` with `seal: true`, write `.httk-job/seal.json`.
   Payload content the job made unsealable (a FIFO, a symlinked `.httk-job`)
   rewrites the intent to `failed`/`protocol_error` with `seal_failed: true`;
   an I/O error leaves the intent, and the commit is retried.
6. **Attempt removal.** Remove `attempts/<A>`, except for the targets
   `failed` and `cancelled`, which keep it as evidence.
7. **Log and release.** Append `committed`, apply requests posted during the
   attempt from the target state (the owner's boundary), and release to
   `target_state` with the settled document, which replaces the intent by
   `release_to`.

After a crash anywhere the job is recovered to `<from>`, and the next
claimant's reconcile resumes at step 3 or finishes the release: a decided
outcome is never relaunched.

| `commit` member | Meaning |
| --- | --- |
| `attempt_id`, `action` | The attempt and its action (`cancel` for a cancelled attempt). |
| `target_state`, `priority` | Where the release goes. |
| `next_step`, `resources`, `join`, `pause` | Action details for the settled document. |
| `failure`, `reason`, `unclean` | The failure, why a retried attempt runs, whether it follows an unclean end. |
| `seal` | Whether to seal (decided at step 2, so a replay is deterministic). |
| `children` | The publication plan: `{job_id, job_key, label, spawn_id, placement, priority, staged, initial_state, embedded_sha256}`, `embedded_sha256` being the 64-hex embedded digest of a child that gets its parent's tree, else `null`, `staged` being `attempts/<A>/outcome.ready/children/jobs/<key>`. |
| `request_id`, `request` | The applied `cancel` request and its audit record. |
| `transactions_failed`, `seal_failed` | The rewrites of steps 3 and 5. |

### Transactions

A job commits data to its own payload with transactions; there are no data
modes.

- **Job side.** A transaction stages a tree that mirrors the payload root in
  `attempts/<A>/txn/<seq>.tmp/` (seq: six digits, increasing within the
  attempt) and commits by renaming it to `txn/<seq>/`. `Attempt.put` stages
  into the attempt's implicit transaction below `data/`, which commits with
  the outcome. A staged path MUST NOT start with `job.json`, `state.json`,
  `seal.json`, `logs`, `attempts` or `workflow`.
- **Manager side** (quiescent; at every commit and reconcile): uncommitted
  `*.tmp` staging is discarded; committed transactions are applied in seq
  order, for every action, because the job committed them by its own choice.
  Each tree passes `walk_untrusted`, and entries are applied top-down: a
  directory whose target is absent is renamed whole, a directory over an
  existing directory is merged, and a file or symlink is renamed over the
  target file or symlink (`move_owned`). A source already gone was applied. A
  directory over a non-directory, a non-directory over a directory, or a
  parent that is not a directory is `data_conflict`. Emptied staging is
  removed.
- **Guarantee:** each committed transaction is applied completely before any
  later attempt starts. Readers MAY see partial application in between; there
  is no all-or-none visibility and no removal operation.

### Children

A runner proposes children in its outcome: complete payloads in
`outcome.ready/children/jobs/<key>/` and `children/spawn.json` (format
`httk-workflow-spawn`, version 2, strict:
`{format, format_version, children: [{job_key, label, placement,
workspace_id?, job_id?, spawn_id?}]}`). Validation (commit step 1) requires
unique labels and keys within the spawn set, this workspace, a `job_id` agreeing with the key, a
real directory per child that passes `walk_untrusted`, no `state.json`,
`seal.json`, `logs`, `attempts` or `workflow` at its top, a valid `job.json` whose key and
placement match and whose `parent` names this job (id, key, placement,
workspace, spawn id), and ids not already present. A child of an exchange
job (`origin` `exchange` in its trusted `state.json`, or a job in an exchange
pool) gets exactly the parent's pool: validation rewrites the staged
`job.json` when the runner named another (idempotently, so a revalidation
after a crash finds the same child). A child of any other job that names an
exchange pool fails the outcome. The plan records each
child's initial `state.json`, inheriting the parent's `origin` and
`exchange_name`.
The manager publishes the children of every outcome action, but a `fail` or
`retry` outcome published through the SDK carries none: its outcome draft,
which native Python runners, the Bash bridge and builder composition all
publish through, discards the children the attempt staged, so a failed parent
leaves no orphans and a retried activation does not stage them twice.


Publication (step 4) works from the plan alone: for each child whose staged
directory still exists, write its initial `state.json` and `submit` it to
`ready`. A staged directory already gone was published. Children are renamed,
not copied, which is safe because the parent is quiescent. The children are
recorded in the parent's `state.json` `children`. A child's workflow is
checked when the child is claimed, not at the parent's commit.

The manager validates label uniqueness per spawn set only. Uniqueness per parent
job, across activations, is a runner-side contract the SDK enforces: a spawn or
call whose label names an existing child of the job reuses that child and
stages nothing.

### Waiting and joins

A `wait` outcome records the join in `state.json` and releases the parent to
`waiting`:

```json
{"children": [{"label": "a", "job_id": "<uuid>", "job_key": "<key>", "placement_hint": "p"}],
 "condition": "all_succeeded", "count": 0, "on_impossible": null, "next_step": "aggregate"}
```

A join may name an existing child of this parent, not only children the same
attempt spawned (a reused child, as `rejoin` entries already do).

| Condition | Satisfied when | Impossible when |
| --- | --- | --- |
| `all_succeeded` | every child succeeded | any child failed or was cancelled |
| `all_terminal` | every child is terminal | never |
| `any_succeeded` | some child succeeded | every child is terminal and none succeeded |
| `any_terminal` | some child is terminal | never |
| `at_least` | at least `count` (≥ 1) succeeded | successes plus non-terminal children < `count` |

**Evaluation reads only.** A manager reads the waiting parent's `state.json`
and lists each unresolved child's placement in the unowned states (one listing
per state and placement per pass). A child not found there is pending (owned
or moving). Only a child unresolved for longer than the join grace (manager
option, default one hour) is looked up again, settled and in every
`owned/<id>/`; a miss there makes the join `unresolvable`. When the join is
decided, the manager claims the parent, re-evaluates from fresh listings, and
releases it: satisfied → `ready` with an activation of `next_step` (reason
`join`); impossible → the `on_impossible` step when it is
`{"action": "advance", "next_step": ...}`, else `failed`; unresolvable →
`failed` (`dependency_failure`). The decision records `observations` (`{label,
job_id, job_key, placement, state, token, failure}`) in every case. It clears
`join` on every outcome except one: `join` is kept when the parent fails at its
wait so the wait can be reopened (`continue`): with `dependency_failure`
because the condition became impossible without an `on_impossible` advance, or
because a child is unresolvable; and with `budget_exhausted` because a
satisfied join, or an `on_impossible` rescue, would advance past the effective
`maximum_activations`. A `continue` restores the declared allowance (with
`keep_budget`, after `set_budget` raises that limit), re-decides the join and
advances. A successful advance and a join naming an
invalid step clear it.

**The revival guard:** `continue`, `restart` or `override_step` of a child is
refused, unless forced, while its parent's current `state.json` observations
name it. The exception is a child that the parent's `join` still names (the
parent is `failed` with a retained `join`, or `waiting` on it again): reviving
the child is then the intended flow and needs no `force`.

### Damage handling

A job's processes can damage its own trusted files (`chmod 000`, a FIFO at
`state.json`); `EACCES`, `EPERM` and `EIO` on a trusted read are damage, not
crashes.

- **`job.json` unreadable at the claim:** the job stays owned without a
  handle, and self-healing (or the claiming CLI owner, or `gc`) quarantines it.
  During recovery such a job goes to quarantine too.
- **`job.json` or `state.json` unreadable after the claim:** the job fails
  with `protocol_error` in a fresh `state.json` that salvages `origin`,
  `exchange_name`, `job_digest`, `unlocked` and `applied_requests` where they
  still parse.
  Without salvaged `applied_requests`, every pending request of the job is
  deleted and recorded as `request_dropped`: none can be told from an applied
  one. A CLI owner gives such a job back instead, or quarantines it when it
  cannot.

## Requests

### Request documents

Operators change jobs only through request files
`.httk-workspace/requests/<job-uuid>.<request-uuid>.json`, written with
`write_file` onto that unique name. The format is `httk-workflow-request`,
version 3, strict, at most 64 KiB:

| Member | Required | Meaning |
| --- | --- | --- |
| `format`, `format_version` | yes | `"httk-workflow-request"`, `3` |
| `request_id`, `job_id` | yes | UUIDs; the file name MUST be `<job_id>.<request_id>.json`. |
| `placement` | yes | The job's placement, a hint for locating it. |
| `action` | yes | `cancel`, `pause`, `continue`, `restart`, `override_step`, `set_priority`, `set_budget`, `set_workflow`, `detach`, `unlock`, `eject`, `delete`, `seal` or `unseal`. |
| `operator`, `reason`, `created_at` | yes | Attribution and ordering. |
| `priority` | `set_priority` only, required | 0–999. |
| `budget` | `set_budget` only, required | An object with the shape of `budget_override`: a non-empty subset of `maximum_attempts_per_activation`, `maximum_total_attempts` and `maximum_activations`, each an integer of at least 1 or `null` (lifted). |
| `workflow` | `set_workflow` only, required | An installed workflow id (`local:`, `adhoc:` or `git+`), never a short name and never a `embedded:` id. |
| `step` | `override_step` only, required | The step to restart at. |
| `no_copy` | `unlock` only, optional; only `true` | Mark the job without copying the installed package (`job unlock --no-copy`, and what the CLI posts to ancestors). Absent means the owner copies the runner (§Unlocked jobs). |
| `destination` | `eject` only, required | An absolute directory, or a missing name in one. |
| `force` | `continue`, `restart`, `override_step`; only `true` | Accept the revival hazard. |
| `keep_budget` | `continue`, `restart`, `override_step`; only `true` | Keep the effective retry limits: do not restore the declared allowance (§Application). |
| `tree` | `eject` only; only `true` | Eject the job's descendants with it. |
| `operator_key`, `signature` | both or neither | An *httk* identity signature, for attribution. An unsigned request applies; one whose signature does not verify is malformed. |

A malformed request stays in place and is quarantined by `gc` after a day, as
is a request whose job cannot be found (settled, owned jobs included).

### Application

**Only the job's owner applies requests (R2).** A manager's requests pass
groups a bounded window of request files by job. Of a job whose attempt it
runs, only a `cancel` acts at once: the attempt and its launches are stopped
(`SIGTERM`, then `SIGKILL` after the cancel grace) and the job commits as
`cancelled`; other requests wait for the boundary. An unowned job, located at
the request's placement, is claimed, reconciled (which applies the requests)
and released. `job request`, `job delete`, `job seal`, `job unseal`, `job detach` and
`job unlock` apply their requests at once, as a CLI owner, when the job is
unowned.

Requests apply in the order of their `created_at` instants (a time without a
zone counts as UTC), then by request id. Each request is
decided by a pure function of `job.json`, `state.json`, the state the job is
at and the request, giving the new document and one effect:

| Action | Applies when | Effect |
| --- | --- | --- |
| `cancel` | not terminal | `Release(cancelled)` |
| `pause` | not terminal, not `paused`, not `waiting` | `Release(paused)` |
| `continue` | `failed` or `paused` (an unlocked job also `succeeded`), revival guard | The declared allowance restored (below); `Release(waiting)` with the retained `join` when the job failed at its wait, no new attempt; else `Release(ready)`: failure cleared, next attempt `manual_continue` if the last one started; `failed`/`retry_exhausted` past an effective attempt budget, `budget_exhausted` past `maximum_activations` (reachable only with `keep_budget`) |
| `restart` | `failed` or `paused` (an unlocked job also `succeeded`), revival guard | The declared allowance restored; `Release(ready)`: failure cleared, `join` dropped, next attempt `manual_continue` if the last one started; `failed`/`retry_exhausted` past an effective attempt budget, `budget_exhausted` past `maximum_activations` (only with `keep_budget`) |
| `override_step` | `failed` or `paused` (an unlocked job also `succeeded`), revival guard | The declared allowance restored; `Release(ready)` with a new activation of `step`, dropping a retained `join`; `failed`/`retry_exhausted` or `budget_exhausted` past an effective budget, as `restart` |
| `set_priority` | always | `Release(<current state>, priority)` |
| `set_budget` | not `succeeded` (unless unlocked) or `cancelled` | `Release(<current state>, priority)` with `budget` merged into `budget_override` (a later value for a key wins); no new attempt, no revival guard |
| `set_workflow` | not `succeeded` (unless unlocked) or `cancelled`; no `workflow/` runner copy in the job; the workflow check (below) | `Release(<current state>, priority)` with `workflow_override: {id, name}` (the name from the installation with exactly that id) and `runner_steps: null`; the `request_applied` entry carries `workflow: {from, to}`; no new attempt, no `force` |
| `detach` | the job has a parent link not yet detached | `Release(<current state>)` with `detached: {at, operator}` |
| `unlock` | not `cancelled`; not already unlocked with its copy made (or `no_copy`; dropped as "already unlocked"); a `succeeded` job has no live seal ("run `job unseal` first"); the effective workflow is installed when a copy is made | `Release(<current state>)` with `unlocked: {at, operator, reason}`; unless `no_copy`, the package copy is made first (§Unlocked jobs) |
| `eject` | the destination is usable; for an unlocked job not into an exchange inbox | `Eject(destination, tree)` (§Eject) |
| `delete` | terminal or `paused`; a succeeded job only after `unseal` (not needed when it is unlocked); no non-terminal parent waits on it | `Discard`: the job directory, its request files and (for an exchange root) its index entry are removed |
| `seal` | `succeeded` without a seal digest; refused for an unlocked job | `Seal`: seal the payload, record `seal`, release back |
| `unseal` | `succeeded`, not yet released | `Unseal`: record `seal: {released: true, ...}`, remove `.httk-job/seal.json`, release back |

**Restoring the declared allowance:** unless it carries `keep_budget`, a
`continue`, `restart` or `override_step` first merges into `budget_override`,
for each limit `L` whose `job.json` value `D` is not `null`, `L = max(E, U + D)`
where `E` is the effective limit (left alone when `null`, i.e. lifted) and `U`
counts the units already started, read from `state.json` before the revival's
own attempt or activation:

- `maximum_attempts_per_activation`: `0` for `override_step` and for a
  `continue` that reopens a wait; else `attempt.ordinal`, less 1 when that
  attempt's `started_at` is `null`, and `0` without an attempt;
- `maximum_total_attempts`: `counters.attempts_total`, less 1 when the current
  attempt is unstarted;
- `maximum_activations`: `counters.activations`, less 1 when the current
  activation has no started attempt (no attempt, or an unstarted one with
  ordinal 1), at least `0`.

The two "less 1" rules do not apply to `override_step`: it abandons the
unstarted attempt or activation while the counters keep its slot, so its `U`
is `counters.attempts_total` and `counters.activations`.

A limit whose `D` is `null` is restored to unlimited by removing its key from
`budget_override` (`null` when no key remains). Only keys that change are
written, and the `request_applied` history entry then carries them as `budget`
(`null` for a restored unlimited limit). Automatic retries never raise a limit.

**The workflow check of `set_workflow`:** the owner looks the id up in the job's
workspace by exact id and refuses it, naming the rule, unless the installation
(2) carries the declaration `$id` of the job's stored `declarations.workflow`
(the same short name when that has none); (3) declares the inputs of
`declared.inputs` by name, destination and hook-consumed flag; (4) declares the
output names of `declarations.workflow.outputs`; (5) declares no parameter the
job has no value for (unless `declared.parameters` lists it too), drops none of
`declared.parameters`, and accepts every parameter value's declared type;
(6) declares the environment names and types of `environment.declared`; and
(7) declares the job's current step (`activation.step`, else `initial_step`),
or with a `join` its `next_step` and `on_impossible` step. A job storing
`declarations.workflow` has empty snapshots where they are absent (`job new`
always writes `declared.inputs`); for a job storing none (a spawned child) the
job's current installation stands in for (2)-(6), and without it the request
is refused. A job of a language realization is refused. For an unlocked job every rule
beyond the installation (the checks above and that of the call slots) is
skipped; a job whose directory holds a runner copy
(§Unlocked jobs) is refused. A switch to the id
`job.json` names clears `workflow_override`; a dropped request's
`request_dropped` entry carries `workflow: {from, to}` too. The posting CLI runs
the same check first.

A request that does not apply is a `Drop`: its id is recorded, a
`request_dropped` entry with the reason goes to `history_tail`, and the job is
released back where it was. A `continue`, `restart` or `override_step` refused by the
revival guard applies when it carries `force`. A request that applies while an
attempt is live is a `Defer` and waits. At a commit boundary an `eject` is
skipped; the requests pass ejects the job after its release. An `eject`
records its request as applied and removes the file before it moves anything,
so a failed delivery returns the jobs with the request applied: eject requests
apply at most once.

### Exactly once

Every effect but `Defer` carries the request id into `applied_requests` in the
same `state.json` write that carries its effect and `release_to` (the release
rule), and the owner deletes the file before the move. A crash before the
deletion leaves a file whose id is in `applied_requests`: any later owner
deletes it on sight, unapplied, so a stale `continue` cannot re-apply after the
job failed again. An id whose file is gone needs no skipping and is dropped
from `applied_requests` at the next write.

### Unlocked jobs

An `unlock` request (`httk job unlock`, `httk job request unlock`) puts a job
in debug mode: it lifts the provenance-integrity rules for that job and marks
it so that collection will not store it by accident. It is a request like the
others (applied by the owner, exactly once), except that the workspace daemon
protocol does not carry it.

- **The marker.** `state.json` `unlocked` records `{at, operator, reason?,
  inherited_from?}`. It is one-way: no request clears it, damage handling
  keeps it (§Damage handling) and `JobRecord.job["unlocked"]` carries it like
  `workflow_override`. A manager seeds `{at, inherited_from: <parent job key>}`
  into the first `state.json` of every child that an unlocked parent
  publishes; such a child has no runner copy. The CLI also posts a
  marker-only `unlock` (`no_copy: true`) to every ancestor of a selected job,
  nearest first; `job request unlock` on a local workspace does the same. Through an
  ssh remote's request envelope `unlock` is refused for a spawned job.
- **The runner copy.** Applying `unlock` without `no_copy` copies the effective
  installation's `package/` to `<payload>/workflow/`, the way the
  store copies a package (no symlinks, modes kept; the crash-temporary name is
  `.httk-job/workflow.<token>`, renamed into place). An existing copy is never
  overwritten (a first unlock that finds one keeps it), and a job that runs a
  language realization copies nothing. The copy is not fsynced and may be torn
  after a power loss; remove it and unlock again, which makes a fresh copy. If
  the effective workflow is not installed, the request is dropped with that
  reason. When a copy exists the manager resolves the runner entry or command
  against it, `HTTK_WORKFLOW_RUNNER_ROOT` names it, and the job's own
  installation (calls, slots, `persist_outputs`, spawn ids) is read from it;
  called workflows, and the build spec, platform probe and build artifacts of a
  compiled workflow, still come from the store. The launch digest of the copy
  excludes the installation's declared build artifacts. The copy is used only while
  the job is unlocked: a `workflow/` in a locked job is inert, and execution
  from it is never automatic. The copy is a top-level payload member
  and the reserved name `workflow` keeps the protocol's own routes (transactions,
  staged inputs, input moves, bundles) from writing it. In a confined attempt
  `workflow/` is a read-only overlay, and an attempt after which `workflow/` exists
  although it was absent at launch (`workflow_pin` `workflow_present` false) fails
  with `protocol_error` ("the attempt created workflow/; it was removed") with its
  outcome voided, exactly like one that rewrites `job.json`, and that `workflow/`
  is deleted (also when the manager recovers a crashed `running` attempt). A tree
  present at launch, such as one the operator placed before `job unlock`, is never
  touched; it was read-only for the attempt. When the manager rebuilds a damaged
  `state.json` it also deletes `workflow/` unless the salvaged pin says it was
  present at launch. A spawned child staged
  with a top-level `workflow/` is refused as carrying a trusted entry. Unlocked jobs are never sealed, so the seal covering `workflow/` matters only
  for the layout. The operator edits it in place. An untrusted exchange bundle
  may not carry it (§Adopt: exchange jobs run only pre-installed
  workflows); trusted bundles (`job transfer`, `job adopt`, `job eject`) carry it
  along.
- **Relaxed.** A different `job.json` digest is pinned again (§`job.json`);
  `continue`, `restart`, `override_step`, `set_budget` and `set_workflow`
  apply to a `succeeded` job; `set_workflow` (refused by the CLI too while a `workflow/` copy exists) skips its compatibility rules
  except the installation and language-realization checks (§Application); no seal is written for a succeeded job (`seal` is
  `{"disabled": true}`) and `seal` is refused; `delete` needs no `unseal`.
  `persist_outputs` skips the pinned-tree digest check and loads the collect
  hook from the runner root unverified.
- **Refused or unchanged.** A succeeded job with a live seal must be unsealed
  before it can be unlocked. Owner-only `state.json`, exactly-once requests,
  the symlink, placement and schema refusals, the sandbox overlays,
  `cancelled` as a final state, the retry budgets and the staged-input digests
  are unchanged. Nothing clears `data/`, `run/` or the existing children when a
  step reruns. `job new --input-move-from-job` out of an unlocked job is
  refused, and so is ejecting it into an exchange inbox.
- **Collection.** `collect` degrades an unlocked job and stores nothing for it,
  unless forced (see {doc}`collecting`). A forced collection with
  `--allow-job-collector` uses the runner copy's collect hook only after the
  job has run again from the copy (`workflow_pin` `source` `"copy"`) with the copy
  unchanged since that launch; otherwise the job degrades with `runner copy
  the job's last attempt did not run from its runner copy; restart it before
  collecting` (the other reasons are `runner copy modified since the job ran: ...
  digest X does not match pinned Y` and `... could not be verified`).
- **Older readers.** An httk-workflow that predates `unlocked` treats such a
  `state.json` as damaged and rebuilds it without the marker; this is accepted.

### Embedded workflows

A job created with `job new --embed-workflow` names no installation: its `workflow.id` is
`embedded:<name>@<sha12>` (the first 12 hex digits of the tree digest), its
`job.json` has an `embedded` member, and its workflow is the tree in
`<payload>/workflow/`. Store installs and lookups refuse the `embedded:` prefix,
so no path confuses an embedded workflow with an installation. The reserved `workflow/`
name is the same one an unlocked job's runner copy uses; the difference is who
vouches for it. An unlocked job's copy is whatever the operator left there, and
the job is launched from it unverified (`source: "copy"`). An embedded workflow's tree
is pinned in `job.json` by the creator: the manager computes the **embedded
digest** (sha256 over the sorted entries framed as `D\0<path>\n` and
`F\0<path>\0<exec 0|1>\0<size>\0<sha256>\n`, descriptor-anchored, refusing
symlinks, special files, hard-linked files and `__pycache__`) after the claim and
before every launch, and a locked job whose tree differs, or is missing, fails
with `protocol_error` ("the job's workflow/ is missing"; "the job's workflow/
cannot be verified: <reason>" for a symlink, special file, hard link or
`__pycache__`, and for a `workflow/` that is a symlink or not a directory
("embedded workflow tree <path>: ... is a symlink or not a directory"), the
"is missing" text being only for a `workflow/` that does not exist (the manager,
precheck and `job why` use the same texts); "workflow/ changed since the job was created: digest X, created
Y"); a match launches with `workflow_pin`
`{id, tree_sha256, source: "embedded", workflow_present: true}`. `assess` reads no
tree: `requires`, slot bindings and the installation of fixed callees and bound
workflows come from `job.json`. An unlocked embedded-workflow job launches unverified and pins the embedded digest (`null`
when it cannot be computed) with `source: "copy"`. The runner is launched from `job.json`'s
`embedded.runner`, with `{package}` expanded to `<payload>/workflow`. The embedded
view exists per job and never enters an id-keyed cache.

The confinement of `workflow/` (read-only overlay for the attempt and its ranks;
creating it voids the attempt) is unchanged and is what the guarantees rest on;
an unconfined attempt already has the manager's authority. Children of an embedded-workflow
job must repeat the parent's `workflow` and `embedded`; `publish_children` copies
the parent's launched `workflow/` into each spec-only child of the parent's own
embedded workflow (descriptor-anchored, into the temporary name `workflow.partial`,
which a proposed child may not contain, verified against the parent's launch pin,
then renamed; a replay keeps a
complete matching copy and recopies anything else), while a child payload that
brings its own `workflow/` stays refused, and a child naming an embedded workflow
its parent does not embed is refused ("spawned child ... names an embedded workflow
its parent does not embed"). Children of an unlocked parent get the parent's current tree with its digest at
publication (editing it during the run is fine), inherit `unlocked` and pin
`source: "copy"`; a locked parent's tree must still equal its launch pin, else
the publication fails ("the parent's workflow/ changed since its attempt
launched"). `a.call` of
`[workflow.calls]` callees works (they must be installed), and `bound_workflows`
(the dependencies checked at claim and precheck, and shipped by transfer) include an
embedded workflow's fixed calls. Untrusted exchange bundles are refused
when any member, the root included, has an `embedded` member (`member <id>: an
untrusted bundle may not hold an embedded workflow (job.json's embedded)`); trusted
eject, adopt and transfer carry the job, which runs at the destination with
nothing installed, and an embedded id is never reported as a missing workflow
(its callees are). `JobRecord.job` holds `embedded`, and the run's workflow
definition URI is `embedded.source` when set, else none. See
{doc}`workflow_packages` for creation and the security model, and
{doc}`collecting` for collection from a verified snapshot.

## The workflow store

A job references a workflow by id, and that workflow, and the transitive
closure of its `[workflow.calls]`, must be installed before a manager starts
the job. A job transfer installs the workflows its jobs need in the
destination when it lacks them, as copies of the source's store entries (see
**Install a copy** below); the exchange never does.

```text
workflows/<slug>--<h16>/
├── install.json
├── package/                    the package sources (no build artifacts, no .git, no symlinks)
└── builds/<platform>/          build.json and artifacts/
```

`h16` is the first 16 hex digits of the SHA-256 of the id; `slug` is the
lowercased short name with every character outside `[a-z0-9._-]` replaced by
`-`, at most 48 characters. Ids are a commit-pinned canonical git URI,
`local:<name>` for a package directory, or `adhoc:<name>@<sha12>` for a runner
file or a bare workflow document.

```json
{"format": "httk-workflow-install", "format_version": 1, "id": "local:hello", "name": "hello",
 "source": "/home/me/hello", "reference": null, "tree_sha256": "<hex>", "installed_at": "<utc>",
 "calls": {"inner": "local:inner"}, "slots": {"forces": "local:pair"}, "requires": ["httk-workflow>=2.1"],
 "runner": {"command": null, "entry": "run.py", "builtin": null},
 "steps": ["start"], "initial_step": "start", "build": false}
```

`calls` maps each fixed-call alias to the installed id it resolved to, and
`slots` each call slot alias to its default's installed id, the default as
written when it was not installed then (a later install of it resolves by that
name), or `null` without a default; a default is never installed with the
workflow and is no dependency of a job.
`runner` names exactly one of `command` (an argv with `{package}` and
`{artifacts}` placeholders), `entry` (an executable package member) or
`builtin` (`cwl`, `pwd`, `jobflow`, `httk-v1`, run by the manager's own
installation). `build.json` is `{format: "httk-workflow-build",
format_version: 1, id, tree_sha256, command, platform, platform_output,
platform_tag, built_at}`.

- **Lookup** by id scans `workflows/` for the `--<h16>` suffix and compares
  the `install.json` id; lookup by short name reads every `install.json` and
  takes the latest `installed_at` among the installations of that name.
  `NAME@REF` keeps only those whose `reference` (the tag or branch a git
  install was given, `null` otherwise) is `REF`, or whose pinned commit starts
  with `REF` (at least 7 characters), when no installation has the whole text
  as its name. Id-shaped text (`local:`, `adhoc:`, `git+`) matches only an id.
- **Install** assembles the tree in a `build` scratch (installing declared
  calls first, recursively, and building for this platform when the package
  has a build section), then moves an existing installation aside to
  `<slug>--<h16>.old.<t>` beside it, moves the new tree into place, and moves
  the old one into the scratch to be discarded. A crash between the two renames
  leaves only `.old.<t>`, which lookups use as the installation and which `gc`
  moves back into place; `gc` removes an `.old.<t>` tree only once the
  installation exists.
- **Install a copy** (the receiving side of a transfer, hidden
  `workflow install --workspace WS --from-store DIR…`) takes another
  workspace's store entry: only `id`, `reference`, `calls` and `slots` from its
  `install.json` (a known id prefix, a git id pinned to a commit, `calls`
  naming the manifest's fixed-call aliases, `slots` its call slot aliases; a
  record without `slots` has none), everything else derived again from the copied
  `package/`, whose tree digest must be the recorded `tree_sha256`. `source`
  records the transfer (`transfer from SRC`) and `installed_at` the time of the
  copy. It never replaces: an installation of the id already here (an
  `.old.<t>` only copy too) is kept, and the final rename onto
  `<slug>--<h16>` fails on an occupied name, so of two concurrent copies the
  first wins and the other reports the winner as present or as another tree.
- **Uninstall** and **build** work in a scratch and move the result.
- **No other races are handled, by decision.** Users do not change workflows
  that jobs are using, and two operators installing one id at once is not
  supported.
- **At launch** the manager records `workflow_pin: {id, tree_sha256, source, workflow_present}` in
  `state.json`. Nothing enforces that the installed digest matches the one
  present at job creation. An unlocked job that has a runner copy is launched
  from that copy, `source` is `"copy"`, and `tree_sha256` is the digest of the copy at launch, or
  `null` when it cannot be digested (§Unlocked jobs).

## Moving jobs

### Bundles

A bundle is a plain directory:

```text
<bundle>/
├── bundle.json
└── jobs/<placement>/<job-key>/     one complete payload per member
```

```json
{"format": "httk-workflow-bundle", "format_version": 1, "transfer_id": "<32 hex>",
 "source_workspace_id": "<uuid>", "created_at": "<utc with zone>",
 "destination": {"workspace_id": null, "locator": "/scratch/out"},
 "members": [{"job_id": "<uuid>", "job_key": "<key>", "placement": "p", "state": "succeeded",
              "priority": 500, "parent_job_id": null}]}
```

`bundle.json` is strict, refuses duplicate keys and non-finite numbers, and is
written as compact sorted JSON with a newline; its exact bytes are the token
of delivery. Members are listed top-down: `members[0]` is the root, every
other member's parent is listed before it, ids and (placement, key) pairs are
unique, and `state`/`priority` are where each member was taken from and where
adoption publishes it. A member's `job.json` MUST agree with its entry (id,
key, placement, parent). Seals travel inside the payloads. A hold records the
destination workspace in `destination.workspace_id`. The optional `owner_key`
is the submitting operator's canonical `ed25519:<base64>` public key written
by `job eject`; it is unsigned. Only untrusted (exchange) adoption uses it. With `--operator IDENTITY`, eject uses
that identity and fails without moving the job if it has no key. Otherwise it
uses the configured default identity when keyed, and omits the member when it
is not. Holds, `job transfer`, manager ejects and exchange returns never write
it; `--operator` with `--hold` is refused. When omitted, `bundle.json` retains
exactly its previous bytes. Untrusted adoption refuses a non-canonical key.

### Eject

`job eject JOB DEST [--tree] [--wait]`, the `eject` request and holds share
one sequence, run by an owner that has claimed the root in any unowned state:

1. **Check the destination:** absolute without `..`, and a real directory or a
   missing name whose parent is a real directory; below `WORKSPACE/exchange`,
   every directory from the workspace root down is opened (and created) with
   `open_dir_under`. A destination whose canonical path (`realpath` of its
   existing prefix) lies below `WORKSPACE/exchange` while its lexical path does
   not reaches the exchange through a symlink and is refused. Otherwise the
   root is given back.
2. **Members.** With `tree`, the descendants are the children recorded in each
   `state.json`, confirmed from the child's side (its `job.json` names the
   parent and it is not detached), located with settling. Each MUST be
   terminal or paused; they are claimed in sorted UUID order. Without `tree`, a
   root with descendants present is refused. Into an exchange's `inbox` (a
   directory named `inbox` beside an `exchange.json`), a member whose
   `state.json` carries `workflow_override` is refused: the untrusted adoption
   there rebuilds `state.json`, which would drop it. On any refusal every
   claimed job, the root included, is given back. `--wait` first posts `pause`
   requests and waits until the jobs are quiescent.
3. **Bundle.** In a fresh `eject` scratch, write `bundle.json` first
   (recording each member's `from` state and priority), then append `ejected`
   to each member that already has a run log (a fresh job gets none and remains
   adoptable as an untrusted bundle), then extract each member (`move_owned` out of
   `owned/`) into `bundle/jobs/<placement>/<key>`.
4. **Record** `<scratch>/eject.json` (`{target, token, transfer_id,
   exchange}`: the target path, the `bundle.json` bytes, and for an exchange
   root `[exchange name, job id, adoption nonce]`, the name read from its
   `state.json` in the scratch and the nonce from its index entry, or `null`)
   beside `bundle/`.
5. **Deliver** with `deliver(…, token = the bundle.json bytes)` to
   `DEST/<name>` (the root's job key, or the client's job key
   `<tag>--<client job UUID>` for an exchange job this workspace adopted; a hold: `transfers/outgoing/<transfer-id>`); a target
   below `WORKSPACE/exchange` is anchored at its parent, opened with
   `open_dir_under` from the workspace root. Across filesystems the bundle is
   first copied to `DEST/.<name>.partial.<transfer-id>` and that copy is
   delivered; never into the exchange, which the enabling rename probe proved
   to share the filesystem. `DONE` removes an exchange root's index entry
   (only while it carries the recorded nonce: a client's resubmission rekeys
   to the same job id under a new adoption, whose entry stays) and discards
   the scratch.
6. **Occupied or failed:** the reconciler's decision runs at once. Without
   `eject.json` nothing was delivered. With it, the delivery happened when
   the target carries the token, or when `bundle/` has left the scratch (only
   its owner moves it) and `eject.json` is still there when looked at again
   afterwards: a recoverer takes the whole scratch, so a scratch gone with it
   was taken (`OwnerLost`; the index is untouched). On delivery the recorded
   exchange root leaves the index, under the recorded nonce. Otherwise the
   partial copy is discarded and every member still in the bundle is submitted
   back to its recorded state and priority. A destination that cannot be read
   keeps the scratch for the reconciler. An `eject.json` that does not decode
   (a torn write) is logged and treated as absent, except that a scratch whose
   bundle is gone as well is kept rather than decided.

The `eject` reconciler makes the same decision after a crash; the scratch is
never empty while a delivery is in doubt, so it always runs.

### Adopt

`job adopt BUNDLE`, transfers and the exchange inbox share one sequence:

1. **Take.** A bundle name may not be `source.json`, `plan.json`,
   `rekey.json`, `.partial` or a record temporary. The bundle is moved with
   `move_once` into a fresh `adopt` scratch (`adopt-untrusted` for the
   exchange), anchored when the source directory is client-writable. Across
   filesystems it is copied into `<scratch>/.partial/<name>` and renamed to
   `<scratch>/<name>` only when complete; the source is untouched.
   On one filesystem `job adopt` takes the bundle by rename. Across
   filesystems the source stays in place unless `job adopt --move` is given,
   which discards it after a successful publication.
   The scratch records `source.json`: `{source, refused_to, copied, nonce}`.
2. **Validate** (the trust boundary): the bundle structure is exactly
   `bundle.json`, `jobs/`, the listed placement and member directories and the
   payloads, with no structural entry a symlink or special file, and every
   member's `job.json` agrees with the manifest. An **untrusted** bundle's
   whole tree also passes `walk_untrusted`, holds no symlink at all, holds only
   fresh jobs (no `state.json`, `seal.json`, `logs`, `attempts`,
   `workflow` or `.httk-job`) in state `ready`, and its root has no parent. A trusted root
   may keep a parent outside the bundle.
3. **Rekey** an untrusted bundle: each member gets the id whose first group
   (eight hex digits) is the client id's and whose rest is that of
   `uuid5(b480bb06-dc60-4d16-a78b-cc6b3af734b4, "<workspace-id>/<client-id>")`,
   so a unique prefix such as `httk job show 8f7047f2` selects the job in both
   workspaces. A client thus chooses the first eight hex digits of its jobs'
   ids here, which can make a prefix selector ambiguous and nothing more;
   keys keep their tags and parent links are mapped. The result is recorded in
   `<scratch>/rekey.json` (format `httk-workflow-rekey`, version 1:
   `{manifest, exchange_names: {new id: client id}}`) before anything is
   renamed; then each `job.json` is rewritten, each member directory renamed,
   and `bundle.json` rewritten. The rewrite also sets `claim.pool`, whatever
   the client wrote, from the recorded `owner_key`: `exchange-` and the first
   16 hexadecimal digits of the SHA-256 of the key text, or `default` without
   a key; a resumed rekey therefore sets the same pool. A client id that
   collides with a rekeyed id is refused.
4. **Deduplicate** with a settled `locate_many` at the members' placements,
   owned jobs included. All present: "already adopted". Some present: refused.
   A trusted root whose parent is present here: refused (adopt the parent's
   tree). "Already adopted" discards the bundle only when it is a duplicate
   delivery: untrusted, copied, or taken from some workspace's
   `transfers/outgoing` or `transfers/incoming`, or from this workspace's
   `tmp/`. An operator's own bundle is refused as already adopted and left in
   place.
5. **Claim the exchange name** (untrusted only): `publish_dir` of
   `exchange-jobs/<client root id>/` with the adoption's nonce and the
   manifest's `owner_key` (absent when none), before anything is published. A
   name claimed by another adoption is refused, unless the indexed job is the
   rekeyed root and present, which is "already adopted". A rerun of the same
   adoption recognizes its own claim only when the key matches.
6. **Plan.** Write `<scratch>/plan.json` (the validated manifest). From here
   only publication remains, and validation never reruns.
7. **Publish** bottom-up: for an untrusted member, write its `state.json`
   (`origin: exchange`, `exchange_name`, its children recorded), then `submit`
   each member to its recorded state and priority. A member directory already
   gone was published. Discard the scratch. Workflows that are not installed
   are reported, not refused.

**Refusals** go, in this order: a copied bundle is discarded (its source is
untouched); with `refused_to` (the exchange's `outbox/rejected`), to a fresh
`<refused_to>/<unique>/<name>` beside `reason.json` (format
`httk-workspace-exchange-rejection`, version 1: `{name, reason,
rejected_at}`), every directory opened without following symlinks; otherwise
back to its source path when that is free; otherwise it stays in the scratch,
which is kept.

**The `adopt` reconciler** resumes from `plan.json` when it exists, and
otherwise from validation; every earlier step is idempotent (the rekey is
deterministic and recorded, and a rerun recognizes its own exchange-index entry
by its nonce). Without `source.json` (a crash right after the take) it records
a fresh one, with `refused_to` set to the exchange's rejections for an
untrusted bundle and to nothing for a trusted one.

### Holds and transfer

`job transfer --job JOB... SRC DST` is hold + copy + adopt + release:

1. **Hold.** SRC ejects each job (with `--tree`, its tree) into its own
   `transfers/outgoing/<transfer-id>/`, recording DST as the destination
   locator. The jobs are now in neither state tree.
2. **Copy and adopt.** On one filesystem DST adopts the hold directly (by
   move; a refusal moves it back). Across filesystems DST adopts by copy. To a
   remote DST the bundle is pushed to DST's
   `transfers/incoming/<transfer-id>.<t>` and adopted there by a remote
   `job adopt`; from a remote SRC it is pulled into a `landing` scratch.
3. **Release.** Only after DST adopted (or reported "already adopted") does
   SRC discard the hold (`take` into a `release` scratch, then discard); a hold
   already gone is fine.

Every run re-drives the holds bound for its destination; `--resume` re-drives
every hold and `--release T` discards one. An abandoned hold is taken back with
`job adopt .httk-workspace/transfers/outgoing/<T>`, after checking that DST
does not have the jobs.

### Crash points

| Crash | Left on disk | Outcome |
| --- | --- | --- |
| eject, after claims | jobs in `owned/<ejector>/` | recovery returns them to their states |
| eject, bundle being built | `bundle.json` and some members in the eject scratch | reconciler: no destination carries the token, so members go back; owned ones are recovered |
| eject, during a cross-filesystem copy | `DEST/.<name>.partial.<T>` | reconciler discards it and rolls back |
| eject, after delivery | the bundle at its destination, only `eject.json` in the scratch | reconciler: the bundle left the scratch, so roll forward and leave the index |
| adopt, after the take | the bundle in an adopt scratch | reconciler resumes from validation |
| adopt, during a cross-filesystem copy | only `.partial/` | reconciler discards the scratch; the source is intact |
| adopt, during the rekey | `rekey.json` | reconciler finishes the recorded rekey |
| adopt, after `plan.json` | some members published | reconciler publishes the rest |
| transfer, after the hold | `outgoing/<T>` | the next run or `--resume` re-drives it |
| transfer, after a remote push | `incoming/<T>.<t>` on DST | re-driven; a stale copy is reported by hygiene |
| transfer, adopted but not released | the jobs in DST, the hold in SRC | the re-drive finds "already adopted" and releases |

**Delivery is at least once.** After a crash between a delivery and the source
side forgetting its copy, a re-drive deduplicates while the destination still
has the jobs; if the destination has meanwhile let them go, the re-delivery
creates a duplicate. Verify the destination before re-ejecting or re-adopting
by hand.

## The exchange extension

### Layout and enabling

```text
WORKSPACE/exchange/
├── exchange.json                      {format: "httk-workspace-exchange", format_version: 1, workspace_id}
├── status.json                        managers: exchange jobs, states, progress
├── inbox/<name>/                      client → workspace: fresh job bundles
├── outbox/<tag>--<client job UUID>/   workspace → client: a returned tree
├── outbox/rejected/<unique>/          {<name>/, reason.json}
├── requests/<id>.json                 client: signed daemon and job actions
├── responses/<id>.json                daemon-signed command results
└── daemon.json, managers.json, launchers/<handle>.log    the workspace daemon's documents
```

`httk workspace exchange enable` creates the directories by descriptor, writes
`exchange.json`, proves with a rename probe that bundles move between
`exchange/` and `.httk-workspace/tmp/`, and only then adds `"exchange"` to
`extensions`; there is no disable. Every trusted access to `exchange/` is
anchored at descriptors opened `O_NOFOLLOW` from the workspace root, bounded
and non-blocking, and files go in only through `write_file`. The exchange
writer must be the workspace owner's account. Every manager of an exchange
workspace MUST confine its attempts; only managers without placement prefixes
that take the `default` pool or an `exchange-` pool serve the exchange.

The **exchange index** `.httk-workspace/exchange-jobs/<exchange-name>/` is
trusted: `.nonce` and `index.json` (`{job_id, placement, adoption_nonce, owner_key}`, with
`owner_key` absent when the bundle had none). This trusted entry is the
authority for ownership: it binds the exchange name, job id, adoption nonce
and key. Job-writable files never grant ownership. A rerun of the same
adoption recognizes the entry only when the key matches.
Readers ignore anything else in an entry, such as the `translated/` markers of
earlier versions. It is created once per exchange root before
the root is published (§Adopt step 5) and removed only by the root's owner when
the root is deleted or returned, and only while it indexes that job under the
adoption nonce the remover read (an eject records it in `eject.json`).

### Inbox adoption

In every tick each serving manager adopts the first inbox entry it can take,
as an untrusted bundle (§Adopt) with refusals going to `outbox/rejected/`.
Eligible names match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and are not reserved.
Several managers race for an entry with `move_once`; the loser moves on. The
taken entry is copied, through anchored descriptors, into a manager-owned
directory, and validation, rekeying and publication work on that copy only.

### Returns

When an indexed exchange root and every member of its tree are terminal (and
its `state.json` says `origin: exchange`), a manager posts an `eject` request
of the tree to `outbox/` itself, with operator `exchange-return` and
request id `uuid5(5d0c8f2e-8f5e-4a43-9a51-7f2b0c3e9b61, "<job-id>/<sha256 of
state.json>")`, so every manager seeing one state posts the same request. The
owner applies it with `deliver`, anchored at `outbox/` opened without following
a symlink, so a client-planted entry or symlink at the target name only makes
the eject roll back, and the changed state yields a fresh request id for the
retry. The returned tree sits at `outbox/<tag>--<client job UUID>/`, with no
per-job parent directory: the name is the client's own job key, the same name
the bundle had in `inbox/`, which is also the name an eject of an adopted
exchange job delivers to. `httk job adopt --move` of that path removes the
whole entry. A manager posts no return while `outbox/<tag>--<client job UUID>`
exists (the client has not fetched the previous copy) or while `outbox` is not
a real directory, looking from the exchange's descriptor; repeated returns of one job back off: 10 s, doubling, capped at one
hour.

### Job control

Authenticated job commands use daemon protocol 5, not the internal request
format. The confined daemon verifies authorized keys, workspace/enrollment,
freshness and active configuration. `job_request` names one internal UUID or
unique UUID prefix and one of all ten internal request actions, with the
optional member `keep_budget` (only `true`, only on `continue`, `restart` and
`override_step`) copied into the internal request. It refuses
force, path/glob/tag selectors and destinations outside the exchange outbox;
eject includes the whole tree. All existing operation guards apply.

The quota-bounded protected ledger (version 4) stores the exact signed command,
a once-written resolved target (job/request IDs, document digest, exchange
adoption nonce where applicable), one execution decision identifying the
winning instance and daemon owner, and the execution result. Only a new
execution winner publishes the deterministic internal request. A restart or
concurrent loser never reposts it or re-resolves its selector. Definitive
stored results replay; an exact pending request reports queued; matching
request-specific state history may establish a result; missing evidence
reports uncertain. Uncertainty authorizes no takeover or re-execution.

### Daemon mailbox

`health`, `start_manager`, `manager_status`, `cancel_manager`, `job_request`
and `job_request_status` share `exchange/requests/`; managers leave all these
commands alone. The daemon signs every result and keeps its protected ledger
in `.httk-workspace/daemon/broker/ledger/`. `daemon.json` version 3 declares
protocol version 5 and the active configuration digest, and lists the approved
configurations with their digests and published resource settings. Fresh signed
`job_request_status` observations bypass ledger admission and consume no
quota. See {doc}`workspace_daemon` for confinement and enrollment upgrades.

### Status and progress

`status.json` (format `httk-workspace-exchange-status`, version 2) is written
by serving managers, each writing the full listing, at most every 10 s and not
when another manager wrote it within the last 10 s:

```json
{"format": "httk-workspace-exchange-status", "format_version": 2, "workspace_id": "<uuid>",
 "updated_at": "<utc>", "truncated": false,
 "jobs": [{"exchange_name": "<uuid>", "job_id": "<uuid>", "job_key": "<key>", "state": "running",
           "progress": {"step": "relax", "fraction": 0.4}}]}
```

`state` is the unowned state, or `running` for an owned job; the document is
bounded to 1 MiB (`truncated` when jobs were cut). A job MAY keep a
`progress.json` of at most 4096 bytes in its payload root, a listed exception
to R5: it is read live, anchored, non-blocking, regular files only, and only an
excerpt is published: at most 32 scalar members with printable keys of at most
64 characters, strings cut to 256 printable characters.

### Client descriptors

The client's open descriptors are not revoked when its bundle is taken. Because
adoption validates and publishes a manager-owned copy, a descriptor the client
kept can change only the taken original, which is discarded, never the adopted
jobs. A client that writes through such a descriptor while the copy is made can
make the copy differ from what it meant to submit; that affects only its own
confined job.

## Seals

A job seal is `<payload>/.httk-job/seal.json`: a signed record of the
payload's files (excluding `attempts/`, `logs/`, `.httk-job/` and `state.json`; a
`workflow/` runner copy of an unlocked job (§Unlocked jobs) is a payload member
and so would be covered, though unlocked jobs are never sealed) whose subject is only `{job_id, job_key}`, so it stays true
wherever the job moves. It is written at

commit step 5 for a `succeed` when the job's `seal_succeeded`, or else the
`seal.succeeded` setting (default on), asks for it; keys come from the
`seal.keys` setting (default `project,identity`), and with no key the seal is
unsigned. A succeeded job without a seal records `seal: {disabled: true}`;
so does a succeeded unlocked job, which is never sealed.

**The authority is `state.json`:** `seal.sha256` is the digest of the seal
document, so a seal a job planted proves nothing. A succeeded job is never
changed by the protocol until `job unseal` records `seal.released` and removes
the seal document; only then does `delete` apply. `job seal` seals a succeeded
job that has none. Both are requests applied by an owner. The one exception is
a file moved out to a new job by `job new --input-move-from-job`: the seal is
not rewritten, and a signed release record created in
`<payload>/.httk-job/releases/` (one file per released path) amends it (see
{doc}`sealing`). A record amends only the seal it names.

The workspace seal `.httk-workspace/seal.json` is a snapshot of every job's
seal digest (`null` when unsealed). It blocks only modifying CLI commands;
managers and jobs never check it. See {doc}`sealing`.


## Maintenance

### Garbage collection

`httk workspace gc`, and a manager every `gc_interval` (without `dead_owners`,
which each tick handles), runs these categories in order, all through the
primitives, so a killed collection leaves the workspace consistent.

| Category | Rule |
| --- | --- |
| `dead_owners` | Probe recoverable owners and recover those proven dead (§Recovery). The only path that removes a dead owner's directories. |
| `attempt_control` | Attempt directories of unowned jobs older than `attempt_control_days`: claim, remove, release back. A failed or cancelled job keeps the attempt its `state.json` names; a job with unfinished owner work is given back untouched. |
| `placement_directories` | `rmdir` of empty placement directories of the unowned states, children first, one attempt each. |
| `requests` | Request files older than a day that are malformed or whose job is not found (settled, owned included) go to quarantine. |
| `manager_logs` | `logs/managers/<owner-id>.log` of owners without `owner.json`, older than `trash_days`. |
| `launcher_logs` | `logs/launchers/<launch id>.*` older than `trash_days` when no owner record carries that launch id, and `exchange/launchers/<handle>.log` older than `trash_days`. |
| `owner_tombstones` | A tombstone older than `owner_tombstone_days` whose owner holds nothing else. |
| `tmp_entries` | Write temporaries older than a day in `requests/` and `owners/*/`, `tmp/trash.<t>` entries and `tmp/push.<hex>/` copies older than a day, and `workflows/*.old.<t>` trees older than a day once their installation exists (an only copy is moved back instead). |

There is no age-based collection of scratch, held bundles or quarantine.

### Workspace check

`httk workspace fsck` MUST run only while no other actor uses the workspace: no
manager, CLI operation, daemon or transfer. It is the one maintenance operation
that relies on this, like `fsck` for a filesystem; `gc`, recovery and the
managers stay safe concurrently. Findings from a busy workspace may be
transient. `--repair` is refused unless the workspace is quiescent: every owner
other than fsck's own proven dead (as `workspace delete` requires) and
`recoverable_owners` empty, so no owned jobs and no dead-owner scratch remain;
`gc` recovers dead owners. The CLI asks for confirmation before repairing
(`--yes` skips it; without a terminal it is required). The repair registers a
CLI owner of its own. The check walks `jobs/` and reports:

| Finding | Meaning |
| --- | --- |
| `unparsable_name` | An entry that is neither a placement directory nor a job name of its position. With `--repair` it is quarantined. |
| `duplicate_job` | One job UUID in two places (a cross-filesystem crash, a duplicate delivery or two concurrent trusted adoptions). |
| `orphan_owned` | `jobs/owned/<id>/` without `owners/<id>/`; resolved by `attest-dead`. |
| `tombstoned_owner_with_jobs` | A recovered owner that holds jobs, launches or scratch again; `gc` recovers it. |
| `unreadable_state` | A `state.json` that exists but does not decode. |
| `foreign_owner` | A job directory another uid owns. |
| `stale_exchange_index` | An exchange index entry whose job one settled lookup does not find (the settle covers the visibility of writes that finished before fsck started). A held job is not looked for: a delivered hold has left the index already. With `--repair` it is removed under the nonce it was read with. |
| `unindexed_exchange_job` | An unowned job with `origin: exchange`, no parent, and the job id this workspace rekeys its `exchange_name` to (an exchange root), without `exchange-jobs/<exchange_name>/`. Children, whose `exchange_name` is their own client id or their root's, and exchange jobs adopted from another workspace are not roots here. With `--repair` the entry is recreated by `claim_exchange_name` under a fresh adoption nonce, without `owner_key`; the root is then no longer daemon-controllable. |

Everything but unparsable entries and stale or missing exchange index entries
is left to the operator.


### Hygiene and inspection

The workspace health checks of `httk project repair` report dead owners not yet
recovered, scratch of unknown owners, temporaries older than a day, and holds
or incoming copies older than seven days, and repair only through `gc`.
`workspace move` and `workspace delete` refuse while any owner is not proven
dead. `status` counts are listings per state; `job show`, `job why` and
`job log` find a job with `locate` and read its documents and both run logs.

### Quarantine

`quarantine/<epoch>-<t>/entry` holds a moved-aside entry, with `reason.json`
(`{source, reason, at}`) when `gc`, `fsck` or a damaged claim put it there;
recovery's quarantine of an unreadable owned job writes no reason. Entries are
won into a `quarantine` scratch first, whose reconciler finishes the move.
Quarantine is removed only by hand.

## Accepted limitations

- **Operator attestation is trusted.** An owner attested dead while it still
  runs can apply a request and run work twice until it fail-stops. Even for a
  dead owner: A records a release and dies before deleting the applied request
  file, an operator attests A at once, and B claims on another node with a
  `requests/` listing cached from before the request was posted; the request
  can then apply twice. Probe-proven deaths wait out the visibility deadline.
- **A frozen owner is not dead.** It holds its jobs until it is cancelled or
  attested; hosts whose allocations cannot be queried need an operator. There
  is no time-based takeover and no "cancel when stale" policy.
- **Shared allocations.** Managers sharing one allocation cannot recover each
  other's jobs before it ends.
- **Unconfined attempts** can leave processes behind (no PID namespace),
  which breaks quiescence; confinement provides it.
- **Paths change between claims.** The host path of a job changes at every
  move, and attempts see it at its real path even when confined. Identity is
  the job UUID or key, never the directory; a job MUST NOT store absolute
  paths to itself across attempts.
- **At-least-once delivery.** A transfer or eject can deliver twice after a
  crash; a re-delivery after the destination let the job go duplicates it.
- **Trusted adoption is check-then-act.** Two operators adopting copies of one
  bundle at once can duplicate it; `fsck` reports `duplicate_job`. A trusted
  bundle carrying a job under another placement than an existing copy is not
  detected.
- **The workflow store is unprotected.** Concurrent installs of one id, or
  changing a workflow that running jobs use, are not handled. The installed
  digest is recorded at launch, not enforced.
- **Eject requests apply at most once.** A failed delivery returns the jobs
  with the request recorded as applied; the operator or the exchange posts it
  again.
- **No all-or-none visibility** of transactions between attempt boundaries.
- **The exchange client** must be the workspace owner's account; what it
  writes through descriptors it kept affects only its own job.
- **A launch client killed with `SIGKILL`** leaves its launch running until the
  attempt ends.
- **The join grace and stale-request quarantine use clocks.** They fail a job
  or move a request aside, never let a second writer in.

- **Nested `.children` names can go stale.** A `.children` directory name is
  fixed when the child is spawned and never rewritten: after exchange rekeying
  it carries the client's pre-rekey key, and after `job detach` or a
  single-child transfer it still names the former parent. Nothing derives
  identity from a placement.
- **Delivery destinations.** An empty directory created concurrently at a
  delivery destination is replaced by the rename.

## Changes from the previous format

Format 3 with the `jobs-v3` layout replaces the earlier format 3 layout (state
markers under `.httk-workspace/state/`, the packed journal, leases and
takeover, request claims, the runner store, transfer receipts and
acknowledgements, and the maintenance lock). There is no migration and no
reading path for the old layout: `layout: "jobs-v3"` is required. A workspace
without it is refused with:

```text
workspace at <root> uses an older job layout (format.json has no "layout": "jobs-v3");
there is no migration: re-create the workspace with `httk workspace init` and submit its jobs again
```

and a workspace of another format version or profile with:

```text
workspace at <root> uses httk-workflow-filesystem format version <v> (profile '<p>'); this version of
httk-workflow reads only format 3 (core-v3). There is no migration: remove old per-user state with
`httk system reset` and recreate the workspace with `httk workspace init`.
```

Bundles of the old transfer format and old remotes are refused likewise.
