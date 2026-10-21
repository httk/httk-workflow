# Sealing jobs, workspaces, and projects

A **seal** is a signed record of what one level of the workflow tree contained
at a moment in time. After a payload is sealed, verifying the seal reports any
change to a covered byte as a discrepancy, and the protocol refuses operations
that would silently invalidate the seal.

Sealing answers one practical question: has this finished result been changed
since it succeeded? It is not encryption and not access control. A seal is
public and detached: anyone can read the payload, and anyone with the signing
key can re-seal it. What a seal provides is detection.

## The three levels

Each level records the level below it, so a project seal pins whole payloads
transitively without re-hashing them.

### Job seal

A job seal records the file hashes of one payload, plus each file's owner
execute bit so a runner cannot quietly be made runnable or unrunnable. It lives
in the job's own directory at `<payload>/.httk-job/seal.json`, so it moves with
the job directory. It names only the job (its id and key), never the workspace
or placement that held it.

It covers the payload's own files but not `attempts/`, `logs/`, `.httk-job/`
and the owner's `state.json`, which change as the job moves without breaking
the seal. The seal never covers itself. `httk seal verify <payload>` checks a
job directory on its own, whether or not it is inside a workspace.

The authority is the job's `state.json`, which records the SHA-256 of the seal
document the manager wrote: a seal document a job planted itself proves
nothing.

### Workspace seal

A workspace seal records the digest of every job's seal, or `null` for a job
without one, which `workspace seal` lists as unsealed. It lives at
`.httk-workspace/seal.json`. `httk workspace verify` lists every job that
drifted since the snapshot (`missing_job`, `unsealed`, `missing`, `mismatch`)
and exits `1` on any drift.

### Project seal

A project seal records the project's loose files and the digest of every
registered *member*'s seal. It lives at `httk_project/seal.json`. The project
level (seal, manifest, repair, verify) belongs to *httk-core*.

A workflow workspace is a project member, recorded in
`httk_project/members.json`. It is registered when the workspace is created,
unregistered when it is deleted or forgotten, and its path is followed when it
moves. *httk-workflow* teaches core's verbs how to seal, exclude, verify and
check it. Every member must be sealed before the project can be.

### Signatures and verification

`httk project seal | unseal | verify-seal | repair | manifest` are core
commands; this guide describes what a workflow workspace contributes to them.
The signature covers a domain-separated digest of the document body, the same
way a signed project manifest is signed, so a seal digest cannot be replayed as
a manifest or any other httk artifact. Verification answers two independent
questions: does the seal still describe this tree, and was it made by a key
this project trusts?

## Auto-sealing succeeded jobs

By default a manager seals each job as part of committing its success. With no
resolvable key the seal is unsigned. A payload the job made unsealable (a FIFO,
a symlinked `.httk-job`) fails the job with `protocol_error` instead; an I/O
error leaves the commit to be retried.

A job's `seal_succeeded` member decides, and when it is unset two workspace
application settings do:

- `seal.succeeded`: whether to seal succeeded jobs. Default on; set it to
  `false` (also `0`, `no`, `off`) to turn it off. A succeeded job without a seal
  records that sealing was disabled.
- `seal.keys`: the comma-separated key refs to sign with. Default
  `project,identity`.

```console
httk workspace settings set --key seal.succeeded --value false default
httk workspace settings set --key seal.keys --value project,identity default
```

## Key refs

A seal is signed by one or more keys, each named by a *ref*:

| Ref | Signs with |
| --- | --- |
| `project` | the project's own signing seed, discovered from the tree |
| `identity` | the default operator identity |
| `identity:<short>` | a named operator identity |
| a path | a base64 Ed25519 seed file |

The `--keys REFS` option on `workspace seal` and `project seal` overrides the
setting (or the project's `seal_keys` member) for that call; `job seal` uses
the setting. A ref that cannot be resolved is skipped with a warning.

## What a seal refuses

- **Succeeded job:** the protocol never changes a succeeded job, sealed or not,
  until `httk job unseal` releases it (recording the release and removing the
  seal document); only then does `job delete` apply. `job seal` seals a
  succeeded job that has none. Both are requests applied by the job's owner.
  The one sanctioned change is moving a file out to a new job (see
  [Release records](#release-records)).
- **Sealed workspace or project:** modifying CLI commands (job creation,
  requests, deletion, eject, adopt, transfer, installs) are refused until it is
  unsealed; managers and jobs do not check it. A workspace cannot be unsealed
  while its project is sealed.

These still work unchanged:

- every read-only command (`status`, `show`, `log`, `why`, `seal verify`);
- `gc` and `fsck`;
- `workflow postprocess`, which writes outside the payload. A sealed job can be
  postprocessed; the output is excluded from the job seal and, when it lives in
  the project tree, from the project seal too;
- moving a job out of an unsealed workspace. The seal travels inside the
  payload, so a job sealed here arrives exactly as sealed, and verifiable, on
  the destination machine.

## Release records

A file too large to copy, such as a trajectory, is handed to a later job by
moving it: `httk job new --input-move-from-job NAME=PATH` (or
`new_job(..., move_inputs=...)`) renames PATH, inside a succeeded job of the
same workspace, into the new job. The job seal is never rewritten, since
workspace and project seals bind its digest. Instead, before the file moves,
the move creates one *release record* for the released path in
`<payload>/.httk-job/releases/`, named by the SHA-256 of the payload-relative
path (`<64 hex>.json`):

```json
{"format": "httk-job-release", "version": 1,
 "body": {"kind": "release", "amends": "<the job seal's body_sha256>",
          "path": "run/traj.dat", "size": 1099511627776, "sha256": "<64 hex>",
          "released_to": {"job_key": "md--<uuid>", "path": "files/trajectory.dat"},
          "at": "2026-10-21T12:00:00.000000Z"},
 "body_sha256": "<64 hex>", "signatures": []}
```

The record is created exclusively, so it is also the claim: a second move of
the same file finds it and is refused. Each record is its own signed body,
signed with the workspace's `seal.keys` exactly as a seal is. Its `kind` keeps
a release body from ever passing for a seal body. Verifying a job seal accepts
a missing sealed file only while a valid record covers it: the record's body
digest matches, its `amends` is the seal's `body_sha256`, its `sha256` and
`size` are the ones the seal records for the path, and every signature it
carries verifies. A record amending a signed seal must carry a signature that
verifies and was made by a key that also signed the seal, so a move out of a
job with a signed seal is refused while `seal.keys` resolves no key; a record
amending an unsigned seal may be unsigned. Verification opens only the records
of the sealed files that are missing, never lists the directory, so nothing
else placed there affects it. A missing file without such a record is still
`missing`, and a record whose file is present covers nothing.
Workspace and project seals stay valid, because the job seal is unchanged.
Resealing a job keeps its release records as history; they amend the old seal
and are ignored against the new one.

The move is refused unless PATH is a regular file of the payload (not a
symlink, `job.json`, `state.json`, or anything below `attempts/`, `logs/` or
`.httk-job/`) on the same filesystem as the workspace, so the rename is
atomic, and unless a sealed job's seal is the one its `state.json` records and
records the file. The move checks the file's size, not its content, against
the seal: a same-size change made in place after sealing is caught at the
receiving job's collect, which records that input as lost. The new job's
`staged_files` takes the digest the seal records, so the file is never
re-hashed. The rename is verified by the file it landed. If the new job cannot
be created, the file is renamed back and its record withdrawn once the file is
seen back home; should that fail, the record is kept and the unfinished payload
is kept in the workspace's `.httk-workspace/tmp/` with a warning rather than
discarded. The same happens when the file vanishes from its job during the
move: the scratch is kept for the operator.

A crash between creating the record and the rename leaves a record whose file
is still in place. Such a record changes nothing, and the next move of the
file replaces it when the record is stale: the file is present, no job of the
workspace has the key the record names, and no other `job new` has a move of
the same file in flight (each move leaves a `moved-<hex>.json` marker in its
scratch directory before creating the record). Otherwise the move is refused
with a message naming the record file,
`<payload>/.httk-job/releases/<64 hex>.json`, and the job it names; that record
is safe to delete by hand when the file is present and that job does not
exist, together with any kept `.httk-workspace/tmp/*.submit.*` scratch
directory whose `moved-*.json` marker names that file. A scratch is kept only
while one of its markers names a file that landed in it; a marker whose file
never moved is dropped when the scratch is reconciled.

## Sealing and unsealing in order

Seals nest downward, so they are written bottom-up and removed top-down.

```console
# Seal: jobs, then the workspace, then the project.
httk job seal <JOB>...       # only succeeded jobs that have no seal yet
httk workspace seal
httk project seal

# Unseal: project first, which frees the workspaces, which free the jobs.
httk project unseal
httk workspace unseal
httk job unseal <JOB>...
```

`workspace seal` records every job's seal digest as it finds it and lists the
jobs without a seal; seal the succeeded ones with `job seal` first when they
should be covered.

`job unseal`, `workspace unseal` and `project unseal` ask for confirmation,
which `--force` skips. Without a terminal and without `--force` they refuse
rather than block.

## Verifying

`httk seal verify [PATH]` verifies the seal at `PATH` (a project root,
a workspace root, or a job payload) and, unless `--shallow` is given, every
seal it references:

```console
httk seal verify
httk seal verify --json
httk seal verify --trusted-key keys/collaborator.pub some/workspace
```

Text output is one line per entry, `<level> <subject> <verdict> <reason>`, with
indented `<kind> <path>` discrepancy lines under any failing entry, then a
final status line. The final word and exit code mirror a signed manifest's
verdicts:

| Every entry | Final line | Exit |
| --- | --- | --- |
| `valid_trusted` | `ok` | 0 |
| valid, but at least one `valid_unknown_key` | `UNTRUSTED` | 3 |
| any `invalid` | `FAILED` | 1 |

`--json` prints `{ "entries": [...], "ok": <no discrepancy or invalid>, "trusted":
<every signer is a trust anchor> }`.

Each entry's verdict is one of:

- `valid_trusted`: a signer is a pinned trust anchor;
- `valid_unknown_key`: the signature verifies but nothing pins the signer;
- `invalid`: the seal no longer describes the tree, or a signature does not
  verify.

By default the project's pinned keys and the local identities' public keys are
trusted, so a tree sealed by its own project or identity verifies as
`valid_trusted` without naming a key. `--trusted-key` adds more, as an
`ed25519:` key, a `sha256:` fingerprint, or a `*.pub` file.

## Not to be confused with

- **A transfer bundle.** An ejected or held bundle is a plain directory with a
  `bundle.json` manifest, unrelated to the signed seals described here,
  although a sealed payload keeps its seal inside the bundle and stays
  verifiable on arrival.
- **`httk project export`.** The core command that packages a project as a
  signed ZIP for distribution is an *export*. In this guide, *seal* means only
  the integrity seal.
