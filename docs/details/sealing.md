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

It covers the payload's own files but not the payload-private scratch
directories `attempts/`, `logs/` and `.httk-job/`. A job legitimately rewrites
that working state, so it may change without breaking the seal. The seal never
covers itself. `httk workflow seal verify <payload>` checks a job directory on
its own, whether or not it is inside a workspace.

### Workspace seal

A workspace seal records the digest of every job's seal. It lives at
`.httk-workspace/seal.json`. Every job must be sealed before the workspace can
be.

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

By default a manager seals each job as soon as it succeeds. Sealing is not part
of the job's success: a missing key, a conflicting existing seal, or a
filesystem error is logged and ignored, and the job stays succeeded.

Two workspace application settings control this:

- `seal.succeeded`: whether to auto-seal succeeded jobs. Default on; set it to
  `false` (also `0`, `no`, `off`) to turn it off.
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

The `--keys REFS` option on `job seal`, `workspace seal` and `project seal`
overrides the setting (or the project's `seal_keys` member) for that call. A
ref that cannot be resolved is skipped with a warning; only resolving no key at
all is an error.

## What a seal refuses

While an entity is sealed, the protocol refuses anything that would change what
the seal commits to:

- **Sealed job:** state changes (submit, transitions, requests), `job delete`,
  and a runner publish that would alter its payload; unsealing it while its
  workspace is still sealed.
- **Sealed workspace:** the same, plus unsealing it while its project is still
  sealed.
- **Sealed project:** `workspace init` that would add a workspace the project
  seal does not cover.
- **In general:** re-sealing a job whose recorded contents differ (unseal it
  first), and changing a policy or setting that a seal depends on.

These still work unchanged:

- every read-only command (`status`, `show`, `log`, `why`, `seal verify`);
- `gc`, `fsck` and `unlock`;
- `workflow postprocess`, which writes outside the payload. A sealed job can be
  postprocessed; the output is excluded from the job seal and, when it lives in
  the project tree, from the project seal too;
- transfers. The seal travels inside the payload and the transfer manifest pins
  its digest, so a job sealed here arrives exactly as sealed, and verifiable,
  on the destination machine.

## Sealing and unsealing in order

Seals nest downward, so they are written bottom-up and removed top-down.

```console
# Seal: jobs, then the workspace, then the project.
httk job seal <JOB>...
httk workspace seal          # or: httk workspace seal --force  (seals unsealed jobs first)
httk project seal

# Unseal: project first, which frees the workspaces, which free the jobs.
httk project unseal
httk workspace unseal
httk job unseal <JOB>...
```

`workspace seal` runs inside the maintenance guard, so the workspace must be
quiescent. Without `--force` it lists the still-unsealed jobs and refuses. With
`--force` it first seals each of them (any quiescent kind, not just succeeded)
and then the workspace.

`job unseal`, `workspace unseal` and `project unseal` ask for confirmation,
which `--force` skips. Without a terminal and without `--force` they refuse
rather than block.

## Verifying

`httk workflow seal verify [PATH]` verifies the seal at `PATH` (a project root,
a workspace root, or a job payload) and, unless `--shallow` is given, every
seal it references:

```console
httk workflow seal verify
httk workflow seal verify --json
httk workflow seal verify --trusted-key keys/collaborator.pub some/workspace
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

- **A transfer's "sealed bundle".** A transfer bundle is sealed in the sense of
  being a finalized, checksummed archive ready to move between machines. That
  is a property of the transport envelope, unrelated to the signed seals
  described here, although a sealed payload keeps its seal inside the bundle and
  stays verifiable on arrival.
- **`httk project export`.** The core command that packages a project as a
  signed ZIP for distribution is an *export*. In this guide, *seal* means only
  the integrity seal.
