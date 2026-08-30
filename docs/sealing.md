# Sealing jobs, workspaces, and projects

*For operators and campaign owners who need finished work to stay provably
unchanged.*

A **seal** is a signed statement of what one level of the workflow tree
contained at a moment in time. Once a payload is sealed, any later change to a
covered byte becomes a discrepancy the moment the seal is verified, and the
protocol refuses the operations that would silently invalidate it.

Sealing answers a narrow, practical question: *has this finished result been
touched since it succeeded?* It is not encryption and not access control — a
seal is public and detached; anyone can read the payload, and anyone with the
signing key can re-seal it. What a seal buys is detection.

## The three levels

Each level records the level below it, so a project seal transitively pins whole
payloads without re-hashing them:

- A **job seal** records the file hashes of one payload — and each file's owner
  execute bit, so a runner cannot be quietly made (un)runnable. It lives at
  `.httk-workspace/seals/jobs/<job_key>.json`. It covers the payload's own files,
  **not** the payload-private scratch directories `attempts/`, `logs/`, and
  `.httk-job/`: those are working state a job legitimately rewrites, so they are
  excluded and may change without breaking the seal.
- A **workspace seal** records, for every job, the digest of that job's seal. It
  lives at `.httk-workspace/seal.json`. Every job must be sealed before the
  workspace can be.
- A **project seal** records the project's loose files and, for every workspace
  nested below it, the digest of that workspace's seal. It lives at
  `httk_project/seal.json`. Every workspace must be sealed before the project
  can be.

The signature is over a domain-separated digest of the document body, exactly as
a signed project manifest is signed, so a seal digest can never be replayed as a
manifest or any other httk artifact. Verification answers two independent
questions: does the seal still describe this tree, and was it made by a key this
project trusts.

## Auto-sealing succeeded jobs

By default a manager seals each job the moment it succeeds, so a finished result
is protected without an operator remembering to. Sealing is a convenience, never
part of the job's success: a missing key, a conflicting existing seal, or a
filesystem error is logged and swallowed, and the job stays succeeded.

Two workspace application settings control it:

- `seal.succeeded` — whether to auto-seal succeeded jobs. Default on; set it to
  `false` (also `0`, `no`, `off`) to turn it off.
- `seal.keys` — the comma-separated key refs to sign with. Default
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
| `identity:<name>` | a named operator identity |
| a path | a base64 Ed25519 seed file |

The `--keys REFS` option on `job seal`, `workspace seal`, and `project seal`
overrides the setting (or the project's `seal_keys` member) for that one call. A
ref that cannot be resolved is skipped with a warning rather than failing the
seal; only resolving *no* key at all is an error.

## What a seal refuses, and what still works

While an entity is sealed, the protocol refuses anything that would change what a
seal commits to:

- **Refused on a sealed job:** state changes (submit, transitions, requests),
  `job delete`, and runner publish that would alter its payload; unsealing it
  while its workspace is still sealed.
- **Refused on a sealed workspace:** the same, plus unsealing it while its
  project is still sealed.
- **Refused under a sealed project:** `workspace init` that would add a workspace
  the project seal does not cover.
- **Refused generally:** re-sealing a job whose recorded contents differ (unseal
  it first), and changing a policy or setting that a seal depends on.

What still works unchanged: every read-only command (`status`, `show`, `log`,
`why`, `seal verify`), `gc` and `fsck`, `unlock`, **`workflow postprocess`** —
it writes outside the payload (see below), so a sealed job can be postprocessed;
its output is excluded from the job seal and, when it lives inside the project
tree, from the project seal too — and **transfers**: the seal travels with the
payload, so a job sealed here stays sealed, and verifiable, on the machine it
moves to.

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
quiescent. Without `--force` it lists the still-unsealed jobs and refuses;
`--force` seals each of them first (any quiescent kind, not just succeeded) and
then the workspace. `job unseal`, `workspace unseal`, and `project unseal` prompt
for confirmation, which `--force` skips; without a terminal and without `--force`
they refuse rather than block.

## Verifying

`httk workflow seal verify [PATH]` verifies the seal at `PATH` — a project root,
a workspace root, or a job payload — and, unless `--shallow`, every seal it
references:

```console
httk workflow seal verify
httk workflow seal verify --json
httk workflow seal verify --trusted-key keys/collaborator.pub some/workspace
```

Text output is one line per entry — `<level> <subject> <verdict> <reason>` —
with indented `<kind> <path>` discrepancy lines beneath any failing entry, then a
final status line. The exit code and that word mirror a signed manifest's
verdicts:

| Every entry | Final line | Exit |
| --- | --- | --- |
| `valid_trusted` | `ok` | 0 |
| valid, but at least one `valid_unknown_key` | `UNTRUSTED` | 3 |
| any `invalid` | `FAILED` | 1 |

`--json` prints `{ "entries": [...], "ok": <no discrepancy or invalid>, "trusted":
<every signer is a trust anchor> }`.

A verdict is one of `valid_trusted` (a signer is a pinned trust anchor),
`valid_unknown_key` (the signature verifies but nothing pins the signer), or
`invalid` (the seal no longer describes the tree, or a signature does not
verify). By default the project's pinned keys and the local identity's public
key are trusted, so a tree sealed by its own project or identity verifies as
`valid_trusted` without naming a key; `--trusted-key` adds more, as an
`ed25519:` key, a `sha256:` fingerprint, or a `*.pub` file.

## Not to be confused with

- **A transfer's "sealed bundle."** A transfer bundle is *sealed* in the sense of
  being a finalized, checksummed archive ready to move between machines. That is
  a property of the transport envelope, unrelated to the signed seal documents
  described here (though a sealed payload keeps its seal inside the bundle and
  stays verifiable on arrival).
- **`httk project export`.** The core command that packages a project for
  distribution as a signed ZIP is an *export*; the word *seal* means only the
  integrity seal described in this document.
