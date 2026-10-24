# Stable ids

A database built from workflow results mints entry ids as it saves: the first
record stored becomes `<base>-<series>-1`, the second `-2`, and so on. The
numbering follows save order, so rebuilding the same store from scratch, or
adding one job and rebuilding, renumbers entries and hands old ids to new
content. That is fine for a throwaway store, but not for one whose ids are
cited, linked to, or served.

The **id ledger** solves this. It is an allocator that maps a stable *source
key* to a permanent id and returns the same id for that key on every later
build. Rebuilds stay identical, and a change to an entry's content becomes a
new revision under the same id rather than a new entry.

## How the ledger works

### The allocator

The ledger owns the per-family id bases and counters. The store does not mint
ids for ledger-managed saves; it receives the id the ledger allocated. See
`httk.store.IdLedger`.

### Keys

A key names which produced thing an id belongs to, as a stable provenance
coordinate. The *httk₂*-native grammar, built by
{py:func}`httk.workflow.ledger_key`, is

```text
<workspace_id>:<job_id>[:<role>[:file:<relpath>]]
```

It names the producing job, optionally one of its declared output roles, and
optionally one file of that output. The literal `file:` segment keeps a role
and a file from being confused. Keys are opaque to the allocator, so colons
inside a role or path are harmless.

### Segments, signatures and git

The ledger file is one `sqlite3` database. Each close that allocated something
appends its new records plus one signed segment covering just them. The
segment's subject carries the per-family base map, the series, the ledger
identity, and the record range it signs. A close that allocated nothing leaves
the file byte-identical.

On reopen, the ledger verifies every segment's signature and checks that the
segments exactly partition the records. This integrity check is always on: an
edit, a deletion in the middle, a reordering, or a mismatch between segment and
record ranges makes the reopen refuse.

A signature is an audit record, not a build gate. The reopen logs who signed
each segment, for manual audit, but does not demand a particular signer unless
you pin one with the optional `IdLedger.open(trusted_keys=...)`.

The in-file checks cannot see a whole-file rollback. Dropping the newest
segments together with their records leaves a self-consistent older ledger,
which only git history can reveal. The committed file is binary;
`sqlite3 <file> .dump` renders it for inspection. Recover a corrupted or lost
ledger by restoring it from git, as the verification errors advise.

## The anchoring rule

A key must derive from a stable identity, never from a path that can move. A
live-collected job is always stable. A job harvested from a v1 tree without a
manifest is identified only by its absolute path, so
{py:func}`httk.workflow.ledger_key` refuses it with
{py:class}`httk.workflow.UnstableIdentityError` (`force=True` overrides this).
A path-derived key that goes stale would hand an old id to new content.
`collect` instead falls back to store-minted, unstable ids for such a job and
warns.

## Using it from `collect`

With `--into`, the ledger is on by default and lives beside the store at
`<into>.ids.sqlite`:

| Flag | Effect |
| --- | --- |
| *(default)* | Allocate ids through `<into>.ids.sqlite`, creating and signing it on first use. The creation is announced, because a file worth keeping appears next to the store; commit it alongside the store. |
| `--id-ledger PATH` | Put the ledger somewhere else, for example a path committed in the database repo. |
| `--no-id-ledger` | Do not use a ledger. The store mints ids, which are **not** stable across rebuilds. Warned once. |

The ledger is signed with the workspace's own seal keys (the workspace
`seal.keys` setting, via `default_workspace_keys`). If no signing key can be
resolved, `collect` warns and continues without a ledger instead of failing.

While allocating, `collect`:

- **skips** any output that already carries an assigned public id; a ledger
  never overwrites one;
- **aliases** on content dedup: when the store deduplicates two jobs'
  content-identical outputs onto one row, the second job's key is recorded as an
  alias of the first job's id, so both keys resolve to the same id;
- **falls back** to store minting for an output it cannot give an explicit id
  (a structure view over a record whose backing dataclass has no `id` field).
  Such ids are stabilized where the id can be threaded through the build, as
  in the altermagnets `build_store` path, not through `collect`.

## Bases and numbering

`collect --into` gives each family a distinct base `<id-base>.<family>`, so ids
look like `<id-base>.<family>-<id-series>-<n>` and records and runs never
collide in a shared ledger. The distinct base is required: the ledger enforces
id uniqueness across all families, so a shared base would make the first
`records` id equal the first `runs` id and break the next rebuild's reopen.

The counter is `max(existing number) + 1` per family. It is monotone and
tolerates gaps, so an entry whose source later disappears keeps its number and
no later entry reuses it.

## Supersession and re-binding

`lookup` resolves a key to its newest binding. Entries are append-only, so
re-binding a key (pointing a source key at a different id when sources are
regrouped) is an explicit, recorded act: the old binding stays in the history
and the newest one wins at lookup. `collect` never re-binds. It only assigns a
fresh key or aliases a new key onto an existing id.
