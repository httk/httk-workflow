# Stable ids

*For campaign owners and data-layer authors who rebuild a store and need its
entry ids to stay the same.*

A database built from workflow results mints entry ids as it saves — the first
record stored becomes `<base>-<series>-1`, the second `-2`, and so on. That
numbering follows *save order*, so rebuilding the same store from scratch, or
adding one job and rebuilding, renumbers entries and hands old ids to new
content. For a throwaway store that is fine; for one whose ids are cited, linked
to, or served, it is not.

The **id ledger** fixes this. It is an allocator that maps a stable *source key*
to a permanent id and hands the same id back for that key forever. Rebuilds stay
identical, and a change to an entry's *content* becomes a new revision under the
same id rather than a fresh entry.

## The three pieces

- **The ledger is the allocator.** It owns the per-family id bases and counters.
  The store no longer mints ids for ledger-managed saves; it is handed the id the
  ledger allocated. See `httk.store.IdLedger`.
- **Keys are provenance coordinates.** A key names *which produced thing* an id
  belongs to, in a stable way. The httk₂-native grammar, built by
  {py:func}`httk.workflow.ledger_key`, is

  ```text
  <workspace_id>:<job_id>[:<role>[:file:<relpath>]]
  ```

  — the producing job, optionally one of its declared output roles, optionally
  one file of that output (marked by a literal `file:` segment so a role and a
  file can never be confused). Keys are opaque to the allocator: embedded colons
  in a role or path are harmless.
- **The seal and git are the witnesses.** The ledger file is one signed seal
  document (`kind="httk-idledger"`), written atomically and re-signed only when
  something was actually allocated. Its signature attests the bytes; git history
  attests the sequence of versions. There is no separate hash chain — those two
  witnesses are enough. A corrupted or lost ledger is recovered by
  **restoring it from git**, and the verification errors say so.

## The anchoring rule

A key must derive from a *stable* identity, never from a path that can silently
move. A live-collected job is always stable. A job harvested from a **v1 tree
with no manifest** is identified only by its absolute path, so
{py:func}`httk.workflow.ledger_key` **refuses** it
({py:class}`httk.workflow.UnstableIdentityError`, with a `force=True` escape
hatch). A path-derived key that goes stale would hand an old id to new content —
the one unforgivable ledger failure — so `collect` degrades such a job to
store-minted (unstable) ids with a loud warning rather than pinning it wrongly.

## Using it from `collect`

With `--into`, the ledger is **on by default**, kept beside the store at
`<into>.ids.json`:

| Flag | Effect |
| --- | --- |
| *(default)* | Allocate ids through `<into>.ids.json`, creating and signing it on first use (announced loudly — a keep-worthy file appears next to the store; commit it alongside the store). |
| `--id-ledger PATH` | Put the ledger somewhere else, e.g. a path committed in the database repo. |
| `--no-id-ledger` | Do not use a ledger; the store mints ids, which are **not** stable across rebuilds. Warned once. |

The ledger is signed with the workspace's own seal keys
(the workspace `seal.keys` setting, via `default_workspace_keys`).
If **no signing key** is resolvable, `collect` falls back to no ledger with a
loud warning rather than failing — a collect never dies for want of a key.

While allocating, `collect`:

- **skips** any output already carrying an assigned public id — a ledger never
  overwrites one;
- **aliases** on content dedup — when the store deduplicates two jobs'
  content-identical outputs onto one row, the second job's key is recorded as an
  *alias* of the first's id, so both keys resolve to the one id;
- **degrades** an output it cannot hand an explicit id to (a structure view over
  a record whose backing dataclass has no `id` field) to store minting; those
  ids are stabilized instead where the id can be threaded through the build (the
  altermagnets `build_store` path), not through `collect`.

## Bases and numbering

`collect --into` gives each family a **distinct** base `<id-base>.<family>`, so
ids are shaped `<id-base>.<family>-<id-series>-<n>` — records and runs never
collide even though they share a ledger. A distinct base per family is required,
not cosmetic: the ledger enforces id uniqueness *globally* (across all
families), so a single shared base would make the first `records` id and the
first `runs` id identical and brick the next rebuild's reopen. The ledger's
counter is `max(existing number) + 1` per family — monotone and tolerant of
gaps, so an entry whose source later disappears keeps its number rather than
letting a later entry reuse it.

## Supersession and re-binding

`lookup` resolves a key to its **newest** binding. Entries are append-only and
never edited, so re-binding a key — pointing a source key at a different id when
sources are regrouped — is an explicit, recorded act, not an in-place mutation:
the old binding stays in the history and the newest one wins at lookup. `collect`
itself never re-binds; it only assigns a fresh key or aliases a new key onto an
existing id.
