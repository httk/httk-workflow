# Transfer completion and bounded metadata

New detached transfers carry a positive `transfer_sequence`, allocated
monotonically for each source/destination workspace pair. The workspace's
`transfers/protocol/issued.json` holds a counter per destination, plus reservations
for detach operations that have not yet published their source ledger. Reserving
before the fencing transition and reusing a reservation on retry prevents sequence
gaps when a process stops before fencing. A reservation is forgotten only after
the sealed ledger is durable.

The destination's `transfers/protocol/received.json` holds merged inclusive
sequence ranges per source. Receiving 1 through N leaves just `[[1, N]]`: no job
UUIDs, transfer UUIDs, digests, or payload paths. Out-of-order imports retain
separate ranges until the intervening transfers arrive. The range state is the
durable replay fence, including after an imported job has left this workspace.
It must be backed up/restored consistently with workspace state; deleting or
rolling it back independently is unsafe.

Import first validates the sealed bundle, publishes the durable payload and state,
and writes its ordinary acknowledgement. It then durably records the sequence
receipt before unlinking the individual `acks/` and `imported/` JSON files. This
can safely happen before source retirement because the compact receipt replaces,
rather than abandons, duplicate detection. A replay validates the whole bundle
again and reconstructs a signed acknowledgement without importing again. Sequenced
acknowledgements omit the historical `acknowledged_at` timestamp; their signature
attributes the returned receipt, not a permanently retained original importer.
The source still checks the acknowledgement signature, job/workspace identity,
payload digest, and sequence against its sealed ledger before retirement.

Retirement durably renames the source bundle and publishes a retired ledger before
reclaiming anything. After reclamation it prunes the ledger. A missing ledger is a
terminal no-op, including when a newer transfer of that job exists. Journal
segments protected by other jobs or managers are recorded once per segment in
`transfers/protocol/journal.json`, rather than retaining one ledger per retired job.
Recovery or another retirement retries this inventory. No cleanup journal writer
is opened. Existing marker-chain, live-manager, retention, and quarantine
protections continue to apply. A header-only writer from a failed detach is
collected using those same reference checks.

Protocol mutations are serialized per workspace by the permanent
`transfers/protocol/lock` inode using POSIX `flock`. Both ends must support shared
filesystem locking and the existing durable write/rename/fsync semantics. The lock
is released by the OS on process death. No operation holds two workspace locks.
This trades concurrent imports within one workspace for simple, atomic updates to
the compact receipt state. It has not been benchmarked at one million jobs or
verified against an actual HPC/NFS power-loss failure.

## Crash and duplicate cases

| Boundary | Durable state and retry |
| --- | --- |
| Import before acknowledgement | The destination marker's transfer provenance recognizes the same import. Retry finishes the seal/receipt without another job. Before this job can detach onward, its import receipt is completed from its authoritative state. |
| Acknowledgement before compact receipt | The individual acknowledgement still exists. Retry records the sequence and prunes the individual records. The source remains sealed until acknowledged. |
| Compact receipt before/partway through pruning | The range prevents a second import even if either individual record is missing. Pruning is idempotent. |
| Acknowledgement before source retirement | The intact source is re-offered; the destination reconstructs its receipt. The source checks it before retiring. |
| Retirement before/partway through reclamation | The retired ledger fences the source and retains the segment inventory. Recovery finishes payload/segment reclamation and ledger pruning. |
| Source ledger already pruned | Acknowledgement by transfer UUID is a no-op. Explicit offers/transfers for absent exact job UUIDs return no work. A missing UUID cannot be distinguished from an unknown UUID without retaining tombstones. |
| Old bundle replayed after the destination sent the job onward | Its sequence remains received. Validation and acknowledgement occur, but no marker or payload is created. |
| Old remote retirement replayed while a newer transfer exists | Automated fetch/relay retire with the actual acknowledgements, including transfer UUID, rather than only the job UUID. The newer ledger is untouched. |

The legacy explicit `retire JOB_ID` operation remains a caller assertion that
payload delivery succeeded. Automated fetch/relay use `--acknowledgements-json`
with the validated destination envelopes. Upgrade both ends for that protocol
extension; it is not silently downgraded to retirement by job ID.

## Residual size and compatibility limits

With all sequenced transfers completed and ordinary finite retention, an emptied
HPC workspace retains four protocol files: one lock and three JSON summaries.
The file count is independent of job count. JSON counters and range endpoints
need logarithmically more digits as the sequence increases. Summary entries scale
with workspace peers, unfinished reservations/sequence holes, and protected
journal segments, rather than completed job identities. Once every issued
transfer has arrived, each peer's receipt ranges coalesce to one interval.

Unsequenced bundles already in flight retain their individual destination
receipts: without an ordered sequence, removing those receipts would allow an old
bundle to recreate a job after it left the workspace. This is a compatibility
exception, not a claim that legacy transfers meet the bound. Existing unlimited
trash/journal retention deliberately retains payloads/history and is also outside
the bound. Protected live state and quarantine are never removed to make a size
test pass. A returning home workspace necessarily retains the N actual jobs and
their live state markers/journals; the boundedness guarantee concerns transfer
bookkeeping there, and the entire control tree of an emptied HPC workspace.

`tests/test_transfer_residuals.py` compares exact metadata file lists for 5 and
50 roundtrips, including shared terminal journal segments, and checks crash
boundaries, delayed replays, concurrent receivers, sequence reservation recovery,
remote incoming staging, and transfer-specific retirement. The existing retirement
suite checks shared/live journal protection, partial reclamation, retention,
sealed integrity, and quarantine preservation.
