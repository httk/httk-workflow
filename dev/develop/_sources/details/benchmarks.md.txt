# Scale benchmarks

```{admonition} Measured on the previous layout
:class: warning

This snapshot predates the current filesystem kernel: it was measured on the
earlier workspace layout, with state markers, a packed journal and leases, and
the runner `benchmarks/run_benchmarks.py` has not been ported yet, so it does
not run against this version. The numbers below are a historical reference
until they are re-measured.
```

This page records a measured scale snapshot from the opt-in runner
`benchmarks/run_benchmarks.py`. It describes one local configuration and is not
a capacity guarantee. Run it with:

```console
PYTHONPATH=src python benchmarks/run_benchmarks.py --profile quick
PYTHONPATH=src python benchmarks/run_benchmarks.py --profile snapshot --json snapshot.json
```

## Method

### Setup

The runner uses a `TemporaryDirectory`, a single Python process, no network,
`durable=False` and the local filesystem. Jobs use a minimal shell runner and
bounded-fan-out placements of the form `bench/<i%64>/<batch>`. The
flat-placement heartbeat case uses one `flat` directory. The snapshot profile
measures `N=2,500` and `N=10,000`; the quick profile uses `N=500`. The join
case always uses one waiting parent and `C=500` terminal children.

### Measurements

- `streamed_submission`: `prepare_job_payload` plus workspace submission, in
  jobs per second.
- `cold_scheduling_tick`: the first tick of a fresh manager over ready jobs.
  The API still requires `maximum_workers=1`, so the benchmark replaces the
  launch method with an eligibility-only no-op and starts no process.
- `warm_scheduling_tick`: the immediately following tick, with the manager's
  in-memory scan cursors warm.
- `time_to_first_claim`: from the first tick call until the wrapped transition
  observes the first `claimed` marker, with one worker and the minimal no-op
  runner.
- `memory_per_active_marker`: `tracemalloc` around a fresh marker-index rebuild
  and eligibility scan. It approximates Python index and bookkeeping
  allocations, not resident set size.
- `heartbeat_under_scan`: a scan of a large flat placement with the manager
  heartbeat hook wrapped. The wrapper forces a heartbeat write at each observed
  opportunity so the maximum gap is visible. It is an instrumented hook
  measurement, not an RSS or shared-filesystem result.
- `join_evaluation`: one manager join-evaluation pass for the waiting parent
  and its terminal children.
- `collect_throughput`: a complete lazy collect drain of terminal jobs.
- `fsck_and_gc`: the combined wall time of a full fsck and gc pass. The JSON
  result also holds separate `fsck_seconds` and `gc_seconds`. gc uses
  `journal_days=0` so that retention is configured and the journal walk runs.

## Snapshot

Measured on 2026-07-27 in `/home/rar/Documents/containers/devel/agents/httk2/httk-workflow`:

| Benchmark | Size | Wall time (s) | Rate |
| --- | ---: | ---: | ---: |
| streamed submission | 2,500 | 3.279033 | 762.42 jobs/s |
| streamed submission | 10,000 | 13.480633 | 741.80 jobs/s |
| cold scheduling tick | 2,500 | 0.852287 | 1.17 ticks/s |
| cold scheduling tick | 10,000 | 3.419358 | 0.29 ticks/s |
| warm scheduling tick | 2,500 | 0.852236 | 1.17 ticks/s |
| warm scheduling tick | 10,000 | 3.360714 | 0.30 ticks/s |
| time to first claim | 2,500 | 0.847354 | 1.18 claims/s |
| time to first claim | 10,000 | 3.356350 | 0.30 claims/s |
| memory per active marker | 2,500 | 4.311729 | 2,069.78 bytes/marker |
| memory per active marker | 10,000 | 17.809655 | 2,117.59 bytes/marker |
| heartbeat under scan | 2,500 | 1.143089 | 17.49 heartbeats/s |
| heartbeat under scan | 10,000 | 4.439381 | 15.98 heartbeats/s |
| join evaluation (`C=500`) | 500 children | 0.669827 / 0.670408 | 746.46 / 745.81 children/s |
| collect throughput | 2,500 | 1.702431 | 1,468.49 records/s |
| collect throughput | 10,000 | 6.818689 | 1,466.56 records/s |
| fsck + gc | 2,500 | 0.873996 (fsck 0.583754, gc 0.290242) | 2,860.42 jobs/s |
| fsck + gc | 10,000 | 3.509589 (fsck 2.326079, gc 1.183510) | 2,849.34 jobs/s |

The snapshot took 125.457 seconds in total. The machine was an Intel Xeon
E5-2680 v4 system with 28 visible CPUs and 92 GiB RAM on a local ZFS
filesystem. CPU frequency, filesystem cache state, Python version, storage and
background load all affect these values, so treat them as a local snapshot,
not a portable promise.

## Linearity check

The ratio below is `t(10,000) / t(2,500)`. A ratio above about 5× flags
possible super-linear behavior in this sample. No measured ratio crosses that
flag, which does not prove asymptotic linearity.

| Benchmark | Ratio |
| --- | ---: |
| streamed submission | 4.11× |
| cold scheduling tick | 4.01× |
| warm scheduling tick | 3.94× |
| time to first claim | 3.96× |
| memory per active marker | 4.13× |
| heartbeat under scan | 3.88× |
| collect throughput | 4.01× |
| fsck + gc | 4.02× |

Join evaluation has no ratio because its child count was fixed at `C=500` in
both snapshot runs.

These numbers come from one local filesystem; shared filesystems behave
differently. Large campaigns should use project-partitioned workspaces and size
each partition from measurements on its own storage and workload; see
{doc}`../campaigns`.
