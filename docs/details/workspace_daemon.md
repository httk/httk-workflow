# Confined workspace daemon

`httk workspace daemon` is an opt-in, foreground Slurm broker for a workspace
on an HPC system. The client mounts one **exchange directory**, never the
workspace. Through it the client sends jobs, collects finished jobs, reads
passive status, and sends signed requests that start managers from
operator-approved launchers, or request status or cancellation by opaque
manager handle. Requests need an authorized *httk* identity signature and
cannot supply commands, shell fragments, environment variables, paths or Slurm
arguments.

Serial execution supports one node and one Slurm task per submission, with
configurable manager workers sharing its capacity. Protected MPI launchers add
direct Slurm PMIx application steps; see [MPI applications](#mpi-applications).
The `mount-daemon` adapter supplies the typed client controls; see
{doc}`remotes`. The existing mount adapter and the ordinary Slurm launcher
lack these confinement guarantees.

## Deployment boundary

### Host requirements

- Linux with a Bubblewrap that supports `--bind-fd`, `--ro-bind-fd`,
  `--ro-bind-data` and `--clearenv` (0.6 or later). With Bubblewrap 0.8.0 or
  later the sandboxes also pass `--disable-userns`, which stops sandboxed code
  from creating nested user namespaces; with older versions that block is
  absent, which `--check` and startup report. Confinement does not depend
  on it; it reduces kernel attack surface.
- Permitted unprivileged user namespaces.
- Slurm 23.11.6 or later.
- The workspace and exchange on one filesystem and one mount (see
  [Layout](#layout)).

Startup refuses missing requirements; there is no unsandboxed fallback. The
Python installation, its packages, the global launchers and the initial
operator environment must be trusted: use a protected installation, not
editable packages in uploaded content.

### Trust

- The remote host, its Slurm, the installed Python/*httk* and the operator's
  global launchers are trusted.
- The client controls the exchange and its own workspace content, including
  everything the trusted side later moves. Payloads run confined: they cannot
  write outside the workspace and the exchange, and cannot use scheduler
  authority beyond the approved launchers.
- The trusted side never parses bundle content. Bundles are verified and
  adopted inside a payload sandbox.
- `status.json` and `managers.json` are informational: the client parses them
  strictly and never acts on them.

### Layout

The workspace and the exchange are siblings in a dedicated parent that
contains nothing else:

```text
/proj/campaign/        dedicated parent: exactly these two entries
  workspace/           workspace data root
  exchange/            the only client-mounted directory
    endpoint.json      public endpoint, written by --initialize and --reload
    requests/ responses/   signed command mailbox
    inbox/             the client drops job bundles here
    outbox/            ejected jobs, rejected/, status.json, managers.json
```

- Anything else in the parent is refused, naming the entry: the broker has
  read-write access to the parent.
- The workspace and exchange must be on one filesystem and one mount, so that
  jobs move by a plain rename. A probe checks this at
  `--initialize` and `--reload`, and the bootstrap rechecks it before every
  sandbox entry.
- The parent must be disjoint from the daemon state and snapshot directories,
  the *httk* data directory and the runtime paths.
- Entries whose names start with `.` or that are not plain file names
  (`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`) are ignored by the brokers.

Provision the parent so transport writers cannot replace the workspace or
exchange directories. The SQLite ledger needs a local filesystem with reliable
locking and durability; do not store it over SSHFS or another network
filesystem.

### Transport accounts

An SSHFS mount of the exchange alone does not confine the server-side account
to that directory; a same-UID unrestricted SFTP or SSH account could modify
private state or trusted code outside the exchange. Enforce the restriction at
the server or use a separate restricted transport identity. Local file-mode
checks cannot verify that server configuration. Mount without
`follow_symlinks`, and put the client mount point outside any local workspace.

### Sandboxes

The broker enters Bubblewrap before it listens for command requests. It sees
the dedicated parent read-write, private state, selected read-only runtime and
scheduler configuration, and the host network that Slurm needs. It moves
directories between the exchange and the workspace staging area and never
reads their content.

Each submitted manager enters a separate Bubblewrap sandbox on the compute node
before it runs its approved prelude. That sandbox has writable workspace data,
private temporary storage, isolated network, PID, IPC, UTS and user
namespaces, no capabilities and disabled nested user namespaces. It receives no
broker-only mounts.

### Runtime mounts

Mount only trusted runtime directories and required files. Do not expose home
directories, credentials, arbitrary Unix sockets or broad system configuration
trees to payloads. Read-only mounts can still contain sockets that grant host
services; file permissions do not disable them. Confinement also depends on a
correctly maintained host kernel and Bubblewrap. Resource and disk exhaustion
are separate operational concerns.

Shared runtime roots and broker-only roots must be disjoint, also after
symlink resolution. Runtime roots may not contain mutable roots or replace the
sandbox's private mounts. These checks reject broad aliases such as a selected
runtime symlink that resolves to `/`.

## Setup and startup

### Daemon launchers

Approved manager configurations are global **daemon launchers**. Create several
to offer different resources or worker counts (see {doc}`launchers`). Setup
reads their `launcher.json` without executing them, and never reads workspace
settings:

```console
httk workflow launcher add --template daemon --global small \
  --set slurm.cpus_per_task=2 --set slurm.mem=4G \
  --set slurm.time_limit=01:00:00 --set manager.workers=2
```

Names match `[a-z][a-z0-9_-]{0,63}`. A `slurm` launcher cannot be approved.
Unknown keys are refused when the launcher is added and again at setup.

Resource keys, per launcher:

| Key | Meaning |
| --- | --- |
| `slurm.partition`, `slurm.account`, `slurm.gres`, `slurm.reservation` | fixed `sbatch` flags when set |
| `slurm.cpus_per_task`, `slurm.mem`, `slurm.time_limit` | optional; see [Resources](#resources) |
| `slurm.mpi` | `pmix` selects MPI; the only MPI selector |
| `slurm.nodes`, `slurm.ntasks`, `slurm.ntasks_per_node` | MPI geometry; a value other than 1 without `slurm.mpi=pmix` is refused |
| `manager.workers`, `manager.command`, `environment.prelude` | manager workers, a single executable manager command, and the prelude run in the payload sandbox; MPI requires `manager.workers=1` |

Site keys apply to the whole daemon. They may be omitted; approved launchers
that set the same key must agree, otherwise setup refuses naming the key:

| Key | Default when unset |
| --- | --- |
| `daemon.readonly_paths` | existing of `/usr`, `/bin`, `/lib`, `/lib64` plus the initializing interpreter's `sys.prefix` and `sys.base_prefix` |
| `daemon.broker_paths` | the directory of `slurm.conf`, plus `/run/munge` when present |
| `daemon.bwrap`, `daemon.python`, `daemon.sbatch`, `daemon.squeue`, `daemon.scancel`, `daemon.scontrol` | discovered on `PATH`; Python is the running interpreter |
| `daemon.cluster`, `daemon.slurm_conf` | discovered, see below |
| `daemon.max_submissions` | 128 |
| `daemon.mpi.control_root`, `daemon.mpi.srun`, `daemon.mpi.pmix_roots`, `daemon.mpi.shm_root`, `daemon.mpi.devices`, `daemon.mpi.max_steps`, `daemon.mpi.termination_grace`, `daemon.mpi.environment.<NAME>` | MPI site settings, see [MPI applications](#mpi-applications); `daemon.mpi.control_root` is required when a launcher has `slurm.mpi=pmix` |

Path lists are colon-separated absolute paths. The initial operator environment
and `PATH` are trusted. Setup discovers `bwrap`, `sbatch`, `squeue` and
`scancel` there and saves their absolute paths. The cluster name is taken from
`daemon.cluster`, `SLURM_CLUSTER_NAME`, `slurm_conf` or a bounded
`scontrol show config` call. Other broker tunables (record limit, polling,
command timeout, request lifetime of 3600 seconds) are fixed.

### Client identities

On each client, initialize or select an *httk* identity and give its public
key to the destination operator; the private key stays on the client:

```console
httk init --name "Your Name" --email you@example.org
python -c 'from httk.core.identity import identity_public_key; print(identity_public_key())'
```

### Initializing and running

```console
httk workspace daemon /proj/campaign/workspace --initialize \
  --exchange /proj/campaign/exchange --launcher small \
  --authorize ed25519:REPLACE_WITH_CLIENT_PUBLIC_KEY
httk workspace daemon /proj/campaign/workspace --check
httk workspace daemon /proj/campaign/workspace
```

Pass `--launcher` and `--authorize` once per item. Setup prints the approved
launcher and key lists. Relative paths are taken from the current directory.

- `--initialize` requires that the exchange does not exist or is an empty
  directory. It creates the exchange and its subdirectories, the workspace
  staging directories, the ledger, the private response key and a fresh
  enrollment, saves the approved settings, publishes the active approval and
  writes `exchange/endpoint.json`. It runs no scheduler or sandbox preflight.
- `--check` enters the real broker sandbox, rechecks the layout and checks the
  scheduler clients, without submitting work or validating compute-node
  execution.
- `--once` processes one bounded scan. Normal startup polls until SIGINT or
  SIGTERM; a site service supervisor can restart the foreground daemon.
- `--state DIR` (default: `workspace-daemons/<workspace-path-hash>` below the
  *httk* data directory) and `--snapshots DIR` (default: `<state>.snapshots`)
  must be repeated on later invocations when not the default. Place state on a
  local filesystem. Snapshots hold immutable runtime copies and must be visible
  at the same path on compute nodes; state and the exchange need not be.

State, snapshots and trusted code must stay outside writable exports, be owned
by you or root, and not be world-writable, nor may the directories leading to
them. Group write permission is accepted, so anyone in a file's group is
trusted like you; on a host with shared project groups, remove group write
with `chmod g-w`.

An interrupted initialization keeps its partial artifacts and refuses
automatic replacement. Keep the ledger and keys when diagnosing failures, and
do not remove them to clear an uncertain submission.

### Resources

Optionally set CPU count, memory and time in a launcher as
`slurm.cpus_per_task`, `slurm.mem` and `slurm.time_limit`. A setting left unset
adds no corresponding `sbatch` flag, so Slurm's partition and site defaults
apply:

- Memory accepts positive integer MiB or K/M/G/T suffixes; KiB rounds up to
  MiB.
- Time accepts the standard
  [Slurm time forms](https://slurm.schedmd.com/sbatch.html); seconds round up
  to minutes.
- Zero or unlimited requests are rejected.
- Values above 1024 CPUs per task, 1,048,576 MiB per node or 10,080 minutes
  are refused at `--initialize` and `--reload` unless `--force` is passed.

Serial launchers use one Slurm task; `manager.workers` runs several concurrent
attempts within the manager's total capacity. Each signed start starts exactly
one manager. `environment.prelude` and a single executable `manager.command`
are frozen at approval and run inside the payload sandbox, followed by the
workspace's live `environment.prelude`, also confined. Daemon managers run
with `--allocation none` and never probe inside the sandbox. The trusted
bootstrap reads their CPU and memory capacity from the actual Slurm allocation
before the sandbox starts, falling back to the approved settings; memory known
from neither is not offered to jobs.

### Changing approved launchers

Stop the daemon, edit the launchers, then run:

```console
httk workspace daemon /proj/campaign/workspace --reload
```

`--reload` keeps the stored launcher names and authorized keys unless
`--launcher` or `--authorize` is given; given lists replace the stored ones.
It prints the resulting lists and rewrites `endpoint.json`. It refuses a
change of the fixed connection (workspace, exchange, state, snapshots,
cluster), naming the key: that needs a new enrollment. Slurm client paths,
`slurm.conf` and broker paths may change on reload.

Reload refuses while the daemon holds its lifetime ledger lock. Startup checks
its chosen snapshot against the active approval after taking the same lock, so
a startup racing with a reload cannot serve revoked keys or old approvals.
Clients read configuration digests live from `endpoint.json` and need no
reconfiguration.

Queued and running jobs keep their immutable snapshot, including prelude and
resource geometry. New starts must name a currently approved launcher and its
exact digest; a retained old snapshot does not authorize them. Reload
preserves the enrollment, ledger, response key and old snapshots. Installed
binaries and site configuration file contents remain operator-maintained
external dependencies.

## Job flow

Send a job from the client with the ordinary eject verb:

```console
httk job eject JOB /mnt/cluster/exchange/inbox
```

The daemon moves each bundle in `inbox` into the workspace; a manager started
by the daemon (`httk workflow manager run --exchange`) adopts it. A refused
bundle appears in `outbox/rejected/<name>`, with the reason in `status.json`.

A job that finishes (`succeeded`, `failed` or `cancelled`), has no parent job
and no unfinished child work is ejected automatically about 60 seconds later to
`outbox/<job_key>`. Fetch it:

```console
httk job adopt /mnt/cluster/exchange/outbox/JOB_KEY
```

A failed job is never retried automatically. To resume one, adopt it, fix it,
and eject it to the `inbox` again.

`outbox/status.json` lists job states, rejected bundles and eject errors, and
`outbox/managers.json` lists the daemon's manager ledger rows. Both are
informational and refreshed every few seconds. Read them with
`httk workflow remote daemon status REMOTE`; add `--handle` for the scheduler
state of one manager through a signed request.

## File command protocol

### Request signatures

The internal version-3 format requires identity signatures. The maintained
client builds and signs documents with its configured *httk* operator
identity; an unsigned request is never executed. The `--authorize` keys
are the allowed Ed25519 public keys, and removing a key
also stops it from replaying old results. This authorization rule is separate
from the ordinary, optional attribution meaning of *httk* identity signatures.

Each signature covers the format and version, the request, workspace and
enrollment IDs, the operation and its fields, `created_at`, `expires_at` and
`operator_key`.

### Time window

Times are integer Unix seconds. The default lifetime and the maximum request
lifetime are 3600 seconds. The clock-skew allowance is 7800 seconds
(130 minutes) in both directions, so first execution requires
`created_at - 7800 <= now <= expires_at + 7800`, with equality accepted. With a
one-hour lifetime the acceptance interval spans 5 hours 20 minutes in total.
The expiry limits admission and execution of the request; a Slurm job already
submitted can stay queued past that deadline.

### Ledger and replay

The protected ledger keeps the exact signed requests. A duplicate never
submits again, even after a restart. A currently authorized key can still
retrieve recorded results after expiry. A stale request that has not acted
is durably refused, subject to ledger capacity. A request recovered before
submission must still meet the time window before it can act.

### Response signatures

Responses are signed by a separate daemon key held in private broker state.
Clients configure its public key through a trusted operator handoff and never
learn a trust anchor from a mailbox response. Signed responses bind the exact
signed request digest and all destination and request identities. Signatures
provide integrity and authorization, not encryption or protection against
mailbox deletion. Slurm keeps using the site's own authentication, such as its
MUNGE socket. Cluster secrets and daemon private keys are never exposed to
payloads.

### Earlier enrollments

Enrollments that use an earlier protocol or ledger layout are refused.
Preserve their state and reconcile outstanding work with the earlier software
before provisioning a new enrollment. There is no automatic migration or
ledger reset.

### Publishing requests

Publish complete UTF-8 JSON by writing a temporary file and atomically
renaming it to `<request_id>.json` in the request directory. Use a fresh random
32-character lowercase hexadecimal request ID for each new operation.
Documents are limited to 16 KiB. Unknown keys, duplicate keys and extra
operation fields are rejected. Besides health checks, the operations are:

| Operation | Additional fields |
| --- | --- |
| `start_manager` | `configuration` and `configuration_digest`: the approved name and SHA256 digest |
| `manager_status` | `handle`: the returned manager handle |
| `cancel_manager` | `handle`: the returned manager handle |

### Responses

Responses appear under the same filename in the response directory, with the
request identities, the canonical request digest and the outcome. Scheduler
IDs and raw scheduler output are not exposed. Verify the identities and digest
when consuming a response: the uploader can interfere with mailbox contents,
and the protected ledger is authoritative.

Responses are committed before publication, and requests are removed only
after their response is published. Malformed requests are discarded with a
bounded diagnostic and no response.

### Request IDs

- Use one active caller per request ID.
- Do not replace a request while the broker may still be consuming it, since
  responses are published before request removal. The maintained client waits
  for removal and checks again for a response before republishing.
- A repeated request ID with unchanged content replays its recorded response.
  Changed content under the same ID is refused.
- Use a new request ID for each status refresh.

### Outcomes

- `submitted` returns an opaque handle.
- `uncertain` means submission may have happened. The daemon will not retry
  it, including after restart; an operator must reconcile it with Slurm.
- `UNKNOWN` status does not mean a job finished. Accounting reconciliation is
  not implemented.
- `cancel_requested` means the filtered cancellation call succeeded, not that
  termination is confirmed. Cancellation filters on controller-side job name
  and user to avoid acting on a reused numeric scheduler ID.

### Quotas

The record limit (4096) limits all durable request records. `daemon.max_submissions` limits
manager starts admitted over the enrollment's lifetime, including refused or
uncertain starts. Both are cumulative quotas, not active-job counts.
Exhaustion returns a nonpersisted `busy` response, retryable with the same
request ID and identical fields. Freeing capacity may need operator action;
there is no automatic history pruning or quota reset. Intentional
re-enrollment requires preserving and reconciling outstanding work, a new
enrollment ID and a new private ledger.

## Site acceptance

The repository has executable scheduler stand-in tests and a required real
Bubblewrap CI gate. Neither replaces site acceptance of SSHFS access controls,
Slurm authentication, compute-node mounts, prelude confinement, cancellation,
restart recovery or the actual simulation executable. Run those checks before
relying on this daemon for production jobs.

## MPI applications

### Execution model

MPI is opt-in through a daemon launcher with `slurm.mpi=pmix`. An MPI allocation runs one manager and
one application step at a time. Every rank enters Bubblewrap before reading
its executable, arguments, environment or working directory from workspace
data. The external step command is always a fixed `srun --mpi=pmix`
bootstrap; `mpirun` is not used. The manager stays network-isolated; only the
trusted allocation launcher and the MPI ranks use host networking.

### MPI launcher settings

The MPI site settings are `daemon.mpi.*` launcher keys, shared by all approved
launchers that set them. `daemon.mpi.control_root` is required; the others
default as shown:

| Key | Default |
| --- | --- |
| `daemon.mpi.control_root` | none; required |
| `daemon.mpi.srun` | `srun` on `PATH` |
| `daemon.mpi.pmix_roots` | none |
| `daemon.mpi.shm_root` | `/dev/shm` |
| `daemon.mpi.devices` | none |
| `daemon.mpi.max_steps` | 128 |
| `daemon.mpi.termination_grace` | 10.0 seconds |
| `daemon.mpi.environment.NAME` | none; one key per protected variable |

```console
httk workflow launcher add --template daemon --global mpi8 \
  --set slurm.cpus_per_task=2 --set slurm.mem=4G --set slurm.time_limit=01:00:00 \
  --set slurm.nodes=2 --set slurm.ntasks=8 --set slurm.ntasks_per_node=4 \
  --set slurm.mpi=pmix --set manager.workers=1 \
  --set daemon.mpi.control_root=/var/tmp/httk-mpi-control \
  --set daemon.mpi.pmix_roots=/var/spool/slurmd
```

### Launcher geometry

- `slurm.mpi=pmix` selects MPI, even with one rank. `slurm.nodes`,
  `slurm.ntasks` or `slurm.ntasks_per_node` other than 1 are refused without it.
- MPI requires one manager worker.
- Nodes default to one. Without a task count, the count is nodes times
  tasks-per-node when placement is given, and one task per node otherwise. An
  explicit task count takes precedence.
- `slurm.cpus_per_task` is CPUs per rank, and `slurm.mem` is memory per node.

Each MPI invocation uses the approved configuration's entire fixed geometry.
The manager advertises the aggregate capacity to workflow matching but runs
ordinary commands on its single manager CPU.

### Running an MPI application

Use the explicit wrapper for distributed applications:

```console
httk workflow mpi run -- /opt/application/bin/program input.dat
```

The same argv works through Python `Attempt.run`, Bash `httk_workflow_run` or
a simulation-code command setting. It needs a running daemon MPI manager and
never falls back to an ordinary local launcher. Output streams to the caller,
stdin is `/dev/null`, and a nonzero application exit status is returned to
the workflow.

Workspace and workflow preludes run inside the manager's sandbox. Put
application-specific node setup in an application script, which runs inside
every rank's sandbox. Only environment variables with portable ASCII
identifier names are forwarded, including module-derived variables such as
`PATH` and `LD_LIBRARY_PATH`. Exported Bash functions are omitted: keep any
needed function definitions in an application script and source them in each
rank.

### Shared memory

The ranks on each node share one allocation-specific backing directory
mounted at `/dev/shm`. This supports POSIX shared-memory files without
exposing unrelated host shared-memory objects. The normal root-owned sticky
`/dev/shm` parent is permitted, and each allocation directory is private to
the runtime UID.

Private PID and user namespaces can affect CMA transports, and private IPC
namespaces prevent cross-rank SysV IPC. Validate the transport your MPI
installation actually uses: a shared-file test alone does not prove that MPI
selected its shared-memory transport. No default forces TCP or disables
shared memory.

### PMIx, devices and environment

`daemon.mpi.pmix_roots` bounds the Slurm-supplied `PMIX_SERVER_TMPDIR`. Only the verified
per-step directory is exposed, at its original path, and the launcher does not
fall back to mounting broad authentication or socket directories. The site's
PMIx layout must fit this contract.

`daemon.mpi.devices` can expose individually approved device paths, such as needed RDMA
devices; it does not establish tested RDMA or GPU support. Protected
`daemon.mpi.environment.NAME` entries can tune the installed MPI transport. Application
manifests cannot override rank or server identities.

### Control socket

Provision `daemon.mpi.control_root` on the batch node so that both the batch and manager
Slurm steps see the same directory; site mount plugins must preserve that
visibility. The manager reaches the private socket at
`/run/httk-mpi/control.sock`, and its requests contain only a random request
ID. The launcher never interprets uploaded application arguments. The
workspace holds bounded application manifests under `.httk-workspace/mpi/`. A
lost connection ends the allocation and leaves uncertain manifests for
diagnosis; they are never resubmitted automatically.

### Cleanup

The site owns cleanup of the private control and node-local shared-memory
directories after Slurm confirms allocation quiescence, including after
cancellation and node failure. Use a trusted per-node epilog, or manual
anchored cleanup of the verified allocation directory. Launcher exit cannot
clean another node's local storage. Do not use age-only deletion, which can
remove live allocation storage.

### Failure handling

- MPI configurations disable requeue, and the bootstrap also refuses a
  restarted batch job.
- Duplicate application IDs never launch again during the allocation.
- Manager exit, client disconnect and service shutdown stop the active step.
- If local launcher termination leaves remote completion uncertain, the
  service stops accepting work, stops its manager and ends the allocation.
- `daemon.mpi.termination_grace` is a local cleanup deadline. Slurm may forward TERM to
  tasks as KILL, so it does not promise graceful application termination.

### Service authorities

Host networking and the PMIx endpoint are additional service authorities. The
site must verify that they do not let ranks launch unconfined processes or
reach other protected services. A generic `MPI_ERR_SPAWN` is inconclusive,
since lack of resources or a configuration error can also cause it. Test with
spare capacity, record the installed Slurm, Open MPI and PMIx versions, and
distinguish explicit unsupported-operation evidence from other errors.

### MPI acceptance

Before production use, test this exact mount and environment configuration
for:

- positive multi-node communication and confirmed shared-memory transport on
  nodes with multiple ranks;
- an allocation-shared `/dev/shm` object, and an unrelated host shared-memory
  canary that stays invisible to ranks;
- host filesystem and process canaries, dynamic MPI spawn, nested scheduler
  commands and exposed authentication or socket routes;
- rank startup failure, client timeout or disconnect, manager death,
  cancellation, requeue refusal, and per-node cleanup after completion and
  after node failure.

Local protocol tests and scheduler stand-ins do not establish real cluster
containment. This development host cannot create Bubblewrap namespaces and has
no Slurm or MPI runtime, so real cluster acceptance remains a deployment
check.

### MPI site probe

The {download}`MPI site probe <../../tools/probe_daemon_mpi.c>` (also at
`tools/probe_daemon_mpi.c` in a source checkout) provides the communication
and spawn checks. Build it on the cluster with
`mpicc -O2 -Wall -Wextra -o probe probe_daemon_mpi.c -lrt`, place the
executable in the workspace and invoke it from a workflow with
`httk workflow mpi run -- ./probe FRESHALPHANUMERICTOKEN`.

- At least two ranks must share a node.
- Add `--spawn` only for the separate spawn check, with independently
  established spare capacity.
- Exit 77 means inconclusive. Spawn success is a failure.
- The probe creates only a named object in the exposed `/dev/shm` and never
  writes an escape proof into the host home directory.

Inspect MPI transport diagnostics separately. The probe is a site test, not a
replacement for the filesystem, process, scheduler and lifecycle checks above.
