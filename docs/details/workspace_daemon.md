# Confined workspace daemon

`httk workspace daemon` is an opt-in, foreground Slurm broker for a workspace
on an HPC system. The client mounts one **exchange directory**, never the
workspace. Through it the client sends jobs, collects finished jobs, reads
passive status, and sends signed requests that start managers from
operator-approved launchers, or request status or cancellation by opaque
manager handle. Requests need an authorized *httk* identity signature and
cannot supply commands, shell fragments, environment variables, paths or Slurm
arguments.

The daemon approves ordinary global `slurm` launchers that set
`manager.confine=bwrap`. Each signed start submits one manager from such a
launcher. The manager is trusted and runs unconfined in its batch job, and it
starts every job attempt inside its own Bubblewrap sandbox, which can write only
that job's directory. Parallel programs start through the same launch prefix as
anywhere else; see [Parallel launches](#parallel-launches). The
`mount-daemon` adapter supplies the typed client controls; see {doc}`remotes`.
The existing mount adapter and an unconfined Slurm launcher lack these
guarantees.

## Trust and execution model

### Trust model

- The remote host, its Slurm, the installed Python and *httk*, the operator's
  global launchers and the workspace settings are trusted operator
  configuration.
- The client controls the exchange and the content of the jobs it sends,
  including everything the trusted side later moves (status documents, ejected
  bundles). Uploaded job content may be arbitrary.
- The broker moves directories between the exchange and the workspace staging
  area and never reads their content.
- The managers are trusted. They adopt staged bundles with the hardened
  adoption walk and execute no bundle code outside an attempt sandbox.
- Each job attempt, and each rank of a parallel launch, runs confined: it can
  write only its own job directory and cannot use scheduler authority. Jobs
  cannot change `manager.confine` or any `confine.*` setting: neither job
  parameters nor declared environment values reach those keys.
- `status.json` and `managers.json` are informational: the client parses them
  strictly and never acts on them.

### Deployment boundary

| Component | Runs | Can write |
| --- | --- | --- |
| broker (`httk workspace daemon`) | in its own Bubblewrap sandbox on the login or service node, with the host filesystem read-only | the dedicated parent of workspace and exchange, and its private state |
| manager | unconfined, as you, in the Slurm batch job: `httk workflow manager run --exchange --idle` | the workspace |
| job attempt | in a Bubblewrap sandbox the manager builds, on the manager's node | its own job directory |
| rank of a confined launch | in a Bubblewrap sandbox the trusted rank helper builds, on every node of the launch | its own job directory, the launch's shared-memory directory |

The manager runs only trusted code: the installation, the frozen launcher
settings and the workspace settings. What it does not do on the trusted side:

- it never runs a job's postprocess scripts; `httk workflow postprocess` and
  `httk collect` are client-side verbs that you run on adopted jobs, unconfined
  wherever you run them;
- it never runs the `[workflow.build].platform` probe of a runner tree that
  arrived in a bundle unless an operator's `httk workflow build` already
  registered a build of that source with the same probe command; otherwise
  the job fails with `runner_not_built` without executing anything. Building a
  compiled runner with `httk workflow build` is an operator action and runs
  unconfined, so review what you build.

Every manager on an enrolled workspace must confine its attempts: once
`httk workspace daemon init` has written the enrollment marker
`.httk-workspace/exchange/enrollment.json`, a manager whose effective
`manager.confine` is not `bwrap` refuses to start, and a running one stops
claiming work if the workspace setting changes. This applies to `--inline`,
process-launched and manually started managers as well, so run them with
`--setting manager.confine=bwrap` or set `manager.confine=bwrap` as a
workspace setting. Only the daemon writes the marker (`init` creates it, and
`check` and `run` restore it when it is missing); a manager run with `--exchange` on a workspace that was
never enrolled creates the staging directories but does not enroll it.

### Host requirements

- Linux with a Bubblewrap that supports `--bind-fd`, `--ro-bind-fd`,
  `--ro-bind-data`, `--new-session` and `--clearenv`, on the broker host and
  on every compute node. With Bubblewrap 0.8.0 or later the sandboxes also
  block nested user namespaces (`--disable-userns`); where that option is
  absent or refused, the broker's startup and the manager log a warning and
  run without that block. It reduces kernel attack surface; confinement does
  not depend on it.
- Permitted unprivileged user namespaces, and a PID namespace with its own
  `/proc`, on the compute nodes. A manager probes this once when it starts and
  refuses to start without it.
- Slurm 23.11.6 or later.
- The workspace and exchange on one filesystem and one mount (see
  [Layout](#layout)).
- For parallel launches, `flock` support on the workspace filesystem (on
  Lustre, mount with `flock` or `localflock`).

Startup refuses missing requirements; there is no unsandboxed fallback. The
Python installation, its packages, the global launchers and the initial
operator environment must be trusted: use a protected installation, not
editable packages in uploaded content.

### Layout

The workspace and the exchange are siblings in a dedicated parent that
contains nothing else:

```text
/proj/campaign/        dedicated parent: exactly these two entries
  workspace/           workspace data root
  exchange/            the only client-mounted directory
    endpoint.json      public endpoint, rewritten when a changed configuration is activated
    requests/ responses/   signed command mailbox
    inbox/             the client drops job bundles here
    outbox/            ejected jobs, rejected/, withdrawn/, managers/, status.json, managers.json
```

- Anything else in the parent is refused, naming the entry: the broker has
  read-write access to the parent.
- The workspace and exchange must be on one filesystem and one mount, so that
  jobs move by a plain rename. A probe checks this at `init`, and the broker
  rechecks it every time it starts.
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

### Broker sandbox

The broker enters Bubblewrap before it listens for command requests. It runs
only *httk* code and the fixed Slurm clients, never client code, so it sees the
whole host filesystem read-only: Slurm clients and site wrappers, `slurm.conf`,
munge, the user database (`/etc/passwd`, SSSD) and DNS work without
configuration. It can write only the dedicated parent and its private state,
and has the host network that Slurm needs. It moves directories between the
exchange and the workspace staging area and never reads their content.
The broker and its Slurm clients inherit the environment the daemon was
started in, so site wrappers find their module and site variables; only
`SBATCH_*`, `SALLOC_*`, `SRUN_*` and `SLURM_*` (except `SLURM_CONF`),
`PYTHON*`, `BASH_ENV`, `ENV`, `LD_PRELOAD` and `LD_LIBRARY_PATH` are dropped.
`HOME` and `XDG_CACHE_HOME` point at a writable private `/tmp/home`, and the
clients whose output the broker parses run in the C locale.

### Confined attempts

A manager with `manager.confine=bwrap` runs each attempt in a Bubblewrap
sandbox with:

- the whole workspace **read-only** at its real path, so parent outputs,
  `stage_input`, `Attempt.parent` and other cross-job reads keep working;
- the attempt's **job directory writable** at its real path, so every
  `HTTK_WORKFLOW_*_DIR` path is valid unchanged;
- the paths of `confine.readonly_paths` read-only, a private `/tmp` with
  `HOME=/tmp/home` and `TMPDIR=/tmp`, its own `/proc`, a minimal `/dev` with
  a private `/dev/shm`, and the devices of `confine.devices`;
- private user, PID, IPC and UTS namespaces, a private network namespace
  unless `confine.isolate_network` is `false`, no capabilities, and nested
  user namespaces blocked when Bubblewrap supports it;
- standard input from `/dev/null` and an environment without the `SLURM_*`,
  `SRUN_*`, `SBATCH_*`, `SALLOC_*`, `PMI_*` and `PMIX_*` variables, with
  `HTTK_WORKFLOW_CONFINED=1`.

The attempt cannot write the workspace root, `.httk-workspace/`, or any other
job. The workflow prelude runs inside the sandbox; the launcher's
`environment.prelude` runs before the manager, and its environment reaches the
attempt through the filter above. Anything a prelude or a code needs at run
time, such as a module tree, a conda or virtual-environment prefix, code
binaries or the installed runner search paths, must therefore be listed in
`confine.readonly_paths`. The default covers `/usr` (with the `/bin`, `/lib`,
`/lib64` and `/sbin` links of merged-`/usr` systems), `/etc`, the manager's
Python installation and the directories *httk* itself is imported from.

Mount only trusted runtime directories. Do not expose home directories,
credentials, arbitrary Unix sockets or broad configuration trees beyond what
jobs need. Read-only mounts can still contain sockets that grant host
services; file permissions do not disable them. Confinement also depends on a
correctly maintained host kernel and Bubblewrap. Resource and disk exhaustion
are separate operational concerns.

Where compute nodes disable network namespaces (`user.max_net_namespaces=0`,
on which Bubblewrap fails with "Creating new namespace failed"), set
`confine.isolate_network=false`: attempts then share the host network but keep
every other restriction above. Ranks of a confined launch always use the host
network.

Job directories never nest and are never symlinks; placement directories may
be operator symlinks, but a job reached through a placement symlink pointing
outside the workspace cannot be confined, and its attempt fails. See
{doc}`taskmanager` for the general confinement settings.

## Setup and startup

### Approving launchers

Approved manager configurations are global `slurm` launchers that set
`manager.confine=bwrap`. Create several to offer different resources or worker
counts (see {doc}`launchers`):

```console
httk workflow launcher add --template slurm --global small \
  --set manager.confine=bwrap --set slurm.cpus_per_task=2 --set slurm.mem=4G \
  --set slurm.time_limit=01:00:00 --set manager.workers=2
httk workflow launcher configure --add-path confine.readonly_paths=/software small
```

`--add-path` extends a colon-separated `confine.*` path list without restating
its computed default. List the launchers in the daemon configuration (see
[Daemon configuration](#daemon-configuration)). The daemon reads the bundles
without executing them and refuses one that:

- is not a global launcher, or whose name does not match
  `[a-z][a-z0-9_-]{0,63}`;
- is not of kind `slurm`;
- does not set `manager.confine=bwrap`, or has a malformed or unknown
  `confine.*` setting;
- asks for resources above the sanity limits (see [Resources](#resources))
  unless the configuration sets `force=true`;
- lies inside the daemon parent, state or snapshot directories, or is
  world-writable.

Every start of the daemon (`check` and `run`) reads the listed bundles again
and freezes their settings, with the digest of their `launcher.json`, into the
runtime snapshot it serves. The broker submits through the installed *httk*
and never runs a bundle's `launcher` executable. Each frozen launcher setting `manager.confine`,
`manager.launch_template`, `manager.launch_mpi`, `manager.bind_cpus`, `manager.confine.block_mpi_spawn` and `confine.*` is pinned on the
managers it starts and cannot be changed by workspace settings; every other
workspace setting still applies live inside the manager, as for any manager.

### Daemon configuration

The daemon configuration is saved in the private state directory
(`<state>/configuration.json`). `init` and `configure` take `--set KEY=VALUE`
for any key; `--add KEY=VALUE` (and, in `configure`, `--remove KEY=VALUE`)
changes one item of a list, while `--set` replaces a whole list with a
comma-separated value. `init` discovers what is not given:

| Key | Meaning | Default at `init` |
| --- | --- | --- |
| `launchers` | approved global `slurm` launchers, the configurations clients can start | none; at least one is required |
| `authorized_keys` | Ed25519 public keys allowed to sign requests | none; at least one is required |
| `bwrap` | the Bubblewrap of the broker sandbox | `bwrap` on `PATH` |
| `python` | runs the broker, and the submitted managers of launchers without `environment.prelude` | the running interpreter |
| `sbatch`, `squeue`, `scancel` | the Slurm clients | on `PATH` |
| `sacct` | optional, used only to report how managers ended | on `PATH` when present |
| `slurm_conf` | optional fixed Slurm configuration file | `SLURM_CONF` when it names a file, else `/etc/slurm/slurm.conf` when present |
| `max_submissions` | lifetime quota of manager starts (see [Quotas](#quotas)) | 128 |
| `force` | `true` approves CPU, memory and time above the sanity limits | `false` |
| `cluster` (`init` only) | the fixed Slurm cluster name | `SLURM_CLUSTER_NAME`, the `ClusterName` of the Slurm configuration, or a bounded `scontrol show config` call |
| `scontrol` (`init` only) | used only to discover the cluster name | on `PATH` |

Executables are resolved when set, and relative paths are taken from the
current directory. An empty value (`--set sacct=`) unsets `sacct` or
`slurm_conf`. The cluster belongs to the fixed enrollment, together with the
workspace, exchange, state and snapshot directories: `configure` refuses
`cluster` and `scontrol`, and a new `slurm_conf` keeps the enrolled cluster.
The initial operator environment and `PATH` are trusted. Other broker tunables
(record limit, polling, command timeout, request lifetime of 3600 seconds) are
fixed.

### Client identities

On each client, initialize or select an *httk* identity and give its public
key to the destination operator; the private key stays on the client:

```console
httk init --name "Your Name" --email you@example.org
python -c 'from httk.core.identity import identity_public_key; print(identity_public_key())'
```

### Initializing and running

```console
httk workspace daemon init /proj/campaign/workspace \
  --exchange /proj/campaign/exchange --add launchers=small \
  --add authorized_keys=ed25519:REPLACE_WITH_CLIENT_PUBLIC_KEY
httk workspace daemon check /proj/campaign/workspace
httk workspace daemon run /proj/campaign/workspace
```

Repeat `--add` once per item. `init` and `configure` print the enrollment and
the configuration, as `httk workspace daemon show WORKSPACE` does (`--json`
for one JSON document). Relative paths are taken from the current directory.

- `init` requires that the exchange does not exist or is an empty directory.
  It creates the exchange and its subdirectories, the workspace staging
  directories, the enrollment marker that enrolls the workspace, the ledger,
  the private response key and a fresh enrollment, saves the configuration,
  publishes the runtime snapshot and writes `exchange/endpoint.json`. It then
  runs the same check as `check`; if that fails, the enrollment is kept and
  printed guidance says to fix the launchers or the configuration and run
  `check` again.
- `check` and `run` first activate the configuration (see
  [Configuration changes](#configuration-changes)).
- `check` enters the real broker sandbox, rechecks the layout and checks the
  scheduler clients, without submitting work or validating compute-node
  execution.
- `run --once` processes one bounded scan. `run` polls until SIGINT or
  SIGTERM; a site service supervisor can restart the foreground daemon.
- `--state DIR` (default: `workspace-daemons/<workspace-path-hash>` below the
  *httk* data directory) must be repeated on later invocations when not the
  default; `--snapshots DIR` (default: `<state>.snapshots`) is remembered by
  the enrollment and only checked when given again. Place state on a local
  filesystem. Slurm writes each manager's output to `<snapshots>/jobs/`,
  so that directory must be reachable at the same path from the batch nodes;
  state and the exchange need not be.

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
`slurm.cpus_per_task`, `slurm.mem` and `slurm.time_limit`, and the geometry as
`slurm.nodes`, `slurm.ntasks` and `slurm.ntasks_per_node`, exactly as for any
`slurm` launcher. A setting left unset adds no corresponding `sbatch` flag, so
Slurm's partition and site defaults apply:

- Memory accepts positive integer MiB or K/M/G/T suffixes; KiB rounds up to
  MiB.
- Time accepts the standard
  [Slurm time forms](https://slurm.schedmd.com/sbatch.html); seconds round up
  to minutes.
- Zero or unlimited requests are rejected.
- Values above 1024 CPUs per task, 1,048,576 MiB per node or 10,080 minutes
  are refused unless the daemon configuration sets `force=true`.

Each signed start submits exactly one manager, with the launcher's batch
directives plus `--parsable`, `--export=<mode>`, `--no-requeue`,
`--input=/dev/null`, `--clusters`, the job name `httk-<handle>` and its output
file. The export mode is the launcher setting `slurm.export`, either `NONE`
(the default when unset) or `NIL`; see the site's `sbatch` manual for what each
means on that cluster. The batch script, piped to `sbatch`, writes nothing into
the workspace. It runs the frozen `environment.prelude` under `set -e` in a
login shell, then the manager: `manager.command` (default `httk`) on the
resulting `PATH` when a prelude is set, otherwise the configured `python`.
With a prelude, the approved launcher's content, not `python`, therefore
decides which interpreter runs the manager; the configuration digest covers that
content. With the default `--export=NONE`, the manager's environment comes from
the login shell and the prelude, not from the daemon; `NIL` was observed to
break the login-shell Lmod environment on one site, so `NONE` is the default.
The workspace's own `environment.prelude` does not apply to daemon submissions.

The manager is `httk workflow manager run --by-path --workspace WORKSPACE
--exchange --idle` with the launcher's `--workers` (from `manager.workers`),
`--allocation` (from `manager.allocation`, default `slurm`) and its pinned
`--setting` values. It probes its Slurm allocation like any manager the
`slurm` launcher starts, places attempts on the allocation's nodes and serves
until Slurm's time limit drains it; see {doc}`taskmanager`.

### Configuration changes

Edit a launcher bundle with `httk workflow launcher configure`, or the daemon
configuration with `httk workspace daemon configure`, then restart the daemon:

```console
httk workspace daemon configure /proj/campaign/workspace --add launchers=large \
  --remove authorized_keys=ed25519:REPLACE_WITH_REVOKED_KEY
httk workspace daemon run /proj/campaign/workspace
```

`configure` approves the result exactly as a start would and saves it only
when it is accepted; it publishes nothing, so it also works while the daemon
runs. Every `check` and `run` compiles the saved configuration with the
current launcher bundles. When the result differs from the active runtime
snapshot, it publishes a new snapshot, rewrites `endpoint.json` and prints the
launchers whose approval changed; otherwise it publishes nothing. Publishing
needs the daemon's ledger lock, so a changed configuration is refused while
the daemon runs: stop it first. Clients read configuration digests live from
`endpoint.json` and need no reconfiguration.

Queued and running managers keep the settings they were submitted with. New
starts must name a currently approved launcher and its exact digest; a
retained old snapshot does not authorize them. Activation preserves the
enrollment, ledger, response key and old snapshots. Installed binaries and
site configuration file contents remain operator-maintained external
dependencies.

### Upgrading

After updating *httk-workflow* on the remote host, restart the daemon. An
enrollment made by an earlier development version without a saved
configuration has one created from its active snapshot (with `force=false`)
at the first start. An enrollment with a different snapshot format is
refused; see [Earlier enrollments](#earlier-enrollments). Update the client
before or together with the remote host: the current client reads both
versions of `managers.json`, an older one only version 1.

## Job flow

Send a job from the client with the ordinary eject verb:

```console
httk job eject JOB /mnt/cluster/exchange/inbox
```

The daemon moves each bundle in `inbox` into the workspace; a manager started
by the daemon (`httk workflow manager run --exchange`) adopts it. Adoption
refuses special files, symlinks pointing outside the bundle and hard-linked
files. A refused bundle appears in `outbox/rejected/<name>`, with the reason in
`status.json`.

A job that finishes (`succeeded`, `failed` or `cancelled`), has no parent job
and no unfinished child work is ejected automatically about 60 seconds later to
`outbox/<job_key>`. Fetch it:

```console
httk job adopt /mnt/cluster/exchange/outbox/JOB_KEY
```

A failed job is never retried automatically. To resume one, adopt it, fix it,
and eject it to the `inbox` again.

`outbox/status.json` lists job states, rejected bundles and eject errors.
`outbox/managers.json` (format version 2) lists the daemon's manager starts
with their ledger state, Slurm job ID, scheduler state, exit code and start and
end times, and the names of the bundles still waiting in `inbox` or in the
workspace (`staged`, at most 1000, with `staged_truncated` set when more
exist). Unknown values are `null`. Both files are informational and refreshed
every few seconds. Read them with `httk workflow remote daemon status REMOTE`;
add `--handle` for the scheduler state of one manager through a signed request.

The daemon checks each submitted manager about once a minute: with `squeue`
while Slurm lists it, then with `sacct` for its final state and exit code. A
manager is final in `COMPLETED`, `FAILED`, `CANCELLED`, `TIMEOUT`,
`OUT_OF_MEMORY`, `NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL` or `DEADLINE`. With
`sacct` configured, a final state seen only by `squeue` waits up to 30 minutes
for accounting to add the exit code, and is then kept without one. A manager
that neither client knows for 30 minutes, which is always the case without
`sacct` or while accounting fails, becomes `GONE`. Once a manager is final, the
last 1 MiB of its Slurm output is copied to `outbox/managers/<handle>.log`, and
its row's `log` names that file. This also covers a manager that failed on its
node before it ever adopted a job.

### Withdrawing waiting jobs

A bundle that no manager will run can be taken back. A signed `withdraw`
request moves every bundle waiting in the workspace, or only the one it names,
unchanged to `outbox/withdrawn/<name>`; adopt it from there with
`httk job adopt`. Bundles still in `inbox` are not touched; take those back
directly. A bundle published just before the request may reach the workspace
right after it, so repeat the withdrawal if `managers.json` still lists it. A
bundle that a manager adopts at the same moment is either withdrawn or
adopted, never lost.

The response's `detail` lists only what that execution moved; `outbox/withdrawn/`
is authoritative. A retried request normally replays the recorded response, but
after a broker restart that interrupted it, the retry may list fewer names.
Update the remote host before using `withdraw`: an older broker discards the
unknown operation without a response.

## File command protocol

### Request signatures

The internal version-3 format requires identity signatures. The maintained
client builds and signs documents with its configured *httk* operator
identity; an unsigned request is never executed. The configured
`authorized_keys` are the allowed Ed25519 public keys, and removing a key
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
jobs.

### Earlier enrollments

Enrollments that use an earlier protocol, snapshot format or ledger layout are
refused. Preserve their state and reconcile outstanding work with the earlier
software before provisioning a new enrollment. There is no automatic migration
or ledger reset.

### Publishing requests

Publish complete UTF-8 JSON by writing a temporary file and atomically
renaming it to `<request_id>.json` in the request directory. Use a fresh random
32-character lowercase hexadecimal request ID for each new operation.
Documents are limited to 16 KiB. Unknown keys, duplicate keys and extra
operation fields are rejected. Besides health checks, the operations are:

| Operation | Additional fields |
| --- | --- |
| `start_manager` | `configuration` and `configuration_digest`: the approved launcher name and its SHA256 digest |
| `manager_status` | `handle`: the returned manager handle |
| `cancel_manager` | `handle`: the returned manager handle |
| `withdraw` | optional `bundle`: the one waiting bundle to withdraw |

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
- `UNKNOWN` status does not mean a job finished; `managers.json` reports the
  final state once accounting knows it.
- `cancel_requested` means the filtered cancellation call succeeded, not that
  termination is confirmed. Cancellation filters on controller-side job name
  and user to avoid acting on a reused numeric scheduler ID.
- `uncertain`, `refused` and `busy` responses may carry a signed `detail`
  string with the scheduler's own error, for example
  `sbatch exited 1: sbatch: error: Batch job submission failed: Invalid account …`.
- `withdrawn` lists the moved bundles in `detail`, comma-separated and cut to
  1000 characters with a trailing `,...`; it has no `detail` when nothing moved.

The foreground daemon logs to stdout: one line per request (operation, request
ID, outcome), each submitted Slurm job ID, every manager state change and
published manager log, every failed Slurm client call with its exit code and
error output, and startup and check results.

While a submitted job runs, Slurm writes its output, with stderr merged in, to
`<snapshots>/jobs/httk-<jobid>.out`, outside the workspace; the
`daemon_submitted` log line names this path. When the manager is final, the
last 1 MiB is published to `outbox/managers/<handle>.log` (see
[Job flow](#job-flow)). The original stays in the `jobs` directory for the
operator to inspect and clean up. The manager's own log is the workspace's
`.httk-workspace/managers.log`, as for every manager.

### Quotas

The record limit (4096) limits all durable request records. `max_submissions`
limits manager starts admitted over the enrollment's lifetime, including
refused or uncertain starts. Both are cumulative quotas, not active-job counts.
Exhaustion returns a nonpersisted `busy` response, retryable with the same
request ID and identical fields. Freeing capacity may need operator action;
there is no automatic history pruning or quota reset. Intentional
re-enrollment requires preserving and reconciling outstanding work, a new
enrollment ID and a new private ledger.

## Parallel launches

### The launch prefix under confinement

Code commands name only the program (`vasp.command = "vasp_std"`); the parallel
start is the attempt's launch prefix, `HTTK_WORKFLOW_LAUNCH`, rendered from
`manager.launch_template` or the built-in Slurm prefix (see
{doc}`taskmanager`). In a confined attempt the rendered prefix cannot run,
because the sandbox has no scheduler access, so `HTTK_WORKFLOW_LAUNCH` is a
launch client instead. Use it exactly as an unconfined prefix:

```console
$HTTK_WORKFLOW_LAUNCH ./program input.dat
```

The client asks the trusted manager to start the launch, and the manager runs
`<rendered launch template> <rank helper>`: the template is rendered from the
placement held in the manager's memory, with a manager-owned nodefile for
`{nodefile}` and `SLURM_HOSTFILE` in the trusted launch directory
`.httk-workspace/managers/<manager_id>/launches/<attempt_id>.<request_id>/`,
which jobs can only read. The manager never interprets the program
and arguments. The code run helpers prepend the prefix themselves, so code
command settings and workflow packages are the same confined and unconfined.

- The client must run from a working directory inside the job directory (the
  attempt's workdir is).
- One launch runs at a time per attempt; further requests wait in arrival
  order.
- The ranks' standard output and standard error stay separate: the client
  reproduces them on its own and exits with the launch's exit status (`2` when
  the launch is refused, `143` when it was stopped without one). Rank
  standard input is `/dev/null`.
- A launch is refused or stopped once the attempt is cancelled, times out, is
  drained, publishes its outcome or exits. Stopping sends `SIGTERM` to the
  launch's process group and `SIGKILL` after the manager's cancellation grace.
- `SIGTERM` or `SIGINT` to the client (for example from a code helper's
  timeout) does not end it: it asks the manager to stop the launch and returns
  only after the ranks are gone. A killed client stops the launch too.
  Accepted limitation: a code helper escalates to `SIGKILL` of the client
  after its own termination grace (10 s by default), and the manager
  escalates the ranks after its cancellation grace, so with ranks that ignore
  `SIGTERM` the helper can return about a second before the manager has
  reaped them. Ranks that honour `SIGTERM` are gone before the helper returns.
- A launch's `exited` or `stopped` status, and therefore the attempt's
  commit, follows the launcher's local process group (for example `srun`).
  When a killed `srun` leaves remote tasks, they end when Slurm cleans up the
  step, possibly a few seconds later.
- The attempt keeps its placement and is not committed, sealed or ejected
  until every launch it made has been reaped. A launch whose processes outlive
  `SIGKILL` is reported as uncertain, and that attempt can start no further
  launches.
- Commands run without the prefix run inside the attempt sandbox on the
  manager's node, as unconfined commands run on that node.

The ORCA run helper starts no prefix by default, since ORCA starts its own
MPI; under confinement only single-node ORCA is supported, and a multi-node
binding is refused. The generic `run` verb, `Attempt.run` and
`httk_workflow_run` never prepend the prefix.

### Rank sandboxes

On every rank the trusted rank helper opens the workspace and the job
directory without following symlinks, refusing the workspace root,
`.httk-workspace` and nested placements, and only then enters Bubblewrap with:

- the workspace read-only and the job directory writable at their real paths,
  and the paths of `confine.readonly_paths`;
- the launch's shared-memory directory at `/dev/shm`, the Slurm step's PMIx
  directory (only when it lies below `confine.pmix_roots`), and the devices of
  `confine.devices`;
- private user, PID, IPC and UTS namespaces, the **host network**, no
  capabilities, and nested user namespaces blocked when Bubblewrap supports
  it;
- the rank's process-manager variables (`PMI_*`, `PMIX_*`, `OMPI_*`, `OPAL_*`
  and the Slurm step identity), then `confine.environment.<NAME>` values, and
  `HOME=/tmp/home`, `TMPDIR=/tmp`.

Inside the sandbox it reads the request, changes to the client's working
directory and adds the attempt's environment, without names starting with
`PMI_`, `PMIX_`, `OMPI_`, `OPAL_`, `SLURM_`, `SLURMD_`, `SRUN_`, `SBATCH_`,
`SALLOC_` or `HTTK_`, and without names that are not portable identifiers,
such as exported Bash functions. Module-derived variables such as `PATH` and
`LD_LIBRARY_PATH` therefore reach the ranks, but MPI tuning variables set in
the attempt do not: set them as `confine.environment.<NAME>`.

### Shared memory

The ranks of one launch on a node share a private directory
`<confine.shm_root>/httk-<token>` (default root `/dev/shm`, mode 0700,
owner checked; the token is a random name the manager chose for the launch), mounted at `/dev/shm`. This supports POSIX shared-memory files
without exposing unrelated host shared-memory objects. The last rank to leave
a node removes the directory. After a node failure or a killed rank a stale
directory may remain; removing it is a site cleanup item, for example in a
trusted epilog once Slurm confirms the job is gone. Do not use age-only
deletion, which can remove a live launch's storage.

Each rank has its own PID namespace, which defeats single-copy transports such
as CMA and XPMEM, and private IPC namespaces prevent cross-rank SysV IPC. Sites
may need to select another mechanism, for example
`confine.environment.OMPI_MCA_btl_vader_single_copy_mechanism=none` for Open
MPI. Validate the transport your MPI installation actually uses: a shared-file
test alone does not prove that MPI selected its shared-memory transport.

### PMIx and devices

`confine.pmix_roots` bounds the Slurm-supplied `PMIX_SERVER_TMPDIR`. Only the
verified per-step directory is exposed, at its original path, and no broad
authentication or socket directory is mounted as a fallback. The site's PMIx
layout must fit this contract.

`confine.devices` exposes individually approved device nodes, such as RDMA or
GPU devices, to attempts and ranks; it does not establish tested RDMA or GPU
support. Host networking and the PMIx endpoint are service authorities: the
site must verify that they do not let ranks launch unconfined processes or
reach other protected services.

### PMI-2 launches (Intel MPI)

Intel MPI under Slurm needs `srun --mpi=pmi2`. Set it on the daemon's approved
launcher, then restart the daemon so `run` picks up the changed launcher:

```bash
httk workflow launcher configure --set manager.launch_mpi=pmi2 small
```

The setting is pinned on the managers the daemon starts, and the built-in
step becomes `srun --mpi=pmi2 ...`. A `manager.launch_template` owns its argv
and must contain `--mpi=pmi2` itself.

With spawn blocking (the default), Slurm's `PMI_FD` socket stays in the rank helper; the rank gets a private
socket as its `PMI_FD`. The helper relays the PMI-1 simple protocol, which
Intel MPI speaks over `srun --mpi=pmi2`, one request at a time and only for
Slurm's PMI-1 commands. `MPI_Comm_spawn` (any `mcmd=` block or `cmd=mcmd`) is
answered with `cmd=spawn_result rc=-1` and never reaches Slurm, so the
application sees a spawn error and the rank's launch stderr gets
`httk-workflow rank: PMI refused MPI_Comm_spawn`.

The relay fails closed. A PMI-2 wire client (for example an application linked
against Slurm's `libpmi2`), a request over 1024 bytes, any other command, or
a request containing `mcmd` anywhere (even in a key, value or service name)
ends the rank's PMI channel with an `httk-workflow rank: PMI refused: ...`
line, and MPI initialization then fails. A `PMI_FD` that is not a socket
refuses the launch.

Spawn blocking is the pinned launcher setting `manager.confine.block_mpi_spawn`,
read only under `manager.confine=bwrap` and ignored without confinement:

- `on` (default): the relay above. `PMI_PORT` is also dropped from the rank
  environment, and a rank whose relay cannot start is killed and refused.
- `auto`: `on` when Slurm sets `PMI_FD`, otherwise `off`.
- `off`: the rank gets Slurm's own `PMI_FD` and `PMI_PORT`, so
  `MPI_Comm_spawn` starts processes outside the sandbox.

The setting does not cover a non-Slurm PMIx server, such as the one of an
Open MPI `mpirun` launch template. Slurm's PMIx plugin does not implement
spawn, so `--mpi=pmix` needs no filter.

For site acceptance, run the spawn probe (see [Launch
acceptance](#launch-acceptance)) and a multi-node Intel MPI job, and confirm
that the sandbox cannot reach the munge socket: keep `/run/munge` and `/run`
out of `confine.readonly_paths`. A rank that can create munge credentials
could contact `srun`'s PMI-2 port or `slurmctld` directly.

### Launch acceptance

Each launch style needs its own site acceptance; none is claimed here:

- the built-in `srun` prefix with `manager.launch_mpi=pmi2` for Intel MPI (the rank helper filters PMI-1
  spawn requests; see [PMI-2 launches](#pmi-2-launches-intel-mpi)) or on a site whose default MPI plugin is PMIx (`confine.pmix_roots`
  set to the `slurmd` spool parent of the step directories);
- Open MPI `mpirun`, for example through
  `manager.launch_template=mpirun -np {procs} --hostfile {nodefile}`;
- NSC `mpprun`;
- Intel MPI hydra.

Before production use, test this exact mount and environment configuration
for positive multi-node communication and the shared-memory transport on
nodes with several ranks; an allocation-shared `/dev/shm` object and an
unrelated host shared-memory canary that stays invisible to ranks; host
filesystem and process canaries, dynamic MPI spawn, nested scheduler commands
and exposed authentication or socket routes; and rank startup failure, client
timeout, cancellation, manager death, and shared-memory cleanup after
completion and after node failure.

The {download}`MPI site probe <../../tools/probe_daemon_mpi.c>` (also at
`tools/probe_daemon_mpi.c` in a source checkout) provides the communication
and spawn checks. Build it on the cluster with
`mpicc -O2 -Wall -Wextra -o probe probe_daemon_mpi.c -lrt`, place the
executable in the job's payload and run it from a confined attempt with
`$HTTK_WORKFLOW_LAUNCH ./probe FRESHALPHANUMERICTOKEN`.

- At least two ranks must share a node.
- Add `--spawn` only for the separate spawn check, with independently
  established spare capacity.
- Exit 77 means inconclusive. Spawn success is a failure.
- The probe creates only a named object in the exposed `/dev/shm` and never
  writes an escape proof into the host home directory.

Inspect MPI transport diagnostics separately. The probe is a site test, not a
replacement for the filesystem, process, scheduler and lifecycle checks above.

## Site acceptance

The repository has executable scheduler stand-in tests and a required real
Bubblewrap CI gate. Neither replaces site acceptance of SSHFS access controls,
Slurm authentication, compute-node mounts and `confine.readonly_paths`,
attempt confinement, cancellation, restart recovery or the actual simulation
executable. Local protocol tests and scheduler stand-ins do not establish real
cluster containment, and real Slurm, PMIx and multi-node acceptance remain a
deployment check. Run those checks before relying on this daemon for
production jobs.
