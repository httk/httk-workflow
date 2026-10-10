# Confined workspace daemon

A workspace on an HPC system can serve a client that only has file access. The
workspace carries the **exchange** extension, the one directory
`WORKSPACE/exchange/` that the client mounts (never the workspace). Through it
the client sends jobs, collects finished jobs and reads passive status. The
exchange is served by the workspace's own managers, not by the daemon.

`httk workspace daemon` is the small, opt-in, foreground Slurm broker next to
it. It starts, queries and cancels managers on signed requests: from
operator-approved launchers, by opaque manager handle, with no commands, shell
fragments, environment variables, paths or Slurm arguments from the client.
Requests need an authorized *httk* identity signature. Jobs keep running, and
the exchange keeps being served by running managers, whether or not a daemon
exists. The `mount-daemon` adapter supplies the typed client controls; see
{doc}`remotes`.

The daemon approves ordinary global `slurm` launchers that set
`manager.confine=bwrap`. Each signed start submits one manager from such a
launcher. The manager is trusted and runs unconfined in its batch job, and it
starts every job attempt inside its own Bubblewrap sandbox, which can write only
that job's directory. Parallel programs start through the same launch prefix as
anywhere else; see [Parallel launches](#parallel-launches). The existing mount
adapter and an unconfined Slurm launcher lack these guarantees.

## Trust and execution model

### Trust model

- The remote host, its Slurm, the installed Python and *httk*, the operator's
  global launchers and the workspace settings are trusted operator
  configuration.
- The client controls the exchange and the content of the jobs it sends,
  including everything the trusted side later moves (status documents, ejected
  bundles). Uploaded job content may be arbitrary.
- The managers are trusted. They adopt exchange bundles by first copying them,
  descriptor-anchored, into their own scratch, then verify that copy, filter
  untrusted content, and execute no bundle code outside an attempt sandbox.
- Each job attempt, and each rank of a parallel launch, runs confined: it can
  write only its own job directory and cannot use scheduler authority. Jobs
  cannot change `manager.confine` or any `confine.*` setting: neither job
  parameters nor declared environment values reach those keys.
- `status.json`, `managers.json` and the manager logs are informational: the
  client parses them strictly and never acts on them.

### Deployment boundary

| Component | Runs | Can write |
| --- | --- | --- |
| broker (`httk workspace daemon`) | in its own Bubblewrap sandbox on the login or service node, with the host filesystem read-only | `WORKSPACE/exchange` and its private state |
| manager | unconfined, as you, in the Slurm batch job: `httk manager run --idle` | the workspace |
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

Every unrestricted manager of the workspace serves the exchange once the
extension is enabled (`httk workspace exchange enable`, which `daemon init` also
does). On a workspace with the exchange extension enabled, every manager must
confine its attempts (`manager.confine=bwrap`); otherwise it refuses to start,
and a running one stops claiming work when the extension appears (the
extension and settings are re-read at every claim pass). This applies to
`--inline`, process-launched and manually started managers as well, so run them
with `--setting manager.confine=bwrap` or set `manager.confine=bwrap` as a
workspace setting.

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
- For parallel launches, a tmpfs at `confine.shm_root` (default `/dev/shm`)
  on the nodes: the launch client's lock lives in
  `<confine.shm_root>/httk-launch-<attempt_id>/`, and that root must be a
  tmpfs, which is checked. The workspace filesystem needs no lock support.

The daemon's ledger needs no local filesystem and no locking: any filesystem
with POSIX rename semantics works (no hard links). Startup refuses missing
requirements; there is no unsandboxed fallback. The Python installation, its
packages, the global launchers and the initial operator environment must be
trusted: use a protected installation, not editable packages in uploaded
content.

### Layout

The exchange is a directory of the workspace, `WORKSPACE/exchange/`. There is
nothing else to provision around it:

```text
WORKSPACE/
  exchange/              the only client-mounted directory
    exchange.json        workspace identity, written when the extension is enabled
    daemon.json          the daemon's public menu, written by the daemon
    status.json          exchange jobs, states and progress, written by the managers
    managers.json        manager rows, written by the daemon
    managers/<handle>.log   published manager logs, written by the daemon
    inbox/               the client drops job bundles here
    outbox/              returned trees in outbox/<client job UUID>/;
                         outbox/rejected/<unique>/{<name>, reason.json}
    requests/ responses/ signed daemon actions and job actions
```

- The broker's sandbox binds only `WORKSPACE/exchange` (and its private state)
  read-write, each opened from a descriptor without following symlinks; the
  rest of the workspace is read-only to it.
- State and snapshot directories must lie outside the workspace, so that the
  client cannot reach the ledger or the configuration; the *httk* data
  directory and the runtime paths must be disjoint from the workspace too.
- Entries whose names start with `.` or that are not plain file names
  (`[A-Za-z0-9][A-Za-z0-9._-]{0,127}`) are ignored by the trusted side.

### Transport account

The exchange writer must be the workspace owner's own account. A separate
transport UID is unsupported: a renamed bundle keeps its owner, which fails the
manager's ownership checks. An SSHFS mount of the exchange alone does not
confine that account to the directory, and restricting the owner account's
SFTP or SSH access is a site matter that local file-mode checks cannot verify.
Mount without `follow_symlinks`, and put the client mount point outside any
local workspace.

### Broker sandbox

The broker enters Bubblewrap before it listens for command requests. It runs
only *httk* code and the fixed Slurm clients, never client code, so it sees the
whole host filesystem read-only: Slurm clients and site wrappers, `slurm.conf`,
munge, the user database (`/etc/passwd`, SSSD) and DNS work without
configuration. It can write only `WORKSPACE/exchange` and its private state,
and has the host network that Slurm needs. It publishes `daemon.json`,
`managers.json` and manager logs there and never reads job content.
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
httk launcher add --template slurm --global small \
  --set manager.confine=bwrap --set slurm.cpus_per_task=2 --set slurm.mem=4G \
  --set slurm.time_limit=01:00:00 --set manager.workers=2
httk launcher configure --add-path confine.readonly_paths=/software small
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
- lies inside the workspace, state or snapshot directories, or is
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
workspace, state and snapshot directories: `configure` refuses
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
  --add launchers=small \
  --add authorized_keys=ed25519:REPLACE_WITH_CLIENT_PUBLIC_KEY
httk workspace daemon check /proj/campaign/workspace
httk workspace daemon run /proj/campaign/workspace
```

Repeat `--add` once per item. `init` and `configure` print the enrollment and
the configuration, as `httk workspace daemon show WORKSPACE` does (`--json`
for one JSON document). Relative paths are taken from the current directory.

- `init` enables the exchange extension of the workspace when it is not yet
  enabled (`httk workspace exchange enable` does the same alone; the exchange is
  always `WORKSPACE/exchange`, there is no `--exchange` option). It creates the
  state and snapshot directories, the ledger, the private response key and a
  fresh enrollment, saves the configuration, publishes the runtime snapshot and
  writes `exchange/daemon.json`. It then runs the same check as `check`; if
  that fails, the enrollment is kept and printed guidance says to fix the
  launchers or the configuration and run `check` again. State left by an
  earlier enrollment, including one made with the earlier SQLite ledger, is
  refused: remove it or give a different `--state`, and initialize again.
- `check` and `run` first activate the configuration (see
  [Configuration changes](#configuration-changes)).
- `check` enters the real broker sandbox and checks the scheduler clients,
  without submitting work or validating compute-node execution.
- `run --once` processes one bounded scan. `run` polls until SIGINT or
  SIGTERM; a site service supervisor can restart the foreground daemon.
- `--state DIR` (default: `workspace-daemons/<workspace-path-hash>` below the
  *httk* data directory) must be repeated on later invocations when not the
  default; `--snapshots DIR` (default: `<state>.snapshots`) is remembered by
  the enrollment and only checked when given again. Slurm writes each manager's
  output to `<snapshots>/jobs/`, so that directory must be reachable at the
  same path from the batch nodes.

State, snapshots and trusted code must stay outside the workspace and outside
writable exports, be owned by you or root, and not be world-writable, nor may
the directories leading to them. Group write permission is accepted, so anyone
in a file's group is trusted like you; on a host with shared project groups,
remove group write with `chmod g-w`.

The ledger is a directory of small records under `<state>/ledger/`, written
with POSIX renames, no hard links and no file locks, so it works on any
filesystem with those semantics. Any number of daemon instances may run against one
enrollment, for example a supervised standby; they share the ledger. An
interrupted initialization keeps its partial artifacts and refuses automatic
replacement. Keep the ledger and keys when diagnosing failures, and do not
remove them to clear an uncertain submission.

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

The manager is `httk manager run --by-path --workspace WORKSPACE
--idle` with the launcher's `--workers` (from `manager.workers`),
`--allocation` (from `manager.allocation`, default `slurm`) and its pinned
`--setting` values. Like every unrestricted confined manager it serves the
exchange. It probes its Slurm allocation like any manager the
`slurm` launcher starts, places attempts on the allocation's nodes and serves
until Slurm's time limit drains it; see {doc}`taskmanager`.

### Configuration changes

Edit a launcher bundle with `httk launcher configure`, or the daemon
configuration with `httk workspace daemon configure`, then restart the daemon:

```console
httk workspace daemon configure /proj/campaign/workspace --add launchers=large \
  --remove authorized_keys=ed25519:REPLACE_WITH_REVOKED_KEY
httk workspace daemon run /proj/campaign/workspace
```

`configure` approves the result exactly as a start would and saves it only
when it is accepted; it publishes nothing and works while a daemon runs. Every
`check` and `run` compiles the saved configuration with the current launcher
bundles. When the result differs from the active runtime snapshot, it publishes
a new snapshot, rewrites `daemon.json` and prints the launchers whose approval
changed; otherwise it publishes nothing. Activation is not refused while a
daemon runs: running instances notice the new configuration at their next
admission or decision and exit, so a supervisor restarts them on it. Clients
read configuration digests live from `daemon.json` and need no reconfiguration.

Queued and running managers keep the settings they were submitted with. New
starts must name a currently approved launcher and its exact digest; a
retained old snapshot does not authorize them. Activation preserves the
enrollment, ledger, response key and old snapshots. Installed binaries and
site configuration file contents remain operator-maintained external
dependencies.

### Upgrading

After updating *httk-workflow* on the remote host, restart the daemon. An
enrollment made by an earlier development version, with a different snapshot
format, protocol or SQLite ledger, is refused; see
[Earlier enrollments](#earlier-enrollments). Update the client before or
together with the remote host: the current client reads `exchange.json`,
`daemon.json` and version 3 of `managers.json`.

## Job flow

Send a job from the client with the ordinary eject verb:

```console
httk job eject JOB /mnt/cluster/exchange/inbox
```

The export is made by a local atomic ejection and then copied into the
mount (`--resume` continues an interrupted copy). Every unrestricted confined
manager of the workspace adopts bundles from `inbox` itself: it takes the
entry, copies it anchored at directory descriptors into its own scratch, and
verifies, filters and publishes only that copy. It refuses special files,
symlinks pointing outside the bundle and hard-linked files. A client that keeps
descriptors open on its entry can change its bytes only until the copy
completes; such a change affects only its own job. A refused bundle appears in
`outbox/rejected/<unique>/`, as the bundle `<name>` and a `reason.json`, which
holds the reason. Eject errors go to the manager log. With no manager running, bundles simply
wait in `inbox`; the client can take one back before any manager does:

```console
httk remote daemon take-back REMOTE NAME [DESTINATION]
```

`take-back` renames the entry to a dot name (managers ignore those), copies it
to `DESTINATION` (default `./NAME`) and removes it. If the name is gone, a
manager took it: cancel the job instead.

Adoption gives the jobs fresh UUIDs and keeps the client's job UUID as the
job's exchange name. Once the root and every job of its tree are finished
(`succeeded`, `failed` or `cancelled`), a manager returns the whole tree to
`outbox/<client job UUID>/<job key>`; while an earlier copy there is not
fetched, the return waits. Fetch it, removing it from the outbox:

```console
httk job adopt --move /mnt/cluster/exchange/outbox/CLIENT_JOB_UUID/JOB_KEY
```

Managers also handle the job actions `stop_job`, `cancel_job` and `eject_job`
from `requests/`, signed by a key of the workspace setting
`exchange.authorized_keys` (comma- or space-separated) and naming the job by
its client UUID; each is applied once and answered with an unsigned
`responses/<id>.json` (`accepted`, or `refused` with a reason; a request
replayed after its job was returned is refused `request_replayed`). The daemon
leaves job actions to the managers. Job actions are protocol-level for now:
there is no client command that publishes them yet.

`job adopt --move` copies the tree across filesystems, verifies it and then
removes the source. A failed job
is never retried automatically. To resume one, adopt it, fix it, and eject it
to the `inbox` again.

`status.json` at the exchange root lists the exchange jobs
(`jobs[{exchange_name, job_id, job_key, state, progress}]`, `updated_at`,
`truncated`), where `progress` is a bounded excerpt of the scalar members of a
`progress.json` the job keeps in its payload; the managers write it. `managers.json` (format version 3) lists the
daemon's manager starts with their ledger state, Slurm job ID, scheduler state,
exit code and start and end times. Unknown values are `null`. Both files are
informational: `status.json` is rewritten at most every 10 seconds while a
manager runs, and `managers.json` when its rows change. Read them with
`httk remote daemon status REMOTE`; add `--handle` for the scheduler
state of one manager through a signed request.

The daemon checks each submitted manager about once a minute: with `squeue`
while Slurm lists it, then with `sacct` for its final state and exit code. A
manager is final in `COMPLETED`, `FAILED`, `CANCELLED`, `TIMEOUT`,
`OUT_OF_MEMORY`, `NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL` or `DEADLINE`. With
`sacct` configured, a final state seen only by `squeue` waits up to 30 minutes
for accounting to add the exit code, and is then kept without one. A manager
that neither client knows for 30 minutes, which is always the case without
`sacct` or while accounting fails, becomes `GONE`. Once a manager is final, the
last 1 MiB of its Slurm output is copied to `managers/<handle>.log` in the
exchange, and its row's `log` names that file. This also covers a manager that
failed on its node before it ever adopted a job.

## File command protocol

### Request signatures

The internal version-4 format requires identity signatures. The maintained
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

Enrollments that use an earlier protocol, snapshot format or ledger layout, or
an SQLite ledger, are refused. Preserve their state and reconcile outstanding
work with the earlier software before initializing a new enrollment. There is
no automatic migration or ledger reset.

### Publishing requests

Publish complete UTF-8 JSON by writing a temporary file and atomically
renaming it to `<request_id>.json` in `exchange/requests/`. Use a fresh random
32-character lowercase hexadecimal request ID for each new operation.
Documents are limited to 16 KiB. Unknown keys, duplicate keys and extra
operation fields are rejected. The operations are `health`, and:

| Operation | Additional fields |
| --- | --- |
| `start_manager` | `configuration` and `configuration_digest`: the approved launcher name and its SHA256 digest |
| `manager_status` | `handle`: the returned manager handle |
| `cancel_manager` | `handle`: the returned manager handle |

There is no operation that moves bundles: the daemon never touches `inbox` or
`outbox`.

### Responses

Responses appear under the same filename in `exchange/responses/`, with the
request identities, the canonical request digest and the outcome. Scheduler
IDs and raw scheduler output are not exposed. Verify the identities and digest
when consuming a response: the uploader can interfere with mailbox contents,
and the protected ledger is authoritative.

The response to an admitted request is committed to the ledger before
publication, and requests are removed only after their response is published.
The refusals decided before admission (`wrong_workspace`, `wrong_enrollment`,
`request_conflict`) and `busy` are published without a ledger record, so a
retry is decided afresh. Malformed or unauthorized requests are
discarded with a bounded diagnostic and no response; one that cannot be
removed is renamed to a `.invalid-…` dot name that later polls skip. The
maintained client removes a response once it has verified it, and the daemon
removes any response older than the request lifetime plus the clock-skew
allowance. The filesystem rules of the mailbox are specified in
[the filesystem protocol](workflow_filesystem_api.md#daemon-mailbox).

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

The foreground daemon logs to stdout: one line per request (operation, request
ID, outcome), each submitted Slurm job ID, every manager state change and
published manager log, every failed Slurm client call with its exit code and
error output, and startup and check results.

While a submitted job runs, Slurm writes its output, with stderr merged in, to
`<snapshots>/jobs/httk-<jobid>.out`, outside the workspace; the
`daemon_submitted` log line names this path. When the manager is final, the
last 1 MiB is published to `managers/<handle>.log` in the exchange (see
[Job flow](#job-flow)). The original stays in the `jobs` directory for the
operator to inspect and clean up. The manager's own log is
`logs/managers/<manager-id>.log` in the workspace, as for every manager.

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
and arguments. The request, status and trusted launch files are specified in
[the filesystem protocol](workflow_filesystem_api.md#confined-launches). The code run helpers prepend the prefix themselves, so code
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
  by its manager until every launch it made has been reaped. A manager that
  takes over the commit, or begins the commit of its published outcome,
  after that manager died first needs evidence that every launch recorded
  for the attempt has ended: its process group is gone on the successor's
  host (a live one there is stopped), Slurm or the site allocation probe
  confirms the allocation ended, or, when neither can tell, its allocation's
  recorded end lies more than 300 seconds (and, for a scheduler that is
  installed but cannot answer, an hour more) back (see
  [launch end evidence](workflow_filesystem_api.md#launch-end-evidence)).
  Otherwise the takeover waits, `httk job why` names the launch, and
  `httk job confirm-launches-ended` is the operator's override. A launch
  whose processes outlive `SIGKILL` is reported as uncertain, and that
  attempt can start no further launches.
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
httk launcher configure --set manager.launch_mpi=pmi2 small
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
