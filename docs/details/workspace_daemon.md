# Confined workspace daemon

A workspace on an HPC system can serve a client that only has file access. The
workspace carries the **exchange** extension, the one directory
`WORKSPACE/exchange/` that the client mounts (never the workspace). Through it
the client sends jobs, collects finished jobs and reads passive status. The
workspace's managers adopt inbox bundles and return finished trees; the daemon
authenticates and dispatches job commands.

`httk workspace daemon` is the small, opt-in, foreground Slurm broker next to
it. It controls jobs and starts, queries and cancels managers on signed requests: from
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
  bundles). Uploaded job content may be arbitrary. Every exchange client uses
  the same transport account, so a client can still replace or delete an inbox
  bundle, including its `owner_key`, before adoption; recorded ownership
  protects it after adoption.
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
| broker (`httk workspace daemon`) | in its own Bubblewrap sandbox on the login or service node, with the host filesystem read-only | `jobs/`, `exchange/`, `.httk-workspace/{owners,requests,tmp,quarantine,exchange-jobs}` and `.httk-workspace/daemon/broker/` |
| manager | unconfined, as you, in the Slurm batch job: `httk workflow manager run --idle` | the workspace |
| job attempt | in a Bubblewrap sandbox the manager builds, on the manager's node | its own job directory |
| rank of a confined launch | in the launch's Bubblewrap keeper sandbox on its node, which the trusted rank helpers build | its own job directory, the launch's shared-memory directory |

The manager is trusted and runs unconfined, so it may read
`WORKSPACE/.httk-workspace/daemon/`. It runs only trusted code: the
installation, the frozen launcher settings and the workspace settings. What
it does not do on the trusted side:

- it never runs a job's postprocess scripts; `httk workflow postprocess` and
  `httk collect` are client-side verbs that you run on adopted jobs, unconfined
  wherever you run them;
- it runs only workflows an operator installed in the workspace
  (`httk workflow install`); workflows never arrive in bundles, so a client's
  job whose workflow is not installed waits. Installing and building a
  workflow (`httk workflow install`, `httk workflow build`) is an operator
  action and runs unconfined, so review what you install.

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
  `--ro-bind-data`, `--new-session` and `--info-fd`, on the broker
  host and on every compute node. With Bubblewrap 0.8.0 or later the sandboxes also
  block nested user namespaces (`--disable-userns`); where that option is
  absent or refused, the broker's startup and the manager log a warning and
  run without that block. It reduces kernel attack surface; confinement does
  not depend on it.
- Permitted unprivileged user namespaces, and a PID namespace with its own
  `/proc`, on the compute nodes. A manager probes this once when it starts and
  refuses to start without it.
- Slurm 23.11.6 or later.
- For parallel launches, a tmpfs at `confine.shm_root` (default `/dev/shm`)
  on the nodes, which is checked: the ranks' per-launch shared-memory
  directories live there. Nothing takes a lock, so the workspace filesystem
  needs no lock support.

The daemon's ledger needs no local filesystem and no locking: any filesystem
with POSIX rename semantics works (no hard links). Startup refuses missing
requirements; there is no unsandboxed fallback. The Python installation, its
packages, the global launchers and the initial operator environment must be
trusted: use a protected installation, not editable packages in uploaded
content.

### Layout

The exchange is a directory of the workspace, `WORKSPACE/exchange/`:

```text
WORKSPACE/
  exchange/              the only client-mounted directory; not visible to confined jobs
    exchange.json        workspace identity, written when the extension is enabled
    daemon.json          the daemon's public menu: identities and the approved
                         configurations with their published resource settings
    status.json          exchange jobs, states and progress, written by the managers
    managers.json        manager rows, written by the daemon
    launchers/<handle>.log  published launcher logs, written by the daemon
    inbox/               the client drops job bundles here
    outbox/              returned trees, one outbox/<tag>--<client job UUID>/ each;
                         outbox/rejected/<unique>/{<name>, reason.json}
    requests/ responses/ signed daemon actions and job actions
  .httk-workspace/daemon/  mode 0700; not visible to confined jobs
    configuration.json  operator configuration; broker read-only
    response.seed       private Ed25519 response-signing seed, mode 0600;
                         broker read-only
    active.json         active runtime-snapshot pointer;
                         written by init/activate
    snapshots/<digest>.json  immutable runtime policy snapshots
    broker/ledger/       the ledger; broker/ is writable for the broker
```

- The daemon runs in one Bubblewrap sandbox. The workspace is read-only at
  its real path, including beneath `/tmp`. Descriptor-anchored writable overlays
  expose only `jobs/`, `exchange/`, `.httk-workspace/{owners,requests,tmp,quarantine,exchange-jobs}`
  and `.httk-workspace/daemon/broker/`. Workflows, settings, configuration,
  response seed, active pointer and snapshots stay read-only. Existing operator
  configuration and enclosing project metadata are available read-only for seals.
- The daemon reserves `/tmp/control`, `/tmp/daemon`, `/tmp/daemon-exchange`,
  `/tmp/daemon-policy.json` and `/tmp/home` inside its private temporary
  filesystem. Startup refuses a workspace or `HTTK_CONFIG_HOME` that is equal
  to, inside or an ancestor of one of these paths, so mount aliases cannot hide
  workspace data or operator configuration.
- Confined jobs do not see `exchange/` or `.httk-workspace/daemon/`; the sandbox
  exposes only `jobs/`, `workflows/` and a content copy of
  `.httk-workspace/format.json` from the workspace. The rest of
  `.httk-workspace/` is not visible.
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
configuration. Its writable paths are the overlays listed above, and it has
host networking for Slurm. Authentication, scheduler handling and workspace
operations share this service process; there is no execution helper or nested
sandbox. One `kind="daemon"` workspace owner lives for the service lifetime.
The owner is Bubblewrap's init, the sandbox's PID 1, under which the service
runs. `--info-fd` supplies the init's host PID, its start time comes from the
namespace-local `/proc/1/stat`, and the bootstrap supplies hostname and boot ID.
A probe on the daemon's host can then prove the owner alive or dead. When
namespace PID 1 exits, the kernel kills and reaps every other process in the
namespace first, so a proven-dead owner means no sandbox process, including one
blocked in a filesystem call, is still running. Once started, the init dies with
the Bubblewrap monitor (`--die-with-parent`), so stopping the monitor normally
ends the sandbox, but only the init's death proves that it has ended: a process
blocked in a filesystem call can outlive the monitor until the call returns, and
a monitor that dies while Bubblewrap is still setting up can leave the init
running. The daemon never probes
peer owners through its private process table.
Instead, before it enters the sandbox (not for `check`), the bootstrap recovers
on the host the workspace's daemon owners recorded on this host that are
proven dead, returning the jobs they held to the states they left. A daemon
owner on another host, or one whose death cannot be proven, stays until a
manager on its host recovers it; `httk workspace attest-dead` records its death
so that the next daemon start on its host or any manager recovers it. A failed
recovery is reported and does not stop the daemon.
The broker and its Slurm clients inherit the environment the daemon was
started in, so site wrappers find their module and site variables; only
`SBATCH_*`, `SALLOC_*`, `SRUN_*` and `SLURM_*` (except `SLURM_CONF`),
`PYTHON*`, `BASH_ENV`, `ENV`, `LD_PRELOAD` and `LD_LIBRARY_PATH` are dropped.
`HOME` and `XDG_CACHE_HOME` point at a writable private `/tmp/home`, and the
clients whose output the broker parses run in the C locale. The filtered
environment is Bubblewrap's own process environment and never part of its
command line, so it is not visible through `/proc/<pid>/cmdline`.

### Confined attempts

A manager with `manager.confine=bwrap` runs each attempt in a Bubblewrap
sandbox with:

- `jobs/` and `workflows/` **read-only** at their real paths, plus
  `.httk-workspace/format.json` as a read-only content snapshot, so parent
  outputs, `stage_input`, `Attempt.parent` and installed workflow reads keep
  working; the rest of `.httk-workspace/` and `exchange/` are absent;
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

After all mounts the sandbox root is remounted read-only. The attempt cannot
write the workspace root, create files in the read-only `jobs/` or `workflows/`
parents, or write another job. The workflow prelude runs inside the sandbox; the launcher's
`environment.prelude` runs before the manager, and its environment reaches the
attempt through the filter above. Anything a prelude or a code needs at run
time, such as a module tree, a conda or virtual-environment prefix or code
binaries, must therefore be listed in
`confine.readonly_paths`. Entries may not contain or lie inside the workspace;
list a narrower directory such as `/software` rather than `/home`. The default
covers `/usr` (with the `/bin`, `/lib`,
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
be operator symlinks. A job reached through a placement symlink pointing
outside the workspace cannot be confined, and its attempt fails. A job reached
through a placement symlink pointing elsewhere inside the workspace runs with
its job directory bound at its real path, but does not see the other jobs of
that placement. See
{doc}`taskmanager` for the general confinement settings.


## Setup and startup

### Approving launchers

Approved manager configurations are global `slurm` launchers that set
`manager.confine=bwrap`. Create several to offer different resources or worker
counts (see {doc}`launchers`):

```console
httk launcher add --template slurm --global small \
  --set manager.confine=bwrap --set slurm.cpus_per_task=2 --set slurm.mem=4G \
  --set slurm.time_limit=01:00:00 --set slurm.time_limit_max=12:00:00 \
  --set manager.workers=2
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
- lies inside the workspace or `WORKSPACE/.httk-workspace/daemon/`, or is
  world-writable.

Every start of the daemon (`check` and `run`) reads the listed bundles again
and freezes their settings, with the digest of their `launcher.json`, into the
runtime snapshot it serves. The broker submits through the installed *httk*
and never runs a bundle's `launcher` executable. Each frozen launcher setting `manager.confine`,
`manager.launch_template`, `manager.launch_mpi`, `manager.bind_cpus`, `manager.confine.block_mpi_spawn` and `confine.*` is pinned on the
managers it starts and cannot be changed by workspace settings; every other
workspace setting still applies live inside the manager, as for any manager.

### Daemon configuration

The daemon configuration is saved at
`WORKSPACE/.httk-workspace/daemon/configuration.json`. `init` and `configure`
take `--set KEY=VALUE` for any key; `--add KEY=VALUE` (and, in `configure`,
`--remove KEY=VALUE`)
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
| `max_submissions` | quota of manager starts kept in the ledger history (see [Quotas](#quotas)) | 128 |
| `force` | `true` approves CPU, memory and time above the sanity limits | `false` |
| `cluster` (`init` only) | the fixed Slurm cluster name | `SLURM_CLUSTER_NAME`, the `ClusterName` of the Slurm configuration, or a bounded `scontrol show config` call |
| `scontrol` (`init` only) | used only to discover the cluster name | on `PATH` |

Executables are resolved when set, and relative paths are taken from the
current directory. An empty value (`--set sacct=`) unsets `sacct` or
`slurm_conf`. The cluster belongs to the fixed enrollment, together with the
workspace and its private daemon directory: `configure` refuses
`cluster` and `scontrol`, and a new `slurm_conf` keeps the enrolled cluster.
The initial operator environment and `PATH` are trusted. Other broker tunables
(record limit, polling, command timeout, request lifetime of 3600 seconds) are
fixed.

### Client identities

On each client, initialize or select an *httk* identity and give its public
key to the destination operator; the private key stays on the client:

```console
httk init --name "Your Name" --email you@example.org
httk identity key
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
  private daemon directory, the ledger, the private response key and a
  fresh enrollment, saves the configuration, publishes the runtime snapshot and
  writes `exchange/daemon.json`. It then runs the same check as `check`; if
  that fails, the enrollment is kept and printed guidance says to fix the
  launchers or the configuration and run `check` again. It refuses when
  `WORKSPACE/.httk-workspace/daemon/` already exists: remove that directory
  and initialize again. An enrollment from the earlier layout is not migrated.
  Reconcile outstanding work first, then run `init`; clients must re-pin the
  new daemon key from the new `daemon.json` handoff. Remove the old state and
  snapshot directories by hand (`workspace-daemons/<hash>` and
  `<hash>.snapshots` below the *httk* data directory by default). A snapshot
  from an earlier policy format is refused at start; there is no migration.
- `check`, `run` and `reload` first activate the configuration (see
  [Configuration changes](#configuration-changes)).
- `check` enters the real broker sandbox and checks the scheduler clients,
  without submitting work or validating compute-node execution.
- `run --once` processes one bounded scan. `run` polls until SIGINT or
  SIGTERM; a site service supervisor can restart the foreground daemon. `run`
  starts the sandboxed daemon as a child process and forwards SIGTERM, SIGINT
  and SIGHUP to it; it exits with the daemon's status (128 plus the signal
  number after a signal) and restarts it only after a configuration change.
- The layout is fixed by the workspace path; nothing is written under the
  *httk* data directory (`~/.local/share/httk/...`). Slurm
  writes manager output to `WORKSPACE/logs/launchers/<handle>.out`, reachable
  from batch nodes through the workspace's shared filesystem. Do not remove
  `logs/launchers/` while the daemon runs: Slurm cannot start a manager whose
  output directory is missing, and the daemon recreates the directory only when
  it starts (`httk workspace daemon run` or `check`). An existing
  workspace may still hold `exchange/managers/`, `logs/batch/` and
  `.httk-workspace/daemon/jobs/` from before; remove them by hand.

Trusted code (the *httk* installation, launcher bundles, Python interpreter
and Slurm clients) must stay outside the workspace, be owned by you or root,
and not be world-writable, nor may directories leading to them. Group write
permission is accepted, so anyone in a file's group is trusted like you; on a
host with shared project groups, remove group write with `chmod g-w`. The
exchange must be the workspace's own `exchange/`.

The ledger is a directory of small records under
`WORKSPACE/.httk-workspace/daemon/broker/ledger/`, written
with POSIX renames, no hard links and no file locks, so it works on any
filesystem with those semantics. Any number of daemon instances may run against one
enrollment, for example a supervised standby; they share the ledger. An
interrupted initialization keeps its partial artifacts and refuses automatic
replacement. Keep the ledger and keys when diagnosing failures, and do not
remove them to clear an uncertain submission. The daemon itself retires
settled history from the ledger after the workspace's
`retention.daemon_history_days` (see [Quotas](#quotas)).

### Resources

Optionally set CPU count, memory and time in a launcher as
`slurm.cpus_per_task`, `slurm.mem` and `slurm.time_limit`, the longest time a
start may request as `slurm.time_limit_max` (default `slurm.time_limit`), and
the geometry as
`slurm.nodes`, `slurm.ntasks` and `slurm.ntasks_per_node`, exactly as for any
`slurm` launcher. A setting left unset adds no corresponding `sbatch` flag, so
Slurm's partition and site defaults apply:

- Memory accepts positive integer MiB or K/M/G/T suffixes; KiB rounds up to
  MiB.
- Time (`slurm.time_limit` and `slurm.time_limit_max`) accepts the standard
  [Slurm time forms](https://slurm.schedmd.com/sbatch.html); seconds round up
  to minutes. `slurm.time_limit` must not exceed `slurm.time_limit_max`.
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

A start may ask for another `--time` with
`httk remote daemon start REMOTE --configuration NAME --time-limit DURATION`,
which the client requires to exceed the manager's 120 s deadline margin. The
daemon refuses it with `time_limit_exceeded` when it exceeds the frozen
launcher's `slurm.time_limit_max` (by default its `slurm.time_limit`), or when
the launcher sets neither. The submitted and uncertain responses carry the
effective `time_limit` as `[D-]HH:MM:SS` (omitted when no limit applies), and
`managers.json` and the `daemon_submitted` log line show it. A start that
recovery answers `uncertain` after its daemon crashed carries only a requested
`time_limit`; without a request it stays null.

The manager is `httk workflow manager run --by-path --workspace WORKSPACE
--idle` with the launcher's `--workers` (from `manager.workers`),
`--allocation` (from `manager.allocation`, default `slurm`) and its pinned
`--setting` values. Like every unrestricted confined manager it serves the
exchange. It probes its Slurm allocation like any manager the
`slurm` launcher starts, places attempts on the allocation's nodes and serves
until Slurm's time limit drains it; see {doc}`taskmanager`.

### Configuration changes

Edit a launcher bundle with `httk launcher configure`, or the daemon
configuration with `httk workspace daemon configure`, then reload it:

```console
httk workspace daemon configure /proj/campaign/workspace --add launchers=large \
  --remove authorized_keys=ed25519:REPLACE_WITH_REVOKED_KEY
httk workspace daemon reload /proj/campaign/workspace
```

`configure` approves the result exactly as a start would and saves it only
when it is accepted; it publishes nothing and works while a daemon runs. Every
`reload`, `check` and `run` compiles the saved configuration with the current
launcher bundles. When the result differs from the active runtime snapshot, it
publishes a new snapshot, rewrites `daemon.json` and prints the launchers whose
approval changed; otherwise it publishes nothing (`reload` prints that the
configuration is unchanged). A refused configuration activates nothing, and
`reload` exits with status 2 and the reason, as `run` does. `reload` works
whether or not a daemon runs. A running daemon, idle or not, notices the new
snapshot at its next mailbox poll or request, once any scheduler check in
progress has finished, logs `daemon_retired`, and stops; a daemon still
starting stops the same way. `httk workspace daemon run` then prints `daemon
restarting on the changed configuration` and starts it again on the snapshot
active at that moment, without compiling the configuration again, while
`run --once` just stops with status 0. Clients read configuration digests live
from `daemon.json` and need no reconfiguration.

The daemon never cancels managers when it stops or restarts: queued and
running managers keep running, and the restarted daemon follows them by their
Slurm job IDs from the ledger. They keep the sbatch settings they were
submitted with. New starts must name a currently approved launcher and its
exact digest; a retained old snapshot does not authorize them. Activation preserves the
enrollment, ledger, response key and old snapshots. Installed binaries and
site configuration file contents remain operator-maintained external
dependencies.

### Upgrading

After updating *httk-workflow* on the remote host, restart the daemon. An
enrollment from the earlier layout is not migrated; reconcile outstanding
work first, then re-enroll as described in
[Earlier enrollments](#earlier-enrollments). A snapshot from an earlier policy
format is refused at start; there is no migration. Update the client before or
together with the remote host: the current client reads `exchange.json`,
`daemon.json` and version 4 of `managers.json`. A bundle carrying `owner_key`
is refused by an older *httk-workflow* because its manifest members differ;
the remote host must run this version before a client sends bundles that carry
`owner_key` (both are then updated together). Exchange jobs adopted before
this change have no recorded key and cannot be controlled through the daemon.

## Job flow

Send a job from the client with the ordinary eject verb:

```console
httk job eject JOB /mnt/cluster/exchange/inbox
```

The bundle is copied into the mount under a hidden partial name and renamed
into `inbox/` when complete; a failed delivery returns the job to the state it
left. `job eject` records the submitting identity's public key as the
optional, unsigned `owner_key` in `bundle.json`; untrusted inbox adoption uses
it to establish ownership. Every unrestricted confined manager of the workspace adopts bundles from
`inbox` itself: it takes the entry, copies it anchored at directory descriptors
into its own scratch, and validates, rekeys and publishes only that copy. An
inbox bundle is untrusted input: it must hold only fresh jobs (never run, no
`state.json`, logs, attempts or seal) in state `ready`, with no symlink, special
file or hard-linked file, and a root without a parent. The job's workflow must
be installed in the workspace (an operator task); until then the adopted job
waits, and `status.json` shows it `ready`. A client that keeps
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
`outbox/CLIENT_JOB_KEY`, where `CLIENT_JOB_KEY` is the client's own job key,
`<tag>--<client job UUID>`, the name the bundle had in `inbox/`; while an
earlier copy there is not fetched, the return waits. Fetch it, removing the
whole entry from the outbox:

```console
httk job adopt --move /mnt/cluster/exchange/outbox/CLIENT_JOB_KEY
```

The daemon executes a `job_request` only for an indexed exchange root adopted
from the inbox whose recorded `owner_key` matches the command-signing key,
which must also remain in `authorized_keys`. Local jobs, keyless submissions,
children, transferred or adopted jobs, and roots with an index entry recreated
by `fsck --repair` are refused with `not_owner`. A mounted-daemon `job request`
with `--operator` must use the identity that ejected the job. `detach` never
applies through the daemon because only roots are controllable and roots have
no parent; children move and stop with their root, daemon ejects include the
whole tree, and children are absent from `status.json`. A
`job_request_status` observation answers `unknown_command` unless signed by the
same key as the original command. Selectors are internal job UUIDs or unique
UUID prefixes from `status.json`; paths, globs and tags are refused. `--force`
is refused. Eject destinations are relative to the mounted exchange outbox,
resolved by the server, and ejects include the whole tree. `--destination` may
be omitted: the job then goes to the outbox root as `outbox/CLIENT_JOB_KEY`
(the name rule above); with `--destination D` it goes to `outbox/D/<name>`. State, parent/join,
runner-step and seal guards still apply. `set_budget` is accepted for such a
root: the typed command carries the action `set_budget` and the field `budget`
(the `--max-*` options of the CLI). Managers need no client keys; existing
`exchange.authorized_keys` settings are left stored but unused.

An idle job that the daemon safely claims changes immediately. A held job,
lost claim race or unfinished execution, commit or release stays queued for
its owner or reconciliation. Results report `applied`, `queued`, `refused` or
`uncertain`, with resolved job/request IDs and observed state when available.
A failed seal or rolled-back eject is a refusal. Missing directories alone
never establish successful deletion or ejection.

`job adopt --move` copies the tree across filesystems, verifies it and then
removes the source; without `--move` the copy in the outbox stays, and the next
return of that job waits until it is gone. A failed job
is never retried automatically. To resume one, adopt it, fix it, and eject it
to the `inbox` again.

`status.json` at the exchange root lists the exchange jobs
(`jobs[{exchange_name, job_id, job_key, state, progress}]`, `updated_at`,
`truncated`), where `progress` is a bounded excerpt of the scalar members of a
`progress.json` the job keeps in its payload; the managers write it. `managers.json` (format version 4) lists the
daemon's manager starts with their ledger state, Slurm job ID, effective
`time_limit`, scheduler state,
exit code and start and end times. Unknown values are `null`. A row stays
until `retention.daemon_history_days` (30 days by default) after its manager
ended and its launcher log was published, or after a start was refused; a start
answered `uncertain` without a Slurm job ID stays for good. Both files are
informational: `status.json` is rewritten at most every 10 seconds while a
manager runs, and `managers.json` when its rows change. Read them with
`httk remote daemon status REMOTE`; add `--handle` for the scheduler
state of one manager through a signed request.

`daemon.json` (format version 3) lists every approved configuration as
`{digest, settings}`. `settings` holds only the allow-listed launcher settings
that are set: `manager.workers`, `slurm.partition`, `slurm.nodes`,
`slurm.ntasks`, `slurm.ntasks_per_node`, `slurm.cpus_per_task`, `slurm.mem`,
`slurm.gres`, `slurm.time_limit` and `slurm.time_limit_max`. Values are the
operator's own, except that `slurm.time_limit` and `slurm.time_limit_max` are
canonical `[D-]HH:MM:SS`, with the maximum resolved to its default, the time
limit. Every other setting stays private, including `slurm.account`,
`slurm.reservation`, `slurm.export`, `environment.prelude`, `manager.command`,
`manager.launch_template`, `manager.launch_mpi`, `manager.allocation`,
`manager.bind_cpus`, `manager.confine` and every `confine.*` setting: they can
reveal site-internal shell setup, launch commands and accounting. The published
launcher logs (`launchers/<handle>.log`) carry the manager's Slurm output, so
anything a prelude prints reaches clients there. The client refuses a document
with any other key. Approval refuses a launcher whose published setting has a
control character or is longer than 256 characters, so the published menu
always stays readable by clients. `httk remote daemon show REMOTE` prints the
validated document of a pinned daemon.

The daemon checks each submitted manager about once a minute: with `squeue`
while Slurm lists it, then with `sacct` for its final state and exit code. A
manager is final in `COMPLETED`, `FAILED`, `CANCELLED`, `TIMEOUT`,
`OUT_OF_MEMORY`, `NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL` or `DEADLINE`. With
`sacct` configured, a final state seen only by `squeue` waits up to 30 minutes
for accounting to add the exit code, and is then kept without one. A manager
that neither client knows for 30 minutes, which is always the case without
`sacct` or while accounting fails, becomes `GONE`. Once a manager is final, the
last 1 MiB of its Slurm output is copied to `launchers/<handle>.log` in the
exchange, and its row's `log` names that file. This also covers a manager that
failed on its node before it ever adopted a job. `httk workspace gc` (category
`launcher_logs`) removes that exchange copy once it is older than
`retention.trash_days`; the row keeps naming it, and `httk remote daemon log`
then reports that no log is published.

## File command protocol

### Request signatures

The daemon protocol version-5 format requires identity signatures. The maintained
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

The protected ledger keeps the exact signed requests for the daemon history,
`retention.daemon_history_days` (see [Quotas](#quotas)). Within it, a
duplicate never submits or republishes an internal job request again, even
after a restart, and a currently authorized key can still retrieve recorded
results after expiry. Once a request is retired, its ID is unknown again: a
status observation of a retired job command reports `unknown_command`,
`manager_status` and `cancel_manager` on a retired handle answer `refused`
with `unknown_manager`, and a repeated request is admitted afresh, where its
own expiry refuses it. A stale
request that has not acted is durably refused, subject to ledger capacity. A
request recovered before submission must still meet the time window before it
can act.

### Response signatures

Responses are signed by the daemon's private seed at
`WORKSPACE/.httk-workspace/daemon/response.seed`, held read-only in the broker.
Clients configure its public key through a trusted operator handoff and never
learn a trust anchor from a mailbox response. Signed responses bind the exact
signed request digest and all destination and request identities. Signatures
provide integrity and authorization, not encryption or protection against
mailbox deletion. Slurm keeps using the site's own authentication, such as its
MUNGE socket. Cluster secrets and daemon private keys are never exposed to
jobs. A plain unconfined workspace copy (for example, your own `rsync` or
`tar`) includes this private directory; a new `init` creates a new key, while
exchange transfers and `job eject` move job bundles only and never include it.

### Upgrading an enrollment

Clients and daemons must upgrade together: protocol 5, ledger 4 and
`daemon.json` 3 are required. Workspace format 3, internal request format 3
and configuration schemas are unchanged. Old enrollments are refused;
there is no automatic migration, history reset or old-protocol adapter.

1. Stop every daemon instance serving the enrollment.
2. Settle all tracked Slurm submissions using their recorded scheduler IDs and
   `squeue`, `sacct` and `scancel` as appropriate; settle held workspace jobs
   through ordinary owner recovery.
3. Archive `.httk-workspace/daemon/` outside its fixed path, preserving the
   ledger and private configuration for reconciliation.
4. Upgrade the protected installation and clients, then initialize a fresh
   enrollment with the intended launchers and authorized keys using
   `httk workspace daemon init WORKSPACE`.
5. Deliver the new `daemon.json` identities through the trusted operator
   handoff and repin each client with `httk remote daemon configure REMOTE
   --exchange PATH`.

Do not reuse old request IDs or caches across enrollments.

### Publishing requests

Publish complete UTF-8 JSON by writing a temporary file and atomically
renaming it to `<request_id>.json` in `exchange/requests/`. Use a fresh random
32-character lowercase hexadecimal request ID for each new operation.
Documents are limited to 16 KiB. Unknown keys, duplicate keys and extra
operation fields are rejected. The operations are `health`, and:

| Operation | Additional fields |
| --- | --- |
| `start_manager` | `configuration` and `configuration_digest`: the approved launcher name and its SHA256 digest; optional `time_limit`, a canonical `[D-]HH:MM:SS` Slurm `--time` of at least one second |
| `manager_status` | `handle`: the returned manager handle |
| `cancel_manager` | `handle`: the returned manager handle |
| `job_request` | `selector`, `action`, `reason`, `configuration_digest`; `priority`, `step` or outbox-relative `destination` only for the corresponding action |
| `job_request_status` | `command_id`: the original daemon command ID |

Job target resolution is frozen in the protected ledger with the exact internal
request document and digest. An exchange-origin target also pins its observed
adoption nonce. A frozen target carries no authority of its own: before a new
execution decision and again before publication, the daemon rechecks that the
pinned adoption is still indexed and still records the signing key, and
otherwise refuses the command (`adoption_changed` or `not_owner`). Managers
apply the same pin before acting on a delayed `cancel`, so a cancel authorized
for one adoption never stops an attempt of a later one. One newly won
execution decision names the winning instance and daemon owner; only that
winner may publish the internal request. Retries never resolve a different
target or repost after a crash. When the winner closed, was recovered or was
declared dead with its exact request still pending, a retry by a later daemon
serves that published request once, as any manager would, under its adoption
pin. Stored definitive results replay; an exact pending request reports queued; matching request
state history can establish a later result. Without such evidence the outcome
is uncertain and grants no permission to take over or re-execute. Fresh signed
status observations may later supersede uncertainty.

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
- A repeated request ID with unchanged content replays its recorded response
  while the ledger keeps it. Changed content under the same ID is refused.
- Use a new request ID for each signed status observation; `job_request_status`
  bypasses ledger admission and consumes no command quota.

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
error output, and startup and check results. A count line summarizes the
managers at a glance; it appears at the first poll and again whenever a count
changes:

```text
2026-10-17 12:00:00,000 INFO daemon_managers starting=0 queued=2 running=3 unknown=0 completed=12 failed=1 cancelled=0
```

`starting` counts starts decided but not yet submitted; `queued` submitted
managers not yet observed or waiting for their allocation (`PENDING`,
`CONFIGURING`, `REQUEUED`, `SPECIAL_EXIT` and the other requeue and hold
states); `running` those in `RUNNING`, `COMPLETING`, `STAGE_OUT`, `SIGNALING`,
`SUSPENDED`, `STOPPED` or `RESIZING`; `completed` and `cancelled` those
states; `failed` the other final states (`FAILED`, `TIMEOUT`, `OUT_OF_MEMORY`,
`NODE_FAIL`, `PREEMPTED`, `BOOT_FAIL`, `DEADLINE`) and `GONE`; and `unknown`
uncertain submissions and managers in `UNKNOWN` or any other state. Refused
starts are not counted. Scheduler states are checked about once a minute.

While a submitted job runs, Slurm writes its output, with stderr merged in, to
`logs/launchers/<handle>.out` in the workspace shared filesystem (the
broker handle is the launch id); the `daemon_submitted` log line names this
path. When the manager is final, the last 1 MiB is published to
`launchers/<handle>.log` in the exchange (see [Job flow](#job-flow)). The
original stays in `logs/launchers/`; `httk workspace gc` removes it once no
owner record carries that launch id and it is older than `trash_days`; the
exchange copy is removed once older than `trash_days` too (category
`launcher_logs`). The manager's own log is
`logs/managers/<manager-id>.log` in the workspace, as for every manager. The
manager's row leaves `managers.json` `daemon_history_days` after the log was
published; a manager whose log could not be published yet stays listed.

### Quotas

The record limit (4096) limits the durable command records the ledger keeps;
fresh job-status observations consume none. `max_submissions` limits the
manager starts it keeps, including refused or uncertain starts. Both bound the
retained history, not active jobs or lifetime totals: at most once an hour,
the daemon retires old history, which frees its quota.

The history is the workspace retention setting
`retention.daemon_history_days`, 30 days by default, which the daemon reads
when it starts; restart it after a change:

```console
httk workspace policy set --key retention.daemon_history_days --value 14 WORKSPACE
```

A request is retired that long after
its response was recorded, and a manager start that long after its manager
reached a final scheduler state (or `GONE`) and its launcher log was
published; a refused start needs no final state. `null` or `"keep"` never
retires anything. Whatever the setting, the daemon never retires a request
before its signed lifetime plus the clock-skew allowance has passed (and keeps
at least the longest request lifetime plus twice the skew plus an hour), so a
replay that could still be accepted always finds its record and never acts
twice. Nothing that can still matter is retired: an unanswered
request, a start whose manager may still be queued or running, and a start
answered `uncertain` without a Slurm job ID, which stays for good because the
daemon cannot tell whether its job exists; reconcile it with Slurm (it keeps
holding its quota slot). A retired request leaves the ledger, its
row leaves `managers.json`, and its replay ends (see
[Ledger and replay](#ledger-and-replay)).

Exhaustion returns a nonpersisted `busy` response, retryable with the same
request ID and identical fields. When the history holds more starts than the
quota, wait for retirement, lower `daemon_history_days`, or raise
`max_submissions` with `httk workspace daemon configure`. Intentional
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
`{nodefile}` and `SLURM_HOSTFILE` in the trusted launch record
`.httk-workspace/owners/<owner-id>/launches/<attempt_id>.<request_id>/`,
which jobs can only read and which the manager writes before the launch
starts. The manager never interprets the program
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
  by its manager until every launch it made has been reaped. If the manager
  dies, its jobs are recovered only once the death proof shows that every
  launch it recorded has ended: its process group is gone on the prober's
  host, or Slurm or the site allocation probe confirms the allocation ended
  (see [launch end evidence](workflow_filesystem_api.md#launch-end-evidence)).
  No clock rule applies; an allocation nobody can ask about needs
  `httk workspace attest-dead`. A launch whose processes outlive `SIGKILL` is
  reported as uncertain, and that attempt can start no further launches.
- Commands run without the prefix run inside the attempt sandbox on the
  manager's node, as unconfined commands run on that node.

The ORCA run helper starts no prefix by default, since ORCA starts its own
MPI; under confinement only single-node ORCA is supported, and a multi-node
binding is refused. The generic `run` verb, `Attempt.run` and
`httk_workflow_run` never prepend the prefix.

### Rank sandboxes

On every rank the trusted rank helper opens the workspace and the job
directory without following symlinks, refusing the workspace root,
`.httk-workspace` and nested placements. The ranks of one launch on one node
then run in one shared sandbox, the launch's *keeper* on that node: the first
helper of the launch on the node starts a supervisor that starts it, and every
helper, that one included, runs its rank in the keeper's namespaces. The keeper is a Bubblewrap
sandbox with:

- `jobs/` and `workflows/` read-only at their real paths,
  `.httk-workspace/format.json` as a read-only content snapshot, the job
  directory writable at its real path, and the paths of
  `confine.readonly_paths`; all other workspace paths, including the rest of
  `.httk-workspace/` and `exchange/`, are absent, and the sandbox root is
  remounted read-only;
- the launch's shared-memory directory at `/dev/shm`, the Slurm step's PMIx
  directory (only when it lies below `confine.pmix_roots`), and the devices of
  `confine.devices`;
- private user, PID, IPC and UTS namespaces with its own `/proc`, the **host
  network**, no capabilities, and nested user namespaces blocked when
  Bubblewrap supports it.

Each rank keeps its own environment: its process-manager variables (`PMI_*`,
`PMIX_*`, `OMPI_*`, `OPAL_*` and the Slurm step identity), then
`confine.environment.<NAME>` values, and `HOME=/tmp/home`, `TMPDIR=/tmp`.
A rank stays a descendant of its own rank helper, so it keeps the CPU binding,
cgroup and signals the launcher gave that task. It enters the keeper's
namespaces as the keeper's unprivileged user, cannot gain privileges, has an
empty capability bounding set, and dies with its helper.

Inside the sandbox the rank reads the request, changes to the client's working
directory and adds the attempt's environment, without names starting with
`PMI_`, `PMIX_`, `OMPI_`, `OPAL_`, `SLURM_`, `SLURMD_`, `SRUN_`, `SBATCH_`,
`SALLOC_` or `HTTK_`, and without names that are not portable identifiers,
such as exported Bash functions. Module-derived variables such as `PATH` and
`LD_LIBRARY_PATH` therefore reach the ranks, but MPI tuning variables set in
the attempt do not: set them as `confine.environment.<NAME>`.

### Namespace scope

The scope of the rank namespaces is **one launch on one node**:

- Ranks of the same launch on the same node share their PID, IPC, UTS and
  mount namespaces. They see distinct process IDs, so runtimes that register
  themselves under their process ID in `/dev/shm` (Intel and LLVM OpenMP) do
  not collide; they can create and attach each other's System V shared memory
  (as VASP's `getshmem.c` does); and single-copy transports that address peer
  processes, such as CMA, can work where the kernel's ptrace policy allows
  them. Ranks of one launch on one node form one trust domain: besides sharing
  the writable job directory and `/dev/shm`, any rank can signal, kill or
  trace the others. Isolation holds between launches and jobs, not between
  ranks of one launch on one node.
- Separate launches, also of the same job, and other jobs never share them:
  their ranks cannot see each other's processes, System V objects or
  shared-memory directories.

The first helper of a launch on a node starts a small supervisor process that
starts and ends the keeper. Every helper exits as soon as its own rank is gone;
the supervisor holds the read end of the ranks' membership FIFO and ends the
keeper once every rank of the launch on the node is gone. Ending the keeper
kills every process left in its PID namespace and frees every System V object
of its IPC namespace, so nothing of a launch outlives it on a node. Only a
keeper that cannot start refuses every later rank of that launch on the node;
a rank whose helper cannot enter the keeper is refused alone. Killing the
supervisor ends every rank of the launch on that node. A rank that starts after
all earlier ranks of its launch on the node are gone gets a fresh keeper.
When the keeper cannot start or does not start in time, each rank's refusal on
the launch's standard error includes the tail of the node's keeper supervisor log.

Bubblewrap must not be installed setuid: the keeper's namespaces must belong
to the job's user, so that the rank helpers can enter them.

### Shared memory

The ranks of one launch on a node share a private directory
`<confine.shm_root>/httk-<token>/shm` (default root `/dev/shm`, mode 0700,
owner checked; the token is a random name the manager chose for the launch),
mounted at `/dev/shm`. This supports POSIX shared-memory files without exposing
unrelated host shared-memory objects. Next to it, `httk-<token>/ns/` holds the
rank helpers' keeper coordination, which no rank sees. The manager removes
`httk-<token>/` on its own node once the launch is reaped, and every rank
helper first removes the directories on its node that no launch record names
any more, judged from two listings one visibility deadline apart. A node that
runs no later launch keeps a stale directory after a failure; removing it is a
site cleanup item. Do not use age-only deletion, which can remove a live
launch's storage.

Validate the transport your MPI installation actually uses: a shared-file
test alone does not prove that MPI selected its shared-memory transport.

### PMIx and devices

`confine.pmix_roots` bounds the Slurm-supplied `PMIX_SERVER_TMPDIR`. Only the
verified per-step directory is exposed, at its original path, and no broad
authentication or socket directory is mounted as a fallback. The site's PMIx
layout must fit this contract.

`confine.devices` exposes individually approved device nodes, such as RDMA or
GPU devices, to attempts and ranks; it does not establish tested RDMA or GPU
support. Each node is opened without following symbolic links and mounted from
that descriptor with device access, as `bwrap --dev-bind` would mount it. Host
networking and the PMIx endpoint are service authorities: the site must verify
that they do not let ranks launch unconfined processes or reach other protected
services.

### PMI-2 launches (Intel MPI)

Intel MPI under Slurm needs `srun --mpi=pmi2`. Set it on the daemon's approved
launcher, then reload the daemon so it picks up the changed launcher:

```bash
httk launcher configure --set manager.launch_mpi=pmi2 small
httk workspace daemon reload WORKSPACE
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
nodes with several ranks; an allocation-shared `/dev/shm` object, System V
shared memory and distinct process IDs among a node's ranks, and an unrelated
host shared-memory canary that stays invisible to ranks; host
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

- At least two ranks must share a node. The probe also checks that a node's
  ranks have distinct process IDs and share System V shared memory.
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

Job control has two site cases of its own. With no manager running, a
`continue` and then a `cancel` of a paused job through
`httk job request --workspace REMOTE:NAME` must return signed `applied`
results, which fresh signed `httk remote daemon status REMOTE --request-id
COMMAND_ID` observations confirm. With a manager holding a running job, a
cancellation must return `queued` with one internal request, which the manager
consumes once before it returns the finished tree through the outbox.
