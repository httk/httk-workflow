# Confined workspace daemon

`httk workspace daemon` is an opt-in, foreground Slurm broker for a file command
mailbox. It accepts health checks, starts a manager from an operator-defined
profile, and requests status or cancellation by opaque manager handle.
Requests cannot supply commands, shell fragments, environment variables, paths
or Slurm arguments.

Serial execution supports one node, one Slurm task and one manager worker per
submission. The `mount-daemon` adapter supplies typed client controls and uses
native mounted-path job transfers; see {doc}`remotes`. Protected MPI profiles add
direct Slurm PMIx application steps as described below. The existing mount adapter and ordinary Slurm launcher
do not acquire these confinement guarantees.

## Deployment boundary

Use Linux with Bubblewrap 0.9.0 or later supporting `--bind-fd`, `--ro-bind-data`,
`--disable-userns` and `--assert-userns-disabled`, permitted unprivileged user
namespaces, and Slurm 23.11.6 or later. Startup refuses missing requirements;
there is no unsandboxed fallback. The Python installation, its packages, policy,
and initial operator environment must be trusted. Use a protected installation,
not editable packages stored in uploaded content.

Provision separate roots for workspace data, requests, responses and broker
state. Their parent directories must prevent transport writers from replacing
these roots. Keep the policy and private state outside the writable export.
The SQLite ledger needs a local filesystem with reliable locking and durability.
It must not be stored over SSHFS or another network filesystem.

An SSHFS mount root alone does **not** restrict the server-side account to that
directory. Enforce that restriction at the server, or use a separate restricted
transport identity. In particular, a same-UID unrestricted SFTP or SSH account
could modify private state or trusted code outside the intended export.
Local file-mode checks cannot verify that server configuration.

The broker enters Bubblewrap before processing workspace content. It sees the
workspace read-only, writable mailbox/state mounts, selected read-only runtime
and scheduler configuration, and the host network needed by Slurm. Each submitted
manager enters a separate Bubblewrap sandbox on the compute node **before**
reading its workspace prelude. That sandbox has writable workspace data, private
temporary storage, isolated network/PID/IPC/UTS/user namespaces, no capabilities,
and disabled nested user namespaces. It receives no broker-only mounts.

Only mount trusted runtime directories and required files. Do not expose home
directories, credentials, arbitrary Unix sockets, or broad system configuration
trees to payloads. Read-only mounts can still contain sockets that grant host
services; file permissions do not disable those services. Confinement also relies
on the host kernel and Bubblewrap being correctly maintained. Resource and disk
exhaustion are separate operational concerns.

Shared runtime roots and broker-only roots must be disjoint, including after
symlink resolution. Runtime roots may not contain mutable roots or replace the
sandbox's private mounts. These checks reject broad aliases such as a selected
runtime symlink resolving to `/`.

## Operator policy and startup

The following illustrates a site-specific policy, not a portable ready-to-run
configuration. Replace identities and paths with the provisioned values. Python,
its libraries, Bash and required simulation libraries must be available at their
original paths through `readonly_paths`; scheduler clients, plugins, configuration
and authentication sockets may additionally use `broker_paths`. Resolve which
files your site needs with its administrators. The protected policy and trusted
installation must also exist at the same paths on compute nodes. Private broker
state and mailboxes need not exist there.

```json
{
  "format": "httk-workspace-daemon-policy",
  "format_version": 1,
  "workspace": "/srv/httk/example/data",
  "workspace_id": "12345678-1234-4234-8234-123456789abc",
  "enrollment_id": "0123456789abcdef0123456789abcdef",
  "requests": "/srv/httk/example/requests",
  "responses": "/srv/httk/example/responses",
  "state": "/var/lib/httk/example",
  "bwrap": "/usr/bin/bwrap",
  "python": "/opt/httk/bin/python",
  "sbatch": "/usr/bin/sbatch",
  "squeue": "/usr/bin/squeue",
  "scancel": "/usr/bin/scancel",
  "cluster": "example",
  "readonly_paths": ["/usr", "/bin", "/lib", "/lib64", "/opt/httk"],
  "broker_paths": ["/etc/slurm", "/run/munge"],
  "slurm_conf": "/etc/slurm/slurm.conf",
  "profiles": {
    "small": {"cpus": 2, "memory_mb": 4096, "time_minutes": 60, "partition": "batch"}
  },
  "max_records": 4096,
  "max_submissions": 128,
  "poll_seconds": 1.0,
  "command_timeout": 30.0,
  "max_output_bytes": 65536
}
```

The workspace ID must match the existing workspace's
`.httk-workspace/format.json` identity.
Generate a fresh unpredictable enrollment ID, for example with
`python -c 'import secrets; print(secrets.token_hex(16))'`.
The enrollment ID is a stale-request barrier, not an authentication secret.
Protect the policy against modification by uploaders and other users.

```console
httk workspace daemon /srv/httk/example/data --policy /etc/httk/example.json --check
httk workspace daemon /srv/httk/example/data --policy /etc/httk/example.json --initialize
httk workspace daemon /srv/httk/example/data --policy /etc/httk/example.json
```

`--check` enters the real broker sandbox and checks scheduler client requirements;
it does not submit a job or validate compute-node execution. `--initialize`
exclusively creates a new enrollment ledger and exits. Ordinary startup refuses
missing or corrupt state. `--once` processes one bounded mailbox scan and exits.
Otherwise the daemon polls until SIGINT or SIGTERM. Run it under the site's
service supervisor if restart supervision is needed.

Do not change policy while submissions remain queued or running: compute-node
bootstrap reloads the protected policy. Treat runtime and policy changes as an
operator maintenance operation. Keep the old enrollment's state when recovering
from failures; never remove it merely to clear an uncertain result.

## File command protocol

The internal version-1 format is intentionally narrow. A health request is:

```json
{
  "format": "httk-workspace-command",
  "format_version": 1,
  "request_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "workspace_id": "12345678-1234-4234-8234-123456789abc",
  "enrollment_id": "0123456789abcdef0123456789abcdef",
  "operation": "health"
}
```

Publish complete UTF-8 JSON by writing a temporary file and atomically renaming it
to `<request_id>.json` in the request directory. Use a fresh random 32-character
lowercase hexadecimal request ID for each new operation. Documents are limited
to 16 KiB; unknown keys, duplicate keys and extra operation fields are rejected.
The other operations are:

| Operation | Additional fields |
| --- | --- |
| `start_manager` | `profile`: an operator-defined profile name |
| `manager_status` | `handle`: the returned manager handle |
| `cancel_manager` | `handle`: the returned manager handle |

Responses appear under the same filename in the response directory. They contain
the request identities, canonical request digest and outcome; scheduler IDs and
raw scheduler output are not exposed. Verify these identities and digest when
consuming a response. The uploader can interfere with mailbox contents; the
protected ledger is authoritative.

Use one active caller per request ID. Do not replace a request while the broker
may still be consuming it: responses are published before request removal. The
maintained client waits for removal and checks for a response again before
republishing.

A repeated request ID and unchanged content replays its recorded response.
Changed content with the same ID is refused. Use a **new request ID** for each
status refresh. Responses are committed before publication and requests are
removed only after publication. Malformed requests are discarded with a bounded
diagnostic and no response.

`submitted` returns an opaque handle. `uncertain` means submission may have
happened: the daemon will not retry it, including after restart. An operator must
reconcile it with Slurm. `UNKNOWN` status does not mean a job finished; accounting
reconciliation is not implemented. `cancel_requested` means the filtered
cancellation call succeeded, not that termination is confirmed. Cancellation
uses controller-side job-name and user filters to avoid acting on a reused
numeric scheduler ID.

`max_records` limits all durable request records. `max_submissions` limits total
admitted manager starts over the enrollment's lifetime, including refused or
uncertain starts. These are cumulative quotas, not active-job counts. Exhaustion
returns a nonpersisted `busy` response that can be retried with the same request
ID and identical fields. Capacity may require operator action. There is no automatic history pruning or
quota reset. Intentional re-enrollment requires preserving/reconciling outstanding
work, a new enrollment ID and a new private ledger.

## Site acceptance

The repository has executable scheduler stand-in tests and a required real
Bubblewrap CI gate. Neither replaces site acceptance of SSHFS access controls,
Slurm authentication, compute-node mounts, prelude confinement, cancellation,
restart recovery, or the actual simulation executable. Run those before relying
on this daemon for production jobs.


## MPI applications

MPI is opt-in through protected policy. An MPI allocation runs one manager and
one application step at a time. Every application rank enters Bubblewrap before
reading its executable, arguments, environment or working directory from workspace
data. The external step command is always a fixed `srun --mpi=pmix` bootstrap.
The manager stays network-isolated; only the trusted allocation launcher and MPI
ranks use host networking. `mpirun` is not used by this path.

Add these fields to an otherwise complete policy, using your site's paths:

```json
{
  "mpi": {
    "srun": "/usr/bin/srun",
    "control_root": "/var/tmp/httk-mpi-control",
    "pmix_roots": ["/var/spool/slurmd"],
    "shm_root": "/dev/shm",
    "devices": [],
    "environment": {},
    "max_steps": 128,
    "termination_grace": 10.0
  },
  "profiles": {
    "mpi-small": {
      "cpus": 2,
      "memory_mb": 4096,
      "time_minutes": 60,
      "mpi": {"nodes": 2, "ranks": 8}
    }
  }
}
```

`cpus` is CPUs per rank and `memory_mb` is MiB per node for MPI profiles.
Each MPI invocation uses the profile's entire fixed geometry. The manager
advertises aggregate capacity to workflow matching but runs ordinary commands on
its single manager CPU. Use the explicit wrapper for distributed applications:

```console
httk workflow mpi run -- /opt/application/bin/program input.dat
```

The same argv works through Python `Attempt.run`, Bash `httk_workflow_run`, or a
simulation-code command setting. It requires a running daemon MPI manager and
never falls back to an ordinary local launcher. Output streams to the caller;
stdin is `/dev/null`. An application's nonzero exit status is returned to the
workflow. Application-specific node setup can be an application script: it runs
inside every rank's sandbox. Workspace and workflow preludes run inside the
manager's sandbox. Only environment variables with portable ASCII identifier
names are forwarded. Exported Bash functions are omitted; retain any needed
function definitions in an application script and source them inside each rank.
Module-derived variables such as `PATH` and `LD_LIBRARY_PATH` are forwarded.

### Shared memory and site configuration

Ranks on each node share one allocation-specific backing directory mounted at
`/dev/shm`. This supports POSIX shared-memory files without exposing unrelated
host shared-memory objects. The normal root-owned sticky `/dev/shm` parent is
permitted; each allocation directory is private to the runtime UID. Private
PID/user namespaces can affect CMA transports, and private IPC namespaces prevent
cross-rank SysV IPC. Validate the transport your MPI installation actually uses;
a shared-file test alone does not prove that MPI selected its shared-memory
transport. No default forces TCP or disables shared memory.

`pmix_roots` bounds the Slurm-supplied `PMIX_SERVER_TMPDIR`. Only the verified
per-step directory is exposed at its original path. The launcher does not fall
back to mounting broad authentication or socket directories. The site's PMIx
layout must fit this contract. `devices` can expose individually approved device
paths, for example needed RDMA devices; it does not establish tested RDMA/GPU
support. Protected `environment` entries can tune the installed MPI transport.
Rank/server identities cannot be overridden by application manifests.

Provision `control_root` on the batch node so both the batch and manager Slurm
steps see the same directory. Site mount plugins must preserve that visibility.
The private socket is available to the manager at `/run/httk-mpi/control.sock`;
requests contain only a random request ID. The launcher never interprets uploaded
application arguments. The workspace holds bounded application manifests under
`.httk-workspace/mpi/`. A lost connection ends the allocation and leaves uncertain manifests for diagnosis
and never automatically resubmits them.

The site owns cleanup of private control and node-local shared-memory directories
**after Slurm confirms allocation quiescence**, including cancellation and node
failure. Use a trusted per-node epilog or manual anchored cleanup of the verified
allocation directory. Launcher exit cannot clean another node's local storage.
Do not use age-only deletion that can remove live allocation storage.

### MPI acceptance and failure handling

MPI profiles disable requeue; the bootstrap also refuses a restarted batch job.
Duplicate application IDs never launch again during the allocation. Manager exit,
client disconnect and service shutdown stop the active step. If local launcher
termination leaves remote completion uncertain, the service stops accepting work,
stops its manager and ends the allocation. `termination_grace` is a local cleanup
deadline; Slurm may forward TERM to tasks as KILL, so it does not promise graceful
application termination.

Host networking and the PMIx endpoint are additional service authorities. The
site must verify that they do not let ranks launch unconfined processes or reach
other protected services. A generic `MPI_ERR_SPAWN` result is inconclusive: lack
of resources or a configuration error can produce it. Test with spare capacity,
record the installed Slurm/Open MPI/PMIx versions, and distinguish explicit
unsupported-operation evidence from other errors.

Before production use, test this exact mount/environment configuration with:

- Positive multi-node communication and confirmed shared-memory transport on
  nodes containing multiple ranks.
- An allocation-shared `/dev/shm` object and an unrelated host shared-memory canary
  that remains invisible to ranks.
- Host filesystem/process canaries, dynamic MPI spawn, nested scheduler commands
  and exposed authentication/socket routes.
- Rank startup failure, client timeout/disconnect, manager death, cancellation,
  requeue refusal and per-node cleanup after completion and node failure.

Local protocol tests and scheduler stand-ins do not establish real cluster
containment. This development host cannot create Bubblewrap namespaces and has
no Slurm or MPI runtime; real cluster acceptance remains a deployment check.

The {download}`MPI site probe <../tools/probe_daemon_mpi.c>` (also in a source
checkout at `tools/probe_daemon_mpi.c`) provides the communication and spawn checks. Build it on the cluster with `mpicc -O2 -Wall -Wextra -o probe
probe_daemon_mpi.c -lrt`, place the executable in the workspace and invoke it from
a workflow with `httk workflow mpi run -- ./probe FRESHALPHANUMERICTOKEN`.
At least two ranks must share a node. Add `--spawn` only for the separate spawn
check with independently established spare capacity. Exit 77 means inconclusive;
spawn success is a failure. The probe creates only a named object in the exposed
`/dev/shm` and never writes an escape proof into the host home directory. Inspect
MPI transport diagnostics separately. The probe is a site test, not a replacement
for the filesystem, process, scheduler and lifecycle checks above.
