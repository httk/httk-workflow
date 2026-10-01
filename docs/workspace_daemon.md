# Confined workspace daemon

`httk workspace daemon` is an opt-in, foreground Slurm broker for a file command
mailbox. It accepts health checks, starts a manager from an operator-defined
serial profile, and requests status or cancellation by opaque manager handle.
Requests cannot supply commands, shell fragments, environment variables, paths
or Slurm arguments.

Serial execution supports one node, one Slurm task and one manager worker per
submission. The `mount-daemon` adapter supplies typed client controls and uses
native mounted-path job transfers; see {doc}`remotes`. MPI execution remains
disabled. The existing mount adapter and ordinary Slurm launcher
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

## MPI and site acceptance

Payload networking is disabled in this milestone; MPI profiles are unavailable.
The inspected Slurm PMIx server rejects spawn requests, which is useful for a
future direct `srun --mpi=pmix` profile. It does not establish isolation for other
PMIx operations, credentials, network services, or an `mpirun`/PRRTE path. See the
[Slurm MPI guide](https://slurm.schedmd.com/mpi_guide.html) and the
[Slurm PMIx implementation](https://github.com/SchedMD/slurm/blob/slurm-26-05-4-1/src/plugins/mpi/pmix/pmixp_client_v2.c).

The repository has executable scheduler stand-in tests and a required real
Bubblewrap CI gate. Neither replaces site acceptance of SSHFS access controls,
Slurm authentication, compute-node mounts, prelude confinement, cancellation,
restart recovery, or the actual simulation executable. Run those before relying
on this daemon for production jobs.
