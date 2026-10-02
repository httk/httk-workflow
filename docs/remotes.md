# Remotes

*For operators who need to reach another machine.* A remote is a bundle that
combines transport, file movement, and command execution: it can move a job or
workspace tree, invoke `httk` there, and report status. Ordinary remotes leave scheduling to the destination workspace launcher; see
{doc}`launchers`. The restricted `mount-daemon` variant instead relays typed
requests to an operator-controlled destination broker.

A remote bundle contains `remote.json` and one executable named `adapter`, with
optional `credentials.json` for values that should not enter the shareable
metadata. Project remotes live at `httk_project/remotes/NAME`; global remotes
live at `~/.config/httk/remotes/NAME`. Project definitions take precedence over
global definitions with the same name.

## Setting one up

Create an SSH remote, configure its connection, and verify that a compatible
`httk` answers on the other machine:

```console
$ httk workflow remote add --template ssh kappa
$ httk workflow remote configure \
      --set host=login.example.org \
      --set username=me \
      --set check_connectivity=yes kappa
$ httk workflow remote check kappa
```

`remote check` invokes the adapter's historical `install` operation; despite
that protocol name, the maintained adapters verify the remote and do not
install anything. Settings that are credentials are stored in
`credentials.json`, which is excluded from signed project manifests. Use
`remote show [--json]` to inspect a definition without printing credential
values; `remote list`, `remote remove`, and `remote import-v1` cover the other
common management tasks.

### Making `httk` available over SSH

`ssh` runs its command in a *non-interactive* shell, so the environment your
login files set up interactively — `module load` lines, a virtualenv — is not
applied, and `httk` is often not even on `PATH`. Put that setup in the remote's
`prelude` setting rather than in `~/.bashrc`, so it applies to httk's ssh
commands only and does not disturb every other tool that logs in over ssh:

```text
$ httk workflow remote configure --set prelude='module load Python/3.13.5-bundle
source ~/venv/bin/activate' kappa
```

The prelude runs (under `set -e`, so a failing line aborts before anything
else) ahead of *every* command the adapter sends over ssh — including the
`httk workspace status` that `remote check` uses to find httk in the first
place. When only the `httk` program lives somewhere non-standard but the
environment is otherwise ready, the narrower `httk_command=/path/to/httk`
setting is enough.

This adapter `prelude` is distinct from a workspace's `environment.prelude`
(below): the adapter prelude bootstraps the shell so `httk` can run at all,
while `environment.prelude` is applied later by the manager once it is already
running on the remote.

Initialize a named workspace on the remote by putting the remote name before
the path:

```console
$ httk workspace init --name runs kappa:/scratch/me/httk/runs
$ httk workspace status kappa:runs
```

The `NAME:WORKSPACE` spelling is a binding, not a filesystem path. Transfer a
job into that workspace and run its manager there:

```console
$ httk job transfer --job JOB default kappa:runs
$ httk workflow run --workspace kappa:runs --count 4
```

The remote invocation asks the owning machine to run
`httk workflow manager run --workspace runs --detach …`. The target workspace
then applies its own `manager.launch`, `manager.workers`, scheduler settings,
and `environment.prelude`, exactly as if the command had been run on the
login node. Fetch finished jobs back with the reverse transfer:

```console
$ httk job transfer kappa:runs default
```

Names listed in `machine_names` are self-addressing: `login:runs` is treated as
a local workspace binding when `login` is configured as one of this machine's
names, so it does not invoke a remote adapter. To use a second tree on the
same host through the adapter contract, create a distinct remote with the
`local` template:

```console
$ httk workflow remote add --template local local-tree
$ httk workspace init --name scratch local-tree:/tmp/me/httk/scratch
```

### Job trees travel together

A child job spawned by another job travels with its parent. Transferring the
parent moves its whole tree of spawned descendants, root first, with their
placements kept, even when a `--state` or `--placement` filter would have
matched only the parent. A child cannot be transferred on its own while its
parent is still in the workspace, because a child may read its parent's files
in place (see {doc}`composing_workflows`). Make one independent first when it
really should leave alone:

```console
$ httk job detach CHILD
$ httk job transfer --job CHILD default kappa:runs
```

A tree leaves only when nothing in it can start while it is moving: every
member except the root must be paused or finished, and no member may still be
waited on by a gather. A fetch of finished work therefore brings a campaign
back whole once it is done, and skips it, with a warning naming the blocking
jobs, while some child is still running. To move a tree that is still in flight,
pause its unfinished children first. `--destination-placement` is refused for a
tree, because the children record where their parent is.

### A mounted filesystem with a separate executor

Use the `mount` template when the remote filesystem is available locally as a
mount (sshfs, NFS, any shared mount) but commands must run on the remote through
a separate execution channel — for example a login node reachable only through a
tunnelled wrapper, with neither `ssh` nor `rsync` available for transfers. Files
move as bytes over the mount; every `httk` command about the workspace runs on
the remote through the executor.

Three settings describe it: `mount_root` is the local path where the remote tree
is mounted, `remote_root` is the same tree as the *remote* machine spells it
(keep it to the project subtree you transfer into — `remote_root=/` maps every
absolute remote path onto the mount and is not recommended), and `exec_command`
is an executor prefix (parsed once with `shlex.split`) that runs one shell command
line on the remote and relays its stdout, stderr and exit status.

```console
$ httk workflow remote add --template mount sigma
$ httk workflow remote configure \
      --set mount_root=/home/me/work/mounts/sigma \
      --set remote_root=/proj/x/users/me/httk \
      --set exec_command="/home/me/bin/hpc run" sigma
```

`configure` refuses unless `mount_root` is an existing directory (set
`check_mount=no` to configure the remote before the filesystem is mounted) and
unless the executor can run a cheap `true` on the remote (set
`check_connectivity=no` to configure it anyway). The `prelude` and `httk_command`
settings mean the same as for `ssh`.

A transfer also refuses when `mount_root` does not exist, but an empty, unmounted
mount point cannot be told apart from a mounted-but-empty one, so run
`httk workflow remote check sigma` before transfers as the operator's safeguard
that the filesystem is actually mounted and `httk` answers on the far side.

For the `mount` adapter, the mount is for transfers only. Never `httk workspace init` on the mount as a
local workspace, and never run `status`, `collect`, `fsck` or any analysis
against the mounted tree: the remote machine owns the workspace, so every `httk`
command about it goes through the executor (`sigma:runs`), exactly as with an
`ssh` remote:

```console
$ httk workspace init --name runs sigma:/proj/x/users/me/httk/runs
$ httk job transfer --job JOB default sigma:runs
$ httk workflow run --workspace sigma:runs --count 4
$ httk workspace status sigma:runs
```

### A mounted filesystem with a confined daemon

Use `mount-daemon` when files are the only channel to the destination. First
provision and start {doc}`workspace_daemon` on the HPC system. Export its data,
request and response directories through the restricted transport account;
keep the policy, trusted installation and private ledger outside that export.
Obtain the public endpoint export through a trusted operator handoff, then map
its workspace and mailbox directories to their local mounted paths:

```console
$ httk workflow remote add --template mount-daemon confined
$ httk workflow remote daemon configure confined --endpoint endpoint.json \
      --mount-root /home/me/mounts/cluster/data \
      --requests /home/me/mounts/cluster/requests \
      --responses /home/me/mounts/cluster/responses
$ httk workflow remote check confined
```

The export pins the response-signing public key, destination identities, approved
configuration digests and maximum request lifetime. It contains no private key.
Re-import it after the operator approves changed configurations. Configure an httk
identity whose public key is authorized by the daemon; a missing signing key is
an error. `configure` checks local paths and workspace identity without publishing
a request. `check` sends a signed health request; it checks a broker response and
does not establish compute-node readiness.

Manual `remote configure --set` remains available for the eight endpoint settings:
`mount_root`, `daemon_requests`, `daemon_responses`, `daemon_workspace_id`,
`daemon_enrollment_id`, `daemon_public_key`, `daemon_configurations` (a JSON map
of names to digests), and `daemon_request_max_age`. Arbitrary commands, preludes,
environment or scheduler overrides are not endpoint settings.

Use **absolute mounted workspace paths** for native job transfers. This explicitly
supports the existing filesystem transfer protocol over a suitable mount: source
fencing, sealed bundles, verified import, acknowledgement and retirement. It does
not run the job runner or its prelude on the client. For example:

```console
$ httk job transfer default /home/me/mounts/cluster/data --job JOB
$ python -c 'import secrets; print(secrets.token_hex(16))'
$ httk workflow remote daemon start confined --configuration small --request-id REQUEST_ID
$ httk workflow remote daemon status confined --handle MANAGER_HANDLE
$ httk workflow remote daemon cancel confined --handle MANAGER_HANDLE --request-id ANOTHER_REQUEST_ID
$ httk job transfer /home/me/mounts/cluster/data default --state succeeded
```

Replace `REQUEST_ID` and `ANOTHER_REQUEST_ID` with separately generated 32-character
lowercase hexadecimal IDs, and retain them. Start and cancel require an explicit
ID; health and status generate one unless supplied. Each call prints its ID to
stderr before dispatch and a validated JSON response to stdout. Reuse **the same
ID and identical fields** after a timeout. Never retry an uncertain submission
with a new ID; reconcile it with the operator. Use a fresh ID for each status
refresh. The client retains exact signed requests in its local httk data directory
before publication. Retries reuse their original timestamps and signatures;
changing the intent, signer or endpoint pin under an existing ID is refused.
Retain the previous endpoint export when updating a catalog: retries of an old
configuration need its original digest, while a new configuration needs a new ID.
Do not delete this local request history to resolve an uncertain operation.
Run only one caller per request ID at a time. The clock-skew allowance is 130
minutes; see {doc}`workspace_daemon` for the acceptance window and replay rules.

`health`, `start`, `status` and `cancel` accept `--wait-seconds` from 0.05 to 120
(default 10). Exit 0 means a positive protocol outcome; `refused`, `busy`,
`uncertain`, and unacknowledged calls exit 2. `UNKNOWN` is a valid status and does
not establish completion. Cancellation acknowledgement does not confirm the job
has terminated. Wait for jobs to finish or quiesce before transferring them back.

Generic `confined:workspace` commands, arbitrary invocation and adapter push/pull
are refused. Manage workspace configuration at the destination through the
operator. The old `mount` adapter still uses its separate executor as described
above; it does not gain confinement from this feature.

The mount must meet the workspace's atomic rename and metadata visibility
requirements (see {doc}`details/taskmanager`). Root paths must be absolute,
existing, disjoint and free of symlink components. Local descriptor checks cannot
prove the server's layout or SSHFS cache coherence. Validate these at the site,
including that SSHFS does not hide server symlinks by following them. Unsupported
FUSE or object-backed mounts must not be used. An uninterruptible filesystem call
may exceed the polling or adapter timeout.

Native transfer retains its existing client trust boundary when parsing workspace
data; this feature adds no client sandbox. The destination daemon enforces payload
confinement independently. MPI configurations require the additional site configuration
and acceptance described in {doc}`workspace_daemon`.

## From Python

The low-level adapter API is in `httk.workflow.adapters`. This example uses the
`local` template so it can be exercised without an SSH server; use the CLI to
configure an SSH remote's persisted settings, because there is no single
high-level Python equivalent of `remote configure --set`:

```python
from pathlib import Path

from httk.workflow.adapters import (
    add_remote,
    probe_remote_workspace,
    resolve_remote,
    run_adapter,
)
from httk.workflow.registry import resolve_workspace

project = Path(".").resolve()
add_remote("local-tree", template="local", project=project)
target = resolve_remote("local-tree", project=project)

# The workspace named runs must already exist in the local registry.
result = run_adapter(
    target.bundle,
    "status",
    {"argv": ["httk", "workspace", "status", "--json", "runs"]},
    timeout=None,
)
workspace_id, root = probe_remote_workspace(target, "runs", timeout=None)
binding = resolve_workspace("local-tree:runs", project=project)
print(result, workspace_id, root, binding)
```

`add_remote` creates a maintained adapter bundle, `resolve_remote` applies
project-before-global resolution, and `run_adapter` executes an
adapter operation. `probe_remote_workspace` validates the remote status
document and returns the remote workspace UUID and root. The registry's
`resolve_workspace` keeps the `NAME:WORKSPACE` binding in one place; a remote
workspace has no local path until the adapter reports it.

## Writing a remote adapter

A custom remote is a versioned bundle with `remote.json`, one executable named
`adapter`, and optional `credentials.json`. The dispatcher answers six
operations — `configure`, `install` (the operation behind `remote check`),
`invoke`, `push`, `pull`, and `status` — with one JSON result per invocation.
The optional `daemon` operation carries typed mailbox requests for `mount-daemon`;
that restricted adapter refuses the four generic execution and transfer operations.
It must implement transport, file movement, and remote command execution while
leaving manager scheduling to the destination workspace's launcher. The engine
refuses malformed metadata, a missing or non-executable dispatcher, unavailable
required binaries, unsupported operations, non-zero dispatcher exits, and
malformed or unsuccessful result documents.

The complete bundle layout, operation request and result documents, settings and
credential handling, and refusal rules are in {doc}`details/adapter_authoring`.

For the transfer completion protocol, crash recovery, and metadata bounds on
quota-limited filesystems, see {doc}`transfer_reclamation`.
