# Remotes

A remote bundles transport, file movement and command execution: it can move a
job or workspace tree to another machine, invoke `httk` there, and report
status. Ordinary remotes leave scheduling to the destination workspace's
launcher; see {doc}`launchers`. The restricted `mount-daemon` variant instead
relays typed requests to a destination broker controlled by an operator, and
exchanges jobs through one mounted exchange directory.

A remote bundle contains `remote.json` and one executable named `adapter`, plus
an optional `credentials.json` for values that should stay out of the shareable
metadata. Project remotes live at `httk_project/remotes/NAME` and global remotes
at `~/.config/httk/remotes/NAME`. A project definition takes precedence over a
global one with the same name.

## SSH remotes

### Setting up an SSH remote

Create an SSH remote, configure its connection, and check that a compatible
`httk` answers on the other machine:

```console
$ httk workflow remote add --template ssh kappa
$ httk workflow remote configure \
      --set host=login.example.org \
      --set username=me \
      --set check_connectivity=yes kappa
$ httk workflow remote check kappa
```

`remote check` invokes the adapter's `install` operation. The name is
historical: the maintained adapters only verify the remote and install nothing.
Settings that are credentials go into `credentials.json`, which is excluded from
signed project manifests. `remote show [--json]` inspects a definition without
printing credential values. `remote list`, `remote remove` and
`remote import-v1` cover the other common management tasks.

### Making `httk` available over SSH

`ssh` runs its command in a non-interactive shell. The environment your login
files set up interactively (`module load` lines, a virtualenv) is not applied,
and `httk` is often not on `PATH`. Put that setup in the remote's `prelude`
setting rather than in `~/.bashrc`, so it applies only to httk's ssh commands
and not to every other tool that logs in over ssh:

```text
$ httk workflow remote configure --set prelude='module load Python/3.13.5-bundle
source ~/venv/bin/activate' kappa
```

The prelude runs under `set -e`, so a failing line aborts before anything else.
It runs ahead of every command the adapter sends over ssh, including the
`httk workspace status` that `remote check` uses to find httk. If only the
`httk` program is somewhere non-standard and the environment is otherwise
ready, the narrower `httk_command=/path/to/httk` setting is enough.

The adapter `prelude` is not the workspace's `environment.prelude`. The adapter
prelude prepares the shell so `httk` can run at all; the manager applies
`environment.prelude` later, once it is already running on the remote.

## Working with remote workspaces

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
then applies its own `manager.launch`, `manager.workers`, scheduler settings and
`environment.prelude`, as if the command had been run on the login node. Fetch
finished jobs back with the reverse transfer:

```console
$ httk job transfer kappa:runs default
```

### Local names and second trees

Names listed in `machine_names` address this machine. `login:runs` is a local
workspace binding when `login` is configured as one of this machine's names,
so it does not invoke a remote adapter. To use a second tree on the same host
through the adapter contract, create a separate remote with the `local`
template:

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
in place (see {doc}`composing_workflows`). Detach a child first if it should
leave alone:

```console
$ httk job detach CHILD
$ httk job transfer --job CHILD default kappa:runs
```

A tree leaves only when nothing in it can start while it moves: every member
except the root must be paused or finished, and no member may still be waited
on by a gather. A fetch of finished work therefore brings a campaign back whole
once it is done. While some child is still running, the fetch skips the tree
with a warning naming the blocking jobs. To move a tree that is still in
flight, pause its unfinished children first. `--destination-placement` is
refused for a tree, because the children record where their parent is.

## Mounted filesystem with a separate executor (`mount`)

Use the `mount` template when the remote filesystem is mounted locally (sshfs,
NFS, any shared mount) but commands must run on the remote through a separate
execution channel. An example is a login node reachable only through a
tunnelled wrapper, with neither `ssh` nor `rsync` available for transfers.
Files move as bytes over the mount, and every `httk` command about the
workspace runs on the remote through the executor.

Three settings describe it:

- `mount_root`: the local path where the remote tree is mounted.
- `remote_root`: the same tree as the remote machine spells it. Keep it to the
  project subtree you transfer into; `remote_root=/` maps every absolute remote
  path onto the mount and is not recommended.
- `exec_command`: an executor prefix, parsed once with `shlex.split`, that runs
  one shell command line on the remote and relays its stdout, stderr and exit
  status.

```console
$ httk workflow remote add --template mount sigma
$ httk workflow remote configure \
      --set mount_root=/home/me/work/mounts/sigma \
      --set remote_root=/proj/x/users/me/httk \
      --set exec_command="/home/me/bin/hpc run" sigma
```

`configure` refuses unless `mount_root` is an existing directory (set
`check_mount=no` to configure the remote before the filesystem is mounted) and
the executor can run a cheap `true` on the remote (set `check_connectivity=no`
to configure it anyway). The `prelude` and `httk_command` settings mean the same
as for `ssh`.

A transfer also refuses when `mount_root` does not exist. An empty, unmounted
mount point looks the same as a mounted but empty one, so run
`httk workflow remote check sigma` before transfers to confirm that the
filesystem is mounted and `httk` answers on the far side.

The mount is for transfers only. Do not run `httk workspace init` on the mount
as a local workspace, and do not run `status`, `collect`, `fsck` or any analysis
against the mounted tree. The remote machine owns the workspace, so every
`httk` command about it goes through the executor (`sigma:runs`), as with an
`ssh` remote:

```console
$ httk workspace init --name runs sigma:/proj/x/users/me/httk/runs
$ httk job transfer --job JOB default sigma:runs
$ httk workflow run --workspace sigma:runs --count 4
$ httk workspace status sigma:runs
```

## Mounted exchange with a confined daemon (`mount-daemon`)

Use `mount-daemon` when files are the only channel to the destination. The
client mounts only the daemon's exchange directory, never the workspace.

### Setting up a confined remote

First provision and start {doc}`workspace_daemon` on the HPC system. Mount its
`exchange` directory locally, for example with SSHFS and no `follow_symlinks`,
through a restricted transport account. The mount point must be outside every
local workspace. Then pin the daemon identities it publishes in
`endpoint.json`:

```console
$ httk workflow remote add --template mount-daemon confined
$ httk workflow remote daemon configure confined --exchange /mnt/cluster/exchange
$ httk workflow remote check confined
```

The remote's settings are `exchange` and the pinned `daemon_workspace_id`,
`daemon_enrollment_id` and `daemon_public_key`. Approved configuration digests
and the request lifetime are read live from `endpoint.json`, so a changed
daemon configuration needs no reconfiguration; a changed identity is refused. Configure
an *httk* identity whose public key the daemon authorizes. `configure` publishes
nothing. `check` sends a signed health request; it checks a broker response and
does not establish compute-node readiness.

### Moving jobs and daemon requests

Jobs move through the exchange with the ordinary `job eject` and `job adopt`:

```console
$ httk job eject JOB /mnt/cluster/exchange/inbox
$ python -c 'import secrets; print(secrets.token_hex(16))'
$ httk workflow remote daemon start confined --configuration small --request-id REQUEST_ID
$ httk workflow remote daemon status confined
$ httk workflow remote daemon status confined --handle MANAGER_HANDLE
$ httk workflow remote daemon cancel confined --handle MANAGER_HANDLE --request-id ANOTHER_REQUEST_ID
$ httk job adopt /mnt/cluster/exchange/outbox/JOB_KEY
```

`--configuration` names one of the global `slurm` launchers the operator
approved for the daemon (each sets `manager.confine=bwrap`); `endpoint.json`
lists them with their digests. Managers started by the daemon adopt bundles
from `inbox` and run every job attempt confined to its own job directory.
`status` without
`--handle` is passive: it prints the informational `status.json` and
`managers.json` from the exchange without a request. With `--handle` it sends
the signed `manager_status` request. The passive `managers.json` also reports
each manager's outcome (scheduler state, exit code, times) and the bundles still
waiting; once a manager's Slurm job has ended, `httk workflow remote daemon log
confined --handle MANAGER_HANDLE` prints its published log.

To give up on bundles that no manager has adopted, run `httk workflow remote
daemon withdraw confined --request-id ID [--bundle NAME]`. It first takes your
bundles still in `inbox` back locally, then asks the broker to return those it
already moved into its staging area. Both end up in `outbox/withdrawn/NAME`; then
`httk job adopt /mnt/cluster/exchange/outbox/withdrawn/NAME`. Update the broker
before using `withdraw`: an old broker silently drops the unknown operation, and
the client then times out. See
{doc}`workspace_daemon` for the job lifecycle, rejected bundles and trust.

### Request ids and retries

Replace `REQUEST_ID` and `ANOTHER_REQUEST_ID` with separately generated
32-character lowercase hexadecimal ids, and keep them. Start, cancel and withdraw
require an explicit id; health and status generate one unless supplied. Each call prints
its id to stderr before dispatch and a validated JSON response to stdout.

- After a timeout, retry with **the same id and identical fields**. Never retry
  an uncertain submission with a new id; reconcile it with the operator.
- Use a fresh id for each status refresh.
- The client stores the exact signed requests in its local httk data directory
  before publication. Retries reuse their original timestamps and signatures;
  changing the intent, signer or endpoint pin under an existing id is refused.
- Retries of an old start request keep its original configuration digest; a
  start of a changed configuration needs a new id.
- Do not delete this local request history to resolve an uncertain operation.
- Run only one caller per request id at a time.

The clock-skew allowance is 130 minutes; see {doc}`workspace_daemon` for the
acceptance window and replay rules.

### Waiting and exit codes

`health`, `start`, `status`, `cancel` and `withdraw` accept `--wait-seconds` from 0.05 to
120 (default 10). Exit 0 means a positive protocol outcome; `refused`, `busy`,
`uncertain` and unacknowledged calls exit 2. `UNKNOWN` is a valid status and
does not establish completion. A cancellation acknowledgement does not confirm
that the job has terminated. Finished jobs appear in the exchange `outbox` on their own.

### What is refused

Generic `confined:workspace` commands, arbitrary invocation, and adapter
push/pull are refused. Manage workspace configuration at the destination
through the operator. The older `mount` adapter still uses its separate
executor as described above and does not gain confinement from this feature.

### Filesystem and trust requirements

The exchange must be a real directory on the destination (see the layout rules
in {doc}`workspace_daemon`). Mount it without `follow_symlinks`, so SSHFS does
not hide server symlinks by following them. Client descriptor checks cannot
prove the server's layout or SSHFS cache coherence, so validate those at the
site. Do not use unsupported FUSE or object-backed mounts. An uninterruptible
filesystem call may exceed the polling or adapter timeout.

The client controls the exchange and its own workspace content; `job adopt`
keeps its usual client trust boundary when parsing bundles. The destination
confines each job attempt independently: its managers run every attempt, and
every rank of a parallel launch, in a sandbox that can write only that job's
directory. The `status.json` and `managers.json` files are informational and
never acted on. Parallel launches need the site configuration and acceptance
described in {doc}`workspace_daemon`. Postprocess scripts are never run by the
destination: run `httk workflow postprocess` on the adopted job locally.

## From Python

The low-level adapter API is in `httk.workflow.adapters`. This example uses the
`local` template so it runs without an SSH server. Use the CLI to configure an
SSH remote's persisted settings; there is no single high-level Python
equivalent of `remote configure --set`.

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

- `add_remote` creates a maintained adapter bundle.
- `resolve_remote` applies project-before-global resolution.
- `run_adapter` executes an adapter operation.
- `probe_remote_workspace` validates the remote status document and returns the
  remote workspace UUID and root.
- The registry's `resolve_workspace` keeps the `NAME:WORKSPACE` binding in one
  place. A remote workspace has no local path until the adapter reports it.

## Writing a remote adapter

A custom remote is a versioned bundle with `remote.json`, one executable named
`adapter`, and an optional `credentials.json`. The dispatcher answers six
operations, `configure`, `install` (the operation behind `remote check`),
`invoke`, `push`, `pull` and `status`, with one JSON result per invocation. The
optional `daemon` operation carries typed mailbox requests for `mount-daemon`;
that restricted adapter refuses the four generic execution and transfer
operations.

An adapter implements transport, file movement and remote command execution,
and leaves manager scheduling to the destination workspace's launcher. The
engine refuses malformed metadata, a missing or non-executable dispatcher,
unavailable required binaries, unsupported operations, non-zero dispatcher
exits, and malformed or unsuccessful result documents.

The complete bundle layout, operation request and result documents, settings
and credential handling, and refusal rules are in {doc}`adapter_authoring`. For
the transfer completion protocol, crash recovery and metadata bounds on
quota-limited filesystems, see {doc}`transfer_reclamation`.
