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
$ httk remote add --template ssh kappa
$ httk remote configure \
      --set host=login.example.org \
      --set username=me \
      --set check_connectivity=yes kappa
$ httk remote check kappa
```

`remote check` invokes the adapter's `install` operation. The name is
historical: the maintained adapters only verify the remote and install nothing.
Settings that are credentials go into `credentials.json`, which is excluded from
signed project manifests. `remote configure NAME --unset KEY` removes a stored connection setting or
private credential; use repeatable `--set KEY=VALUE` to add or replace it.
`remote show [--json]` inspects a definition without
printing credential values. `remote list`, `remote remove` and
`remote import-v1` cover the other common management tasks.

### Making `httk` available over SSH

`ssh` runs its command in a non-interactive shell. The environment your login
files set up interactively (`module load` lines, a virtualenv) is not applied,
and `httk` is often not on `PATH`. Put that setup in the remote's `prelude`
setting rather than in `~/.bashrc`, so it applies only to httk's ssh commands
and not to every other tool that logs in over ssh:

```text
$ httk remote configure --set prelude='module load Python/3.13.5-bundle
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

The `NAME:WORKSPACE` spelling is a binding, not a filesystem path. Install the
job's workflow there, transfer the job and run its manager there:

```console
$ httk workflow install --workspace kappa:runs 'git+https://github.com/httk/workflows-vasp#vasp-relax'
$ httk job transfer --job JOB default kappa:runs
$ httk workflow run --workspace kappa:runs --count 4
```

Workflows never travel with jobs: a transferred job whose workflow is not
installed at the destination waits there, and the transfer warns about it. The
remote invocation asks the owning machine to run
`httk workflow manager run --workspace runs --detach …`. The target workspace
then applies its own `manager.launch`, `manager.workers`, scheduler settings and
`environment.prelude`, as if the command had been run on the login node. Fetch
finished jobs back with the reverse transfer, naming each job by its UUID:

```console
$ httk job transfer --job JOB_UUID kappa:runs default
```

A transfer holds the jobs on the source, copies them, adopts them at the
destination and only then releases the hold, so an interrupted transfer is
finished by running it again or with `httk job transfer --resume`;
`httk transfer status` lists what a workspace holds. The steps and their
recovery are in the `job transfer` section of {doc}`workflow_cli`.

### Local names and second trees

Names listed in `machine_names` address this machine. `login:runs` is a local
workspace binding when `login` is configured as one of this machine's names,
so it does not invoke a remote adapter. To use a second tree on the same host
through the adapter contract, create a separate remote with the `local`
template:

```console
$ httk remote add --template local local-tree
$ httk workspace init --name scratch local-tree:/tmp/me/httk/scratch
```

### Job trees travel together

A child job spawned by another job travels with its parent. `--tree` moves a
job together with its spawned descendants, with their placements kept; without
it, a job whose descendants are present is refused. Every descendant must be
paused or terminal, so a fetch of finished work brings a campaign back whole
once it is done; to move a tree still in flight, pause its unfinished children
first. A child that should leave alone is detached first, because a child may
read its parent's files in place (see {doc}`composing_workflows`):

```console
$ httk job transfer --tree --job ROOT_UUID kappa:runs default
$ httk job detach CHILD
$ httk job transfer --job CHILD default kappa:runs
```

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
$ httk remote add --template mount sigma
$ httk remote configure \
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
`httk remote check sigma` before transfers to confirm that the
filesystem is mounted and `httk` answers on the far side.

The mount is for transfers only. Do not run `httk workspace init` on the mount
as a local workspace, and do not run `status`, `collect`, `fsck` or any analysis
against the mounted tree. The remote machine owns the workspace, so every
`httk` command about it goes through the executor (`sigma:runs`), as with an
`ssh` remote:

```console
$ httk workspace init --name runs sigma:/proj/x/users/me/httk/runs
$ httk workflow install --workspace sigma:runs ./my-workflow
$ httk job transfer --job JOB default sigma:runs
$ httk workflow run --workspace sigma:runs --count 4
$ httk workspace status sigma:runs
```

## Mounted exchange with a confined daemon (`mount-daemon`)

Use `mount-daemon` when files are the only channel to the destination. The
client mounts only the workspace's exchange directory (`WORKSPACE/exchange`), never the workspace.

### Setting up a confined remote

First provision {doc}`workspace_daemon` on the HPC system. Mount the workspace's
`exchange` directory (`WORKSPACE/exchange`) locally, for example with SSHFS and
no `follow_symlinks`, through the workspace owner's account (see
[Filesystem and trust requirements](#filesystem-and-trust-requirements)). The
mount point must be outside every local workspace. Then pin the identities the
exchange publishes in `exchange.json` and `daemon.json`:

```console
$ httk remote add --template mount-daemon confined
$ httk remote daemon configure confined --exchange /mnt/cluster/exchange
$ httk remote check confined
```

The remote's settings are `exchange` and `daemon_workspace_id`, which are
required, and `daemon_enrollment_id` and `daemon_public_key`, which are needed
only for signed requests. `configure` pins the workspace from `exchange.json`
and, when a daemon has published `daemon.json`, the daemon identities; without
`daemon.json` it pins the workspace only, which is enough to send and fetch
jobs and read status. Approved configuration digests and the request lifetime
are read live from `daemon.json`, so a changed daemon configuration needs no
reconfiguration; a changed identity is refused. Configure an *httk* identity
whose public key the daemon authorizes. `configure` publishes nothing.
`check` reads `exchange.json` and, when the daemon is pinned, `daemon.json` and
sends a signed health request; it checks a broker response and does not
establish compute-node readiness.

### Moving jobs and daemon requests

Jobs move through the exchange with the ordinary `job eject` and `job adopt`:

```console
$ httk job eject JOB /mnt/cluster/exchange/inbox
$ python -c 'import secrets; print(secrets.token_hex(16))'
$ httk remote daemon start confined --configuration small --request-id REQUEST_ID
$ httk remote daemon status confined
$ httk remote daemon status confined --handle MANAGER_HANDLE
$ httk remote daemon cancel confined --handle MANAGER_HANDLE --request-id ANOTHER_REQUEST_ID
$ httk job adopt --move /mnt/cluster/exchange/outbox/CLIENT_JOB_UUID/JOB_KEY
```

`job eject` copies the bundle into the mount under a hidden partial name and
renames it into `inbox/` when complete; the job must be fresh (never run) and
have no parent there, since managers adopt it as untrusted input with fresh
UUIDs, keeping your job UUID as its exchange name. Its workflow must be
installed in the workspace, an operator task; until then the adopted job waits.
`job adopt --move` copies the
returned tree, verifies it and removes the source from the exchange.
`--configuration` names one of the global `slurm` launchers the operator
approved for the daemon (each sets `manager.confine=bwrap`); `daemon.json`
lists them with their digests. Every unrestricted confined manager of the workspace (no `--placement-prefix` or pool restriction), whether
started by the daemon or not, adopts bundles from `inbox` and runs every job
attempt confined to its own job directory; the daemon only starts, queries and
cancels managers. `status` without `--handle` is passive: it prints the
informational root `status.json` and `managers.json` from the exchange without
a request. With `--handle` it sends the signed `manager_status` request.
`managers.json` also reports each manager's outcome (scheduler state, exit
code, times); once a manager's Slurm job has ended, `httk remote
daemon log confined --handle MANAGER_HANDLE` prints its published log.

To give up on a bundle that no manager has adopted, run `httk remote
daemon take-back confined NAME [DESTINATION]`. It is client-only: it renames
`inbox/NAME` to a dot name (managers ignore it), copies it out and removes it.
If the name is gone, a manager took it. See {doc}`workspace_daemon` for the job
lifecycle, rejected bundles, `status.json` and signed job control.

### Request ids and retries

Replace `REQUEST_ID` and `ANOTHER_REQUEST_ID` with separately generated
32-character lowercase hexadecimal ids, and keep them. Start and cancel
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

`health`, `start`, `status` and `cancel` accept `--wait-seconds` from 0.05 to
120 (default 10). Exit 0 means a positive protocol outcome; `refused`, `busy`,
`uncertain` and unacknowledged calls exit 2. `UNKNOWN` is a valid status and
does not establish completion. A cancellation acknowledgement does not confirm
that the job has terminated. Finished jobs appear in the exchange `outbox` on their own.

### What is refused

Generic `confined:workspace` commands (including `job transfer`), arbitrary
invocation, and adapter push/pull are refused. Manage workspace configuration at the destination
through the operator. The older `mount` adapter still uses its separate
executor as described above and does not gain confinement from this feature.

### Filesystem and trust requirements

The exchange must be a real directory on the destination (see the layout in
{doc}`workspace_daemon`). Mount it without `follow_symlinks`, so SSHFS does
not hide server symlinks by following them. The exchange writer must be the
workspace owner's account: a separate transport UID is unsupported, because a
renamed bundle keeps its owner and fails the manager's ownership checks.
Restricting that account's SFTP or SSH access to the exchange is a site matter
that the client cannot verify. Client descriptor checks cannot
prove the server's layout or SSHFS cache coherence, so validate those at the
site. Do not use unsupported FUSE or object-backed mounts. An uninterruptible
filesystem call may exceed the polling or adapter timeout.

The client controls the exchange and its own workspace content; `job adopt`
keeps its usual client trust boundary when parsing bundles. The destination
confines each job attempt independently: its managers run every attempt, and
every rank of a parallel launch, in a sandbox that can write only that job's
directory. The `status.json`, `managers.json` and manager log files are informational and
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
and credential handling, and refusal rules are in {doc}`adapter_authoring`.
