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

Each push or pull is one uncompressed `tar` stream over a single ssh
connection, however many paths it carries. The local side needs only `ssh`;
the remote needs a POSIX `tar` on the login shell's `PATH`. A push keeps file
modes with `tar -p`, but a pull extracts through Python's safe `tarfile`
filters: pulled files lose group/other write and setuid/setgid bits, and pulled
directories take default permissions, so a setgid bit is not carried over.

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
`httk workspace status` that `remote check` uses to find httk, except the
`tar` that carries a push or pull: that stream must stay free of anything a
prelude prints. If only the
`httk` program is somewhere non-standard and the environment is otherwise
ready, the narrower `httk_command=/path/to/httk` setting is enough.

The adapter `prelude` is not the workspace's `environment.prelude`. The adapter
prelude prepares the shell so `httk` can run at all; the manager applies
`environment.prelude` later, once it is already running on the remote.

### Passwords and second factors

The `ssh` adapter runs ssh with `BatchMode=yes`: it logs in with keys only and
never prompts. A site that asks for a password or a one-time code therefore
answers `Permission denied (keyboard-interactive)`.

The preferred remedy is OpenSSH connection sharing. Log in once and let later
ssh calls, including httk's, reuse that connection without authenticating
again. Add a block to `~/.ssh/config`:

```text
Host kappa kappa.example.org
    HostName kappa.example.org
    User me
    ControlMaster auto
    ControlPath ~/.ssh/cm-%C
    ControlPersist 12h
```

Open the shared connection, answering the prompts, and close it when done:

```console
$ ssh -fN kappa
$ ssh -O exit kappa
```

ssh matches `Host` blocks against the host name on its command line, not
against `HostName`; httk passes the remote's `host=` setting (a `username@`
prefix plays no part in the match). Either configure the remote with `host=`
set to the alias, or list the full hostname on the `Host` line too, as above. The shared connection must be open whenever httk
reaches the remote; a manager that runs unattended cannot reopen it.

Where connection sharing is not an option, set `interactive_auth=yes`. ssh then
prompts in your terminal for the password or code; httk never sees the secret.
httk keeps its own shared connection open for 10 minutes after its last use
(`ControlPersist=600`, with the socket under `$XDG_RUNTIME_DIR/httk` or
`/tmp/httk-ssh-UID`), so a command such as `remote check` prompts once. That
control socket replaces any `ControlPath` from `~/.ssh/config`: if you already
share connections there, keep the default (or `per-call`), or you are prompted
despite the open connection. Configuring checks connectivity,
so the configure command itself already prompts:

```console
$ httk remote configure --set interactive_auth=yes kappa
```

For sites that forbid connection sharing, `interactive_auth=per-call` prompts on
every ssh call instead. Either way, prompts happen only when the
command runs in a foreground terminal. Anything without one (a detached
manager, a cron job, a backgrounded shell job) keeps batch mode and fails with a
hint instead of hanging. The adapter's `timeout_seconds` (300 s for the `ssh`
template) still bounds each operation, including the time you spend typing.
`httk monitor` never prompts; for a remote that needs a second factor, open the
shared connection first, for example with `httk remote check NAME`.

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
finished jobs back with the reverse transfer, selecting them by state (add
`--placement PREFIX` for one campaign, or name single jobs with
`--job JOB_UUID`):

```console
$ httk job transfer --kind succeeded kappa:runs default
```

A transfer holds the jobs on the source, copies them, adopts them at the
destination and only then releases the hold, so an interrupted transfer is
finished by running it again or with `httk job transfer --resume`;
`httk transfer status` lists what a workspace holds. All jobs of one transfer
travel together, so it opens the same few ssh connections whether it moves one
job or a thousand, and it reports each step on stderr (`-q` silences that). The
steps and their recovery are in the `job transfer` section of {doc}`workflow_cli`.

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
$ httk job transfer --kind succeeded --tree kappa:runs default
$ httk job detach CHILD
$ httk job transfer --job CHILD default kappa:runs
```

## Mounted filesystem with a separate executor (`mount`)

Use the `mount` template when the remote filesystem is mounted locally (sshfs,
NFS, any shared mount) but commands must run on the remote through a separate
execution channel. An example is a login node reachable only through a
tunnelled wrapper, with no direct `ssh` login available for transfers.
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
jobs and read status. The active configuration digest and request lifetime are
read live from protocol 5's `daemon.json`; changed configuration needs no
reconfiguration, while a changed identity is refused. Configure an *httk*
identity whose public key the daemon authorizes. `configure` publishes nothing.
`check` reads `exchange.json` and, when the daemon is pinned, `daemon.json` and
sends a signed health request; it checks a broker response and does not
establish compute-node readiness.

### Moving jobs and daemon requests

Jobs move through the exchange with the ordinary `job eject` and `job adopt`:

```console
$ httk job eject JOB /mnt/cluster/exchange/inbox
$ httk remote daemon show confined
$ httk remote daemon start confined --configuration small
$ httk remote daemon status confined
$ httk remote daemon status confined --handle MANAGER_HANDLE
$ httk job request pause --workspace confined:runs --reason 'operator pause' JOB_UUID
$ httk remote daemon cancel confined --handle MANAGER_HANDLE
$ httk job adopt --move /mnt/cluster/exchange/outbox/CLIENT_JOB_KEY
```

`job eject` copies the bundle into the mount under a hidden partial name and
renames it into `inbox/` when complete. `job eject` records your identity's
public key as optional `owner_key` in `bundle.json`; only untrusted (exchange)
adoption uses it. The job must be fresh (never run) and
have no parent there, since managers adopt it as untrusted input with fresh
UUIDs, keeping your job UUID as its exchange name. The fresh UUID keeps the
first eight hex digits of yours, so a unique prefix such as
`httk job show 8f7047f2` selects the job in both workspaces. Its workflow must be
installed in the workspace, an operator task; until then the adopted job waits.
`job adopt --move` copies the
returned tree, verifies it and removes the source from the exchange.
`--configuration` names one of the global `slurm` launchers the operator
approved for the daemon (each sets `manager.confine=bwrap`); `daemon.json`
lists them with their digests and published resource settings (task layout,
CPUs, memory, GRES, partition, workers and time limits), and `show` prints it. Every unrestricted confined manager of the workspace (no `--placement-prefix` or pool restriction), and
every manager the daemon starts, adopts bundles from `inbox` and runs every job
attempt confined to its own job directory; the daemon only starts, queries and
cancels managers. Adoption puts the jobs you eject in your key's exchange pool,
and a manager the daemon starts for your key runs only those, then exits once
it has nothing left to do; a keyless bundle waits for a manager an operator
starts on the host. Only the key that started a manager may query or cancel it. `status` without `--handle` is passive: it prints the
informational root `status.json` and `managers.json` from the exchange without
a request. With `--handle` it sends the signed `manager_status` request.
`managers.json` also reports each manager's outcome (scheduler state, exit
code, times); once a manager's Slurm job has ended, `httk remote
daemon log confined --handle MANAGER_HANDLE` prints its published log. The
published logs, like `managers.json`, are readable by every client of the
exchange, not only the key that started the manager.

To give up on a bundle that no manager has adopted, run `httk remote
daemon take-back confined NAME [DESTINATION]`. It is client-only: it renames
`inbox/NAME` to a dot name (managers ignore it), copies it out and removes it.
If the name is gone, a manager took it. See {doc}`workspace_daemon` for the job
lifecycle, rejected bundles, `status.json` and signed job control.

`job request` against a `mount-daemon` workspace sends one signed typed
command per UUID or UUID-prefix selector. A batch is sequential and
non-atomic: earlier selectors may already be queued if a later one fails.
Selectors must be UUIDs or lowercase UUID prefixes; paths, globs and tags are
refused. `--force` is refused before any command is sent. Eject destinations
are relative to the daemon exchange `outbox`. `--operator` selects the local
identity that signs each command. The daemon executes a command only for an
exchange root submitted with that signing key, which must also be authorized;
`--operator` must select the identity that ejected the job.
`job request` exits 0 when each command is applied or queued, and 1 for a
refusal or uncertain result. Each output row includes the resolved job id,
outcome, internal request id, daemon command id, explanation and observed
state when available.

For a single daemon-mediated job command, `--request-id ID` fixes the
32-character lowercase hexadecimal command id. Retry with the same selector,
action, reason and action-specific fields to reuse its exact cached signature,
even if the active configuration changed. Any semantic change under that id
is refused. This option is valid only for one selector on a `mount-daemon`
workspace; local and ordinary remote requests reject it. To inspect the
original job command, run `httk remote daemon status confined --request-id ID`.
That command sends a fresh signed observation request. Without `--handle` or
`--request-id`, `remote daemon status` remains passive and informational.

### Request ids and retries

Each call generates a fresh 32-character lowercase hexadecimal request id
unless `--request-id ID` supplies one, prints it to stderr before dispatch, and
prints a validated JSON response to stdout.

A call that exits without an acknowledged result has not necessarily failed: the
request may still be queued in the exchange, and the daemon applies it when it
picks it up, until the request expires. The error message names the id.

- To keep waiting for that request, rerun with **`--request-id ID` and
  identical fields**. Rerunning `start` without it sends a new request and may
  start a second manager; check `httk remote daemon status confined` first.
- Never retry an uncertain submission with a new id; reconcile it with the
  operator.
- Use a fresh id (the default) for each status refresh.
- `remote daemon status --request-id ID` refers to the original job command ID;
  it sends a separate fresh status request each time.
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
For daemon-mediated `job request pause --wait`, the client sends the original
pause once, then polls with a fresh signed status request every second until
the job is paused, the command is refused or uncertain, a terminal state
supersedes the pause, or the finite nonnegative `--timeout` expires. Passive
exchange status is never used as evidence that the request ran. Refusal,
uncertainty, supersession and timeout exit 1.

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
