# Setting up workspaces

A **workspace** is a directory tree that holds jobs and their state. A
**project** groups workspaces together with the remotes, launchers and settings
they share, and is what you copy when work moves to another machine.
**Managers** are the processes that run the jobs in a workspace. This page
sets all of that up; {doc}`running` then creates and runs jobs in it.

## Project and workspace

```console
httk init --name "Your Name" --email you@example.org
httk project init --name campaign .
httk workspace init --name default workspace
httk workspace status
```

`httk init` creates your per-user operator identity, used to attribute and
sign what you publish; it is idempotent. `project init` writes the project
anchor `httk_project/`. `workspace init` creates a workspace and registers it
under a name in the per-user registry, so every later command can address it
with `--workspace NAME`; a workspace's directory path works too, registered or
not. Without `--workspace`, commands use the enclosing workspace, then the
project's default, then the per-user default. A workspace
is a directory of its own (`workspace init` refuses a non-empty directory, so
a project root is never a workspace); a project records its workspaces as
members, and the first workspace initialized inside a project becomes its
default.

```console
httk workspace list
httk workspace default default
httk workspace status --json default
```

State changes are synchronized to storage by default. `workspace init
--no-durable` makes a throwaway workspace faster at the price of recoverability
after a node crash.

## Settings

A workspace carries a flat map of *application settings*, small values a
runner resolves at run time, such as the VASP command:

```console
httk workspace configure default --set vasp.command=vasp_std
httk workspace show default
httk workspace configure default --unset vasp.command
```

One `configure` call applies every `--set` and `--unset` in one validated
write; for a workspace on a remote it is one forwarded command, so one ssh
connection.

The manager exports each scalar setting into the attempt environment under its
`HTTK_` name, so `vasp.command` becomes `HTTK_VASP_COMMAND`. A real
environment variable is a deployment override and wins over the workspace
setting; a job's own parameter of the same name wins over both.

`httk workspace settings show` marks each setting by who reads it:
*recognized* (by *httk-workflow*, an installed code-support package, or a
workflow installed in the workspace whose manifest declares the key; see
{doc}`details/workflow_packages`), *unrecognized* (in a namespace
*httk-workflow* owns, such as `slurm.`, but read by nothing, so it has no
effect; an unknown `confine.` key even stops managers from starting), or
*undeclared* (nothing installed declares reading it, though a workflow may
still read it without declaring it, or a future workflow may):

```console
$ httk workspace settings show default
foo.bar	1	undeclared: nothing installed declares reading it; a workflow may still read it without declaring it, or a future workflow may
mywf.threshold	0.5	recognized by workflow mywf
slurm.account	"proj"	recognized by httk-workflow
slurm.qos	"high"	unrecognized: httk-workflow reads no setting slurm.qos; pass it with slurm.sbatch_options=--qos=high
```

Setting an unrecognized key still stores it but prints a `warning:` line on
standard error; setting an undeclared key prints a milder `notice:` line. On a terminal the markers are also colored; set `NO_COLOR` to
turn that off. The namespaces `manager`, `slurm`, `confine`, `environment`,
`seal`, `postprocess`, `children`, `io` and `httk_v1` belong to *httk-workflow*, so a
workflow reads its own keys outside them, and a code package uses its code's
namespace, such as `vasp.`.

The SDK itself reads `children.placement` (`nested`, the default, or `flat`)
from the attempt's settings snapshot to place spawned children; see
{doc}`campaigns`. Unlike other settings it is not overridden by job parameters
or `HTTK_CHILDREN_PLACEMENT`: only the workspace setting, or a manager's pinned
setting override, counts.

`io.workers` (an integer from 1 to 32, default 8) sets how many jobs a batch
command works on at once in this workspace: holding, adopting and releasing
jobs in `job push`, `job pull`, `job transfer` and `job adopt`, writing the
small files (up to 1 MiB) of a tar stream pushed into the workspace, and
reading the rows and per-state counts of `job list`. Each job's own steps
still run in order, and the output is the same as with one worker. The
overlap helps most where file operations are slow round trips, as on shared
HPC filesystems; how much depends on the filesystem. On a fast local disk it
still helps the durable writes of transfers, while writing many small pushed
files can be somewhat slower than with `1`. `1` restores strictly
one-at-a-time processing. A value outside 1..32 is refused when set, and the
`HTTK_IO_WORKERS` variable that attempts see does not affect commands. Each
end of a transfer uses its own workspace's value:

```console
httk workspace settings set --key io.workers --value 16 default
httk workspace settings set --key io.workers --value 4 cluster:default
```

`io.sync` (`auto`, the default, `syncfs` or `fsync`) sets how jobs that a
transfer lands in this workspace (the far side of a push, the local side of a
pull, and holds copied from another filesystem or kept with `--keep`) are made
durable before they are adopted, so that a crash of the node cannot leave a
published job with missing or partial files. `syncfs` makes the whole landing
durable with one call for its filesystem, on a descriptor opened (and synced
once, which also flushes what was pending before) before the first file lands;
it flushes other users' pending writes on that filesystem too, and Ctrl-C
waits for it to finish. `fsync` synchronizes every file on its own, which
costs one round trip a file on a shared filesystem. `auto` uses `syncfs` on
Linux 5.8 or later for Lustre, ext4, XFS and tmpfs, and `fsync` elsewhere
(older kernels report no write errors through `syncfs`); `syncfs` forces it on
any filesystem type you vouch for, still falling back to `fsync` below Linux
5.8 or where the call is not available (found by the first call on a
filesystem, made before anything lands). tmpfs is never persistent, whatever
the setting. A value other than the three is refused when set, and one stored
otherwise counts as `fsync`; the setting is read afresh at every landing, so a
running daemon sees a change at once, and a `--no-durable` workspace
synchronizes nothing. Job creation (`job new`, campaigns, `job submit`),
workflow installation and builds, and `job adopt PATH` of a bundle on the
workspace's filesystem are made durable the same way, quietly, before the job
or package is published. `job new`, campaigns and `job submit` create jobs in
batches, each costing one `syncfs` before its jobs are submitted and reported:
a batch closes at 256 jobs, once 10 seconds have passed since its first job
(checked between jobs, so a slow instantiate hook delays it), and at the end of
the input, and a job that moves inputs (`--input-move-from-job`) has a batch of
its own. Creating N jobs thus costs one `syncfs` per batch plus one (600 jobs
cost 4 when each batch fills within 10 seconds). The library's `new_jobs`
publishes each job before it asks for the next item (N + 1 `syncfs`) unless
it is given `batch=`. Bundles and `job submit --move` payloads that were
written before httk ran are synced together as they are, so a write error
there is reported only if nobody has
seen it yet; when one is reported, nothing is taken, and the files may not be
intact on disk: re-create them from their source before trying again, since a
retry would not notice the loss.

The command that runs `job transfer`, `job push` or `job pull` prints the
strategy on standard error, unless `-q`: `syncing 120 landed file(s) to disk
(syncfs: lustre, Linux 5.14)`, or `made 3 landed bundle(s) durable (fsync:
zfs is not vetted for auto)` when every file was synchronized as it was
written. For a push, that is the destination's line, relayed with its name in
front. When the synchronization fails, nothing of that landing is adopted.
Where the landing was synchronized with one `syncfs` and `io.sync_errors` is
`fail` (see below), the transfer then tries once more by itself, since the
error may be another user's on the same filesystem, which `syncfs` reports to
everyone on it:

- a push is delivered once more, the same bundles to a fresh landing at the
  destination, where every file is synchronized as it is written (which
  another user's error cannot fail); this costs one more connection and one
  more remote `httk` start, only when it happens, and a push from a remote
  workspace delivers its local copies again rather than fetching them anew. If
  that delivery is refused too, a push from a local workspace takes the jobs
  it held itself back into the source (`not_moved`; run the transfer again),
  and from a remote workspace its jobs stay held there (`job transfer
  --resume`);
- a pull is fetched once more into a fresh landing under a fresh
  synchronization (one more fetch, only when it happens); if that fails too,
  the chunk fails and its jobs stay held at the source, and `job transfer
  --resume` delivers them again.

A failure is not retried otherwise. A destination (or, for a pull, this
workspace) on `io.sync fsync`, whose per-file errors are exact, fails at once:
a push from a local workspace takes the jobs it held itself back into the
source (`not_moved`; run the transfer again), and a pull, or a push from a
remote workspace, keeps the jobs held at the source for `job transfer
--resume`. A hold an earlier run left is never taken back, since an earlier
delivery may have adopted it (see {doc}`details/workflow_cli`). Job
creation (`job new`, campaigns), `job submit` of copies and workflow
installation have no retry either: the command fails, and for job creation
and `job submit` none of that batch is submitted (the batches before it are,
and the message says how many). Running it again is safe: for `job new` and
campaigns with `--duplicate-tag skip`, so that the jobs already submitted are
not created twice (a tagless job is); for `job submit`, which has no such
check, give it only the payloads it did not report as submitted. `job adopt
PATH` and `job submit --move` take files written before httk ran, so after a
failed synchronization re-create them from their source first, as described
above, rather than merely running the command again. Copies from
another filesystem whose `syncfs` failed are made once more with every file
synchronized as it is written, and the chunk fails (jobs held at the source)
only if that fails too. The retried delivery or fetch runs under the same
`--adapter-timeout` as the first one; when it times out, its outcome is
unknown and the jobs stay held at the source, and `job transfer --resume`
then reports those that arrived as `already_adopted` and releases them.

```console
httk workspace settings set --key io.sync --value fsync default
```

`io.sync_errors` (`fail`, the default, or `ignore`) sets what a write error
that a `syncfs` reports does. `syncfs` cannot tell whose write failed: on a
busy shared login node another user's late quota error (`EDQUOT`) on the same
filesystem fails our landing too, which `fail` handles by the retries above.
`ignore` trusts the writes that returned successfully: the landing is still
flushed, and after a failed `syncfs` another full `syncfs` is made; a warning
says whether that one completed without reporting a further error
(`... reported a writeback error, which io.sync_errors=ignore trusts (a
second flush reported no further error)`; this does not prove that the data
whose write failed reached the disk) or failed too (`... the flush may be incomplete`), and the
jobs are published anyway, with no retry. Choose it for speed when you expect
no quota trouble and accept removing a batch whose files turn out damaged: any
file can then have lost data, `job.json`, `state.json` and bundle records
included (a job may be unreadable, not only its outputs wrong), compute nodes
see the loss without any crash, and a `job submit --move` payload, the only
copy, is consumed all the same. A write error that `write()` itself reports
(a quota already full) still fails. `ignore` changes nothing else: under
`io.sync fsync` every per-file error fails; a `syncfs` that the system does not
support falls back to `fsync` when the first, priming call finds it so (or,
for bundles written before httk ran, at their one synchronization), but if it
reports so only at the final call, the landing fails under `ignore` too, without
a retry; and workflow installation and builds always fail on an error (a
damaged package would run for every later job). A value other than the two is
refused when set, and one stored otherwise counts as `fail` (with a warning);
like `io.sync`, it is read afresh at every landing.

```console
httk workspace settings set --key io.sync_errors --value ignore default
```

Engine tunables (the visibility deadline of a shared filesystem and the
retention limits of `gc`) are *workspace policy*, kept apart from application
settings and edited with `workspace policy show|set`; see
{doc}`details/taskmanager`.

## Installed workflows

A workspace runs only workflows installed in it, under `workflows/`. Install
once per workspace, locally or on a remote:

```console
httk workflow install --workspace default 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workspace workflows default
```

`job new --install` installs a missing workflow on the fly. A job transfer
(`job push`, `job pull`, `job transfer`) installs the workflows its jobs need
in the destination when it lacks them (see {doc}`details/remotes`).

## Environment preludes

Shell setup an HPC job needs (`module load`, `source activate`) goes in a
prelude, not in the command setting. There are two layers, both run under
`set -e` in a login shell, so a failing line aborts the job instead of running
it in a half-configured environment:

```console
httk workspace settings set --key environment.prelude --value "module load httk" default
httk workspace workflow-prelude set --workflow vasp.relax --value "module load VASP/6.4.2" default
```

`environment.prelude` applies to every job of the workspace and runs before
the manager starts; after it, the manager command is looked up on the resulting
`PATH`. A workflow prelude applies only to that workflow's jobs and runs after
the workspace prelude. Preludes are workspace-local: a job transferred to
another workspace runs under that workspace's preludes.

## Launchers

A launcher decides *where managers start*. The built-in `process` launcher
starts them on the current machine. On a cluster, add the packaged Slurm
launcher once, then select it per workspace together with the scheduler
directives it should use:

```console
httk launcher add --template slurm --global cluster
httk launcher check cluster
httk workspace settings set --key manager.launch --value cluster \
  --key slurm.partition --value batch --key slurm.time_limit --value 02:00:00 default
httk workflow run --count 4 --workspace default
```

`--key` and `--value` repeat in pairs, the i-th `--key` taking the i-th
`--value`; every pair is validated before any is stored, in one write.

`httk workflow run --count 4` now submits four managers through Slurm and
prints their scheduler job ids. `--inline` runs one manager in the current
process regardless of the launcher, which is the way to see manager output
while debugging; `--launcher NAME` picks another launcher for one invocation.
Launchers live at `httk_project/launchers/NAME` or
`~/.config/httk/launchers/NAME`, project first.

The full guide, {doc}`details/launchers`, lists every `slurm.*` and `manager.*`
setting, shows how to keep separate CPU and GPU launcher profiles, and gives
the Python API; {doc}`details/launcher_authoring` specifies the launcher bundle
for other schedulers.

## Remotes

A remote reaches another machine: it moves files and runs `httk` there. It
does not schedule anything; the destination workspace's launcher does that.

```console
httk remote add --template ssh kappa
httk remote configure --set host=login.example.org --set username=me kappa
httk remote configure --set cli_prelude='module load Python/3.13' kappa
httk remote check kappa
```

`ssh` runs commands in a non-interactive shell, so the remote's `cli_prelude`
setting is where `httk` is put on `PATH` for those commands. It runs before
each `httk` command the CLI runs on the remote, not at job start: jobs get
their environment from `environment.prelude` and the per-workflow prelude
(see [Environment preludes](#environment-preludes)). A workspace on the
remote is created and addressed by writing the remote name before it:

```console
httk workspace init --name runs kappa:/scratch/me/httk/runs
httk workspace settings set --key manager.launch --value cluster kappa:runs
httk workspace status kappa:runs
httk workflow run --workspace kappa:runs --count 4
```

`NAME:WORKSPACE` is a binding, not a path. Jobs are created locally, pushed
with `httk job push kappa:runs` (every job waiting to start) after
`httk workflow install --workspace kappa:runs …` installed their workflow
there, and the finished ones brought back with `httk job pull kappa:runs`
(every succeeded job); the end left out is the workspace the command runs in,
else the default workspace. A push leaves an `away` copy of each job in its
source until the pull brings the job back. `httk job transfer SRC DST` is the
general form; see {doc}`running`. Besides `ssh`, the packaged
templates are `local` (a second tree on this machine), `mount` (files over a
shared mount, commands through a separate executor) and `mount-daemon` (files
as the only channel, through an exchange directory: the workspace's managers serve it, and the confined broker only starts, queries and cancels managers).

The full guide, {doc}`details/remotes`, covers the mount variants, moving job
trees, and the Python adapter API; {doc}`details/adapter_authoring` specifies
the adapter bundle, and {doc}`details/workspace_daemon` the confined broker.

## Moving a project between machines

A project directory is self-describing. Copy the whole tree, including its
workspaces, and run `httk project repair` inside it: every member workspace is
registered under the name recorded in `httk_project/members.json`, and
`httk workflow run`, `--workspace NAME` and the rest work without a new
`init`. It is idempotent and refuses to overwrite a name that already points
elsewhere.

## Further reading

- {doc}`details/taskmanager` covers workspace policy, durability and
  filesystem requirements, resources and scheduling, owners and recovery, and
  maintenance with `fsck` and `gc`.
- {doc}`details/workflow_cli` is the complete command reference, including
  the `workspace`, `launcher`, `remote`, `config` and `project` groups.
- {doc}`campaigns` spreads one very large run across several workspaces.
