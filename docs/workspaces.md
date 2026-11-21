# Setting up workspaces

A **workspace** is a directory tree that holds jobs and their state. A
**project** groups workspaces together with the remotes, launchers and settings
they share, and is what you copy when work moves to another machine.
**Managers** are the processes that run the jobs in a workspace. This page
sets all of that up; {doc}`running` then creates and runs jobs in it.

## Project and workspace

```console
httk setup --name "Your Name" --email you@example.org
httk project init --name campaign .
httk workspace init --name default workspace
httk workspace status
```

`httk setup` creates your per-user operator identity, used to attribute and
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
httk workspace set -w default vasp.command=vasp_std
httk workspace show -w default
httk workspace show -w default vasp.command
httk workspace unset -w default vasp.command
```

`workspace show`, `set` and `unset` address one key namespace: application
settings under their dotted names, the [policy](details/taskmanager.md#workspace-policy) as
`policy.visibility_deadline_seconds` and `policy.retention.MEMBER`, and the
[per-workflow preludes](#environment-preludes) as `prelude.WORKFLOW_ID`; an application
setting cannot start with `policy.` or `prelude.`. A setting an older release
stored under such a name is not reachable through these verbs: remove it by
editing the workspace's `.httk-workspace/format.json`, and store its value
under a key outside those namespaces. One `set` or `unset` validates every
change and stores them all in one write, or none; for a workspace on a remote
each verb is one forwarded command, so one ssh connection. `show` with no key prints every key,
one `key<TAB>value` line each (values as JSON, so a prelude's newlines appear
as `\n`, and application settings get a third column, below); one key prints
its bare value; `--json` prints one flat object.

The manager exports each scalar setting into the attempt environment under its
`HTTK_` name, so `vasp.command` becomes `HTTK_VASP_COMMAND`. A real
environment variable is a deployment override and wins over the workspace
setting; a job's own parameter of the same name wins over both.

`httk workspace show` marks each setting by who reads it:
*recognized* (by *httk-workflow*, an installed code-support package, or a
workflow installed in the workspace whose manifest declares the key; see
{doc}`details/workflow_packages`), *unrecognized* (in a namespace
*httk-workflow* owns, such as `slurm.`, but read by nothing, so it has no
effect; an unknown `confine.` key even stops managers from starting), or
*undeclared* (nothing installed declares reading it, though a workflow may
still read it without declaring it, or a future workflow may):

```console
$ httk workspace show -w default
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
httk workspace set -w default io.workers=16
httk workspace set -w cluster:default io.workers=4
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
Linux 5.8 or later for Lustre, ext4, XFS and tmpfs, on ZFS only with Linux
5.17 or later and OpenZFS 2.4.0 or later, and `fsync` elsewhere (older kernels
report no write errors through `syncfs`); `syncfs` forces it on any filesystem
type you vouch for, still falling back to `fsync` below Linux 5.8 or where the
call is not available (found by the first call on a filesystem, made before
anything lands). tmpfs is never persistent, whatever the setting. A value
other than the three is refused when set, and one stored otherwise counts as
`fsync`; the setting is read afresh at every landing, so a
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
cost 4 when each batch fills within 10 seconds) for the payloads; where `syncfs`
is trusted for the workspace's own protocol steps (a vetted local filesystem,
`io.sync` not `fsync`), a batch of eight jobs or more is then named complete
and submitted with one more `syncfs` each (plus one per command), instead of
three fsyncs per job. Once a batch's jobs start being named complete, each of
them is submitted even when it is not reported (an error or interrupt names
those by id). The library's `new_jobs`
publishes each job before it asks for the next item (N + 1 `syncfs`) unless
it is given `batch=`. Bundles and `job submit --move` payloads that were
written before httk ran are synced together as they are, so a write error
there is reported only if nobody has
seen it yet; when one is reported, nothing is taken, and the files may not be
intact on disk: re-create them from their source before trying again, since a
retry would not notice the loss.

ZFS was first left to `fsync`: it never records a write error where `syncfs`
looks for one (the filesystem's error sequence). A `syncfs` on ZFS always
committed the dataset's whole intent log; from OpenZFS 2.4.0 it also returns
that commit's error itself, which Linux passes on from 5.17 (5.8 to 5.16 take
another, untraced path). On a suspended pool it then fails with `EIO`, where
`fsync` waits until the pool returns (`failmode=wait`, the default). A
command with no retry (`job new`, `job submit`, `job adopt PATH`, workflow
installation) then fails where it used to wait and succeed, once the removal
of what it staged, which also waits for the pool, is done. A transfer refuses
that landing, removes it (waiting for the pool) and delivers it once more with
every file synchronized on its own, so it can still succeed once the pool is
back. OpenZFS
2.1.16, 2.2.8 and 2.3.0 return success on a suspended pool, and 2.3.4 fails
only when the pool is suspended already when it is called (a suspension
during its commit is waited out and reported as success), so `auto` keeps
`fsync` below 2.4.0, and also when `/sys/module/zfs/version` cannot be read
or parsed; do not force `syncfs` on ZFS below that gate. The version is
compared by its leading `major.minor.patch`, so a distribution build such as
`2.4.1-1ubuntu5.1` counts as 2.4.1 (its own patches are not verified) and any
later release is trusted as well. On ZFS a failed `syncfs` is always this
landing's own failure, so `io.sync_errors ignore` (below) does not apply
there. A ZFS `syncfs` also misses a failed write of pages written through
`mmap` (a quota or a full pool), which `fsync` reports, so on ZFS `auto`
synchronizes bundles and payloads written before httk ran file by file;
files that an instantiate hook writes through `mmap` during `job new` are
the accepted exception. When such a synchronization fails on ZFS because
the pool was suspended, the message's advice to re-create the files is more
than needed: ZFS keeps the data of a suspended pool and writes it once the
pool is back. A
`syncfs` covers only the filesystem it is called on: whatever is mounted inside
a synced tree from another filesystem, a directory or a single file, is
synchronized file by file.

The command that runs `job transfer`, `job push` or `job pull` prints the
strategy on standard error, unless `-q`: `syncing 120 landed file(s) to disk
(syncfs: lustre, Linux 5.14)`, or `made 3 landed bundle(s) durable (fsync:
zfs 2.2.2 is below 2.4.0)` when every file was synchronized as it was
written. For a push, that is the destination's line, relayed with its name in
front. When the synchronization fails, nothing of that landing is adopted.
Where the landing was synchronized with one `syncfs` and `io.sync_errors` is
`fail` (see below), the transfer then tries once more by itself, since the
error may be another user's on the same filesystem, which `syncfs` reports to
everyone on it:

- a push from a local workspace synchronizes each segment's landing only when
  the destination adopts it, about one visibility deadline after it landed
  (the destination answers the segment's upload at once, its landing still
  unsynchronized, and synchronizes it first thing when it is asked to adopt
  it), so that the system's own writeback has mostly run by then and a
  storage hiccup has had time to clear; under `io.sync fsync` that changes
  nothing (every file was synchronized as it was written). When that
  synchronization fails, nothing of the segment is adopted, and it is
  delivered once more at once, the same bundles to a fresh landing with every
  file synchronized as it is written (which another user's error cannot fail),
  and adopted before any later segment (so an explicitly selected child is
  still adopted before its parent). One write error usually fails every
  segment then waiting at the destination (up to 8), each delivered once more
  in turn; every segment staged afterwards is synchronized file by file at
  once. If a segment's second delivery is refused too, its jobs stay held at
  the source for `job transfer --resume` (they were sent, so they are never
  taken back);
- a push from a remote workspace (remote to remote) synchronizes each segment
  as it lands, and a failure is delivered once more from its relayed local
  copies rather than fetched anew; this is one more request on the
  destination's transfer session, per segment (the segment is staged once
  more, before anything of it is adopted), and every later segment is then
  synchronized file by file at once. If that delivery is refused too, its jobs
  stay held at the source (`job transfer --resume`);
- a pull synchronizes each landing here only when it adopts it, as a push
  does; when that fails, the landing is discarded and fetched once more at once
  into a fresh landing, with every file synchronized as it is written here (as
  every later fetch of the session then is), and adopted before any later
  landing. If that fails too, the pull stops there (that segment and every
  later one stay held at the source, and no later page of the selection is
  held); their jobs stay held at the source, and `job transfer --resume`
  delivers them again.

A failure is not retried otherwise. A destination (or, for a pull, this
workspace) on `io.sync fsync`, whose per-file errors are exact, fails at once:
a push from a local workspace takes the jobs it held itself back into the
source (`not_moved`; run the transfer again), and a push from a remote
workspace keeps the jobs held at the source for `job transfer --resume`; a pull
stops (that segment and every later one stay held at the source), resumable
with `job transfer --resume`. A hold an earlier run left is never taken back, since an earlier
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
httk workspace set -w default io.sync=fsync
```

`io.sync_errors` (`fail`, the default, or `ignore`) sets what a write error
that a `syncfs` reports does, except on ZFS, where such an error always fails. `syncfs` cannot tell whose write failed: on a
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
httk workspace set -w default io.sync_errors=ignore
```

Engine tunables (the visibility deadline of a shared filesystem and the
retention limits of `gc`) are *workspace policy*, kept apart from application
settings and addressed as the `policy.*` keys of `workspace show|set|unset`;
see {doc}`details/taskmanager`.

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
httk workspace set -w default environment.prelude="module load httk"
httk workspace set -w default prelude.vasp.relax="module load VASP/6.4.2"
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
httk launcher add --kind slurm --global cluster
httk launcher check cluster
httk workspace set -w default manager.launch=cluster \
  slurm.partition=batch slurm.time_limit=02:00:00
httk workflow run --count 4 --workspace default
```

Every pair is validated and all of them are stored in one write, or none is.

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
httk remote add kappa --kind ssh host=login.example.org username=me
httk remote set kappa cli_prelude='module load Python/3.13'
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
httk workspace set -w kappa:runs manager.launch=cluster
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
general form; see {doc}`running`. A transfer to or from a remote runs over one
transfer session, so the remote's `httk` must be the same version as this
machine's: otherwise the transfer stops before anything is held, with "the httk
on REMOTE is too old (or too new) for this transfer: install the same httk
version on both ends". Besides `ssh`, the packaged
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
