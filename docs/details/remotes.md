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
and `httk` is often not on `PATH`. Put that setup in the remote's `cli_prelude`
setting rather than in `~/.bashrc`, so it applies only to httk's ssh commands
and not to every other tool that logs in over ssh:

```text
$ httk remote configure --set cli_prelude='module load Python/3.13.5-bundle
source ~/venv/bin/activate' kappa
```

The prelude runs under `set -e`, so a failing line aborts before anything else.
It runs ahead of every `httk` command the adapter sends over ssh, including the
`httk workspace status` that `remote check` uses to find httk and the adoption
that receives a push. That command's stdin carries the pushed bundles, so a
prelude must not read stdin, and whatever it prints on stdout comes before the
command's JSON answer: the transfer then reports an answer it cannot read and
keeps the holds for `--resume`. The `tar` that carries a pull runs without the
prelude, since that stream must stay free of anything a prelude prints. If only the
`httk` program is somewhere non-standard and the environment is otherwise
ready, the narrower `httk_command=/path/to/httk` setting is enough.

The `cli_prelude` is not the workspace's `environment.prelude`. It runs before
each `httk` command the CLI runs on the remote over ssh or a mount executor, so
that `httk` can run at all; it does not run at job start. The manager applies
`environment.prelude` later, when a launcher starts it, and the per-workflow
prelude when a job of that workflow runs.

Earlier releases called this setting `prelude`. A remote that still stores
`prelude` is refused when a command would use it, and `--set prelude=...` is
refused; rename it with
`httk remote configure NAME --unset prelude --set cli_prelude=...`.

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
Configuring checks connectivity, so the configure command itself already
prompts:

```console
$ httk remote configure --set interactive_auth=yes kappa
```

Prompts happen only when the command runs in a foreground terminal. Anything
without one (a detached manager, a cron job, a backgrounded shell job) keeps
batch mode and fails with a hint instead of hanging. The adapter's
`timeout_seconds` (300 s for the `ssh` template) still bounds each operation,
including the time you spend typing.

### Connection reuse

On its own, `interactive_auth=yes` prompts on every ssh call. Set
`connection_reuse=yes` to have httk keep one shared connection open for 10
minutes after its last use (`ControlPersist=600`, with the socket under
`$XDG_RUNTIME_DIR/httk` or `/tmp/httk-ssh-UID`), so a command such as
`remote check` prompts once and the following calls, also those without a
terminal, log in no more:

```console
$ httk remote configure --set interactive_auth=yes --set connection_reuse=yes kappa
```

Connection reuse is a separate setting, and off unless set, because some sites
forbid connection sharing; it speeds up key-based logins too. `remote
configure` on an `ssh` remote that leaves it unset prints a notice naming the
setting. To make reuse the default for every remote that sets none, set it in
the per-user configuration; a remote then opts out with
`--set connection_reuse=no`:

```console
$ httk config set connection_reuse yes
```

When `~/.ssh/config` names a `ControlPath` for the host, httk adds no control
options and ssh reuses your own master connection instead. That reuse needs a
running master or `ControlMaster auto`: with `ControlMaster no` and no master
open, every call authenticates again. httk's own connection sends a keepalive
every 15 seconds, so one that died (after a suspend or a network change) is
noticed within a minute. To close it at once, name its socket:
`ssh -O exit -o ControlPath=$XDG_RUNTIME_DIR/httk/%C HOST`. Calls join a shared
connection only when they resolve the same socket directory: a cron job without
`XDG_RUNTIME_DIR` looks under `/tmp/httk-ssh-UID` and does not find a
connection opened from a desktop session.

`httk workflow monitor` prompts at most once per remote workspace, when it
starts and before its display opens, if `interactive_auth` is `yes` and a
terminal is attached; it then keeps that one connection, a far-side
`httk workflow session`, for its whole run, whatever `connection_reuse` says.
The monitor's reconnect after `r` runs without a terminal and joins a shared
connection opened earlier, for example by `httk remote check NAME`, while it is
open. Against a far side that cannot serve a session, the monitor falls back to
one call per read and, while `connection_reuse` is unset (on the remote and in
the per-user configuration), shares a connection anyway, so its reads and
actions log in once and then reuse it until it has been idle for 10 minutes;
`connection_reuse=no`, on the remote or as the per-user default, turns that
sharing off as well.

## Working with remote workspaces

Initialize a named workspace on the remote by putting the remote name before
the path:

```console
$ httk workspace init --name runs kappa:/scratch/me/httk/runs
$ httk workspace status kappa:runs
```

The `NAME:WORKSPACE` spelling is a binding, not a filesystem path. Install the
job's workflow there, push the jobs waiting to start and run its manager there:

```console
$ httk workflow install --workspace kappa:runs 'git+https://github.com/httk/workflows-vasp#vasp-relax'
$ httk job push kappa:runs
$ httk workflow run --workspace kappa:runs --count 4
```

`httk job push [SRC] DST` moves every `ready` job of SRC; the SRC left out is
the workspace the command runs in, else the default workspace (the per-user
one is created on first use). SRC keeps an inert *away copy* of each pushed job
(state `away`; `--no-keep` keeps none) so the jobs stay accounted for:
`httk job list --state away` lists them, a pull of the finished jobs replaces
them, and `httk job recover JOB` returns a job lost on the remote to the state
it left, to push again. See *Away copies* in {doc}`workflow_cli`.

Workflows travel with the jobs. A transfer (push, pull or `job transfer`, local
or remote ends, either direction) installs in DST every workflow the moved jobs
need that DST lacks and SRC has, under the same id, before adopting them: each
job's effective workflow (after `job request set_workflow`) and the closure of
its `[workflow.calls]`. One query asks DST which of them it has; the missing
store entries then travel as their sources only (`install.json` and
`package/`, never `builds/`, and `requires` is not checked there), each is
installed after its tree digest is verified (an integrity check of the copy:
SRC is trusted, and the digest does not prove that the content belongs to the
id), and one with `[workflow.build]` is built for DST's platform after the
adoption, as is one DST already had without a build for its platform. Nothing
is ever replaced: a DST that has the id with another tree keeps it, the jobs
run it, and the transfer warns. A shipped workflow is recorded as installed now, so it becomes DST's
newest installation of its short name. A build that fails, or a workflow
installed in neither workspace, leaves the jobs waiting at DST with a warning
(`httk workflow build --workspace DST ID` builds it later, as does the next
transfer); `--no-workflows` ships nothing, and an SRC running an older
httk-workflow ships nothing either, with a warning. A copy a failed call left
in DST's `.httk-workspace/tmp/push.<hex>/` is named in a warning and removed by
`httk workspace gc` after a day. The workspace daemon's exchange never installs a workflow a
client sends. The
remote invocation asks the owning machine to run
`httk workflow manager run --workspace runs --detach …`. The target workspace
then applies its own `manager.launch`, `manager.workers`, scheduler settings and
`environment.prelude`, as if the command had been run on the login node. Fetch
finished jobs back with a pull, which moves every `succeeded` job of SRC into
DST, by default the same workspace a push takes from (add `--placement PREFIX`
for one campaign; `--job JOB_UUID`, `--state STATE` or `--step STEP` replaces
the implicit state, and `--step` without `--state` selects from every unowned
state):

```console
$ httk job pull kappa:runs
```

Push and pull are the everyday forms of `httk job transfer SRC DST`, which
names both ends and an explicit selection (`--job`, `--state`, `--step`):

```console
$ httk job transfer --state succeeded --placement camp kappa:runs default
```

A transfer holds the jobs on the source, copies them, adopts them at the
destination and only then releases the hold, so an interrupted transfer is
finished by running it again or with `httk job transfer --resume`;
`httk transfer status` lists what a workspace holds. All jobs of one transfer
travel together, so the cost is the same whether it moves one job or a thousand:
a push to a remote workspace is one ssh connection and one remote `httk` start,
a transfer session (below) that also ships and builds the workflows the
destination lacks and retries a failed synchronization there (see
{doc}`../workspaces`); a pull from a remote workspace is one connection and one
`httk` start too, a transfer session that holds, copies the workflows and the
jobs, and releases; a remote-to-remote transfer one connection and one start at
each end, relayed through a temporary directory here (see *Remote to remote* in
{doc}`workflow_cli` for the relay and `TMPDIR`). Every transfer to or from a
remote workspace needs its transfer session, so both machines must run the same
*httk* version (see *The transfer session* below). On a site where
every Python start is slow, that is what a transfer costs. It reports each step
on stderr (`-q` silences that). The
steps and their recovery are in the `job transfer` section of {doc}`workflow_cli`.

### The transfer session

A transfer session is one connection and one far-side `httk` start, the hidden
`httk workflow transfer-session`, started through the adapter `session`
operation (as the monitor's session is), that carries typed requests and their
data in binary frames. A push from a local workspace to a remote one opens one
before it drives or makes its first hold (one that cannot open holds nothing),
a pull from a remote
workspace to a local one at its first request to the source (the check of its
selection, or for `--resume` the listing of its holds); either closes it at the
end of the command,
whatever the number of pages it holds (see *Paged holds* in
{doc}`workflow_cli`). A remote-to-remote transfer opens one to each end, SRC's
first and then, when DST is named, DST's, both before anything is selected or
held on either end (with `--resume` and no DST, each destination's before its
holds are driven); every bundle passes through a temporary directory here (the
default temporary location, `TMPDIR` when set), removed once what it holds is
settled: one fetch of about 16 GiB (never splitting a bundle, so possibly
more), and the workflow store entries separately. What one fetch
brings is delivered to a destination's session in the segments of a push (so
no staged segment is larger than one, except a single bundle). So an empty
selection still connects to both ends. A remote
source never takes a hold back: a refusal, an unknown outcome or a failure of
that directory (a full disk) leaves the holds held there for `--resume`.

A push holds and delivers its jobs a page at a time (when it keeps no away
copies, as `job transfer` without `--keep` and `job push --no-keep`, the next
page is held while the current one is delivered, so up to twice `io.workers`
operations can run on the source; `job push` keeps away copies by default and
so does not; see *Paged holds* in {doc}`workflow_cli`). Over the session it first
asks the destination which of the workflows a page's jobs need it lacks or has
unbuilt, installs and builds those (once a command per workflow; a failure is
reported and the jobs wait, the push goes on), and only then travels in
segments, so that each segment can start as soon as it is adopted. A segment
closes at 256 bundles, 16 GiB of file data or 16,384 archive members, whichever
comes first (a hold larger than that is a segment of its own; a hold is never
split). Each segment is its own delivery, with its own archive, landing,
synchronization, adoption and answer, and its adopted holds are released at
once after its adoption, so a failure late in a large push costs only the segments it reached.
A segment is *staged* first (the `stage` request: received and its jobs
looked for once) and adopted with an `adopt_staged` request once the
destination's visibility deadline has passed since, while later segments are
copied. A push's own segment is synchronized to disk by its `adopt_staged`,
first thing, before its jobs are looked for the second time and adopted (so
about one deadline after it landed, when the system's own writeback has mostly
run; earlier when the staged segments below fill up or the push ends); a
segment relayed from a remote source is synchronized before its `stage`
answers, so that its retry can go from the same relay. At most 8 staged segments of about 32 GiB in all wait; while those
slots do not fill before the oldest one's deadline passes, a long push waits
about one deadline in all rather than one a segment (see *Staged segments* in
{doc}`workflow_cli`). A stop gives up the staged segments not yet adopted: the
destination discards them, and their holds stay for `--resume` (they were
sent, so they are never taken back). A
push therefore costs one connection and one remote start in every case. A
failed synchronization of a segment's landing is retried once, as for any push,
and after that every later segment of the session is synchronized file by file
at once. When a push's own segment fails to synchronize in its `adopt_staged`,
the destination gives it up and keeps one retry slot open for the very next
request: the push stages the same bundles once more at once, with every file
synchronized as it is written, as that segment's retry, which the destination
puts first among its staged segments, and adopts it before any later one, so
the adoption order holds (an explicitly selected child before its parent). Any
other request gives the slot up. One write error usually fails every segment
then staged without a file-by-file synchronization (up to 8), each retried so
in turn; if a retry fails too, that segment's jobs stay held at the source for
`--resume` (they were sent, so they are never taken back). When the far side refuses one bundle's member, only that bundle is
refused (taken back when this run held it) and the rest of its segment is
delivered once more without it; when it refuses a whole segment (an unknown or
sealed workspace, a full quota, or, conservatively, a member it cannot attribute
to a bundle), nothing more is sent: this run's holds in that segment and in the
segments never sent are taken back, an earlier run's stay held, and the
segments staged before it (sent, not yet adopted) stay held too.
Before the first segment, a session that fails while the workflows are installed
has adopted nothing: the holds stay for `httk job transfer --resume`. An answer that cannot be read, a far side that dies, or a
segment answered by another workspace than the first stops the push there: that
segment's holds and every later one's stay held, and running the transfer again
(or `httk job transfer --resume`) finishes it, reporting what had arrived as
already adopted.

Over the session a pull first has the source check and resolve its selection
(a selector naming no job stops it before anything is driven), then drives the
holds an earlier run left bound for this destination, listed a page at a time,
then has the source list the selection once and hold it a page at a time, each
page driven before the next is asked for, so no answer grows with the
selection (each answer is held to 448 MiB). For a page it fetches the store
entries of the workflows the jobs need that this workspace lacks (once a
command per workflow) and installs and builds them here, all before the page's
first adoption, and then fetches the page's holds in segments of 256 bundles. The source
closes each fetch once 16 GiB of file data or 16,384 archive members went (a
hold is never split), so a segment may take several fetches, each of what is
left of it.
Each fetch lands in its own scratch directory here, is checked and synchronized
before anything of it is adopted, and is adopted once the visibility deadline has
passed since its jobs were first looked for, while later fetches go on (one
fetch is reserved before it is asked for; a fetch never splits a bundle, so one
large bundle can take the staged bytes beyond their 32 GiB until it is
adopted); the holds it adopted are released on the source right after. A
landing is synchronized only when it is adopted, first thing; when that fails,
it is discarded and fetched once more at once, with every file synchronized as
it is written, as every later fetch of the session then is, and adopted before
any later landing. A hold the source cannot send (another actor released it, or
a file of it was replaced or changed size while it was read) fails alone and the
rest of its segment is fetched once more without it. A source that dies or
answers something else, or a refusal naming no hold (a full quota here, say),
stops the pull: nothing of that fetch was adopted, its holds and every later one
stay held there, and no later page is held; `httk job transfer --resume` lists
the holds over a new session and drives them (run the transfer again to select
what was never held). A hold too large to describe in one answer (448 MiB as
encoded; at about 40 to 60 bytes a job, a tree of several million jobs, more
than a bundle can hold, see below) fails alone and stays held on the source,
and its message names the remedy by hand: copy its bundle
`.httk-workspace/transfers/outgoing/<transfer id>/` from the source to the
destination's machine, run `httk job adopt BUNDLE` in the destination
workspace, then `httk job transfer --release TRANSFER_ID REMOTE:WS`; with
`--keep`, `httk job transfer --release TRANSFER_ID --keep --destination-id ID
REMOTE:WS`, ID being the `workspace_id` that `httk workspace status --json` of
the destination reports (a hold of a remote source records no destination id,
and a release without one keeps no away copies). A failed release leaves the
jobs here with their holds on the source, reported with the remedy.

Transfers to and from a remote workspace require the transfer session: there
is no other way for a transfer to reach a remote. Opening it checks the far
side before anything is selected, held or changed on either end: its hello must
name this side's transfer protocol (a single version; there is no range and no
optional request), this side's first request names it in turn, and the far
side refuses any other version before it does anything, and a round trip of all
256 byte values must come back intact. When that fails, the transfer stops with
a nonzero exit, nothing held and nothing changed on either end, and one of four
messages:

- a far side of another protocol version (older or newer: another hello, or a
  refusal of this side's protocol naming its own), or a far-side `httk workflow`
  without the command (an older one, whose argparse refuses it with `httk
  workflow: error: argument GROUP: invalid choice: 'transfer-session'` and exit
  status 2): "the httk on REMOTE is too old (or too new) for
  this transfer: install the same httk version on both ends";
- a far side whose shell ends with exit status 127 (no `httk` there, or a
  command in its `cli_prelude` not found): "`httk` was not found on REMOTE, or a
  command in its cli_prelude was not found (exit status 127; DETAIL)", with the
  remedy: make `httk` reachable from a
  non-interactive shell there, or configure the remote's `httk_command` or
  `cli_prelude`;
- an adapter that refuses the `session` operation (a `mount-daemon` remote,
  which moves jobs through its exchange with `job eject` and `job adopt` instead),
  answers it with its own result document, or whose channel does not carry binary data intact: "the adapter of REMOTE
  cannot open a transfer session";
- anything else, such as a far side of this protocol refusing for a reason of
  its own (its error is kept), a crash, a session that
  does not answer in time, or a refused or broken ssh connection (ssh's own exit
  status 255): "could not open a transfer session to REMOTE: DETAIL".

Holds an earlier run left stay held, untouched, and `--resume` drives them once
both ends run the same *httk* (or the cause of the third kind is gone). The remote setting `transfer_session` was
removed with the transfers that did not use a session: `httk remote configure
--set transfer_session=…` is refused as an unknown setting, and a value an
earlier version persisted is ignored (`--unset transfer_session` removes it).

Once open, a session is never reopened: a request that fails (the far side
ends, breaks the protocol, or fails the request fatally) has an unknown
outcome. The adapter timeout (`--adapter-timeout`, else the remote's
`timeout_seconds`) bounds each wait, not a whole request: a request fails when
the far side sends nothing and takes no data for that long, or when neither its
reported progress nor any data moves for that long outside a filesystem
synchronization (a landing's first `syncfs`, the end of its extraction with the
fsyncs of its directories, its commit), the wait of the destination's
visibility deadline before adopting (what is left of it for a staged segment), or a build. A landing's synchronization
covers the whole end of its extraction, however it ends (the writes still
running, the directory times and fsyncs, or the discard of a refused landing):
a far side hung there, still reporting its progress, is caught only once it
stops answering altogether (the idle bound), an accepted limitation. The far side reports progress every 10 seconds, so
the bound is at least 30 seconds whatever is configured. Time in which this side
offers nothing (a slow reader of a download, a slow source of an upload) is not
held against the far side. This side holds at most 512 MiB of what the far side
sent before it stops reading, and no single response document may be larger;
an answer that large (a page of holds, or the adoption of a tree of about a
million jobs) takes a few GB of Python objects to read on each side, an accepted
cost at that scale. A tree whose adoption answer exceeds it (an accepted
limitation) is still copied and adopted, but its answer cannot be read: the push
reports an unknown outcome and keeps the hold, and `httk job transfer --resume`
completes it, its second delivery answered "already adopted" (a short answer)
and the hold released, at the cost of copying the tree a second time; `httk job
eject` and `httk job adopt` by hand avoid that second copy.

What bounds the size of one tree: its `bundle.json` (about 220 bytes a member)
and an adoption's `plan.json` and `replaces.json` are held to 512 MiB, about
2.4 million members, and a tree whose `bundle.json` would be larger is refused
before any job moves; its description in an answer of holds takes about 40 to
60 bytes a job (448 MiB); the answer of its adoption about 400 to 450 bytes a
job (512 MiB). A push, and a transfer between two remotes, therefore moves a
tree of up to about a million jobs in one run, bounded by the adoption answer
(a larger one needs the `--resume` above); a pull, which adopts here, up to the
2.4 million of `bundle.json`. These figures grow
with the length of the job keys and the destination's paths.

Staged segments belong to their session: their tokens mean nothing on another
connection, and a `bye` (before the far side answers it) or the end of its
input discards those whose adoption had not begun; a far side that ends
otherwise leaves them to recovery. A connection that stays half open keeps them, and their
owners, until the transport notices (an accepted limitation, as for a request in
flight); a far side that dies leaves them to recovery, which discards them.

A failed or closed session kills the far side together with everything it
started. A session opened from a terminal, so that ssh can prompt, is the
exception: only the adapter process itself is killed, so a process it started
that keeps its pipes open can delay the end of the session by a few seconds and
outlive it.

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

A child job spawned by another job travels with its parent. `job push` and
`job pull` always move a root together with its spawned descendants, with
their placements kept; `job transfer` does so with `--tree` and without it
refuses a job whose descendants are present. Every descendant must be paused or
terminal, so a pull of finished work brings a campaign back whole once it is
done. A root that finished before its children (an `any_terminal`,
`any_succeeded` or `at_least` join) is refused and stays, never split, until
they finish; to move a tree still in flight, pause its unfinished children
first. A child that should leave alone is detached first, because a child may
read its parent's files in place (see {doc}`composing_workflows`):

```console
$ httk job pull kappa:runs
$ httk job detach CHILD
$ httk job push --job CHILD kappa:runs
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
  one shell command line on the remote and relays its stdin, stdout, stderr and
  exit status. A push streams its bundles on the executor's stdin into the
  remote adoption, so the executor must pass stdin through (no `docker exec`
  without `-i`, no wrapper that redirects `</dev/null`). An executor that drops
  stdin makes the adoption read an empty stream, which it refuses: nothing
  arrives, and the transfer reports each hold `not_moved` (a local SRC takes back
  the jobs this run held) or, from a remote SRC, `failed` with the holds kept;
  a hold left by an earlier run is never taken back (`failed`, held).

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
to configure it anyway). The `cli_prelude` and `httk_command` settings mean the same
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
UUIDs, keeping your job UUID as its exchange name. One submission's
`bundle.json` may take at most 1 MiB, about 5,000 jobs; `job eject` into an
inbox refuses a larger tree before any job leaves (submit it in parts). The
fresh UUID keeps the
first eight hex digits of yours, so a unique prefix such as
`httk job show 8f7047f2` selects the job in both workspaces. Its workflow must be
installed in the workspace, an operator task; until then the adopted job waits.
`job adopt --move` copies the
returned tree, verifies it and removes the source from the exchange.
`--configuration` names one of the global `slurm` launchers the operator
approved for the daemon (each sets `manager.confine=bwrap`); `daemon.json`
lists them with their digests and published resource settings (task layout,
CPUs, memory, GRES, partition, workers and time limits), and `show` prints it. Every unrestricted confined manager of the workspace (no `--placement` or pool restriction), and
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
