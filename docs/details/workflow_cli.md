# Project and workflow command line in detail

This is the complete command reference, from workspaces and jobs to projects,
signed manifests, and remotes. Installing *httk-workflow* registers lazy
`config` (Setup); `workspace`, `remote`, `launcher`, `daemon` and `campaign`
(Organize); `workflow`, `job`, `run`, `monitor`, `push`, `pull` and `manager`
(Run); `collect` and `postprocess` (Results); and `v1` (Legacy) commands with
*httk-core*, which lists them under those headings in `httk --help`:

```console
httk workflow --help
```

`httk workflow …` covers workflow installation, inspection and building;
workspace and job management are the top-level trees `httk workspace …` and
`httk job …`, running managers is `httk run`, and collecting results is
`httk collect`. Every group and command answers `--help`,
and a mistyped action is reported by its group. {doc}`taskmanager` explains
what the commands do to a workspace.

Short forms: `-w` is `--workspace`, `-s` is `--state`, `-n` is `--dry-run`,
`-p` is `--parameter` and `-i` is `--input`, wherever the long option exists.
`--yes` pre-confirms a command that would otherwise ask on the terminal (a
`delete`, `remove` or `unseal`, `workspace repair`); `--force` always
overrides a safety check instead.

## The command tree

```text
httk workspace          show | set | unset | init | register | list | default | move | forget | delete | status | owners (managers) | attest-dead | check | repair | gc | seal | unseal | verify
httk job new            SOURCE | --command TEMPLATE [--campaign [--partition NAME] [--campaign-index N]] [job options]
httk job                new | submit | cancel | pause | resume | restart | set | delete | seal | unseal | detach | unlock | eject | adopt | list | show | log | why | precheck | debug | verify | move
httk push               [SRC] DST [--job JOB] [--state STATE] [--step STEP] [--placement P] [--no-keep] [--no-workflows] [--json]
httk pull               SRC [DST] [--job JOB] [--state STATE] [--step STEP] [--placement P] [--no-workflows] [--json]
httk collect            [PATH] [--workspace WORKSPACE] [--into PATH] [--dry-run] …   (--campaign [--partition NAME]…: the campaign's partitions)
httk workflow list      [--workspace WORKSPACE] [--available] [--json]
httk workflow show      TARGET… [--workspace WORKSPACE] [--json]
httk workflow install   SOURCE… [--workspace WORKSPACE] [--no-calls] [--no-build] [--json]
httk workflow uninstall SELECTOR… [--workspace WORKSPACE] [--check] [--json]
httk workflow build     [--workspace WORKSPACE] [--list] WORKFLOW…
httk workflow fetch     URI… [--json]
httk workflow forget    SELECTOR… [--json]
httk run                [--workspace WORKSPACE] [manager options]   (--campaign [--partition NAME]…: a manager per partition; a mount-daemon workspace: --configuration NAME [--time-limit D] [--request-id ID] [--wait-seconds S])
httk manager            status | log | cancel   (the managers a mount-daemon remote's daemon started)
httk daemon             init | set | unset | show | check | run | reload   (the confined workspace daemon, on its host)
httk launcher           list | add | set | unset | show | check | remove
httk monitor            [--workspace WORKSPACE …] [--refresh SECONDS]
httk postprocess        SCRIPT [JOB…] [--workspace WORKSPACE]
httk v1                 collect
httk remote             list | add | set | unset | check | import-v1 | show | remove | take-back
httk config             show | set | unset | import-v1
httk campaign           init | show | set | unset | remove   (job new, collect and run take --campaign)
httk setup | identity    (core-owned: per-user configuration and named operator identities)
httk project            init | show | import-v1 | export | check | repair | manifest | verify | seal | unseal   (core-owned)
```

### Configuration conventions

Concept groups live directly below `httk`: `httk launcher`, `httk remote`,
`httk campaign` and `httk config`. `workflow` retains workflow installation,
inspection and building, and follows the workspace default like every other
command (`list` and `show` never create it); only `list --available`, `fetch`
and `forget` address this machine.

Use `show [--json]` to inspect configuration. Workspaces, launchers, remotes,
the per-user configuration and campaigns change it with `set KEY=VALUE...`, which
adds or replaces values, and `unset KEY...`, which removes overrides. Supported
list settings also take `--add KEY=VALUE` and `--remove KEY=VALUE` on `set`;
these change individual members, whereas `unset` removes the whole override.
One `set` applies its pairs, then `--add`, then `--remove`, and saves only when
every change is valid. A workspace's `show`, `set` and `unset` take the
workspace with `-w/--workspace` and address one key namespace: application
settings, `policy.*` members and `prelude.WORKFLOW_ID` preludes. The daemon
configuration follows the same grammar with the daemon's workspace path as its
object: `daemon set WORKSPACE KEY=VALUE... [--add K=V] [--remove K=V]` and
`daemon unset WORKSPACE KEY...`.

| Configuration | Inspect | Modify | Remove the definition |
| --- | --- | --- | --- |
| Workspace application settings | `workspace show [KEY...]` | `workspace set KEY=VALUE...`, `workspace unset KEY...` | `workspace forget NAME` keeps files; `workspace delete NAME` deletes them after confirmation |
| Workspace policy | `workspace show policy.KEY` | `workspace set policy.KEY=JSON...`; `workspace unset policy.KEY` restores the default | Required format members cannot be deleted |
| Workflow preludes | `workspace show prelude.WORKFLOW_ID` | `workspace set prelude.WORKFLOW_ID=TEXT\|@FILE`, `workspace unset prelude.WORKFLOW_ID` | Unset a workflow's prelude |
| Installed workflows | `workflow list`, `workflow show` | `workflow install` | `workflow uninstall` |
| Launcher | `launcher show NAME` | `launcher set NAME KEY=VALUE...`, `launcher unset NAME KEY...`; `set --add` / `--remove` for confinement path lists | `launcher remove NAME` |
| Remote | `remote show NAME` | `remote set NAME KEY=VALUE...`, `remote unset NAME KEY...`, including private credentials | `remote remove NAME` |
| User configuration | `config show` | `config set KEY=VALUE...`, `config unset KEY...`; `set --add` / `--remove` for `machine_names` | Unset its configurable keys |
| Campaign | `campaign show` | `campaign set partitions.NAME=WORKSPACE`, `campaign unset partitions.NAME`; set or unset `assignment` | `campaign remove` clears the map, keeping jobs and workspaces |
| Workspace daemon | `daemon show PATH` | `daemon set PATH KEY=VALUE...`, `daemon unset PATH KEY...`; `set --add` / `--remove` for launchers and authorized keys | Enrollment removal is not provided |

A workspace `set` or `unset` validates every change and stores them in one
write of the workspace's `format.json`, so either all of them apply or none
does; on a remote workspace it is one forwarded command. Removing daemon enrollment or disabling the exchange is not
offered: exchange clients, running managers and retained submission state must
be settled first.

### Workspace selection

A `WORKSPACE`, positional or given with `--workspace`, is a registered name,
`REMOTE:NAME` for a workspace on a remote, or else the path of a local
workspace directory (one holding `.httk-workspace/`, relative to the current
directory; no upward search). A registered name wins over a same-named
directory, so `./NAME` addresses the directory. The registry verbs
`workspace forget`, `delete`, `move` and `default` take registered names only.
When the workspace is omitted, commands resolve it in this order: the closest
enclosing workspace containing `.httk-workspace/format.json`, the project's
recorded default, the registry's default, then an auto-created per-user
default.

The registry is machine-owned and stores only absolute local paths, in
`$XDG_CONFIG_HOME/httk/workspaces.json`. When a command reaches a remote
workspace, the far side resolves the plain name in its own registry.

Remote-capable commands (workspace status, settings, `gc`, `check`, `repair`,
`job list`, `show`, `log`, `why`, `request` and `delete`, `workflow list`,
`show`, `install`, `uninstall` and `build`, and `run`) go through the adapter. Jobs are created locally, moved with
`push` and brought back with `pull` (or `job move` in general).

## Workspaces

### `workspace` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init PATH [KEY=VALUE...]` | create a workspace in a new or empty directory of its own (`REMOTE:PATH` on a remote), or adopt an existing one, registering its name (basename or `--name`) and recording it in the project's `members.json`; the pairs seed application settings | `--name`, `--no-durable` |
| `workspace list [--json] [REMOTE:]` | list local or owning-machine workspaces | |
| `workspace default [--unset] [NAME]` | read or record this project's default name | |
| `workspace register [PATH...]` | register copied workspaces on this machine under the names their project's `members.json` records | `--name` (one path only), `--json` |
| `workspace move NAME NEW_PATH` | move a workspace on its filesystem and update every local record of its path (registry, project members, a configured daemon's enrollment); refused while its daemon runs, an owner is not proven dead or daemon manager jobs may still run | `--record-only` (the directory is already at NEW_PATH), `--no-durable` |
| `workspace forget NAME...` | deregister names, leaving workspaces on disk | |
| `workspace delete NAME...` | destroy workspaces and deregister them, after confirmation | `--yes` skips the confirmation |
| `workspace status [NAME...]` | job counts by state, the seal, the owners with their liveness, and the held transfers (transfer id, root job, members, destination, age; `transfers` in `--json`; a hold whose manifest cannot be read is named, `transfers_unreadable` in `--json`, without changing the exit status) (remote: over the adapter) | `--json` |
| `workspace owners [NAME...]` | list the owners (managers, CLI processes, daemons) with their liveness; `managers` is an alias | `--kind manager\|cli\|daemon`, `--json` |
| `workspace attest-dead OWNER [NAME] --reason TEXT` | declare an owner and its launches ended, so the next manager tick or `workspace gc` recovers its jobs | `--operator NAME`, `--force` |
| `workspace show [KEY...]` | print every key (application settings with a column marking each recognized, unrecognized or undeclared, then `policy.*`, then `prelude.*`), or the selected keys; one KEY prints its bare value | `-w`, `--json`, `--adapter-timeout` |
| `workspace set KEY=VALUE...` | store application settings, policy members and preludes in one validated write, printing the changed keys as one JSON object; warns about unrecognized settings and notes undeclared ones (remote: one forwarded command) | `-w`, `--no-durable`, `--adapter-timeout` |
| `workspace unset KEY...` | remove application settings and preludes and restore policy defaults in one write, printing the keys as a JSON list (remote: one forwarded command) | `-w`, `--no-durable`, `--adapter-timeout` |
| `workspace check [NAME...]` | check the job tree, read-only, for what the kernel cannot resolve; only while nothing else uses the workspace | `--json` |
| `workspace repair NAME...` | check like `workspace check`, then quarantine unparsable entries and fix the exchange index, after confirmation; quiescent workspace only | `--yes`, `--json` |
| `workspace gc [NAME...]` | collect what the retention policy allows and recover dead owners (remote: over the adapter) | `--dry-run`, `--category CATEGORY` (repeatable), `--json` |
| `workspace seal [NAME...]` | record every job's seal digest under one signed workspace seal, listing the unsealed jobs | `--keys REFS` |
| `workspace unseal [NAME...]` | remove a workspace's seal, refused while its project is sealed | `--yes` skips the confirmation |
| `workspace verify [NAME...]` | verify a workspace seal and list the jobs that drifted since it; drift exits `1` | `--trusted-key`, `--json`, `--deep` (also verify each recorded job seal), `--content` (implies `--deep`, also checks uncompressed digests) |

### Creating, moving, and removing workspaces

`workspace init` creates and registers an explicit workspace. The workspace is
a directory of its own, never a project root: `init` refuses a non-empty
directory. *httk-workflow* owns its top level: `.httk-workspace/`, `jobs/`
(every job is `jobs/<state>/<placement>/<job_key>~p<NNN>~<token>`, or flat below
`jobs/owned/<owner-id>/` while an owner holds it), `workflows/`, `logs/`,
`postprocess/` and, once enabled, `exchange/`. A canonical path may have only
one registered name.

```console
httk workspace init --name my-workspace runs/my-workspace
```

A workspace on a cluster is addressed as `REMOTE:NAME`. Its owning machine
chooses the path given to `workspace init REMOTE:PATH`.

`workspace move NAME NEW_PATH` renames the workspace directory to `NEW_PATH`,
which must not exist, in one atomic same-filesystem rename, then updates every
record of its path on this machine: the registry entry (same name), the
`members.json` of the project it leaves and of the project it joins, and a
configured daemon's enrollment (the active runtime snapshot is published again
naming the new path, and `exchange/daemon.json` is reinstalled; identities,
launchers and keys are unchanged). It refuses while the workspace daemon runs
(stop its `httk daemon run` process first), while any other owner of the
workspace is not proven dead (it lists the owners and the jobs they hold:
stop its managers first), while daemon manager jobs may still be queued or
running on the old path (let them end and a daemon observe them), while the
enclosing project is sealed, and when `NEW_PATH` is registered under another
name. A daemon manager start answered `uncertain` (sbatch failed after it may
have accepted the job) never blocks, since nothing reconciles it; the move
warns of it instead: if Slurm accepted that job, it fails on the old path, so
look for it with `squeue --name=httk-<handle>` and cancel it with `scancel`. A
destination project that cannot record the workspace (it is sealed, or the
name is taken there) leaves the move done and is reported as `not recorded in
<members.json>`. It prints what it updated and what names the old
path elsewhere and must be updated by hand: scripts and service units, manager
jobs a launcher queued in a scheduler before the move (they fail when they
start; start them again), and a mounted exchange path set on a `mount-daemon`
remote, which `httk remote set REMOTE exchange=PATH` re-pins. Remotes on other
machines address the workspace by name and need no change. Requests a client
signed for the previous daemon configuration are refused as
`stale_configuration`, as after a `daemon reload`; clients read the new
configuration digests from `daemon.json`. Records already collected into a store keep
the path the job had when it was collected.

Across filesystems the rename is refused: copy the directory with a copy that
keeps modes and ownership (`cp -a`, `rsync -a`; a daemon's private directory must
stay mode 0700 and owned by its user), remove the original, then run
`workspace move --record-only NAME NEW_PATH`, which only updates the records;
the old path must no longer be a workspace. `--record-only` cannot check that
`NEW_PATH` is the same workspace, since the registry records no workspace id;
name the right directory. Every step is
repeatable, so `--record-only` also completes a move that was interrupted after
the rename. `workspace forget` only removes the name.

### Sealing a workspace

Sealing is described in full in {doc}`sealing`. `workspace status` shows a
`sealed` line (and JSON field). `workspace seal` records the seal digest of
every job, recording and listing jobs without a seal as unsealed; `workspace
verify` reports drift since then. A sealed workspace refuses modifying CLI
commands; managers and jobs do not check it.

### Application settings and preludes

Application settings are a flat map of small dotted values a runner resolves
when it runs, such as `vasp.command`; a value that parses as JSON is stored as
that scalar, a bare word as a string. `workspace init PATH KEY=VALUE...` seeds
them, and a workspace bound to a remote is also seeded from that remote's
whitelisted settings (a cluster's `vasp_command` becomes `vasp.command`). They
are snapshotted into each attempt, so never store credentials there; see
[application settings](taskmanager.md#application-settings).

```console
httk workspace set -w my-workspace vasp.command=vasp_std \
  environment.prelude="module load httk" prelude.relax-vasp=@prelude.sh
httk workspace show -w my-workspace prelude.relax-vasp
```

`environment.prelude` applies to every job and `prelude.WORKFLOW_ID` only to the
jobs of one workflow id, after it; both run under `set -e`. A workflow prelude
value is stored verbatim, never parsed as JSON, and `@FILE` reads it from a file
(on the far side for a remote workspace, which receives the pairs as given). Preludes are
workspace-local: a transferred job runs under the destination's preludes. See
[environment preludes](../workspaces.md#environment-preludes).

### Policy, owners, and checks

The workspace policy (`policy.visibility_deadline_seconds` and the
`policy.retention.*` limits) is stored in `format.json`; see
[workspace policy](taskmanager.md#workspace-policy). `workspace owners` lists
who holds jobs, with the liveness the read-only death proof gives from this
host. An owner whose death no probe can prove is declared dead by the operator:

```console
httk workspace attest-dead OWNER WORKSPACE --force --reason "node rebooted"
```

The death proof runs first: an owner proven alive is refused, and one it cannot
decide needs `--force`. Attesting an owner that is still running can apply a
request twice and run work twice, so attest only after confirming that the owner
process and every launch it started are gone; see
[owners, death and recovery](taskmanager.md#owners-death-and-recovery).
`workspace check` and `workspace repair` must run only while no manager, CLI
operation, daemon or transfer uses the workspace; findings from a busy
workspace may be transient, and `repair` is refused until every owner is proven
dead and recovered by `workspace gc`. `repair` asks for confirmation first;
`--yes` answers it, and is required without a terminal. Checking and
`workspace gc` are described in
[checking a workspace](taskmanager.md#checking-a-workspace) and
[freeing disk](taskmanager.md#freeing-disk).

## Workflows and builds

A job runs only a workflow installed in its workspace. Installed workflows live
in `workflows/<slug>--<h16>/` and are selected by id, short name (the most
recently installed one of that name) or `NAME@REF` (see
[Short names](workflow_uris.md#short-names)).

### `install`, `uninstall`, `fetch` and `forget`

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow install SOURCE...` | install into a workspace: a package directory, a git URI, a runner file or workflow document (installed ad hoc), or a workflow name known here; a git URI without `#SUBDIR` whose repository root carries `httk_plugin.toml` installs every workflow that plugin lists; declared calls are installed too (call slot defaults never are: a `notice:` names each one not installed) and a `[workflow.build]` package is built | `--workspace`, `--no-calls`, `--no-build`, `--language`, `--json`, `--adapter-timeout` |
| `workflow uninstall SELECTOR...` | remove installed workflows by id, short name or `NAME@REF`; a selector several installations share is refused | `--workspace`, `--check` (warn about unfinished jobs using it), `--json` |
| `workflow fetch URI...` | fetch git workflows into this machine's cache, without a workspace | `--json` |
| `workflow forget SELECTOR...` | forget fetched git workflows on this machine (a pinned URI removes that commit, an unpinned URI or a short name its whole lineage) | `--json` |

```console
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp-atomate2@wf-v0.2.0'
httk workflow install --workspace kappa:runs ./my-workflow
```

The command prints `ID<TAB>NAME` per installed workflow. A git source is
installed under its canonical URI pinned to the full commit. A `REMOTE:WS`
workspace gets the source pushed through the remote's adapter (a git source is
fetched there by its pinned URI). Without `--workspace`, `install`,
`uninstall` and `build` use the default workspace like every other command,
creating the per-user default when it is missing (as `job new` does); when the
project's recorded default does not resolve, `install` names `workflow fetch`
and `workspace init`. See {doc}`/details/workflow_uris`.

### `list` and `show`

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow list` | the workflows installed in the workspace; with `--available`, the workflows a name selects on this machine (registered, plugin, pip-installed workflow repositories, fetched git workflows) | `--workspace`, `--available`, `--json` |
| `workflow show TARGET...` | describe a target's installation in the workspace; a target the workspace does not hold (or every target, when there is no workspace) as resolved on this machine: a registered id or alias, git URI, runner file or package directory | `--workspace`, `--language`, `--json` |

`list` and `show` are inspections and never create a workspace: without
`--workspace` they use the enclosing workspace, the project's default, or the
per-user default only when it already exists. With none of them, `list` fails
with a hint naming `workflow list --available` and `workspace init`, and `show`
describes every target as this machine resolves it.

Each description starts with `described from: workspace WS` or
`described from: this machine` (`described_from` and `workspace` in `--json`).
As a report, `show` runs a directory package's runner entry with
`--describe`. When the manifest's declared `steps` disagree with what the
runner reports, it prints a prominent `WARNING: step drift` line
(`manifest_step_drift` in `--json`), but still exits `0`. Package authoring is
documented in {doc}`workflow_packages`.

### `build`: foreground builds of installed workflows

```console
httk workflow build [--workspace WORKSPACE] [--json] WORKFLOW...
httk workflow build [--workspace WORKSPACE] [--json] --list
```

`build` builds installed workflows (by id, `NAME@REF` or short name, which
selects the newest installation of that name) that declare
`[workflow.build]` for this platform, in the foreground, and registers the
artifacts in the installation's `builds/<platform>/`; a `REMOTE:WS` workspace
builds on the remote. `install` already builds for the platform it runs on; run
`build` on each other platform whose managers serve the workspace. `--list`
prints the registered builds. A target's installed call slot defaults, and the
workflows they call, are built with it; a default that is not installed is
skipped. The exit status is `1` when any target failed.
The manager passes the artifacts to the runner through
`HTTK_WORKFLOW_RUNNER_ARTIFACTS`, and a job whose workflow is not built for the
manager's platform stays `ready` and unclaimed there.

## Jobs

### `job` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new SOURCE\|--command TEMPLATE` | scaffold and submit jobs of a workflow | `--workspace`, `--campaign` (with `--partition`, `--campaign-index`), `--install`, `--embed-workflow`, `--call`, `--call-parameter`, `--parameter`, `--environment`, `--language`, `--input`, `--input-move-from-job`, `--input-from`, `--file`, `--files`, `--tag`, `--placement`, `--priority`, `--start-step`, `--duplicate-tag`, `--json` |
| `job submit SOURCE...` | submit prepared payload directories | `--workspace`, `--move`, `--json` |
| `job cancel\|pause\|resume\|restart JOB...` | post one `cancel`, `pause`, `continue` or `restart` request per job (remote: signed envelope or typed daemon command) | `--workspace`, `--state`, `--step`, `--placement`, `--operator`, `--reason` (default: the command), `--adapter-timeout`, daemon-only `--request-id`; `pause`: `--wait`, `--timeout`; `resume`: `--tree`; `resume`, `restart`: `--force`, `--keep-budget` |
| `job set JOB... KEY=VALUE...` | change jobs through requests: `workflow=`, `budget.max_attempts_per_activation=`, `budget.max_total_attempts=`, `budget.max_activations=`, `priority=`, `step=` | as the request verbs; `--force`, `--keep-budget` with `step=` |
| `job delete JOB...` | delete terminal or paused jobs (remote: over the adapter; a workspace daemon: a signed request) | `--workspace`, `--yes`, `--operator`, `--adapter-timeout`, daemon-only `--request-id` |
| `job seal JOB...` | seal succeeded jobs that carry no seal | `--workspace`, `--operator`, `--adapter-timeout`, daemon-only `--request-id` |
| `job unseal JOB...` | release succeeded jobs from their seal, so they may be deleted | `--workspace`, `--yes` skips the confirmation, `--operator`, `--adapter-timeout`, daemon-only `--request-id` |
| `job detach JOB...` | make spawned jobs independent of their parents, permanently | `--workspace`, `--operator`, `--adapter-timeout`, daemon-only `--request-id` |
| `job unlock [--no-copy] JOB...` | put jobs, and every ancestor of them, in debug mode (one-way) and, unless `--no-copy`, give each selected job its own copy of the runner to edit | `--workspace`, `--no-copy`, `--reason TEXT` (default `httk job unlock`), `--operator`, `--adapter-timeout` |
| `job eject JOB [DEST]` | move a quiescent job out into the bundle `DEST/<job-key>` (remote: an eject request; for a workspace daemon `DEST` is under its outbox, the outbox itself when omitted) | `--workspace`, `--tree`, `--wait`, `--timeout`, `--operator`, `--json`, `--adapter-timeout`, daemon-only `--request-id` |
| `job adopt BUNDLE...` | move ejected bundles into the workspace, in turn | `--workspace`, `--move`, `--json` |
| `job list` | list jobs a page at a time (remote: over the adapter) | `--workspace`, `--state`, `--step`, `--placement`, `--limit`, `--after`, `--tag-contains`, `--counts`, `--json` |
| `job show JOB...` | describe jobs from their documents (remote: over the adapter) | `--workspace`, `--no-children`, `--json` |
| `job log JOB...` | print run logs (remote: over the adapter) | `--workspace`, `--limit`, `--json` |
| `job why JOB...` | explain why jobs are not progressing (remote: over the adapter) | `--workspace`, `--json` |
| `job precheck` | check pending jobs before they start, without changing the workspace | `--workspace`, `--placement`, `--json` |
| `job debug JOB` | drive one job to a terminal state in the foreground | `--workspace`, `--start-step`, `--placement`, `--follow-children`, `--timeout`, `--log-level` |
| `job verify JOB...` | verify job seals against their payloads; a JOB that is a payload directory (holding `job.json`) is verified where it is, also outside any workspace | `--workspace`, `--content`, `--trusted-key`, `--json` |
| `httk push [SRC] DST` | move the `ready` jobs (as `job move --tree --state ready`); the omitted SRC is the enclosing or default workspace | `--job`, `--state`, `--step`, `--placement`, `--json`, `-q`/`--quiet`, `--adapter-timeout` |
| `httk pull SRC [DST]` | move the `succeeded` jobs (as `job move --tree --state succeeded`); the omitted DST is the enclosing or default workspace | `--job`, `--state`, `--step`, `--placement`, `--json`, `-q`/`--quiet`, `--adapter-timeout` |
| `job move SRC DST` | move jobs between two workspaces | `--job`, `--state`, `--step`, `--placement`, `--tree`, `--resume`, `--release`, `--destination-id`, `--json`, `-q`/`--quiet`, `--adapter-timeout` |

When giving more than one `JOB`, name the workspace explicitly.

### Selecting jobs

`JOB` is a job UUID, a `tag--uuid` job key, any unique prefix of either, or a
path inside the workspace. A job directory names one job; a placement directory
names every job below it; and a glob expands to matching entries. Relative
paths are resolved from the current working directory and must remain inside
the selected workspace. A job's directory name changes at every move, so
scripts should use UUIDs or keys.

Remote `job show`, `job log`, `job why` and `job delete` accept canonical
lowercase job UUIDs only; resolve keys and prefixes locally first.

### Creating jobs

`job new SOURCE` scaffolds and submits jobs of a workflow installed in the
workspace; `job new --command TEMPLATE` generates the workflow instead of
SOURCE. See {doc}`/quickstart`.

```console
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workspace WORKSPACE vasp.relax --input structure=POSCAR --tag silicon
httk job new --workspace WORKSPACE vasp.relax --input-from structure structures/ --parameter kpoint_density=30.0 --placement project/screening
httk job new vasp.relax --input structure=POSCAR --parameter publish_data=true
httk job new ./my_runner.py --start-step characterize --parameter sites=8
httk job new --command 'my_executable {input}' --file input=input_files/a.dat --tag a
```

SOURCE is told apart as `httk workflow install SOURCE` tells its kinds apart,
in this order: an existing runner file, workflow document or package directory
(holding `httk_workflow.toml`), then a git URI
`git+https://HOST/PATH[@REF][#SUBDIR]`, then a name. A directory without
`httk_workflow.toml` is read as a name (unless `--language` names a directory
format), so a directory that happens to share a workflow's name where the job
is created does not hide the workflow; give `./NAME/` a manifest, or another
name, to mean the directory. A name is an installed workflow's id or short name
(the most recently installed one of that name), `NAME@REF` by the tag, branch
or commit prefix it was installed from (see
[Short names](workflow_uris.md#short-names)), or a registered or packaged
workflow name. A workflow that is not installed is refused, unless `--install`
installs it first (with its calls); a package directory is installed again when
its sources differ from the installed ones, compared by the installed digest's
rules. A runner file, command template or bare workflow document is always
installed ad hoc, as `adhoc:<name>@<digest>`. The job records the installed
workflow's id, pinned to the full commit for a git workflow.

A runner file given as SOURCE is run in describe mode to infer its workflow and
initial step, so it must call `httk_workflow_runner WORKFLOW STEP...` before
any work. Give SOURCE before `--input-from` (or after `--`): `--input-from` takes
every word up to the next option, so a SOURCE after it is read as an input
file, and `job new` then names that word. A SOURCE that looks like a path (it
holds `/` or ends in `.py`, `.sh`, `.bash`, `.cwl`, `.json`, `.yaml` or `.yml`)
but does not exist is refused as `workflow install` refuses it, not looked up
as a name.

Other options:

- `--embed-workflow` embeds the workflow in the job's `<job>/workflow/` instead
  of naming an installation (see {doc}`workflow_packages`, *Embedded workflows*).
  It works with every SOURCE and with `--command`, installs nothing, and is
  refused together with `--install`.
  The job's workflow id is `embedded:<name>@<sha12>`. Language realizations,
  `[workflow.build]` packages and uninstalled fixed callees are refused, and the
  tree must hold no symlink, special file, hard-linked file or `__pycache__`.
- `--parameter NAME=VALUE` supplies an implementation parameter; VALUE is JSON
  when it parses as JSON, and `NAME=@FILE` reads a JSON file. For example, the
  workflows-vasp workflows keep their results in the persistent workdir `run/`
  and copy a curated set into `data/` only with `--parameter publish_data=true`.
- `--environment NAME=VALUE` overrides one declared workflow environment entry.
- `--call ALIAS=WORKFLOW` binds the workflow's call slot ALIAS to WORKFLOW
  (a workflow name or commit-pinned git URI, like a `[workflow.calls]`
  reference (never a path), installed first with `--install`, which also
  installs an unbound slot's default) instead of its
  default; `--call-parameter ALIAS NAME=VALUE` gives the bound workflow one
  parameter, spelled as for `--parameter`. Both repeat. See
  [call slots](composing_workflows.md#call-slots).
- `--language LANGUAGE` selects the language of a bare document (see
  [Running workflow documents](#running-workflow-documents)).
- `--priority` (0 is highest) and `--start-step` override the workflow's defaults.
- `--duplicate-tag {refuse,skip,allow}` decides what happens when a job with
  the same `--tag` already exists in the same placement (exactly that
  placement, not its subtree) in any state, away and running included. The
  default `refuse` fails with `a job with tag 'TAG' already exists in placement
  'PLACEMENT': KEY (STATE); pass --duplicate-tag allow to create another or
  --duplicate-tag skip to skip it` (the API raises
  `httk.workflow.DuplicateTagError` with `tag`, `placement` and `existing`).
  `skip` leaves the existing job, warns `skipped tag TAG: already KEY (STATE)`
  and goes on; a batch ends with `submitted N jobs (K skipped)`, and
  `new_job(..., duplicate_tag="skip")` returns `None`. `allow` creates another.
  Jobs without a tag are never checked, `job submit` and spawned children are
  unaffected, and deleting a job frees its tag. The check precedes the
  creation, so two creators racing on one tag can both succeed;
  `workspace check` notes jobs sharing a tag in one placement (a `noted`
  finding `duplicate_tag`, not a failure). Re-running a creation script with
  `skip` thus creates only the jobs that do not exist yet, wherever the
  existing ones are, as *httk* v1's fixed task directories did.

The command prints one tab-separated `job_key<TAB>directory` line per job, or a
report with `--json`. The directory is where the job sits now; it changes as
the job moves between states.

### Inputs, files, and batches

- `--input NAME=PATH` stages one declared input.
- `--input-move-from-job NAME=PATH` moves (renames, never copies) PATH into
  the declared input NAME's destination. PATH must be a regular file (not a
  symlink) inside the payload of a succeeded job of the same workspace, not
  `job.json`, `state.json`, `workflow/` or anything below `attempts/`, `logs/` or
  `.httk-job/`, and on the workspace's filesystem; anything else is refused.
  A sealed job keeps its seal: a signed release record amends it (see
  {doc}`sealing`), and the file's `staged_files` digest is the one the seal
  records, so the file is never hashed. An unsealed job's file is hashed, with
  a warning. If the job is not created, the file moves back. Repeatable; not
  for the same NAME as `--input`, and not with `--input-from`.
- `--input-from NAME SOURCE...` loads a file or the readable files in a
  directory and creates one job per file for a batch. A directory file with no
  registered reader whose name follows a structure convention (`POSCAR*`,
  `*.vasp`) is read as POSCAR; any remaining unreadable files are skipped and
  named on one stderr line.
- `--file NAME=PATH` stages anything else; a NAME without `/` lands in
  `files/`.
- `--files DIR` stages every regular file directly inside DIR under its own
  name. Subdirectories and other non-file entries are skipped with one stderr
  warning.

`job.json` lists the `--file`/`--files` files, and the declared inputs'
destinations, under `staged_files` with the size and SHA-256 of each as
staged, and `collect` records each `--file`/`--files` file as a `files` input
of the job's run, labelled by its payload-relative path (see
[Collecting results](collecting.md#staged-inputs-record-the-exact-file)).

After a batch, one final stderr line reports `submitted N jobs`. If a batch
fails partway, it instead reports `submitted and reported N of M jobs before
failing` and exits `2`; the error's notes, printed after it, name by id the
jobs that were submitted too without being reported (a batch of eight or
more is submitted as a whole before its jobs are reported), those named
complete but not published yet (the command's close or recovery publishes
them), and any whose durability is unresolved. In a batch, `--tag` becomes a
*prefix* combined with each item's derived tag (`run7-si2o`); for a single
job, `--tag` is the whole tag.

### Command templates

With `--command TEMPLATE`, `shlex`-style words are turned into a one-step
Bash runner, installed ad hoc as `adhoc:command@<digest>`. The template is an
argv-only word list with no shell syntax. Each `{name}` placeholder must have a
matching `--parameter NAME=VALUE` or `--file NAME=PATH`. Parameter placeholders
resolve through `httk_workflow_parameter` at run time; file placeholders resolve
to the absolute path of the file's copy in the job workdir (`$PWD/<basename>`,
since the command runs there), never to the staged payload file, so the command
cannot rewrite the input `collect` records. `{{` and `}}` emit literal braces.

Each `--file` input is copied into the job workdir under its basename before
the command runs; two inputs with the same basename are refused at `job new`,
as is a `--file` staged below the workdir `run/`. Existing workdir entries are
never replaced, so a retry keeps working on the copy an earlier attempt left.
The generated runner can be edited and passed back as `job new FILE`.

### Running workflow documents

Run a PWD, CWL, or jobflow document directly with
`job new DOCUMENT`; the document is installed ad hoc and run by
the manager's built-in realization:

```console
httk job new --workspace WS flow.cwl --input message=echo
httk job new --workspace WS workflow.json --parameter pwd_module_path='["."]'
httk job new --workspace WS maker.json
```

`--language` accepts `cwl`, `pwd`, `jobflow`, and `httk-v1` for bare document
inputs. See {doc}`/details/workflow_compat`.

### Requests

`job cancel`, `job pause`, `job resume` and `job restart` post one request
per job: `cancel`, `pause`, `continue` and `restart`. `job set JOB...
KEY=VALUE...` posts the requests its pairs name, in this order: `workflow=WORKFLOW`
(`set_workflow`: an installed id, short name or `NAME@REF`, resolved once in the
jobs' workspace, on the remote side for a remote one; every job is checked
against it before anything is posted, and a workspace daemon remote refuses it),
the budget keys `budget.max_attempts_per_activation=N|unlimited`,
`budget.max_total_attempts=N|unlimited` and `budget.max_activations=N|unlimited`
(together one `set_budget` request), `priority=N` (`set_priority`) and
`step=STEP` (`override_step`). A word holding `=` is a pair and every other word
a job selector; an unknown key is refused naming the valid ones, and the command
stops at the first request that does not exit `0`. `--reason TEXT` is recorded
with each request (default: the command, such as `httk job pause`).

Jobs are the `JOB` selectors joined with every job in a
`--state STATE` (repeatable, an unowned state); `--step STEP` (repeatable)
narrows both and alone selects from every unowned state, and `--placement
PREFIX` narrows `--state`, or a `--step` without `JOB`, to a placement
subtree. `--state`, `--step` and `--placement` select in a local workspace
only, and a selection with no job exits `1` with `no jobs selected`. `job
delete`, `job seal`, `job unseal`, `job detach`, `job unlock` and `job eject`
post the requests of the same names (see their sections). An `away` job
accepts no request but `delete` (see *Away copies* below). The CLI applies
each at once to a job no owner holds; a job held by an owner gets it once, at
that owner's next boundary. What each action does is tabulated in
[the task-manager guide](taskmanager.md#requests). `--force` (on `job resume`,
`job restart`, and `job set` with `step=`) accepts reviving a child a parent's
observations name and publishes a `step=` outside the runner's recorded steps.
`job restart` refuses, posting nothing and exiting non-zero, a job that
recorded a failed or cancelled child its rerun spawning step would reuse: it
prints an `error:` line per such child and the remedy (`job resume --tree`, or
`httk job delete` the child); `--force` restarts anyway. The revival guard does
not apply to a child that its parent's join still names (the parent failed at
its wait, or waits on it again). A revival (`resume`, `restart`, `step=`)
restores the job's declared retry allowance; `--keep-budget` keeps the current
limits instead. `job resume --tree` (also over ssh, not for a workspace daemon
remote) also resumes the failed descendants of the job reachable through
retained joins, deepest first, before the job itself, forwarding
`--keep-budget` to each; see {doc}`/restarting_jobs`.

After publication, the CLI immediately serves the pending requests of each
unowned job, from a snapshot taken then, oldest first by `created_at` instant
and request id, up to and including the published one. It posts and serves the
requests of at most 256 jobs at a time. A request of the command whose outcome
is `uncertain`, and on an error or interrupt every request of the current batch
not served yet, is withdrawn: its file is removed while the command holds the
job (claiming it if need be, and giving it back only once the removals are
durable; if they cannot be made durable the command stops with its jobs held
for recovery), with a warning naming it. A request stays pending, with a
warning, when its application had begun under this or any earlier owner (the
job's run log records it, or cannot be read: its effect may have started), when
its job was returned with an attempt that may still be running (a manager
signals a cancelled attempt before it records the cancel), when
it is a `delete` or `eject` of a job adopted from an exchange (a previous owner
may have begun it), when its job cannot be held (another owner holds it, or it
is not found: it may still be applied), and when it is `queued`. Only a `SIGKILL` (or a signal the CLI does not handle) can
leave one batch's requests behind unannounced (an interrupted withdrawal names
the requests it did not settle); local `job resume --tree` withdraws
the same way, one job at a time. On an interrupt, the outcomes known so far are
printed first. Each request prints its job id, request id, outcome (`applied`, `queued`, `refused` or `uncertain`) and
an explanation. A job held by another owner, or one needing a manager's
reconciliation, keeps its request queued for that owner or manager, and so
does a job with more than 256 pending requests; when an earlier request of the
job cannot be served, the published one reports the same outcome and reason.
Exit status is `0` for applied or queued requests and `1` for a refusal or
uncertain result. `job pause --wait` waits for
queued pauses and exits `0` only when each reaches `paused`. A superseded,
dropped or timed-out pause exits `1`. `--timeout SECONDS` requires `--wait`.

In a durable workspace whose filesystems are all in `syncfs` mode (the protocol
predicate: `io.sync` is `auto` or `syncfs`, `auto` resolves to `syncfs` there
after the prime, and the filesystem is not Lustre), a request verb (or `job
detach`) whose action ends in a release or a drop (`cancel`, `pause`, `set_priority`,
`set_budget`, `set_workflow`, `detach`, `continue`, `restart`,
`override_step`) serves each contiguous run of at least eight eligible jobs of
a batch as one: the posts, the claims, the run-log lines with the state
temporaries, the state replaces, the request unlinks and the release moves
each end at one commit point, a `syncfs` per filesystem, instead of fsyncs per
job. A job is eligible when its request is its only request file, it is not
linked as parent or child to another job of the command, and, once claimed, it
needs no reconciliation, its request was not applied already, its `logs/` is
plain and its request files are unchanged; any other job is served as before,
in input order (one that fails a check after its claim is finished at once,
still held). A batch has none of this unless a run of eight such jobs (none
with a request file yet) exists before anything is posted; otherwise the batch
of jobs is posted and served exactly as before. Each job keeps the order of its
own durable steps and the outcome the serial path gives it. A failed commit
point finishes every job whose state was replaced and gives back the others,
their requests withdrawn while the command still holds them (a
`request_withdrawn` line follows the run log's `request_applied`), reports
every outcome and stops the command, withdrawing its unserved requests; an
interrupt does the same, and so does a job whose own step fails before its
state is replaced (it stays held, `uncertain`). When a seal, a required
withdrawal or one of these finishes fails, or a job
was moved but its move could not be made durable (reported "moved, durability
unresolved"), the command stops after reporting, its jobs held for recovery,
and withdraws only the requests of jobs it still holds. Under `io.sync`
`fsync`, on Lustre or any other filesystem without the predicate, in a
workspace that is not durable, and for runs shorter than eight jobs, every job
is served one at a time as before.

`job delete` is batched the same way. A job whose
delete applies is moved into the command owner's trash scratch at its own
commit point, after the claims and run-log lines (and after every job of the
batch whose delete is dropped, such as a `ready` one, was released back); only
once that move is durable are its tree removed and its request files unlinked,
at one last commit point. The delete of an exchange-origin job, whose exchange
index entry goes first, is finished alone after its claim, and the
waiting-parent check holds as before. A failed commit point or an interrupt
before a job's trash move gives it back with its request withdrawn; a job
already moved into the trash is finished, unless its move cannot be made
durable (reported "moved to trash, durability unresolved"): then the command
stops with its jobs and trash held for recovery, which disposes of the trash.
When the final request unlinks cannot be made durable either, the command
stops the same way (reported "job deleted; its request unlink is not yet
durable"), and recovery makes them durable before anything else. A deleted
job's tree that cannot be removed (a persistent permission error, say) is left
in the control `tmp/` as `trash.<token>` for `workspace gc`, and its line says
so. The outcomes known when a command stops are printed, in input order,
before the error. Known limit, as for the serial delete: a crash after a job's
trash move is durable and before its request unlinks are can leave request
files that name the deleted job id; a job later restored under that id would
receive them.

`job delete`, `job seal`, `job unseal`, `job detach` and `job unlock` use the
same immediate serving path, in the same batches with the same withdrawal, and
print one concise line per job: done, or the `queued`, `refused` or `uncertain`
outcome with its explanation.

For an ordinary remote workspace, a request verb with `--workspace REMOTE:NAME
JOB ...` keeps the signed two-leg request-envelope exchange: the client
signs the request documents with the identity selected by `--operator`, then
sends them back for verbatim publication; `job seal`, `job unseal`, `job detach`,
`job unlock` and `job eject` reach a remote workspace the same way (`job delete`
runs `job delete` there). With `job resume --tree` the far
side expands the selection to the failed descendants, deepest first; the
client signs only `continue` requests, each requested job among them, notes
each added job on stderr, and has the far side post and serve them one at a
time, stopping at the first that does not resume. The far side also applies
the `restart` refusal before anything is signed. A mounted-daemon remote instead
sends one signed typed command for each selector, also for `job delete`,
`job seal`, `job unseal`, `job detach` and `job eject` (whose `DEST` is then a
directory under the exchange outbox, the outbox itself when omitted). Daemon batches are
non-atomic: earlier commands may have been accepted if a later command fails.
A mounted daemon executes a command only for an exchange root submitted with
the signing key; other targets are refused with `not_owner`.
The daemon accepts canonical job UUIDs and UUID prefixes; glob and tag
selectors are refused before publication. Daemon `--force` is unsupported.

Use `--request-id` with exactly one daemon-mediated job command to retry the
same command after an uncertain transport result. An exact retry must retain
the selector, action, reason, options and configuration pin. It reuses the
original command ID and cached signature; changing any command field is
rejected. The option is not available for local jobs or ordinary remote
workspaces.

For `pause --wait`, the client sends the pause command once and then polls
with fresh signed observations until the daemon reports `paused`, refusal,
terminal supersession or timeout. Passive `status.json` is not evidence that
a command completed. Run `httk manager status REMOTE --request-id ID` to
inspect a previously sent daemon job command; each inspection uses a fresh
signed observation request. Without `--handle` or `--request-id`, status
remains passive and informational.

### Inspecting and debugging jobs

`job list`, `job show`, `job log`, and `job why` read one workspace without
writing anything, and `job debug` drives a single job to a terminal state in the
foreground. See {doc}`taskmanager` for what each reports.

```console
httk job list --workspace WORKSPACE --state ready
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
httk job debug --workspace WORKSPACE --follow-children PAYLOAD_OR_JOB
```

`job debug` exits `0` on success, `3` on failure, and `4` when the job stopped
without finishing.

`job list --json` returns `format`, `format_version`, `jobs`, and `next_after`.
Each row has `job_key`, `job_id`, `state`, `step`, `phase`, `placement`,
`priority`, `token` and `owner_id`. `step` is the step the job is at: its
current activation's, or the initial step of its `job.json` before the first
activation (earlier releases reported null there). To page a large workspace,
use `--limit N` and pass the returned `next_after` (`<state>:<cursor>`) to
`--after`; it is null when the listing is exhausted. Paging is weakly
consistent: a job that moves between pages can appear twice or be missed, so
deduplicate by `job_id`. `--counts` adds per-state totals, which need a full
directory walk and ignore `--step`.

`--state STATE` and `--step STEP` select jobs in the same way across `job
list`, `job move`, the request verbs, `collect` (also with `--campaign`) and
`postprocess`: each
repeats (the values of one option are alternatives), and a job must match
every option given. A job whose step cannot be determined (an unreadable
`job.json` or a damaged `state.json`) matches no `--step`: `job list` logs a
warning, a request verb leaves it out with a note, `job move` reports an
error, and `collect` counts it as
unreadable. `--step` reads each examined job's `job.json` and `state.json`, so
like `--tag-contains` a sparse `--step` page with `--limit` may examine up to
`max(100 × limit, 10000)` jobs (two small reads each) and come back partial,
with a `next_after` that resumes the scan. The step a job starts at is the
`--start-step` of `job new` and `job debug`;
`job set JOB step=STEP` names the step a job resumes at.

`job why` also lists **held** jobs: a job held for a transfer is in neither
state tree, and `job why JOB` shows its hold, destination and the way back.

### Sealing, detaching and unlocking jobs

`job seal` and `job unseal` are the per-job half of {doc}`sealing`. `job seal`
seals a succeeded job that has no seal yet, with the workspace's `seal.keys`.
`job unseal` releases a succeeded job from its seal, after confirmation, which
is what lets `job delete` remove it.

`job detach` permanently makes a spawned job independent of its parent: it no
longer moves with its parent's tree, it can be transferred on its own, and
`Attempt.parent` reads `None` for it. It stays in any join that references it.
A job with no parent is refused.

`job unlock [--no-copy] [--reason TEXT] JOB...` puts jobs in debug mode (see
{doc}`/restarting_jobs`). It posts an `unlock` request per selected job and,
marker-only and nearest first, per ancestor of each (by the parent recorded in
`job.json`, also for a detached job), and applies them at once to unowned jobs.
Ancestor lines end with ` (ancestor)`; an ancestor already unlocked prints
`already unlocked (ancestor)` and is not an error, and a parent that cannot be
found prints `<job_id>` `parent <key> not found; not unlocked` without changing
the exit status. It exits `1` when a request of a selected job or ancestor is
refused or uncertain. Without `--no-copy`
it copies the job's installed `package/` to `workflow/` in the job directory
(a reserved payload name), where the manager then runs the runner from, while
the job is unlocked, and the operator edits it. The marker cannot
be cleared by any request. A succeeded job with a live seal is refused until
`job unseal`, and a cancelled job is refused. `job unlock` reaches an ssh remote
through the signed request envelope (it copies, and refuses `--no-copy`; a spawned job is refused there:
unlock spawned jobs on the workspace host with `httk job unlock`, which also
unlocks their ancestors), and a
workspace daemon refuses it (the daemon does not carry `unlock`; run it on the
daemon host). `job show` prints an `unlocked` row, `job why` a "debug mode"
check, and the JSON report gains `unlocked` and `paths.runner_copy` (`<payload>/workflow`); `workflow_pin.source` (`installed` or `copy`) appears in the JSON report too. The jobs
are collected only with `httk collect --force`.

### Ejecting and adopting jobs

`job eject` and `job adopt` move jobs out of a workspace and into one without
either workspace knowing the other. Reach an unregistered workspace with
`httk -C DIR` or `--workspace`.

```console
httk job eject JOB /scratch/out
httk job eject --tree --wait JOB /scratch/out
httk job adopt --workspace other /scratch/out/JOB_KEY
```

`job eject` takes one root in any unowned state and moves it into
`DEST/<job key>`: a plain directory holding `bundle.json` and the complete job
directories under `jobs/<placement>/<job key>/`, with seals inside the
payloads. DEST is an absolute directory, or a missing name in one. A root whose
descendants are present is refused unless `--tree` is given; with `--tree` every
descendant must be terminal or paused, and they travel in the same bundle.
`--wait` first posts `pause` requests and waits (up to `--timeout`, default
600 s) until the jobs can move, so a running job leaves paused. `job eject`
records the selected identity's public key as the optional, unsigned
`owner_key` in `bundle.json`; `--operator IDENTITY` selects that identity and
eject fails without moving the job if it has no key. Without `--operator`, the
configured default identity is used when keyed, and the member is omitted when
not. `--hold` and `--operator` cannot be combined. Only untrusted (exchange)
adoption uses `owner_key`. Across
filesystems the bundle is copied to a hidden partial name in DEST and renamed
into place when complete. The command prints `ejected N job(s) to PATH`. A
failed delivery returns every job to the state it left.

The hidden protocol option `--hold` puts the bundle in the workspace's own
`.httk-workspace/transfers/outgoing/<transfer id>/` instead, the first leg of
`job move`; DEST, when given, and `--destination-id` are then only recorded.

`job adopt BUNDLE...` validates each bundle and publishes each job into the state
and priority it left, printing `JOB_KEY<TAB>STATE<TAB>DIRECTORY` per job and a
warning naming workflows that are not installed here. Several bundles are
adopted in turn by one command; a refused one does not stop the rest, and the
command exits `0` only when every bundle was adopted or already was. `--json`
prints one document, `{"bundles": [...]}`, with an entry per BUNDLE in argument
order: `bundle` (as given) and `status` (`adopted`, `already_adopted`,
`refused`, `taken` or `failed`, the last for an error that is no refusal of
the bundle, such as a full disk), plus `already_adopted`, `published` and
`missing_workflows` for an adopted bundle, or a `message` for the others. On one filesystem it
takes the bundle by rename. Across filesystems it copies the bundle and leaves
the source in place, unless `--move` is given, which removes the source after a
successful publication (the way to fetch an exchange return). A bundle whose
jobs are all present already reports `already adopted`; one with only some of
them present, or whose root's parent is present here, is refused. Bundle
structure is checked strictly: no symlink or special file in protocol
positions, every member's `job.json` agreeing with the manifest, and an adopted
job's placement satisfying the
[placement rule](workflow_filesystem_api.md#placement-rules).

The receiving end of a push is the far side's transfer session (see *The
transfer session* in {doc}`remotes`): it extracts each segment's `tar` stream
whole into a fresh landing with the same filters as a pull (a malformed stream
or a refused member adopts nothing), then adopts every top-level bundle
directory the stream carried; a bundle whose `bundle.json` records another
destination workspace id is refused and discarded there. A hold directory,
`.httk-workspace/transfers/outgoing/<transfer id>/`, is itself a bundle: copied
by hand to another machine, `job adopt` takes it like any ejected bundle (the
remedy for a hold too large for a transfer session's answer; release the hold on
its source afterwards with `job move --release`).

Both commands are crash-safe and take no file locks: each records what it needs
to finish before it moves anything, so an interrupted eject or adoption is
rolled forward or back once the killed command's death is proven and its
scratch recovered, by a manager tick or by `workspace gc` on its host. Delivery is at least once: verify the destination
before ejecting or adopting a copy again by hand. Neither works in a sealed
workspace or project; a sealed job keeps its seal.

## Moving jobs between workspaces

`job move` moves jobs between two workspaces, each a registered name
(`REMOTE:WS` for a workspace on a remote) or else a workspace directory; a
registered name wins over a same-named directory, so `./NAME` addresses the
directory. Any combination of local and remote ends works, and a
remote-to-remote transfer is relayed through the client (a temporary directory
here, see *Remote to remote* below):

```console
httk push [SRC] DST [--no-keep] [--job JOB … | --state STATE … | --step STEP …] [--placement PREFIX] [--no-workflows] [--json] [-q]
httk pull SRC [DST] [--job JOB … | --state STATE … | --step STEP …] [--placement PREFIX] [--no-workflows] [--json] [-q]
httk job move (--job JOB … | --state STATE … | --step STEP …) [--placement PREFIX] [--tree] [--keep] [--no-workflows] SRC DST [--json] [-q]
httk job move --resume [--keep] [SRC] [DST]
httk job move --release TRANSFER_ID [--release TRANSFER_ID …] [--keep [--destination-id ID]] [WS]
httk job recover [--workspace WS] (JOB … | --all [--placement PREFIX])
httk workspace status [WS] [--json]
```

`push` and `pull` are the everyday forms: `push REMOTE:WS` moves
every `ready` job (created, waiting to start) of the workspace the command
runs in, else the default workspace, to REMOTE:WS, and `pull REMOTE:WS`
moves every `succeeded` job of REMOTE:WS back. With one positional push takes
it as DST and pull as SRC; the omitted end resolves like any omitted
workspace (the enclosing workspace, the project's default, else the per-user
default, which is created on first use), and with none the command is
refused. Both always act as `--tree`, and `--job`, `--state` or `--step`
replaces their implicit state (`--step` without `--state` selects from every
unowned state except `away`; `--state away` is refused by push, pull and
transfer). `--resume`, `--release` and `--destination-id` stay on `job move`.

Jobs are selected by name with `--job` (a UUID, key, unique prefix, path or
glob), or by state with `--state STATE`, which takes every job of SRC in that
state (any state of `job list --state` except `owned`), optionally only under a
placement prefix with `--placement PREFIX`. Both repeat and combine; a job
selected twice moves once. `--step STEP` (repeatable) narrows `--job` and
`--state` to jobs at that step; alone it selects from every state but `owned`
(and `--placement` then narrows it as it narrows `--state`), and a `--job` at
another step is left out with a note on stderr. `httk job
transfer --state succeeded sigma:runs default` brings every finished job home
in one command. A tree is never split: `--state` and `--step` leave out every
job whose (not detached) parent is in SRC, so
a child moves only inside its root's bundle, and a note on stderr counts the
children that did not move that way. Without `--tree`, a selected job whose
descendants are present is refused, so a campaign whose finished roots have
children is fetched with `--state succeeded --tree`; a root's tree then carries
all of its descendants, which must be terminal or paused (a failed, cancelled
or paused child travels with a succeeded root). A root whose children are not
all terminal or paused (a parent that an `any_terminal`, `any_succeeded` or
`at_least` join let finish first) is refused and stays, never split. A child that should move alone
is detached first. A selection is held page by page (see *Paged holds* below).

A transfer is four steps. It **holds** each selected job on SRC (with `--tree`,
its terminal or paused descendants too) in SRC's own
`.httk-workspace/transfers/outgoing/<transfer id>/`, recording DST, after which
the jobs are in neither workspace's state trees. It **copies** the held bundle
to DST (on one filesystem adoption simply moves it in; a local DST receives it
from a remote SRC, and a remote DST from a local SRC, over one transfer session,
segment by segment, below), **adopts** it there into the states and priorities
it left, and only then **releases** the hold. Every remote end is reached over
its transfer session, which both machines' *httk* must speak in the same
version: an end that cannot open one stops the transfer before anything is held
or changed (see *The transfer session* in {doc}`remotes` for the messages).
A remote end is never probed first: its commands name the workspace by the
plain name it has on the far side. On a remote SRC,
`--job` needs canonical job UUIDs; `--state` and `--step` are resolved there.
These steps run a page of holds at a time (*Paged holds*, below).

The workflows the held jobs need (each job's effective workflow and the
closure of its calls, as SRC resolves them) that DST lacks and SRC has are
installed in DST under the same ids, and a shipped workflow with
`[workflow.build]` is built there, as is a needed one DST already had without a
build for its platform. For a local DST this happens, builds included, before
the first adoption of each page that needs a workflow not yet attempted for that
DST in the command (one attempt per workflow and destination, a failed one
included), from SRC's store (a remote SRC's entries fetched with one request on
the pull's transfer session), so a page's adopted jobs can start at once. Over a
push's transfer session the same holds for a remote DST: before the first segment, one
request asks DST which of the workflows it has (`present`, `unbuilt` or
`missing`), and one more installs the missing ones and builds the unbuilt ones
(again before each page that needs one not yet attempted), so every segment can start as soon as it is adopted, even while later segments
are still on their way; a failed installation or build is reported and the push
goes on (those jobs wait); a manager skips, and does not fail, a ready job
whose workflow is not installed yet. An installation DST already has is never replaced,
even with another tree. `--no-workflows` skips all of this. With
`--json` each outcome lists `workflows`, one `{id, status}` per needed workflow:
`installed`, `present`, `mismatch` (DST keeps its other tree, which the jobs
run), `missing` (installed in neither), `refused` or `failed` (with a
`reason`), or `unbuilt` (installed, its build failed; the reason names the
`httk workflow build` remedy). Adoption warns about jobs whose workflows DST
still lacks. The command prints one line per hold,
`TRANSFER_ID<TAB>STATUS<TAB>N job(s)<TAB>DESTINATION`, and exits `0` only when
every hold was adopted or already was. Progress, one line per step, goes to
stderr unless `-q`/`--quiet` is given.

Each hold is its own bundle, but the number of ssh connections and remote
`httk` starts does not grow with the number of jobs. Local to remote takes one
ssh connection and one remote `httk` start, whatever it ships, builds or
retries and however many pages it holds: the push runs over one transfer
session (see *The transfer session* in {doc}`remotes`); remote to local one
connection and one start too, whatever it ships or retries and however many
pages it holds, over one transfer session to SRC (`--resume` without DST: one
session for each remote end in play); remote to remote one connection and one
start at each end (two and two in all, whatever the pages and segments). A
retry after DST's failed `syncfs` adds no connection.

A push is delivered in segments, each closed at 256
bundles, 16 GiB of file data or 16,384 archive members (a larger hold is a
segment of its own; a hold is never split), and the earlier holds and the new
ones never share a segment. Each segment is its own delivery, with its own archive,
landing, synchronization and adoption document, and the holds it adopted are
released right after its adoption (see *Staged segments* below for when that is). A push over a session is
therefore not all-or-nothing: the segments adopted before a failure stay
adopted and released, and after an unknown outcome only the failed segment and
those after it stay held for `--resume`. The rules below apply to each segment as to a delivery, and two
kinds of failure differ:

- A **refusal** (a `refused` document: definitively nothing of that segment was
  adopted) takes the segment's holds that this run made back into SRC
  (`not_moved`) and keeps the earlier runs' held (`failed`, with the remedies
  below). A refusal of one bundle's member names that bundle: only that bundle
  is refused so, and the rest of its segment is delivered once more without it
  (a second refusal of the rest is final). A refusal of a whole segment that
  names no bundle (an unknown or sealed workspace, a full quota, an unwritable
  landing) would meet every later segment too: nothing more is sent, and the
  segments not yet sent are settled by the same refusal, so this run's holds
  among them are taken back and earlier runs' stay held. A member refusal the
  far side cannot attribute to a bundle counts as such a refusal
  (conservatively). A refusal the far side marks as caused by an incomplete
  stream (this side aborted its upload, say because a hold changed while it was
  read) stops nothing.
- An **unknown outcome** (an unreadable answer, a far side that dies or fails,
  a segment answered by another workspace id than the first) stops the
  session: that segment's holds are `failed` with an unknown outcome, and every
  segment not yet sent stays held, `failed` with the `--resume` remedy. This
  includes this run's own new holds when the unknown outcome is the earlier
  holds' segment, since those are delivered first: they are never taken back
  on an unknown outcome.

A push from a local workspace (and a pull, here) synchronizes a staged
segment's landing only when it adopts it, before its second look (see *Staged
segments*); a failure there gives the segment up and delivers it once more at
once, with every file synchronized as it is written, adopted before any later
segment; a remote-to-remote relay synchronizes at its stage. After a retried
`syncfs` failure every later segment of the session is synchronized file by
file at once. Under `syncfs` only the first landing of a
session (and of a pull, or of a local transfer's copies) on a filesystem primes
its sync barrier with a `syncfs` of its own; the later ones only commit. A
writeback error recorded on that filesystem before such a landing began can
then fail its commit too, which is answered as any failed `syncfs` is (the
segment delivered once more with every file synchronized as it is written). When the session fails while the workflows
are installed, before the first segment, nothing has been adopted and every
hold stays held.

**Staged segments.** Before a segment is adopted, the jobs it carries (and
its roots' parents) are looked for in DST twice: once when it is staged, and,
for those not found then, once more at least one visibility deadline
(`visibility_deadline_seconds`, 5 seconds by default) after the first look
ended. A job either look found counts as present (one found first and moved
out of sight by the second is not taken for absent), and each bundle's
adoption looks for its jobs afresh as it always did. A job another host
publishes after the second look, or that shows up too late for it, is caught
only by that fresh look: the accepted race of concurrent transfers, as before.
Rather than wait for that segment by segment, a push whose DST serves the
`stage` and `adopt_staged` requests of a transfer session *stages* each
segment: DST receives it into a landing scratch of its own (held by an owner of
its own until the segment is adopted or given up), checks the stream's end,
synchronizes the landing (a push's own segment: only when it is adopted, with
the second look after that synchronization even at a zero deadline; a relayed
one: now) and takes the first look, and answers at once; the
push goes on copying the next segments and has each staged segment adopted
(`adopt_staged`, one request a segment, in the order they were staged) once its
deadline has passed, and the rest at the end of the command, across pages. A
pull stages each fetch's landing in the local DST the same way between its
fetches (only once SRC's answer naming what it sent was checked); a
remote-to-remote transfer stages at DST as a push does, in the segments of a
push (a fetched part larger than one, such as a fetch whose last bundle is
large, is staged in several), and the relay is removed once its
segments are staged; a local-to-local transfer stages each page (on one
filesystem without `--keep`, the first look is over the original holds, which
stay where they are) and holds the next pages while the staged ones mature.

At most 8 staged segments, holding at most 32 GiB of file data, wait for their
adoption, after which the oldest is adopted (waiting out what is left of its
deadline) before another is staged. A push declares each segment's bytes and
DST admits it by that declaration, then counts what actually landed (hard-link
copies included), so a session's staged bytes exceed 32 GiB by at most one
segment; a single bundle larger than that is staged only when nothing else is,
and is adopted before another is staged (several bundles above it are refused
for capacity). A DST that refuses a stage for capacity uses nothing of it: the
oldest staged segment is adopted and the segment sent again. A pull reserves
one fetch (16 GiB) before it asks for it and counts what landed after it; a
fetch never splits a bundle, so one bundle can take it beyond the 32 GiB until
it is adopted. A local-to-local transfer's copied stages (with `--keep` or
across filesystems) count their copies; one on one filesystem without `--keep`
copies nothing. The wall time of a long transfer is about the copying time plus
one deadline only while those slots do not fill before the oldest segment's
deadline has passed (a fast copy fills them, and then each further segment
waits for the oldest one's adoption).

A staged segment's outcome is final only once it was adopted and released, and
its refusals are those of its `stage` request (the rules above). A stop (an
unknown outcome, a whole-segment refusal, another workspace id, a failed
session, a local failure) gives up the staged segments not yet adopted: DST
discards their landings and their holds stay held, `failed` with the
`--resume` remedy. They were sent, so they are never taken back, not even when
a later segment is refused as a whole; that refusal takes back only this run's
holds of the refused segment and of the segments never sent, and an earlier
run's holds stay held in either case. A refusal of `adopt_staged` itself
(nothing adopted) and a `stage` answered by another workspace (nothing adopted
either) keep their holds and stop as well; a lost answer to `adopt_staged` is
an unknown outcome of that segment. A staged segment whose adoption completed
but whose stage could not be cleaned up afterwards (its landing discarded, its
owner closed) is still reported adopted, with a `warning:` line; DST then
stages nothing more on that session (the next segment's `stage` is refused
before anything of it is used, so that segment is reported not delivered and
the pair stops, every later hold kept for `--resume`), and DST tries that
cleanup once more when the session ends, leaving it to recovery if it fails
again. A local DST's stage does the same. A source session that fails while a
relayed segment's holds are released stops at once: nothing more is adopted.
When the far side ends a session (a `bye`, which it answers only after
discarding them, or the end of its input), its staged segments whose adoption
had not begun are discarded (a discard that outlasts what this side waits for
the far side at its end, 5 seconds for the `bye` answer and 2 more for the
process to exit, is cut short when the far side is stopped, and what is left
is discarded by recovery once that process is known dead); when it dies they are left to recovery, which
discards them once its process is known dead: nothing ever adopts a staged
segment unasked. One whose adoption had begun is recovered as any interrupted
adoption is, which may still publish its jobs; `--resume` then reports them
already adopted. An interrupt (Ctrl-C) gives up the staged segments the same
way; the holds already adopted are released only over a session that is still
usable. While
DST's far side looks for a segment's jobs (the two looks, each bundle's fresh
look), every directory it lists counts as progress of its request, so a slow
listing of a large placement is not taken for a far side that stopped
answering.

A pull from a remote SRC into a local DST over a transfer session first drives
the holds an earlier run left bound for DST (every hold for `--resume` without
DST), listed a page at a time, then has SRC select the jobs and hold them a page
at a time, each page driven before the next is asked for (*Paged holds*). For a
page it fetches the store entries DST lacks with one request, installs and
builds them, and then fetches the page's holds in segments of 256 bundles, in
the order they were held. SRC closes each fetch once the bundles it sent hold 16 GiB
of file data or 16,384 archive members (directories and hard links included; a
hold is never split), so a segment may take several fetches, each of what is
left of it. Each fetch is one archive into its own landing scratch in DST: the
stream's end is checked before the landing is synchronized, the landing
synchronized (`io.sync`, read for each landing) and staged before anything of it is
adopted (*Staged segments* above), and the holds it adopted, or found adopted already, are released on SRC
right after its adoption, over the same session (with `--keep` recording DST's workspace
id in the away copies). While SRC sends a hold, each file is checked against its
own open descriptor: a file replaced, shrunk or grown since it was listed aborts
the fetch, which never ends as complete. A file changed in place without changing
its size is not detected (an accepted limitation: nothing writes a held bundle).
Per segment:

- a failed `syncfs` commit of a landing under `io.sync_errors` `fail` discards
  it and fetches it once more into a fresh landing with every file
  synchronized as it is written, as every later fetch of the session then is,
  for every destination of the command; a second failure (an fsync error of the
  re-fetch) stops the pull as a refusal naming no hold does (below);
- a hold SRC cannot send (it changed or vanished while it was read, which
  aborts the fetch, or it is gone from SRC: another actor released or took it,
  which the outcome says, naming where to check) or whose member DST refuses
  fails alone, and the rest of its segment is fetched once more without it.
  After a second such refusal in the segment the rest of it is not fetched:
  each of those holds is reported "not fetched: another hold of its segment was
  refused twice", stays held on SRC, and `--resume` retries it (a hold that
  keeps failing so holds back the rest of its segment until then);
- a refusal that names no hold, by SRC or by DST (a full disk or quota, a
  landing scratch that cannot be made or used, a failed synchronization under
  `io.sync_errors` `ignore`, an fsync error after the re-fetch), a failed
  session, or an answer naming other holds than arrived stops the pull, as a
  whole-segment refusal stops a push: nothing of that fetch was adopted, its
  landing is discarded, and its holds and every later segment's stay held on
  SRC (`failed`, with the `--resume` remedy). A bundle whose refusal in DST
  cannot be attributed to it (one holding a name DST's filesystem cannot hold,
  say) stops the pull at its segment on every `--resume`: take that hold back
  on SRC by hand (`httk job adopt --workspace SRC
  SRC/.httk-workspace/transfers/outgoing/<transfer id>`, on SRC's machine),
  then `--resume` moves the rest;
- a release that fails leaves the jobs in DST and their holds on SRC: the
  outcome says "the jobs are in DST, but the hold was not released" and names
  `--resume` or the hand release; a session failing in it stops the pull too.

**Remote to remote.** A transfer between two remote workspaces passes every
bundle and store entry through this machine: each fetch lands in a temporary
directory, the *relay* (`httk-transfer-*` in the default temporary location,
`TMPDIR` when set), removed once what it holds is settled. A relay holds one
fetch: SRC closes it once 16 GiB of file data (or 16,384 archive members) went,
but never inside a bundle, so it may exceed that by the last bundle, and a
single larger bundle is a fetch of its own. The workflow store entries a page needs are
fetched into a relay of their own, separately. A relay that cannot be made,
written, read or removed (a full disk, say) stops that destination as a refusal
naming no hold does. A relay is not a workspace landing: nothing in it is synchronized
(DST makes what it adopts durable, segment by segment, as in a push); a fetch
over SRC's session is extracted by the same untrusted-archive rules as a
landing and its stream's end is checked before anything of it is sent on.
Both ends' sessions are opened, and their versions checked, before anything is
selected or held on either end: SRC's first, then DST's when it is named (with
`--resume` and no DST, each destination's before its holds are driven). An end
that cannot open its session stops the transfer with nothing held or changed on
either end (see *The transfer session* in {doc}`remotes`). An empty selection
therefore still connects to DST, and fails when DST cannot be reached. SRC
holds page by page as a pull does; for each page DST is asked which workflows
it lacks or has unbuilt, their store entries are fetched from SRC into a relay
and installed and built over DST's session, all before the page's first
segment; then each segment is fetched into its own relay (SRC closes a fetch at
16 GiB or 16,384 members, so a segment may take several), adopted over DST's
session by the rules of a push (a failed `syncfs` delivered once more from the
same relay with every file synchronized, as every later segment to that DST
then is; a refused member's bundle fails alone and the rest go once more from
the relay; the first adopting workspace id is kept), and released on SRC over
its session right after (with `--keep` recording DST's workspace id). One
connection and one start at each end.

A remote SRC never takes a hold back: every refusal, of one bundle or of a whole
segment, leaves the holds held there (`failed`, with `--resume`; a hold an earlier
run left, refused by DST, by SRC or by the relay, names the conditional remedy
above, its hand take-back to be run on SRC's host). An unknown outcome at DST, a refusal of a whole segment, an
answer from another workspace id, a failure of the relay here (a full disk, a
relay that cannot be made, or this side's relay that cannot be read while it is
sent) or a refusal by SRC naming no hold stops that destination: the segment and
every later hold stay held on SRC, and what earlier segments settled (adopted,
released) stays so. A failure of
SRC's session (in a fetch or a release) stops every destination of the command.
A release that fails after the adoption leaves the jobs in DST and their holds on
SRC ("not released"). `--resume` lists SRC's holds over its session and drives
them the same way.

Each answer of a page (holds, errors, notes and diagnostics alike) is held to
448 MiB as encoded (the largest message of a transfer session is 512 MiB), the
diagnostics cut to their last 64 KiB and each error to the start that takes 4
KiB encoded (both marked): a hold made or an error found that does not fit goes
first in the next answer, which holds nothing new, and the note and the
diagnostics give way to the first of them, so every answer carries at least one
when there is one to carry. A hold whose own description does not fit the answer
even with nothing else in it (at about 40 to 60 bytes a job, a tree of several
million jobs, more than one bundle can hold) is answered by its transfer id
only: it fails alone, stays held on SRC, and the message names the remedy by
hand (an accepted limitation): copy its bundle
`.httk-workspace/transfers/outgoing/<transfer id>/` from SRC to the
destination's machine, run `httk job adopt BUNDLE` in the destination
workspace, then `httk job move --release TRANSFER_ID SRC`, or, to keep away
copies, `httk job move --release TRANSFER_ID --keep --destination-id ID
SRC`, ID being the `workspace_id` that `httk workspace status --json DST`
reports (such a hold records no destination id, and `--keep` alone keeps
none). A tree's `bundle.json` (about 220 bytes a member) is held to 512 MiB,
about 2.4 million jobs, and a larger tree is refused before any job moves; the
answer of its adoption takes about 400 to 450 bytes a job. A push of a tree of
more than about a million jobs, whose adoption answer exceeds 512 MiB, is
copied and adopted, but its answer cannot be read (an accepted limitation): it
reports an unknown outcome and keeps the hold, and `--resume` completes it (the
second delivery is answered "already adopted" and the hold released), at the
cost of a second copy; `httk job eject` and `httk job adopt` by hand avoid that.
A pull adopts here, up to the bundle's bound. Also accepted: on a filesystem whose `fstat` of an open file reports
another device or inode than `lstat` of its path, SRC refuses every fetch as
changed; push from SRC's machine instead, or eject and adopt by hand.

A bundle DST refuses (for example because some of its jobs are already there,
or because its `bundle.json` records another destination workspace id) stays
held on SRC and is reported; one whose jobs are all at DST already is "already
adopted", and its hold is released. When a remote DST refuses a delivery before
adopting anything (the workspace name is unknown there, the stream was
refused), a local SRC takes back at once the holds of that delivery that this
run made, and reports them `not_moved` with DST's reason; a remote SRC keeps
them held (`failed`). A hold that existed before the run (left by an earlier
run, re-driven now) is never taken back, since an earlier delivery of it may
have been adopted with its answer lost: it stays held and is reported
`failed` with the refusal. Check with `httk job show` on DST: deliver it again
with `--resume`, or, if the jobs are there, release it (`job move
--release T`), or, if they are not, take it back with
`httk job adopt --workspace SRC SRC/.httk-workspace/transfers/outgoing/<transfer id>`
(the message names both). This is why the earlier holds and the run's new ones
travel in separate deliveries. The rule cannot cover two transfers of one SRC
to one DST running at once: the run that created a hold may still take it back
after the other run delivered it, leaving the jobs in both workspaces, so do
not run them concurrently. A refusal marked `sync_failed` is first delivered once more,
to a fresh landing with every file synchronized as it is written there; only
that second answer counts (a refusal then takes the holds back as above; one
from another workspace id than the first answer is treated as unreadable, as
is a delivery answered by another workspace id than the batch's earlier
delivery). An
answer that cannot be read (a banner on the connection, a crash, a timeout)
marks every hold of that call `failed` and leaves it held: whether the jobs
arrived is unknown, and `--resume` delivers again (they then report "already
adopted") and releases. Releasing such a hold by hand instead, after checking
with `httk job show` that DST has the jobs, keeps away copies only with
`httk job move --release T --keep --destination-id ID SRC`, where ID is the
`workspace_id` that `httk workspace status --json REMOTE:WS` reports for DST.

**Paged holds.** A transfer from a local SRC (a push, local to local) and a pull
from a remote SRC over its transfer session hold the selection page by page and
drive each page (copy, adopt, release) before holding the next, so that no
answer of the session grows with the selection and a running manager at DST
gets jobs from the first page on. The `--job` selectors are resolved and checked
first (on a remote SRC, by SRC, in the session's first request): a bad one
stops the command before anything is held or driven. The holds an earlier run
left bound for DST are driven next, a page at a time from one listing; they are
never taken back, whatever DST answers. A remote SRC lists the names of its
holds once for the session (sorted, frozen, 16 bytes each; a first page asked
for anew lists them anew) and answers each page from that listing, freeing it
once the last page is answered: a hold made after
the listing (by another run) is not driven by this one, and one gone since is
passed over. Then the
`--state` (or `--step`) listing and the jobs present in SRC (`owned` and `away`
included) are taken once, and the selection is held page by page, in order: the
`--job` jobs first, one at a time, then the listed ones, together over
`io.workers` unless one of them has its parent in SRC. A page closes after 256
jobs to hold (those a page left count first in the next), or 4,096 jobs looked
at, and stops holding once it has 256 errors (found deciding and holding
together) or its holds carry 16,384 jobs, checked between attempts (between
batches of `io.workers` roots when they are held together, so it may carry one
batch more; a tree is never split); what it did not attempt goes first in the
next page. A page that holds nothing
(every job of it failed, or moves only with its root) is no failure: paging goes
on. The note on the children that move only with their root counts those that
did not move with it once the last page is held. Each destination keeps for the
whole command its transfer session, the workspace id its first adoption
answered, the synchronization file by file after a failed `syncfs`, and its
workflow attempts.

A push from a local SRC to a remote DST that keeps no away copies
(`job move` without `--keep`, or `push --no-keep`) holds one page ahead:
a second thread holds the next page while the current one is staged, adopted
and released (so a page costs about the longer of the two, not their sum),
starting each page only once the one before was handed over. It holds nothing
else, prints nothing (its warnings go to stderr through logging), and hands
each page over with everything it completed, a failure on SRC included, which
then stops the destination as an error between pages does. With its pooled
holds and the releases of the page before, up to twice `io.workers`
operations can run on SRC at once. `push` keeps away copies by default
(like `--keep`), so it holds no page ahead unless given `--no-keep`; nor do
pulls, remote to remote, local to local and the holds an earlier run left.

With `--json` the `transfers` list the earlier holds first (by transfer id,
page by page), then the selection's holds in the order they were held, page by
page. The document has `changed` (a count, possibly `0`) whenever there was a
selection, an empty one included; it is absent when nothing was selected
(`--resume`). A pull's far side reports with each page
`remaining`, the selected jobs not attempted yet, those that moved with an earlier
hold not counted; `consumed`, how far it got (it grows with every answer that is
not the last: an answer that consumed nothing more is taken as a far side out of
step, which stops the pull as a failed session does); and
`carried`, the jobs of the holds it made but did not report yet. A stop names the
count not attempted ("at most" after a page whose answer was lost; when the first
page's answer was lost, that how much was attempted is not known; when the stop
came before the selection's listing, or SRC refused the first page, that the
selection was not attempted) and
the held jobs not yet reported, which stay held on SRC for `--resume`. A pull
asks SRC for no new selection while it has holds of the current one to report.

Memory grows per page, except for what lives for the whole command: the
listing (state, placement and name of each listed job), the ids present in SRC
and those of the candidates no page looked at yet (16 bytes each, besides the
set's own overhead), the job keys moved so far and those left to move with their
root, the names of the earlier holds listed once (a local SRC; a remote SRC
keeps them, 16 bytes each, until their last page), and the workflow attempts
and shipped workflows (one entry a workflow). The outcomes and errors are not
kept: each is appended to a private temporary file (unlinked at once, in
`TMPDIR` when set) once it is final, after its release and its workflows'
report, and the report is printed from that file at the end, exactly as it
would be printed from memory (each of standard output and standard error); a
command interrupted or failing before then prints no report. The file takes
about 0.65 KB a single-job hold, about 0.65 GB for a million: for a very
large transfer, point `TMPDIR` at a disk-backed directory, since a `tmpfs`
`/tmp` is memory. The files are made before anything is held, so a `TMPDIR`
that cannot take them stops the command with nothing held or moved. Should
writing them fail later (a full disk), the transfer goes on unchanged: one
warning says that the report could not be spooled, the rest of it is kept in
memory, and the report printed at the end is still complete. The report is
not also held in memory, so a spool that cannot be read back at the end (an
I/O error on the file just written) stops the report where it is, perhaps
leaving standard output with an incomplete JSON document, and the command
exits with status 2 after one diagnostic on standard error: the transfers
themselves are done as reported so far, and `httk workspace status` shows what
is still held. Every page
driven also adopts the staged segments of every destination whose deadline
has passed, so that one destination's last staged segment does not hold back
the report of the others. The workflows a page's holds need are resolved in
the workflow store once for each root workflow a page, so a workflow installed
meanwhile is seen by the next page.

Paging is not the same as holding everything first, deliberately:

- A transfer is not all-or-nothing: what was adopted and released before a
  failure or a stop stays done. A page's segments still staged (not yet
  adopted) when the stop comes are given up and stay held (*Staged segments*),
  so a stop can leave earlier pages' holds for `--resume` too.
- A listed job that changed since the listing (its state, placement, priority
  or directory) is left out and counted, on stderr (`N selected job(s) changed
  since they were listed: left out; run again to select them`) and as `changed`
  in the `--json` document; it is no error and leaves the exit status `0`. A
  `--job` job that moved is looked up by id once more; one that still cannot be
  claimed is an error.
- With `--keep`, a job released in one page leaves an away copy, so its parent
  selected in a later page is refused (a tree member that is away blocks its
  tree), where holding everything first held both; the refusal is reported.
  While the child's page is still staged (not yet adopted and released, see
  *Staged segments*), the child is a hold, not an away copy, and its parent is
  held by itself in the later page; which of the two happens depends on how
  far the staged pages got. A take-back in one page can change what a later
  page finds likewise, except for the page a push holds ahead, which was held
  before (as when everything is held first).
- A job adopted into SRC after the listing can be held alone although its parent
  is then present (whether a parent is present is a conservative filter, not a
  guarantee the claim makes), and the descendants a selected root gets after
  the listing move with it (a tree's members are read when the root is held).
- Local to local takes one owner a page; each page's wait of the visibility
  deadline runs while the next pages are held (up to 8 pages staged ahead,
  *Staged segments*).
- A push opens its transfer session before its first hold, so one that cannot
  open holds nothing.

**Stops.** A failed session, an unknown outcome, a refusal naming no hold, an
answer from another workspace id than the first, and, from a local SRC to a
local DST, a delivery that fails as a whole stop that destination: no new page
is held for it and nothing more is driven, and a failure of a pull's session to
SRC stops every destination of that SRC. The unsent holds of the page follow
the rules of their segments above. A page a push was holding ahead is cut as
soon as the destination stops, before its next claim (or batch of `io.workers`
claims; those running finish, and after Ctrl-C the command waits for them,
saying so), and its holds, never sent, are taken back after a refusal naming
no hold (`not_moved`), and otherwise (an unknown outcome, a failure of SRC, an
interrupt) stay held for `--resume`, reported as not delivered. The command then exits nonzero and says, for each stopped
destination, why, how many selected jobs were not attempted (or that the
selection was not, when the stop came before it; the holds not yet listed, when
it came while the earlier holds were), and that
`httk job move --resume SRC DST` drives the holds left on SRC; `--resume`
re-drives holds and does not select the rest, so run the transfer again for
that. A page of a pull whose answer was lost may have been held: the message
says so, never that nothing was held, and `--resume` drives it. A failure of
SRC with no destination to carry it (an unknown remote workspace for
`--resume`, say) is reported on its own, with the `--resume` remedy. An error
between pages (an owner that cannot be registered, a hold that cannot be read)
stops the destination (reading SRC's holds: every destination) and the pages
already driven are still reported.

The hidden protocol option `job move --hold` still holds everything first.

**Recovery.** Delivery is at least once. Every run, and
`job move --resume`, first re-drives the holds of SRC bound for DST
(`--resume` without DST re-drives every hold to the destination it records), so
a transfer interrupted at any step is finished by running it again. A pushed
hold records the destination locator only (no probe runs before a push); DST's
workspace id is learnt from the adoption and recorded in the away copies. A
hold that does record an id (a local DST, or `--destination-id`) is adopted
only by that workspace: the far side refuses a bundle that records another
workspace's id. Accepted limitation: if a push's answer was unreadable and the
remote workspace NAME is then pointed at another workspace before `--resume`,
the resume delivers to the new workspace and the jobs exist in both; after such
a change, check with `httk job show` or `httk job list` on the remote before
resuming. `workspace status`
lists the holds of a workspace (transfer id, root job, member count,
destination, age), `job why JOB` explains a held job, and `httk project check`
reports holds and incoming copies older than seven days. A transfer
abandoned for good is taken back into SRC with
`httk job adopt --workspace SRC SRC/.httk-workspace/transfers/outgoing/<transfer id>`,
which is safe only after checking that DST does not have the jobs;
`job move --release T` discards a hold whose jobs DST has (add `--keep
--destination-id ID` to keep away copies, as above). The hidden protocol option
`job move --hold (--job JOB … | --state STATE … | --step STEP …) SRC DST` only holds jobs in a local SRC,
recording DST as given, for a later `--resume` or `--release`; with `--json` it
prints `{"holds": [...], "held_before": [...], "errors": [...]}`, where
`held_before` lists, in the same shape, the other holds of SRC already bound for
the same DST locator. `--release T --keep --destination-id ID` keeps away copies
that record ID as their destination workspace when the hold recorded none (a
push learns that id from the adoption report).

### Away copies

`push` leaves in SRC an *away copy* of every job it moves
(`--no-keep` opts out), and `job move --keep` does the same; a plain
`job move` moves. An away copy is a job in the seventh unowned state,
`away` (`jobs/away/<placement>/…`). It never runs, is not collected and cannot
be transferred or requested; only `job delete` works on it. Its `state.json`
records `away`: the destination workspace id and locator, the transfer id, the
state and priority the job left, and the time. After a keeping push the
command prints `kept N away copies in SRC (httk job list --state away; httk job
recover JOB to resend)`, and `--json` outcomes carry `"kept": true|false`.
`httk job list --state away` lists what you are still waiting for, and `job
why` and `job show` explain an away job.

`pull` (or any trusted transfer back) replaces the away copy when the job
returns from the workspace it was pushed to. A bundle from another workspace
is refused: `KEY is away at X, but this bundle comes from workspace Y; recover
or delete the copy first`. An interrupted transfer finished with `--resume` is
released with the `--keep` of the run that releases it (a push re-drives with
keeping unless `--no-keep`).

`job recover (JOB … | --all [--placement PREFIX]) [--workspace WS]` returns an
away tree, its root and the spawned descendants, to the states and priorities
it left, so that a job lost at its destination can be pushed again. A job that
is not a root is refused with a message naming its root. It prints `recovered
KEY: N job(s) back to STATE`; refusals print `KEY: <message>` on stderr and exit
`1`. It works on local workspaces only. Nothing checks whether the job still
runs at its destination: recovering one that does and pushing it elsewhere
runs it twice. A late return of the old remote job is refused afterwards:
nothing is overwritten and the hold stays on the remote, so delete one of the
two, or adopt the bundle in another workspace. Away copies are not kept
through the workspace daemon's exchange.

## Running managers

### `run`

| Command | What it does | Notable options |
| --- | --- | --- |
| `run` | run managers through the workspace launcher until the workspace is idle, or keep serving with `--keep-running`; `--campaign` starts a manager per campaign partition (see [Campaigns](#campaigns)) | `--workspace`, `--campaign`, `--partition`, `--workers`, `--worker-resource`, `--allocation`, `--count`, `--pool`, `--capability`, `--placement`, `--keep-running`, `--idle-timeout`, `--idle-grace`, `--time-limit`, `--deadline-margin`, `--drain-timeout`, `--join-grace-seconds`, `--gc-interval`, `--inline`, `--launcher`, `--detach`, `--log-level`, `--log-file`, `--json-logs`, `--adapter-timeout` |

It follows the binding: a local workspace uses its `manager.launch` setting
(the built-in `process` launcher by default), and a remote workspace invokes the
same command on its owner. It also takes `--setting KEY=VALUE` (repeatable, not
shown in `--help`), which pins `manager.confine`, `manager.launch_template`,
`manager.launch_mpi`, `manager.confine.block_mpi_spawn`, `manager.bind_cpus` or
a `confine.*` key on the managers for their lifetime; any other key is refused.

The workspace settings `manager.launch`, `manager.count`, `manager.workers`, and
`manager.command` supply the defaults of `--launcher`, `--count` (managers at
the launch site), `--workers` (attempts per manager) and the manager command
used after `environment.prelude`. `--inline` forces one in-process manager, and
`--detach` returns after starting the managers. See {doc}`launchers`.

Through a Slurm launcher whose effective settings (workspace settings
overridden by the launcher's) set `slurm.time_limit` or `slurm.time_limit_max`,
`--time-limit` sets the jobs' Slurm `--time` instead of being passed to the
managers, bounded by `slurm.time_limit_max` (default `slurm.time_limit`);
otherwise it stays the managers' own deadline; see {doc}`launchers`.
`--time-limit`, `--deadline-margin`, `--worker-resource` and `--allocation`
are explained in {doc}`taskmanager` under resources, time requirements and
allocations; the startup banner, idle summary and logs under running managers.

On a workspace of a `mount-daemon` remote (`-w REMOTE:WS`), `run` asks the
remote's daemon to start one approved manager configuration with a typed,
signed request: `--configuration NAME` (required there) names it, `--time-limit`
sets the job's Slurm `--time` (above 120 s and up to the configuration's
`slurm.time_limit_max`), `--request-id` fixes the request ID for a retry and
`--wait-seconds` (0.05 to 120, default 10) bounds the mailbox wait. No other
manager option applies there, and `--configuration`, `--request-id` and
`--wait-seconds` are refused for every other workspace. `httk manager status`,
`log` and `cancel` follow the started managers; see {doc}`workspace_daemon`.

Every confined manager without placement prefixes that takes the `default` pool
or an `exchange-` pool serves the workspace's `exchange/` extension once
`httk daemon init` has enabled it.
Without `--keep-running`, such a manager does not exit as idle within `--idle-grace`
seconds of starting (default 120), nor while it can still adopt an inbox entry
or return a finished exchange tree of its pools. The grace also holds any
manager whose ready jobs wait for unavailable confinement; still blocked after
it, the manager exits with status 2. See {doc}`taskmanager`.

### `precheck`: readiness before an attempt

```console
httk job precheck --workspace WORKSPACE
httk job precheck --workspace WORKSPACE --placement project/screening --json
```

This read-only, advisory report checks `ready`, `waiting`, and `paused` jobs:
environment resolution, the workflow and its calls installed and built, claim
eligibility against the live managers, the engine modules of a built-in format,
and required inputs. It exits `1` for any problem; see
[the precheck](taskmanager.md#the-precheck).

### `launcher`: the bundles that start managers

| Command | What it does | Notable options |
| --- | --- | --- |
| `launcher list` | list manager launchers visible to this project | `--json` |
| `launcher add NAME [KEY=VALUE...]` | create a launcher from the packaged `slurm` kind, with initial settings | `--kind`, `--global`, `--non-interactive` |
| `launcher set NAME [KEY=VALUE...]` | update launcher settings: the pairs, then `--add`, then `--remove`, validated together | `--add KEY=PATH[:PATH...]`, `--remove KEY=PATH[:PATH...]` |
| `launcher unset NAME KEY...` | remove launcher setting overrides | |
| `launcher show NAME...` | describe launchers and their settings, each marked by whether the launcher reads it | `--json` |
| `launcher check NAME...` | check a launcher's required binaries | `--adapter-timeout` |
| `launcher remove NAME...` | remove launcher bundles | `--yes` |

See {doc}`launchers` and {doc}`launcher_authoring`.

### `monitor`: inspect and control workspaces interactively

| Command | What it does | Notable options |
| --- | --- | --- |
| `monitor` | open the curses workspace monitor | `--workspace WORKSPACE` (repeatable), `--refresh SECONDS`, `--adapter-timeout SECONDS`, `--non-interactive` |

See {doc}`monitor` for the panes, keys, and read budget.

## Collecting results

### `httk collect`: workspaces and calculation trees

`httk collect [PATH]` is a top-level command. What it collects depends on PATH:

| PATH | What is collected |
| --- | --- |
| omitted | the workspace resolved as every workspace command resolves it (`--workspace`, the enclosing workspace, the project default) |
| not a directory | the registered workspace of that name |
| a workspace root | that workspace |
| a directory inside a workspace | refused: collect the workspace root, narrowing with `--placement` |
| any other directory | the calculation tree below it, through the recognized-calculation collectors |
| `--campaign` (no PATH) | the selected partitions of this project's campaign, one workspace after another (see [Campaigns](#campaigns)) |

| Options | Apply to |
| --- | --- |
| `--state`, `--step`, `--placement`, `--job JOB` (repeatable; not with `--placement`), `--raw`, `--allow-job-collector`, `--force` | a workspace only |
| `--dry-run`, `--prefer NAME`, `--exclude PATTERN`, `--collector DIR` | a calculation tree only |
| `--into PATH`, `--id-base BASE`, `--id-series SERIES`, `--no-id-ledger`, `--id-ledger PATH`, `--no-bare-runs`, `--upgrade`, `--records TYPE=NAME[,NAME...]`, `--degraded`, `--fail-fast`, `--batch-size N` | both |

An option for the other kind of target is refused. For a workspace, `collect`
streams one `CollectedJob` summary per finished job as JSON lines; `--raw`
streams `JobRecord` records. With `--into`, a sealed id ledger at
`<into>.ids.sqlite` keeps entry ids stable across rebuilds
({doc}`/details/stable_ids`), `--upgrade` lets an existing store take an
additive layout change, and `--records TYPE=NAME[,NAME...]` (repeatable, once
per type) declares and stores that entry type only as the named registered
records, converting other values to the first, for example
`--records structures=atomistic-numeric-unitcell-structure` for a float-only
structure store (see {doc}`/details/collecting`). Every form ends with one
`httk-workflow-collect-summary` line, and the command exits nonzero when any
job was degraded, failed to store, or had an unreadable `job.json`. A job in
debug mode (`job unlock`) is degraded unless `--force` is given, which collects
it (through its registered collector; the runner copy's hook only with
`--allow-job-collector`) and, with
`--into`, stores its run with no mark that it was unlocked; its report carries
`"unlocked": true` on every line, and `--raw` is not gated. `--force` also lets
`--postprocess` collect such a job. See {doc}`/details/collecting`.

`--postprocess SCRIPT` (workspace only) collects the declared output files of
that postprocess script's last run on each selected job instead of the job
itself: `httk collect WS --postprocess NAME [--job JOB]... [--state S]
[--step STEP] [--placement P] [--into STORE --id-base BASE [--upgrade]]`. The job must
already be collected into the store. It is refused together with `--raw`,
`--allow-job-collector`, `--no-bare-runs`, `--fail-fast`, `--batch-size`,
`--degraded`, `--records`, tree-only options and tree targets, and ends with one
`httk-workflow-postprocess-collect-summary` line. See
{doc}`/details/collecting`.

### `postprocess`: run a curated script

| Command | What it does | Notable options |
| --- | --- | --- |
| `postprocess SCRIPT [JOB...]` | run the declared script SCRIPT for each selected job | `--workspace WS`, `--workflow-dir PKG`, `--state`, `--step`, `--placement`, `--output-dir DIR`, `--timeout`, `--json` |

```console
httk postprocess relaxation-report --workspace WS
httk postprocess relaxation-plot <job-id>
```

Output is written outside the job payload, so a sealed job can be
postprocessed. The output root is `<workspace>/postprocess` by default, the
`postprocess.directory` workspace setting when set, or `--output-dir DIR` for
one invocation; each job's directory below it is
`<root>/<placement>/<job_key>/<NAME>/`. A `JOB` selector cannot be combined with
`--state`, `--step` or `--placement`. With `--json`, each result is one
`httk-workflow-postprocess` object (version 2); without it, one
`job_key<TAB>script<TAB>returncode<TAB>output_dir` line. The command exits `0`
only when every selected script ran and returned `0`. Every completed run also
leaves a receipt `.httk-postprocess.json` in its output directory (script,
workflow, job, times, exit status and the declared `outputs`), which
`httk collect --postprocess` reads.

### `v1 collect`: harvesting finished *httk* v1 trees

```console
httk v1 collect --workflow-dir PKG ROOT
httk v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

`v1 collect` ends with one `httk-workflow-v1-collect-summary` line reporting
`finished`, `unfinished_by_status`, and `skipped_no_rundir`. Each collected
report carries `identity_stable`: `false` for a task whose identity is
path-derived because it has no `ht.manifest`. With `--into`,
`--records TYPE=NAME[,NAME...]` selects the stored record kinds as for
`httk collect`.

## Remotes

Named after `git remote`, a *remote* is one machine this project can reach,
together with the bundle of adapter operations that reaches it. `REMOTE:NAME`
names a workspace on a remote. The name `local` is reserved for the built-in
remote meaning "this machine", so `remote add local` is refused.

### `remote` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote list` | list the remotes this project can reach | `--json` |
| `remote add NAME [KEY=VALUE...]` | create a remote from a packaged adapter; pairs then run exactly what `remote set` runs on the new bundle, and if that fails the remote stays created | `--kind`, `--global`, `--non-interactive`, `--adapter-timeout` |
| `remote set NAME KEY=VALUE...` | run the adapter's `configure` operation with new settings and store them when it succeeds | `--adapter-timeout` |
| `remote unset NAME KEY...` | remove stored settings or credentials, running `configure` with the remaining settings | `--adapter-timeout` |
| `remote check NAME...` | check that `httk` answers on remotes, with their stored settings; a `mount-daemon` remote sends its pinned daemon a typed health request and reports its `daemon.json` (identities, approved configurations, digests and published resource settings) under `daemon` | `--adapter-timeout` |
| `remote import-v1 SOURCE...` | map legacy *httk* v1 computer bundles | `--name` (one source only), `--global` |
| `remote show NAME...` | describe remotes and their stored settings, locally (a `mount-daemon` remote's pins included; the mount is never read) | `--json` |
| `remote remove NAME...` | remove remote bundles; refused while a registered workspace holds a transfer bound for the remote | `--yes` skips only the confirmation |
| `remote take-back REMOTE NAME [DESTINATION]` | take your bundle back from a `mount-daemon` remote's `inbox` before a manager takes it | `DESTINATION` defaults to `./NAME` |

For a `mount-daemon` remote, `remote set REMOTE exchange=PATH` (or `remote add
NAME --kind mount-daemon exchange=PATH`) pins the workspace published in the
mounted `EXCHANGE/exchange.json` and, when present, the daemon identities in
`EXCHANGE/daemon.json`.

| Command | What it does | Notable options |
| --- | --- | --- |
| `manager status REMOTE` | print passive status, inspect a manager with `--handle`, or inspect an original job command with `--request-id` | mutually exclusive `--handle` / `--request-id`; `--wait-seconds` |
| `manager cancel REMOTE` | request manager cancellation | required `--handle`; `--request-id`, `--wait-seconds` |
| `manager log REMOTE` | print a manager's published log | required `--handle` |

The daemon itself is set up on its host with `httk daemon` (see
{doc}`workspace_daemon`):

| Command | What it does | Notable options |
| --- | --- | --- |
| `daemon init WORKSPACE [KEY=VALUE...]` | approve global `slurm` launchers, create and enable the exchange when it is not enabled and enroll the workspace, then check the sandbox | `--add KEY=VALUE` |
| `daemon set WORKSPACE KEY=VALUE...` | change the daemon configuration (values, then `--add`, then `--remove`), activated by `reload` or when the daemon next starts | `--add KEY=VALUE`, `--remove KEY=VALUE` |
| `daemon unset WORKSPACE KEY...` | clear `sacct` or `slurm_conf`, or reset `max_submissions` or `force` to its default | |
| `daemon show WORKSPACE` | describe the enrollment and its daemon configuration | `--json` |
| `daemon check WORKSPACE` | activate the configuration and check the real sandbox and scheduler clients | |
| `daemon run WORKSPACE` | activate the configuration and run the confined Slurm broker | `--once` |
| `daemon reload WORKSPACE` | activate the changed configuration and launcher bundles; a running daemon restarts on it | |

`remote show NAME` reports which file each setting came from, but never a
credential value.

### Remote definitions and templates

Remote definitions are versioned directories containing `remote.json` and one
executable `adapter`; project definitions shadow global ones. {doc}`adapter_authoring`
is the reference for writing your own: the bundle layout, the request and
result documents of the six operations (`configure`, `install`, `invoke`,
`push`, `pull`, and `status`), and the optional `daemon` operation. The module
packages four maintained adapters for `remote add --kind`:

- `local`: same-machine transport;
- `ssh`: one `tar` stream per transfer plus command execution over SSH;
- `mount`: a locally mounted remote filesystem plus a command executor;
- `mount-daemon`: files as the only channel, through a mounted exchange
  directory, plus typed requests to a confined broker.

The first three use the target workspace's `manager.launch` setting.
`mount-daemon` refuses generic `REMOTE:NAME` operations; move jobs with
`job eject` into the exchange inbox and `job adopt --move` out of its outbox.
See {doc}`/details/remotes` for configuration and request-ID retry rules.

### Remote settings

`remote set NAME KEY=VALUE...` persists only the machine-level keys
`check_connectivity`, `check_mount`, `cli_prelude`, `connection_reuse`,
`exec_command`, `exchange`, `host`, `httk_command`, `interactive_auth`,
`legacy_settings`, `mount_root`, `port`, `remote_root`, `username`,
`vasp_command`, and `vasp_pseudo_library` in the shareable `remote.json`. Scheduler profile values are workspace settings
(`slurm.*`, `manager.workers`) and are refused as remote settings. Other keys
are stored in the remote's `credentials.json` beside it, with mode `0600`;
project manifests exclude that file.

`remote import-v1` maps legacy *httk* v1 computer bundles by reading
assignment-only configuration; legacy shell executables are never run, and
legacy `SLURM_*` values belong in a `slurm` launcher instead.

`local` copies files in this filesystem and runs child processes; `ssh` moves
files as one `tar` stream over a single `ssh` connection per push or pull
and runs commands on the configured host; `mount`
copies through a locally mounted view and runs commands through `exec_command`.
`httk_command` overrides how `httk` is spelled on the far side. Every subprocess
an adapter starts is an argument vector, quoted element-wise where `ssh` needs a
far-side shell.

### httk on the target: `remote check`

*httk* is never installed on a remote for you. The requirement is that the
adapter's *non-interactive* shell can run `httk`, with *httk-workflow* installed
beside the core. Run `remote check` once after configuring a new remote: it
confirms that the host answers, that `httk` is found (also trying
`python3 -m httk.core.cli`), and that the workflow command group exists. If the
check fails, set up *httk₂* on the remote reachable from a non-interactive
shell, use the remote's `cli_prelude` setting, or point `httk_command` at it.

## Configuration and projects

### Per-user configuration

| Command | What it does | Notable options |
| --- | --- | --- |
| `config show [KEY]` | print the configuration, or one member | `--json` |
| `config set [KEY=VALUE...]` | store members: the pairs, then `--add`, then `--remove`, validated together and saved once | `--add machine_names=NAME`, `--remove machine_names=NAME`; `machine_names` is a comma-separated list of names this machine answers to; `connection_reuse` (`yes`/`no`) is the `connection_reuse` of every remote that sets none |
| `config unset KEY...` | remove members | |
| `config import-v1 [SOURCE]` | read a legacy `~/.httk` configuration | |

User configuration lives below `$XDG_CONFIG_HOME/httk/` (`config.json`, the
core-managed `identity.json` and `keys/`, global remotes and launchers);
`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` override it.

### Projects

The project anchor and every project verb (`init`, `show`, `import-v1`,
`export`, `check`, `repair`, `manifest`, `verify`, `seal`, and `unseal`) belong
to *httk-core*. *httk-workflow* registers the
**workspace** as a project-*member* kind, so core's verbs delegate a
workspace's internals to it: what to leave out of a manifest, how to seal it,
its seal digest, how to verify it, and its health checks.

A workspace inside a project is recorded in `httk_project/members.json`:
registered on `httk workspace init`, unregistered on `workspace delete` or
`workspace forget`, and its path followed on `workspace move`. On another
machine, `httk workspace register` (per workspace) or `httk project repair` (every
member) registers copied workspaces under their recorded names.

`httk project repair` handles conditions that quietly break a project later:
dead owners not yet recovered, scratch of unknown owners, old temporaries, holds
and incoming copies older than seven days, a workspace missing from the
registry, and a member not yet adopted on this machine. It repairs only through
`gc` and adoption. `httk project check` only reports, and `--no-adopt` skips adopting.

To seal a project, run `httk workspace seal` and then `httk project seal`.
Verify the whole tree with `httk project verify`.

### `job verify` and `workspace verify`: verify seals

| Command | What it does | Notable options |
| --- | --- | --- |
| `job verify JOB...` | verify each job's seal: its signatures and the digests of the files it seals; a payload directory is verified where it is, inside a workspace or free-standing | `--workspace`, `--json`, `--trusted-key KEY_OR_FINGERPRINT` (repeatable), `--content` |
| `workspace verify [WORKSPACE...]` | verify a workspace seal and report every job that drifted since the snapshot | `--json`, `--trusted-key`, `--deep` (also verify every recorded job seal, as `job verify`), `--content` (implies `--deep`; as `job verify --content`) |

`--content` also decompresses job files and checks them against their sealed
`uncompressed.sha256` manifests. The final status word and exit code of
`job verify` and `workspace verify --deep` mirror `project verify --manifest`:
`ok` (exit 0) when every entry is `valid_trusted`, `UNTRUSTED` (exit 3) when no
entry is invalid but a signer is untrusted, `FAILED` (exit 1) on any invalid
entry, an unsealed job included. See {doc}`sealing`.

## Signed manifests and operator identity

### Signed manifests

```console
httk project manifest .
httk project verify --manifest
httk project verify --manifest --trusted-key keys/collaborator.pub
```

The *httk₂* manifest is deterministic canonical JSON lines compressed with
bzip2. It records sorted POSIX paths, regular-file sizes and SHA-256 hashes,
empty directories, and symlink targets; special files are rejected. A
domain-separated body digest is signed with Ed25519. A member workspace is left
out of the project manifest: it is covered through the workspace seal chain
instead.

### What a verified manifest proves

The signing key lives in the tree it signs: `httk_project/keys/project.seed`
has mode `0600` and is excluded from the manifest, but not absent. **Anybody who
can write the project directory can re-sign it.** A manifest proves that the
tree is exactly what somebody with the seed described; it is not a tamper seal
against an attacker with write access. Verification compares the signing key
with a trust anchor that did not come from the manifest: the key pinned in
`project.json` at `httk project init`, plus any key named with `--trusted-key`.

| Verdict | Exit | Meaning |
| --- | --- | --- |
| `valid_trusted` | `0` | the manifest describes this tree and a pinned key signed it |
| `valid_unknown_key` | `3` | the manifest describes this tree, but nothing here pins the key that signed it |
| `invalid` | `1` | the tree does not match the manifest, the signature does not verify, or the manifest names another project |

A project made before key pinning existed verifies as `valid_unknown_key`
until a key is adopted explicitly with
`httk.workflow.projects.pin_project_key` or `trust_project_key`.

### Operator identity

Operator identity (the recorded name, email, default, and named identities)
lives in `$XDG_CONFIG_HOME/httk/identity.json`, managed by *httk-core*. `httk
setup` creates the first named identity and its key pair and makes it the
default; `httk identity add --name NAME --email EMAIL SHORT` adds more, and
`httk identity default SHORT` changes the default.

A request verb records the selected identity's `Name <email>` label and signs the
request with that identity's key. Omitting `--operator` selects the default
identity. A selector containing `<` is a literal `Name <email>` label, signed
with the default identity's key; any other selector must be a configured short
name. For remote requests the control-center identity signs locally. If a
configured identity's key file is missing or unreadable, the request fails
loudly.

Signatures are detached, cover the canonical JSON of the whole document, and are
domain-separated from every other *httk* signature. They are optional: a
request without one is applied, but one whose signature does not verify is
malformed and quarantined by `gc`. A signature means attribution, not
authorization: anyone who can write the workspace's request directory can still
post an unsigned request. Job control through the workspace daemon is the
exception: the daemon accepts only commands signed by a key in its own
`authorized_keys` policy (see {doc}`workspace_daemon`).

## Campaigns

| Command | What it does | Notable options |
| --- | --- | --- |
| `campaign init` | define the project's partition map and assignment policy | `--partition NAME=WORKSPACE`, `--assignment` |
| `campaign set KEY=VALUE...` | set individual partitions or the assignment policy | `partitions.NAME=WORKSPACE`, `assignment=POLICY` |
| `campaign unset KEY...` | remove partitions or reset the assignment to `hash` | `partitions.NAME`, `assignment` |
| `campaign remove` | clear campaign configuration, keeping jobs and workspaces | `--yes` skips confirmation |
| `campaign show` | show the partition map | `--json` |

A campaign is a thin convention over registered workspaces: a partition map,
stored in the project, that spreads a very large body of work across many
workspaces without a new scheduler. Roots are assigned to partitions by policy,
and spawned children always inherit their parent's workspace. `job new`,
`collect` and `run` act on the campaign with `--campaign` (and `-w/--workspace`
is refused, since each partition names its workspace):

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new SOURCE --campaign` | assign each new job to a partition and submit it into that partition's local workspace (`hash`: by the tag known before scaffolding, not a hook-derived one) | `--partition NAME` (required by `explicit` assignment; a pin otherwise), `--campaign-index N` (round-robin start, default 0), every `job new` option |
| `collect --campaign` | collect the selected partitions' local workspaces one after another as one sweep (one summary, one `--into` store) | `--partition NAME` (repeatable; default all), every workspace `collect` option except `--job` and `--postprocess` |
| `run --campaign` | start a manager per selected partition: through its launcher locally, or on the remote owner | `--partition NAME` (repeatable; default all), `--workers`, `--worker-resource`, `--count`, `--launcher`, `--idle-timeout`, `--adapter-timeout` |

The assignment policy picks a new job's partition: `hash` hashes the job's tag
(`--tag`, or the tag an input supplies; without one the job is refused; a tag
an instantiate hook derives does not count, so such a workflow needs `--tag`
or `--partition`), `round-robin` numbers the jobs of one `job new` batch from
`--campaign-index` (refused under the other policies and with `--partition`),
and `explicit` takes `--partition`. A batch is checked in every partition it
reaches before anything is submitted (an uninstalled workflow, a taken or
repeated tag); see {doc}`/campaigns` for the failures that can still leave a
partial fan-out. SOURCE is a workflow installed in the
partition's workspace, or with `--install` a package directory, git URI or
workflow name to install there first; a runner file, document or `--command`
is refused. A remote partition is refused by `job new` and `collect` (submit
locally and `push`; `pull` it home to collect). `collect --campaign --into`
uses the default id ledger as a calculation tree does, signed with the
project's keys. See {doc}`/campaigns`.

## Protocol spellings and removed commands

One machine runs these argument vectors on another over an adapter. They are
protocol, not operator interface, and hidden from `--help` where they are not
ordinary commands; their spelling is frozen because the far side may run a
different *httk*. A transfer itself runs over the hidden `workflow
transfer-session`, whose one protocol version both ends must speak (see *The
transfer session* in {doc}`remotes`); the release and the listing below are what
an operator's `job move --release` and `workspace status` run on a remote:

```text
httk workflow transfer-session
httk job move --release TRANSFER_ID [--release TRANSFER_ID …] [--keep [--destination-id ID]] WS
httk workspace status --json WS
httk job request-envelopes ACTION --workspace WS --operator LABEL --reason TEXT [--priority N] [--override-step S] [--workflow W] [--destination D] [--max-attempts-per-activation N|unlimited] [--max-total-attempts N|unlimited] [--max-activations N|unlimited] [--force] [--keep-budget] [--tree] JOB_ID …
httk job publish-requests --workspace WS --document JSON [--document JSON …] [--wait] [--timeout S] [--sequential]
httk workspace status --by-path --json PATH
httk run --by-path --workspace PATH
```

The hidden `--by-path` switch makes the workspace argument a literal path with
no registry lookup, so one machine can address another machine's workspace.
The hidden `job delete --confirmed` is the remote half of a confirmed delete.

### Removed commands and options

The filesystem kernel of this version replaced state markers, the journal,
leases, receipts and the maintenance lock, and the command line has since renamed
some commands and options. These spellings no longer parse:

| Removed | Now |
| --- | --- |
| `httk runner publish`, `runner describe`, `--runner-search-path` | `httk workflow install --workspace WS`; `job new --install` |
| `job new --publish`, `--data-mode`, `--workdir-mode` | install-before-run; workflows choose their data with parameters such as `publish_data` |
| `job submit --placement` | the placement comes from the payload's `job.json` |
| `job seal --keys` | the workspace's `seal.keys` setting |
| `job confirm-launches-ended` | `workspace attest-dead` once the owner and its launches are gone |
| `job eject --resume` | an interrupted eject is rolled forward or back by recovery |
| `httk workflow transfer offer\|receive\|retire\|reclaim` | `job move` (whose first leg is the hidden protocol option `job eject --hold`), `job adopt`, `job move --release` |
| `workspace unlock` | removed with the maintenance lock; there is nothing to release (`job unlock` is unrelated: it puts one job in debug mode) |
| `workspace forget --force` | `workspace forget` |
| `workspace fsck --quarantine-unrepairable` | `workspace repair` |
| `workspace seal --force` | `job seal` the remaining jobs first, or seal the workspace with them listed as unsealed |
| `manager run --lease-seconds`, `--takeover-grace-factor`, `--unsafe-persistent-takeover`, `--unsafe-isolated-takeover` | recovery only after the death proof or `workspace attest-dead` |
| `transfer send`, `transfer fetch`, `workspace status REMOTE` (by remote) | `job move --job JOB … SRC DST`; `workspace status WS` lists one workspace's holds (`REMOTE:WS` for a remote one) |
| the remote setting `transfer_session` (`off` forced the adapter operations) | nothing: transfers always use a transfer session, and both ends must run the same *httk* version; `remote set NAME transfer_session=…` is refused as an unknown setting |
| `job adopt --stdin-tar` (with the hidden `--token`, `--sync`, `--built-for`), `workflow install --stdin-tar`, `--from-store`, `--source-label`, `--build`, `--also-build`, `workflow list --built-for` | the transfer session's own requests; `job adopt BUNDLE…` adopts bundles on disk |
| the adapter operation `deliver` | the transfer session (`session` operation) |
| `job list --kind`, `job move --kind` | `--state STATE` (and `--step STEP` to select by step) |
| `--force` on `job delete`, `job unseal`, `workspace delete`, `workspace unseal`, `launcher remove`, `remote remove`, `campaign remove` | `--yes` (pre-confirms the prompt; `workspace delete` now asks too) |
| `--durable` | nothing: durability is the default; `--no-durable` opts out |
| `config configure --set/--unset/--add/--remove`, `config set KEY VALUE` | `config set KEY=VALUE... [--add KEY=VALUE] [--remove KEY=VALUE]`, `config unset KEY...` |
| `campaign configure --set/--unset` | `campaign set KEY=VALUE...`, `campaign unset KEY...` |
| `remote configure NAME... --set KEY=VALUE --unset KEY` | `remote set NAME KEY=VALUE...`, `remote unset NAME KEY...` (one remote per call) |
| `remote add --template KIND` | `remote add --kind KIND NAME [KEY=VALUE...]` (one remote per call) |
| `remote check --set KEY=VALUE` | `remote set` first: `remote check` checks the stored settings |
| `launcher configure NAME... --set/--unset/--add/--remove`, `--add-path`, `--remove-path` | `launcher set NAME [KEY=VALUE...] [--add KEY=PATH[:PATH...]] [--remove KEY=PATH[:PATH...]]`, `launcher unset NAME KEY...` |
| `launcher add --template KIND --set KEY=VALUE` | `launcher add --kind KIND NAME [KEY=VALUE...]` (one launcher per call) |
| several NAMEs in one `remote add`/`configure` or `launcher add`/`configure` call; exit status 1 for their validation errors | one NAME per `remote add/set/unset` and `launcher add/set/unset` call; a validation error of these commands exits 2 |
| `job new --step`, `job debug --step` | `--start-step` |
| `job new --workflow NAME`, `--workflow-dir DIR`, `--from-runner FILE` | `job new SOURCE` (the kind is detected as by `workflow install SOURCE`) |
| `job new --from-command TEMPLATE` | `job new --command TEMPLATE` |
| `job new --name NAME` | nothing: the tag (`--tag`) is the readable job handle; `job.json` keeps its derived `name` |
| `campaign submit --workflow W --key K [--index N]` | `job new W --campaign` (`--tag` is the hash key, `--partition NAME` the explicit one, `--campaign-index N` the round-robin start) |
| `campaign collect` | `collect --campaign [--partition NAME]...` (with `--into X` it keeps the default id ledger: a store built by the ledger-less `campaign collect --into X` gets a fresh `X.ids.sqlite` on its next sweep unless `--no-id-ledger` is passed or the store is rebuilt) |
| `campaign start-managers` | `run --campaign [--partition NAME]...` |
| `--format` on `job new`, `workflow describe`, `workflow install` | `--language` |
| `manager run --idle`, `workflow run --idle` | `--keep-running` |
| `manager run --placement-prefix`, `workflow run --placement-prefix` | `--placement` |
| `launcher check --launcher-timeout` | `--adapter-timeout` |
| `httk init` | `httk setup` |
| `httk system reset [-f]` | `httk setup --reset [--yes]` |
| `workspace configure NAME... --set KEY=VALUE --unset KEY`, `workspace settings set\|unset --key KEY --value VALUE` | `workspace set -w WS KEY=VALUE...`, `workspace unset -w WS KEY...` |
| `workspace show NAME... [--key KEY]`, `workspace settings show` | `workspace show -w WS [KEY...]` (also lists `policy.*` and `prelude.*`) |
| `workspace policy show\|set\|unset --key KEY --value JSON` | `workspace show\|set\|unset` with `policy.KEY` (e.g. `policy.retention.trash_days=14`) |
| `workspace workflow-prelude show\|set\|unset --workflow ID --value TEXT` | `workspace show\|set\|unset` with `prelude.ID` (`prelude.ID=@FILE` reads a file) |
| `workspace init --setting KEY=VALUE`, `workspace init PATH PATH...` | `workspace init PATH [KEY=VALUE...]` (one path per call) |
| `httk project adopt` | `httk project repair` (adopts members unless `--no-adopt`) |
| `httk project repair --dry-run` | `httk project check` |
| `httk project manifest create` | `httk project manifest` |
| `httk project manifest verify [--manifest PATH]` | `httk project verify --manifest`, or `--manifest-file PATH` |
| `httk project verify-seal` | `httk project verify` |
| `httk project verify-export ZIP` | `httk project verify ZIP` |
| `httk project unseal --force` | `httk project unseal --yes` |
| `job request cancel\|pause\|restart` | `job cancel`, `job pause`, `job restart` (`--reason` is optional; it defaults to the command, e.g. `httk job pause`) |
| `job request continue` | `job resume` |
| `job request set_priority --priority N`, `override_step --override-step S`, `set_workflow --workflow W`, `set_budget --max-attempts-per-activation N --max-total-attempts N --max-activations N` | `job set JOB... priority=N step=S workflow=W budget.max_attempts_per_activation=N budget.max_total_attempts=N budget.max_activations=N` |
| `job request delete\|seal\|unseal\|detach\|unlock` | `job delete`, `job seal`, `job unseal`, `job detach`, `job unlock` (which reach a remote workspace as signed requests too; over a remote they record the reason `httk job VERB`, and only `job unlock` takes `--reason`) |
| `job request eject --destination D` | `job eject JOB [D]` (for a workspace daemon, `D` is under its outbox) |
| `workflow precheck` | `job precheck` |
| `job transfer` | `job move` |
| `job push`, `job pull` | `httk push`, `httk pull` |
| `httk transfer status [WS]` | `httk workspace status [WS]` (the held transfers; `transfers` in `--json`) |
| `httk seal verify [PATH]` | `httk job verify JOB...` (a payload directory too), `httk workspace verify [--deep] [--content]`, `httk project verify` |
| `httk workflow run`, `httk manager run` (and the remote vector `httk workflow manager run`) | `httk run` |
| `httk workflow monitor` | `httk monitor` |
| `httk workflow postprocess --script SCRIPT [JOB...]` | `httk postprocess SCRIPT [JOB...]` |
| `httk workflow describe TARGET...` | `httk workflow show TARGET...` (the installation in the workspace, else this machine's resolution) |
| `httk workflow list` without `--workspace` (this machine's workflows) | `httk workflow list --available`; `workflow list` lists the default workspace |
| `httk workflow install URI...` without `--workspace` (fetch only) | `httk workflow fetch URI...`; `workflow install` installs into the default workspace |
| `httk workflow uninstall SELECTOR...` without `--workspace` (forget fetched) | `httk workflow forget SELECTOR...`; `workflow uninstall` removes from the default workspace |
| `httk workspace workflows [NAME...]` | `httk workflow list [-w NAME]` |
| `httk workspace adopt [PATH...]` | `httk workspace register [PATH...]` |
| `httk workspace fsck [NAME...]` | `httk workspace check [NAME...]` |
| `httk workspace fsck --repair [--yes] NAME...` | `httk workspace repair [--yes] NAME...` |
| `httk workspace daemon init\|show\|check\|run\|reload WS` | `httk daemon init\|show\|check\|run\|reload WS` |
| `workspace daemon init WS --set KEY=VALUE` | `daemon init WS KEY=VALUE...` |
| `workspace daemon configure WS --set/--add/--unset/--remove` | `daemon set WS KEY=VALUE... [--add KEY=VALUE] [--remove KEY=VALUE]`, `daemon unset WS KEY...` |
| `httk workspace exchange enable [WS]` | `httk daemon init WS` (creates and enables the exchange) |
| `httk remote daemon configure REMOTE --exchange PATH` | `httk remote set REMOTE exchange=PATH` (or `remote add NAME --kind mount-daemon exchange=PATH`) |
| `httk remote daemon health REMOTE`, `remote daemon show REMOTE` | `httk remote check REMOTE` (the health response and `daemon.json`) |
| `httk remote daemon start REMOTE --configuration NAME` | `httk run -w REMOTE:WS --configuration NAME` |
| `httk remote daemon status\|log\|cancel REMOTE` | `httk manager status\|log\|cancel REMOTE` |
| `httk remote daemon take-back REMOTE NAME` | `httk remote take-back REMOTE NAME` |

Workspaces and bundles of the earlier layout are refused with a message that
there is no migration: re-create the workspace with `httk workspace init` and
submit its jobs again.

A job whose `runner.path` pinned a `pkg:httk.workflow.runners/vasp_*` form
breaks too: the VASP workflows left the module for
[workflows-vasp](https://github.com/httk/workflows-vasp). Create the job again
from a workflows-vasp URI (see {doc}`/vasp_runners`).
