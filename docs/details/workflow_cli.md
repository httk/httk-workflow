# Project and workflow command line in detail

This is the complete command reference, from workspaces and jobs to projects,
signed manifests, and remotes. Installing *httk-workflow* registers lazy
`workflow`, `workspace`, `job`, `launcher`, `remote`, `manager`, `campaign`,
`config`, `seal`, `transfer`, `v1`, and `collect` commands with *httk-core*:

```console
httk workflow --help
```

`httk workflow …` covers workflow installation and execution; workspace and job
management are the top-level trees `httk workspace …` and `httk job …`, and
collecting results is `httk collect`. Every group and command answers `--help`,
and a mistyped action is reported by its group. {doc}`taskmanager` explains
what the commands do to a workspace.

## The command tree

```text
httk workspace          show | configure | init | adopt | list | default | move | forget | delete | status | owners (managers) | attest-dead | workflows | settings show|set|unset | workflow-prelude show|set|unset | policy show|set|unset | fsck | gc | seal | unseal | verify | exchange enable | daemon init|configure|show|check|run
httk job                new | submit | request | delete | seal | unseal | detach | eject | adopt | list | show | log | why | debug | transfer
httk collect            [PATH] [--workspace WORKSPACE] [--into PATH] [--dry-run] …
httk workflow list      [--workspace WORKSPACE] [--json]
httk workflow describe  TARGET… [--workspace WORKSPACE] [--json]
httk workflow install   SOURCE… [--workspace WORKSPACE] [--no-calls] [--no-build] [--json]
httk workflow uninstall SELECTOR… [--workspace WORKSPACE] [--check] [--json]
httk workflow build     [--workspace WORKSPACE] [--list] WORKFLOW…
httk workflow precheck  [--workspace WORKSPACE] [--placement P] [--json]
httk workflow run       [--workspace WORKSPACE]  (the recommended spelling of `manager run`)
httk manager            run
httk launcher           list | add | configure | show | check | remove
httk workflow monitor   [--workspace NAME …] [--refresh SECONDS]
httk workflow postprocess
httk v1                 collect
httk remote             list | add | configure | check | import-v1 | show | remove | daemon
httk config             show | configure | set | unset | import-v1
httk seal               verify [PATH] [--json] [--trusted-key KEY] [--shallow]
httk campaign           init | show | configure | remove | submit | collect | start-managers
httk transfer           status
httk init | identity    (core-owned: per-user configuration and named operator identities)
httk project            init | show | import-v1 | export | repair | adopt | manifest create | manifest verify | seal | unseal | verify-seal   (core-owned)
```

### Configuration conventions

Concept groups live directly below `httk`: `httk launcher`, `httk remote`,
`httk manager`, `httk campaign`, `httk config`, and `httk seal`. `workflow`
retains workflow installation, inspection, building and execution.

Use `show [--json]` to inspect configuration and `configure` to change it.
Repeat `--set KEY=VALUE` to add or replace a value and `--unset KEY` to remove
an override. Supported list settings also offer `--add KEY=VALUE` and
`--remove KEY=VALUE`; these change individual members, whereas `--unset`
removes the whole override. Workspace, launcher and remote configuration
removes overrides before setting new values; user, campaign and daemon
configuration applies flags in argument order.

| Configuration | Inspect | Modify | Remove the definition |
| --- | --- | --- | --- |
| Workspace application settings | `workspace show [NAME...]` | `workspace configure [NAME...] --set KEY=VALUE --unset KEY` | `workspace forget NAME` keeps files; `workspace delete --force NAME` deletes them |
| Workspace policy | `workspace policy show` | `workspace policy set --key KEY --value JSON`; `workspace policy unset --key KEY` restores the default | Required format members cannot be deleted |
| Workflow preludes | `workspace workflow-prelude show` | `workspace workflow-prelude set` / `unset` | Unset a workflow's prelude |
| Installed workflows | `workspace workflows`, `workflow list --workspace` | `workflow install --workspace` | `workflow uninstall --workspace` |
| Launcher | `launcher show NAME` | `launcher configure NAME --set KEY=VALUE --unset KEY`; `--add` / `--remove` for confinement path lists | `launcher remove NAME` |
| Remote | `remote show NAME` | `remote configure NAME --set KEY=VALUE --unset KEY`, including private credentials | `remote remove NAME` |
| User configuration | `config show` | `config configure --set KEY=VALUE --unset KEY`; `--add` / `--remove` for `machine_names` | Unset its configurable keys |
| Campaign | `campaign show` | `campaign configure --set partitions.NAME=WORKSPACE --unset partitions.NAME`; set or unset `assignment` | `campaign remove` clears the map, keeping jobs and workspaces |
| Workspace daemon | `workspace daemon show PATH` | `workspace daemon configure PATH --set KEY=VALUE --unset KEY`; `--add` / `--remove` for launchers and authorized keys | Enrollment removal is not provided |

Local workspace batches validate before writing; remote workspace batches use
single-key operations sequentially, so a transport failure can leave earlier
changes applied. Removing daemon enrollment or disabling the exchange is not
offered: exchange clients, running managers and retained submission state must
be settled first.

### Workspace selection

A `WORKSPACE` is an optional registered local name. When it is omitted,
commands resolve the workspace in this order: the closest enclosing workspace
containing `.httk-workspace/format.json`, the project's recorded default, the
registry's default, then an auto-created per-user default. Remote names are
written `REMOTE:NAME` at use time.

The registry is machine-owned and stores only absolute local paths, in
`$XDG_CONFIG_HOME/httk/workspaces.json`. When a command reaches a remote
workspace, the far side resolves the plain name in its own registry.

Remote-capable commands (workspace status, settings, `gc`, `fsck`, `job list`,
`show`, `log`, `why`, `request` and `delete`, `workflow install`, `build` and
`run`) go through the adapter. Jobs are created locally and moved with
`job transfer`.

## Workspaces

### `workspace` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init [OPTIONS] PATH...` | create a workspace in a new or empty directory of its own, or adopt an existing one, registering each name (basename or `--name`) and recording it in the project's `members.json` | `--name` (one path only), `--setting KEY=VALUE`, `--no-durable` |
| `workspace exchange enable [WORKSPACE]` | enable the `exchange/` extension of a workspace (idempotent) | |
| `workspace daemon init WORKSPACE` | approve global `slurm` launchers, enable the exchange when needed and enroll the workspace, then check the sandbox | `--set KEY=VALUE`, `--add KEY=VALUE`, `--state`, `--snapshots` |
| `workspace daemon configure WORKSPACE` | change the daemon configuration, activated when the daemon next starts | `--set`, `--add`, `--unset`, `--remove`, `--state`, `--snapshots` |
| `workspace daemon show WORKSPACE` | describe the enrollment and its daemon configuration | `--json`, `--state`, `--snapshots` |
| `workspace daemon check WORKSPACE` | activate the configuration and check the real sandbox and scheduler clients | `--state`, `--snapshots` |
| `workspace daemon run WORKSPACE` | activate the configuration and run the confined Slurm broker | `--once`, `--state`, `--snapshots` |
| `workspace list [--json] [REMOTE:]` | list local or owning-machine workspaces | |
| `workspace default [--unset] [NAME]` | read or record this project's default name | |
| `workspace adopt [PATH...]` | register copied workspaces on this machine under the names their project's `members.json` records | `--name` (one path only), `--json` |
| `workspace move NAME DEST_DIR` | move a local workspace and update its registry path | `--no-durable` |
| `workspace forget NAME...` | deregister names, leaving workspaces on disk | |
| `workspace delete --force NAME...` | destroy workspaces and deregister them | |
| `workspace status [NAME...]` | job counts by state, the seal, and the owners with their liveness (remote: over the adapter) | `--json` |
| `workspace owners [NAME...]` | list the owners (managers, CLI processes, daemons) with their liveness; `managers` is an alias | `--kind manager\|cli\|daemon`, `--json` |
| `workspace attest-dead OWNER [NAME] --reason TEXT` | declare an owner and its launches ended, so the next manager tick or `workspace gc` recovers its jobs | `--operator NAME`, `--force` |
| `workspace workflows [NAME...]` | list the workflows installed in a workspace | `--json` |
| `workspace show [NAME...]` | print application settings | `--key KEY`, `--json` |
| `workspace configure [NAME...]` | set and unset application settings | `--set KEY=VALUE`, `--unset KEY` (repeatable), `--json` |
| `workspace settings show\|set\|unset` | print, store or remove one application setting | `--key KEY`, `--value VALUE` |
| `workspace workflow-prelude show\|set\|unset` | print, store or remove one workflow's prelude | `--workflow WORKFLOW`, `--value VALUE` (`@FILE` reads a file) |
| `workspace policy show\|set\|unset` | print, store or reset a policy member | `--key KEY`, `--value JSON`, `--json` |
| `workspace fsck [NAME...]` | check the job tree for what the kernel cannot resolve | `--repair`, `--json` |
| `workspace gc [NAME...]` | collect what the retention policy allows and recover dead owners (remote: over the adapter) | `--dry-run`, `--category CATEGORY` (repeatable), `--json` |
| `workspace seal [NAME...]` | record every job's seal digest under one signed workspace seal, listing the unsealed jobs | `--keys REFS` |
| `workspace unseal [NAME...]` | remove a workspace's seal, refused while its project is sealed | `--force` skips the confirmation |
| `workspace verify [NAME...]` | verify a workspace seal and list the jobs that drifted since it; drift exits `1` | `--trusted-key`, `--json` |

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

`workspace move NAME DEST_DIR` is an atomic same-filesystem rename and refuses
cross-filesystem moves. `workspace move` and `workspace delete` refuse while
any owner of the workspace is not proven dead: stop its managers first.
`workspace forget` only removes the name. To move across filesystems, stop
managers, copy the tree, forget the old name, and re-register it with
`workspace init --name NAME <newpath>`.

### Sealing a workspace

Sealing is described in full in {doc}`sealing`. `workspace status` shows a
`sealed` line (and JSON field). `workspace seal` records the seal digest of
every job, recording and listing jobs without a seal as unsealed; `workspace
verify` reports drift since then. A sealed workspace refuses modifying CLI
commands; managers and jobs do not check it.

### Application settings and preludes

Application settings are a flat map of small dotted values a runner resolves
when it runs, such as `vasp.command`; a value that parses as JSON is stored as
that scalar, a bare word as a string. `workspace init --setting KEY=VALUE` seeds
them, and a workspace bound to a remote is also seeded from that remote's
whitelisted settings (a cluster's `vasp_command` becomes `vasp.command`). They
are snapshotted into each attempt, so never store credentials there; see
[application settings](taskmanager.md#application-settings).

```console
httk workspace settings set --key vasp.command --value vasp_std my-workspace
httk workspace settings set --key environment.prelude --value "module load httk" my-workspace
httk workspace workflow-prelude set --workflow relax-vasp --value @prelude.sh my-workspace
httk workspace workflow-prelude show --json my-workspace
```

`environment.prelude` applies to every job and `workflow-prelude` only to the
jobs of one workflow id, after it; both run under `set -e`. A workflow prelude
`VALUE` is stored verbatim, and `@FILE` reads it from a file. Preludes are
workspace-local: a transferred job runs under the destination's preludes. See
[environment preludes](../workspaces.md#environment-preludes).

### Policy, owners, and checks

The workspace policy (`visibility_deadline_seconds` and the `retention.*`
limits) is stored in `format.json`; see
[workspace policy](taskmanager.md#workspace-policy). `workspace owners` lists
who holds jobs, with the liveness the read-only death proof gives from this
host. An owner whose death no probe can prove is declared dead by the operator:

```console
httk workspace attest-dead OWNER WORKSPACE --reason "node rebooted"
```

The death proof runs first: an owner proven alive is refused, and one it cannot
decide needs `--force`. Attesting an owner that is still running can apply a
request twice and run work twice, so attest only after confirming that the owner
process and every launch it started are gone; see
[owners, death and recovery](taskmanager.md#owners-death-and-recovery).
`workspace fsck` and `workspace gc` are described in
[checking a workspace](taskmanager.md#checking-a-workspace) and
[freeing disk](taskmanager.md#freeing-disk).

## Workflows and builds

A job runs only a workflow installed in its workspace. Installed workflows live
in `workflows/<slug>--<h16>/` and are selected by id or short name.

### `install` and `uninstall`

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow install SOURCE...` | install into a workspace: a package directory, a git URI, a runner file or workflow document (installed ad hoc), or a workflow name known here; declared calls are installed too and a `[workflow.build]` package is built | `--workspace`, `--no-calls`, `--no-build`, `--format`, `--json`, `--adapter-timeout` |
| `workflow uninstall SELECTOR...` | remove installed workflows by id or short name | `--workspace`, `--check` (warn about unfinished jobs using it), `--json` |

```console
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow install --workspace kappa:runs ./my-workflow
```

The command prints `ID<TAB>NAME` per installed workflow. A git source is
installed under its canonical URI pinned to the full commit. A `REMOTE:WS`
workspace gets the source pushed through the remote's adapter (a git source is
fetched there by its pinned URI). Without `--workspace`, `install` only fetches
a git URI into this machine's cache and `uninstall` forgets fetched git
workflows (a pinned URI removes that commit, an unpinned URI or a short name its
whole lineage). See {doc}`/details/workflow_uris`.

### `list` and `describe`

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow list` | without `--workspace`, the workflows a name selects on this machine (registered, plugin, fetched git workflows); with it, the workflows installed in that workspace | `--workspace`, `--json` |
| `workflow describe TARGET...` | describe a registered id or alias, git URI, runner file or package directory; with `--workspace`, an installation there | `--workspace`, `--format`, `--json` |

As a report, `describe` runs a directory package's runner entry with
`--describe`. When the manifest's declared `steps` disagree with what the
runner reports, it prints a prominent `WARNING: step drift` line
(`manifest_step_drift` in `--json`), but still exits `0`. Package authoring is
documented in {doc}`workflow_packages`.

### `build`: foreground builds of installed workflows

```console
httk workflow build [--workspace WORKSPACE] [--json] WORKFLOW...
httk workflow build [--workspace WORKSPACE] [--json] --list
```

`build` builds installed workflows (by id or short name) that declare
`[workflow.build]` for this platform, in the foreground, and registers the
artifacts in the installation's `builds/<platform>/`; a `REMOTE:WS` workspace
builds on the remote. `install` already builds for the platform it runs on; run
`build` on each other platform whose managers serve the workspace. `--list`
prints the registered builds. The exit status is `1` when any target failed.
The manager passes the artifacts to the runner through
`HTTK_WORKFLOW_RUNNER_ARTIFACTS`, and a job whose workflow is not built for the
manager's platform stays `ready` and unclaimed there.

## Jobs

### `job` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new [OPTIONS]` | scaffold and submit jobs of an installed workflow | `--workspace`, exactly one of `--workflow`, `--workflow-dir`, `--from-runner`, `--from-command`; `--install`, `--parameter`, `--environment`, `--format`, `--input`, `--input-from`, `--file`, `--files`, `--tag`, `--name`, `--placement`, `--priority`, `--step`, `--json` |
| `job submit SOURCE...` | submit prepared payload directories | `--workspace`, `--move`, `--json` |
| `job request ACTION JOB_ID...` | post one operator request per job (remote: over the adapter) | `--workspace`, `--operator`, required `--reason`, `--priority`, `--step`, `--destination`, `--force`, `--wait`, `--timeout`, `--adapter-timeout` |
| `job delete JOB...` | delete terminal or paused jobs (remote: over the adapter) | `--workspace`, `--force`, `--adapter-timeout` |
| `job seal JOB...` | seal succeeded jobs that carry no seal | `--workspace` |
| `job unseal JOB...` | release succeeded jobs from their seal, so they may be deleted | `--workspace`, `--force` skips the confirmation |
| `job detach JOB...` | make spawned jobs independent of their parents, permanently | `--workspace` |
| `job eject JOB [DEST]` | move a quiescent job out into the bundle `DEST/<job-key>` | `--workspace`, `--tree`, `--wait`, `--timeout`, `--hold`, `--destination-id`, `--json` |
| `job adopt BUNDLE` | move an ejected bundle into the workspace | `--workspace`, `--move`, `--json` |
| `job list` | list jobs a page at a time (remote: over the adapter) | `--workspace`, `--kind`, `--placement`, `--limit`, `--after`, `--tag-contains`, `--counts`, `--json` |
| `job show JOB...` | describe jobs from their documents (remote: over the adapter) | `--workspace`, `--no-children`, `--json` |
| `job log JOB...` | print run logs (remote: over the adapter) | `--workspace`, `--limit`, `--json` |
| `job why JOB...` | explain why jobs are not progressing (remote: over the adapter) | `--workspace`, `--json` |
| `job debug JOB` | drive one job to a terminal state in the foreground | `--workspace`, `--step`, `--placement`, `--follow-children`, `--timeout`, `--log-level` |
| `job transfer SRC DST` | move jobs between two workspaces | `--job`, `--tree`, `--resume`, `--release`, `--json`, `--adapter-timeout` |

When giving more than one `JOB_ID`, name the workspace explicitly.

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

`job new` scaffolds and submits jobs of a workflow installed in the workspace.
See {doc}`/quickstart`.

```console
httk workflow install --workspace WORKSPACE 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workspace WORKSPACE --workflow vasp.relax --input structure=POSCAR --tag silicon
httk job new --workspace WORKSPACE --workflow vasp.relax --input-from structure structures/ --parameter kpoint_density=30.0 --placement project/screening
httk job new --workflow vasp.relax --input structure=POSCAR --parameter publish_data=true
httk job new --from-runner ./my_runner.py --step characterize --parameter sites=8
httk job new --from-command 'my_executable {input}' --file input=input_files/a.dat --tag a
```

`--workflow` names an installed workflow by id or short name. A workflow that
is not installed is refused, unless `--install` installs it first (with its
calls): a git URI `git+https://HOST/PATH[@REF][#SUBDIR]`, a package directory
(`--workflow-dir`), or a workflow name known on this machine. A runner file,
command template or bare workflow document is always installed ad hoc, as
`adhoc:<name>@<digest>`. The job records the installed workflow's id, pinned to
the full commit for a git workflow.

A runner supplied with `--from-runner` is run in describe mode to infer its
workflow and initial step, so it must call `httk_workflow_runner WORKFLOW
STEP...` before any work.

Other options:

- `--parameter NAME=VALUE` supplies an implementation parameter; VALUE is JSON
  when it parses as JSON, and `NAME=@FILE` reads a JSON file. For example, the
  workflows-vasp workflows keep their results in the persistent workdir `run/`
  and copy a curated set into `data/` only with `--parameter publish_data=true`.
- `--environment NAME=VALUE` overrides one declared workflow environment entry.
- `--format FORMAT` selects the format of a bare document (see
  [Running workflow documents](#running-workflow-documents)).
- `--priority` (0 is highest) and `--step` override the workflow's defaults.

The command prints one tab-separated `job_key<TAB>directory` line per job, or a
report with `--json`. The directory is where the job sits now; it changes as
the job moves between states.

### Inputs, files, and batches

- `--input NAME=PATH` stages one declared input.
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

After a batch, one final stderr line reports `submitted N jobs`. If a batch
fails partway, it instead reports `submitted N of M jobs before failing` and
exits `2`. In a batch, `--tag` becomes a *prefix* combined with each item's
derived tag (`run7-si2o`); for a single job, `--tag` is the whole tag.

### Command templates

With `--from-command TEMPLATE`, `shlex`-style words are turned into a one-step
Bash runner, installed ad hoc as `adhoc:command@<digest>`. The template is an
argv-only word list with no shell syntax. Each `{name}` placeholder must have a
matching `--parameter NAME=VALUE` or `--file NAME=PATH`. Parameter placeholders
resolve through `httk_workflow_parameter` at run time; file placeholders resolve
to the absolute staged path. `{{` and `}}` emit literal braces.

Each `--file` input is also copied into the job workdir under its basename
before the command runs; existing workdir entries are never replaced. The
generated runner can be edited and passed back with `--from-runner`.

### Running workflow documents

Run a PWD, CWL, or jobflow document directly with
`job new --from-runner DOCUMENT`; the document is installed ad hoc and run by
the manager's built-in realization:

```console
httk job new --workspace WS --from-runner flow.cwl --input message=echo
httk job new --workspace WS --from-runner workflow.json --parameter pwd_module_path='["."]'
httk job new --workspace WS --from-runner maker.json
```

`--format` accepts `cwl`, `pwd`, `jobflow`, and `httk-v1` for bare document
inputs. See {doc}`/details/workflow_compat`.

### Requests

`job request ACTION JOB_ID...` posts one request per job and requires
`--reason`. The actions are `cancel`, `pause`, `continue`, `override_step`
(with `--step`), `set_priority` (with `--priority`), `detach`, `eject` (with
`--destination`), `delete`, `seal` and `unseal`. The job's owner applies each
once, at its next boundary; a manager claims an unowned job to apply it. What
each action does is tabulated in
[the task-manager guide](taskmanager.md#requests). `--force` accepts reviving a
child a decided join already consumed (`continue`, `override_step`) and
publishes an `override_step` outside the runner's recorded steps.

The command exits `0` once the requests are posted and warns when no live
manager serves the job. `--wait` is valid only for `pause` and exits `0` only
when each requested job was observed `paused`; a superseded, dropped or
timed-out pause exits `1`. `--timeout SECONDS` requires `--wait`.

`job delete`, `job seal`, `job unseal` and `job detach` post the request and,
as a short-lived CLI owner, apply it at once to an unowned job. Each job prints
`<job> <done>` when applied (`removed`, `sealed`, `unsealed`, `detached`),
`<job> queued <reason>` when an owner holds the job and will apply it at its
next boundary, or `<job> refused <reason>` when it cannot apply. Queued requests
exit `0`; any refusal exits `1`.

For a remote workspace, `job request ACTION --workspace REMOTE:NAME JOB_ID ...`
asks the owning machine for unsigned request documents, signs them with the
identity selected by `--operator`, and sends the signed documents back for
verbatim publication. Make `--adapter-timeout` longer than `--timeout` when
both are given. An adapter timeout during publication leaves the outcome
indeterminate: requests may already be posted.

### Inspecting and debugging jobs

`job list`, `job show`, `job log`, and `job why` read one workspace without
writing anything, and `job debug` drives a single job to a terminal state in the
foreground. See {doc}`taskmanager` for what each reports.

```console
httk job list --workspace WORKSPACE --kind ready
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
httk job debug --workspace WORKSPACE --follow-children PAYLOAD_OR_JOB
```

`job debug` exits `0` on success, `3` on failure, and `4` when the job stopped
without finishing.

`job list --json` returns `format`, `format_version`, `jobs`, and `next_after`.
Each row has `job_key`, `job_id`, `state`, `step`, `phase`, `placement`,
`priority`, `token` and `owner_id`. To page a large workspace, use `--limit N`
and pass the returned `next_after` (`<state>:<cursor>`) to `--after`; it is
null when the listing is exhausted. Paging is weakly consistent: a job that
moves between pages can appear twice or be missed, so deduplicate by `job_id`.
`--counts` adds per-state totals, which need a full directory walk.

`job why` also lists **held** jobs: a job held for a transfer is in neither
state tree, and `job why JOB` shows its hold, destination and the way back.

### Sealing and detaching jobs

`job seal` and `job unseal` are the per-job half of {doc}`sealing`. `job seal`
seals a succeeded job that has no seal yet, with the workspace's `seal.keys`.
`job unseal` releases a succeeded job from its seal, after confirmation, which
is what lets `job delete` remove it.

`job detach` permanently makes a spawned job independent of its parent: it no
longer moves with its parent's tree, it can be transferred on its own, and
`Attempt.parent` reads `None` for it. It stays in any join that references it.
A job with no parent is refused.

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
600 s) until the jobs can move, so a running job leaves paused. Across
filesystems the bundle is copied to a hidden partial name in DEST and renamed
into place when complete. The command prints `ejected N job(s) to PATH`. A
failed delivery returns every job to the state it left.

`--hold` puts the bundle in the workspace's own
`.httk-workspace/transfers/outgoing/<transfer id>/` instead, the first leg of
`job transfer`; DEST, when given, and `--destination-id` are then only recorded.

`job adopt BUNDLE` validates the bundle and publishes each job into the state
and priority it left, printing `JOB_KEY<TAB>STATE<TAB>DIRECTORY` per job and a
warning naming workflows that are not installed here. On one filesystem it
takes the bundle by rename. Across filesystems it copies the bundle and leaves
the source in place, unless `--move` is given, which removes the source after a
successful publication (the way to fetch an exchange return). A bundle whose
jobs are all present already reports `already adopted`; one with only some of
them present, or whose root's parent is present here, is refused. Bundle
structure is checked strictly: no symlink or special file in protocol
positions, every member's `job.json` agreeing with the manifest, and an adopted
job's placement satisfying the
[placement rule](workflow_filesystem_api.md#placement-rules).

Both commands are crash-safe and take no file locks: each records what it needs
to finish before it moves anything, so an interrupted eject or adoption is
rolled forward or back once the killed command's death is proven and its
scratch recovered, by a manager tick or by `workspace gc` on its host. Delivery is at least once: verify the destination
before ejecting or adopting a copy again by hand. Neither works in a sealed
workspace or project; a sealed job keeps its seal.

## Moving jobs between workspaces

`job transfer` moves jobs between two workspaces, each a registered name
(`REMOTE:WS` for a workspace on a remote) or else a workspace directory; a
registered name wins over a same-named directory, so `./NAME` addresses the
directory. Any combination of local and remote ends works, and a
remote-to-remote transfer is relayed through the client:

```console
httk job transfer --job JOB [--job JOB …] [--tree] SRC DST [--json]
httk job transfer --resume [SRC] [DST]
httk job transfer --release TRANSFER_ID [WS]
httk transfer status [WS] [--json]
```

A transfer is four steps. It **holds** each selected job on SRC (with `--tree`,
its terminal or paused descendants too) in SRC's own
`.httk-workspace/transfers/outgoing/<transfer id>/`, recording DST, after which
the jobs are in neither workspace's state trees. It **copies** the held bundle
to DST (on one filesystem adoption simply moves it in; otherwise the adapter's
`push` or `pull` lands it in DST), **adopts** it there into the states and
priorities it left, and only then **releases** the hold. A remote SRC needs
canonical job UUIDs. Adoption warns about jobs whose workflows DST has not
installed. The command prints one line per hold,
`TRANSFER_ID<TAB>STATUS<TAB>N job(s)<TAB>DESTINATION`, and exits `0` only when
every hold was adopted or already was.

A bundle DST refuses (for example because some of its jobs are already there)
stays held on SRC and is reported; one whose jobs are all at DST already is
"already adopted", and its hold is released.

**Recovery.** Delivery is at least once. Every run, and
`job transfer --resume`, first re-drives the holds of SRC bound for DST
(`--resume` without DST re-drives every hold to the destination it records), so
a transfer interrupted at any step is finished by running it again. A hold
records DST's workspace id, so only that workspace adopts it. `transfer status`
lists the holds of a workspace (transfer id, root job, member count,
destination, age), `job why JOB` explains a held job, and `httk project repair
--dry-run` reports holds and incoming copies older than seven days. A transfer
abandoned for good is taken back into SRC with
`httk job adopt --workspace SRC SRC/.httk-workspace/transfers/outgoing/<transfer id>`,
which is safe only after checking that DST does not have the jobs;
`job transfer --release T` discards a hold whose jobs DST has.

## Running managers

### `manager` and `run`

| Command | What it does | Notable options |
| --- | --- | --- |
| `run` | run managers through the workspace launcher until the workspace is idle, or keep serving with `--idle` | `--workspace`, `--workers`, `--worker-resource`, `--allocation`, `--count`, `--pool`, `--capability`, `--placement-prefix`, `--idle`, `--idle-timeout`, `--time-limit`, `--deadline-margin`, `--drain-timeout`, `--join-grace-seconds`, `--gc-interval`, `--inline`, `--launcher`, `--detach`, `--log-level`, `--log-file`, `--json-logs`, `--adapter-timeout` |
| `manager run` | the same, the advanced spelling | as `run` |

Both follow the binding: a local workspace uses its `manager.launch` setting
(the built-in `process` launcher by default), and a remote workspace invokes the
same command on its owner. Both also take `--setting KEY=VALUE` (repeatable, not
shown in `--help`), which pins `manager.confine`, `manager.launch_template`,
`manager.launch_mpi`, `manager.confine.block_mpi_spawn`, `manager.bind_cpus` or
a `confine.*` key on the managers for their lifetime; any other key is refused.

The workspace settings `manager.launch`, `manager.count`, `manager.workers`, and
`manager.command` supply the defaults of `--launcher`, `--count` (managers at
the launch site), `--workers` (attempts per manager) and the manager command
used after `environment.prelude`. `--inline` forces one in-process manager, and
`--detach` returns after starting the managers. See {doc}`launchers`.

`--time-limit`, `--deadline-margin`, `--worker-resource` and `--allocation`
are explained in {doc}`taskmanager` under resources, time requirements and
allocations; the startup banner, idle summary and logs under running managers.

Every confined manager without placement prefixes that takes the `default` pool
serves the workspace's `exchange/` extension once it is enabled with
`httk workspace exchange enable`; there is no `--exchange` option.

### `precheck`: readiness before an attempt

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --placement project/screening --json
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
| `launcher add NAME...` | create launchers from the packaged `slurm` template | `--template`, `--set`, `--global`, `--non-interactive` |
| `launcher configure NAME...` | update launcher settings | `--set`, `--unset`, `--add KEY=PATH[:PATH...]`, `--remove KEY=PATH[:PATH...]` |
| `launcher show NAME...` | describe launchers and their settings | `--json` |
| `launcher check NAME...` | check a launcher's required binaries | `--launcher-timeout` |
| `launcher remove NAME...` | remove launcher bundles | `--force` |

See {doc}`launchers` and {doc}`launcher_authoring`.

### `monitor`: inspect and control workspaces interactively

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow monitor` | open the curses workspace monitor | `--workspace NAME` (repeatable), `--refresh SECONDS`, `--adapter-timeout SECONDS`, `--non-interactive` |

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

| Options | Apply to |
| --- | --- |
| `--state`, `--placement`, `--raw`, `--allow-job-collector` | a workspace only |
| `--dry-run`, `--prefer NAME`, `--exclude PATTERN`, `--collector DIR` | a calculation tree only |
| `--into PATH`, `--id-base BASE`, `--id-series SERIES`, `--no-id-ledger`, `--id-ledger PATH`, `--no-bare-runs`, `--upgrade`, `--degraded`, `--fail-fast`, `--batch-size N` | both |

An option for the other kind of target is refused. For a workspace, `collect`
streams one `CollectedJob` summary per finished job as JSON lines; `--raw`
streams `JobRecord` records. With `--into`, a sealed id ledger at
`<into>.ids.sqlite` keeps entry ids stable across rebuilds
({doc}`/details/stable_ids`), and `--upgrade` lets an existing store take an
additive layout change. Every form ends with one
`httk-workflow-collect-summary` line, and the command exits nonzero when any
job was degraded, failed to store, or had an unreadable `job.json`. See
{doc}`/details/collecting`.

### `postprocess`: run a curated script

| Command | What it does | Notable options |
| --- | --- | --- |
| `postprocess [OPTIONS] [JOB...]` | run one declared script for each selected job | `--workspace WS`, `--script NAME` (required), `--workflow-dir PKG`, `--state`, `--placement`, `--output-dir DIR`, `--timeout`, `--json` |

```console
httk workflow postprocess --workspace WS --script relaxation-report
httk workflow postprocess --script relaxation-plot <job-id>
```

Output is written outside the job payload, so a sealed job can be
postprocessed. The output root is `<workspace>/postprocess` by default, the
`postprocess.directory` workspace setting when set, or `--output-dir DIR` for
one invocation; each job's directory below it is
`<root>/<placement>/<job_key>/<NAME>/`. A `JOB` selector cannot be combined with
`--state` or `--placement`. With `--json`, each result is one
`httk-workflow-postprocess` object (version 2); without it, one
`job_key<TAB>script<TAB>returncode<TAB>output_dir` line. The command exits `0`
only when every selected script ran and returned `0`.

### `v1 collect`: harvesting finished *httk* v1 trees

```console
httk v1 collect --workflow-dir PKG ROOT
httk v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

`v1 collect` ends with one `httk-workflow-v1-collect-summary` line reporting
`finished`, `unfinished_by_status`, and `skipped_no_rundir`. Each collected
report carries `identity_stable`: `false` for a task whose identity is
path-derived because it has no `ht.manifest`.

## Remotes

Named after `git remote`, a *remote* is one machine this project can reach,
together with the bundle of adapter operations that reaches it. `REMOTE:NAME`
names a workspace on a remote. The name `local` is reserved for the built-in
remote meaning "this machine", so `remote add local` is refused.

### `remote` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote list` | list the remotes this project can reach | `--json` |
| `remote add NAME...` | create remotes from a packaged adapter template | `--template`, `--global`, `--non-interactive` |
| `remote configure REMOTE...` | run adapters' `configure` operation | `--set KEY=VALUE`, `--unset KEY`, `--adapter-timeout` |
| `remote check REMOTE...` | check that `httk` answers on remotes | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote import-v1 SOURCE...` | map legacy *httk* v1 computer bundles | `--name` (one source only), `--global` |
| `remote show NAME...` | describe remotes and their settings | `--json` |
| `remote remove NAME...` | remove remote bundles; refused while a registered workspace holds a transfer bound for the remote | `--force` skips only the confirmation |
| `remote daemon configure REMOTE` | pin the identities in the mounted `exchange.json` and `daemon.json` | required `--exchange` |
| `remote daemon health REMOTE` | check the confined daemon | `--request-id`, `--wait-seconds` |
| `remote daemon start REMOTE` | start one approved manager configuration | required `--configuration`, `--request-id`; `--wait-seconds` |
| `remote daemon status REMOTE` | print the passive `status.json` and `managers.json`, or with `--handle` inspect a manager by signed request | `--handle`, `--request-id`, `--wait-seconds` |
| `remote daemon cancel REMOTE` | request manager cancellation | required `--handle`, `--request-id`; `--wait-seconds` |
| `remote daemon log REMOTE` | print a manager's published log | required `--handle` |
| `remote daemon take-back REMOTE NAME [DESTINATION]` | take your bundle back from `inbox` before a manager takes it | `DESTINATION` defaults to `./NAME` |

`remote show NAME` reports which file each setting came from, but never a
credential value.

### Remote definitions and templates

Remote definitions are versioned directories containing `remote.json` and one
executable `adapter`; project definitions shadow global ones. {doc}`adapter_authoring`
is the reference for writing your own: the bundle layout, the request and
result documents of the six operations (`configure`, `install`, `invoke`,
`push`, `pull`, and `status`), and the optional `daemon` operation. The module
packages four maintained templates for `remote add --template`:

- `local`: same-machine transport;
- `ssh`: rsync plus command execution over SSH;
- `mount`: a locally mounted remote filesystem plus a command executor;
- `mount-daemon`: files as the only channel, through a mounted exchange
  directory, plus typed requests to a confined broker.

The first three use the target workspace's `manager.launch` setting.
`mount-daemon` refuses generic `REMOTE:NAME` operations; move jobs with
`job eject` into the exchange inbox and `job adopt --move` out of its outbox.
See {doc}`/details/remotes` for configuration and request-ID retry rules.

### Remote settings

`remote configure --set KEY=VALUE` persists only the machine-level keys
`check_connectivity`, `check_mount`, `exec_command`, `exchange`, `host`,
`httk_command`, `legacy_settings`, `mount_root`, `port`, `prelude`,
`remote_root`, `username`, `vasp_command`, and `vasp_pseudo_library` in the
shareable `remote.json`. Scheduler profile values are workspace settings
(`slurm.*`, `manager.workers`) and are refused as remote settings. Other keys
are stored in the remote's `credentials.json` beside it, with mode `0600`;
project manifests exclude that file.

`remote import-v1` maps legacy *httk* v1 computer bundles by reading
assignment-only configuration; legacy shell executables are never run, and
legacy `SLURM_*` values belong in a `slurm` launcher instead.

`local` copies files in this filesystem and runs child processes; `ssh` moves
files with `rsync` over `ssh` and runs commands on the configured host; `mount`
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
shell, use the remote's `prelude` setting, or point `httk_command` at it.

## Configuration and projects

### Per-user configuration

| Command | What it does | Notable options |
| --- | --- | --- |
| `config show [KEY]` | print the configuration, or one member | `--json` |
| `config configure` | set/unset keys or add/remove machine names | `--set`, `--unset`, `--add machine_names=NAME`, `--remove machine_names=NAME` |
| `config set KEY VALUE` | store one member | `machine_names` is a comma-separated list of names this machine answers to |
| `config unset KEY` | remove one member | |
| `config import-v1 [SOURCE]` | read a legacy `~/.httk` configuration | |

User configuration lives below `$XDG_CONFIG_HOME/httk/` (`config.json`, the
core-managed `identity.json` and `keys/`, global remotes and launchers);
`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` override it.

### Projects

The project anchor and every project verb (`init`, `show`, `import-v1`,
`export`, `repair`, `adopt`, `manifest create | verify`, `seal`, `unseal`, and
`verify-seal`) belong to *httk-core*. *httk-workflow* registers the
**workspace** as a project-*member* kind, so core's verbs delegate a
workspace's internals to it: what to leave out of a manifest, how to seal it,
its seal digest, how to verify it, and its health checks.

A workspace inside a project is recorded in `httk_project/members.json`:
registered on `httk workspace init`, unregistered on `workspace delete` or
`workspace forget`, and its path followed on `workspace move`. On another
machine, `httk workspace adopt` (per workspace) or `httk project adopt` (every
member) registers copied workspaces under their recorded names.

`httk project repair` handles conditions that quietly break a project later:
dead owners not yet recovered, scratch of unknown owners, old temporaries, holds
and incoming copies older than seven days, a workspace missing from the
registry, and a member not yet adopted on this machine. It repairs only through
`gc` and adoption. `--dry-run` only reports, and `--no-adopt` skips adopting.

To seal a project, run `httk workspace seal` and then `httk project seal`.
Verify the whole tree with `httk project verify-seal` or `httk seal verify`.

### `seal verify`: verify a sealed tree

| Command | What it does | Notable options |
| --- | --- | --- |
| `seal verify [PATH]` | verify the seal at `PATH` (a project, workspace, or job payload; default `.`) and, unless `--shallow`, every seal it references | `--json`, `--trusted-key KEY_OR_FINGERPRINT` (repeatable), `--shallow` |

The final status word and exit code mirror `manifest verify`: `ok` (exit 0)
when every entry is `valid_trusted`, `UNTRUSTED` (exit 3) when no entry is
invalid but a signer is untrusted, `FAILED` (exit 1) on any invalid entry. See
{doc}`sealing`.

## Signed manifests and operator identity

### Signed manifests

```console
httk project manifest create .
httk project manifest verify
httk project manifest verify --trusted-key keys/collaborator.pub
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
init` creates the first named identity and its key pair and makes it the
default; `httk identity add --name NAME --email EMAIL SHORT` adds more, and
`httk identity default SHORT` changes the default.

`job request` records the selected identity's `Name <email>` label and signs the
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
post an unsigned request. The exchange's signed job control is the exception:
there a signature by a key in `exchange.authorized_keys` is required (see
{doc}`workspace_daemon`).

## Campaigns

| Command | What it does | Notable options |
| --- | --- | --- |
| `campaign init` | define the project's partition map and assignment policy | `--partition NAME=WORKSPACE`, `--assignment` |
| `campaign configure` | edit individual partitions or the assignment policy | `--set partitions.NAME=WORKSPACE`, `--unset partitions.NAME`, `--set assignment=POLICY`, `--unset assignment` |
| `campaign remove` | clear campaign configuration, keeping jobs and workspaces | `--force` skips confirmation |
| `campaign show` | show the partition map | `--json` |
| `campaign submit` | assign one root job to a partition and submit it there | `--workflow` (required), `--install`, `--key` (required), `--index`, `--input`, `--input-from`, `--parameter`, `--file`, `--tag`, `--placement`, `--priority`, `--name`, `--json` |
| `campaign collect` | collect every partition, one workspace after another | `--partition`, `--state`, `--placement`, `--raw`, `--into PATH`, `--id-base BASE`, `--id-series SERIES`, … |
| `campaign start-<wbr>managers` | start a manager per selected partition | `--partition`, `--workers`, `--worker-resource`, `--count`, `--launcher`, `--idle-timeout`, `--adapter-timeout` |

A campaign is a thin convention over registered workspaces: a partition map,
stored in the project, that spreads a very large body of work across many
workspaces without a new scheduler. Roots are assigned to partitions by policy,
and spawned children always inherit their parent's workspace. `campaign submit
--workflow` names a workflow installed in the partition's workspace, or with
`--install` a package directory, git URI or workflow name to install there
first. See {doc}`/campaigns`.

## Protocol spellings and removed commands

One machine runs these argument vectors on another over an adapter. They are
protocol, not operator interface, and hidden from `--help` where they are not
ordinary commands; their spelling is frozen because the far side may run a
different *httk*:

```text
httk job eject --hold --json --workspace WS [--tree] [--destination-id ID] -- JOB DST
httk job adopt --json --workspace WS BUNDLE
httk job transfer --release TRANSFER_ID WS
httk transfer status --json WS
httk job request-envelopes ACTION --workspace WS --operator LABEL --reason TEXT [--priority N] [--step S] [--destination D] [--force] JOB_ID …
httk job publish-requests --workspace WS --document JSON [--document JSON …] [--wait] [--timeout S]
httk workspace status --by-path --json PATH
httk workflow manager run --by-path --workspace PATH
```

The hidden `--by-path` switch makes the workspace argument a literal path with
no registry lookup, so one machine can address another machine's workspace.
The hidden `job delete --confirmed` is the remote half of a confirmed delete.

### Removed commands and options

The filesystem kernel of this version replaced state markers, the journal,
leases, receipts and the maintenance lock. These spellings no longer parse:

| Removed | Now |
| --- | --- |
| `httk runner publish`, `runner describe`, `--runner-search-path` | `httk workflow install --workspace WS`; `job new --install` |
| `job new --publish`, `--data-mode`, `--workdir-mode` | install-before-run; workflows choose their data with parameters such as `publish_data` |
| `job submit --placement` | the placement comes from the payload's `job.json` |
| `job seal --keys` | the workspace's `seal.keys` setting |
| `job confirm-launches-ended` | `workspace attest-dead` once the owner and its launches are gone |
| `job eject --resume` | an interrupted eject is rolled forward or back by recovery |
| `httk workflow transfer offer\|receive\|retire\|reclaim` | `job transfer`, `job eject --hold`, `job adopt`, `job transfer --release` |
| `workspace unlock` | nothing to unlock: there is no maintenance lock |
| `workspace forget --force` | `workspace forget` |
| `workspace fsck --quarantine-unrepairable` | `workspace fsck --repair` (unparsable entries only) |
| `workspace seal --force` | `job seal` the remaining jobs first, or seal the workspace with them listed as unsealed |
| `manager run --lease-seconds`, `--takeover-grace-factor`, `--unsafe-persistent-takeover`, `--unsafe-isolated-takeover` | recovery only after the death proof or `workspace attest-dead` |
| `transfer send`, `transfer fetch`, `transfer status REMOTE` | `job transfer --job JOB … SRC DST`, `workspace status REMOTE` |

Workspaces and bundles of the earlier layout are refused with a message that
there is no migration: re-create the workspace with `httk workspace init` and
submit its jobs again.

A job whose `runner.path` pinned a `pkg:httk.workflow.runners/vasp_*` form
breaks too: the VASP workflows left the module for
[workflows-vasp](https://github.com/httk/workflows-vasp). Create the job again
from a workflows-vasp URI (see {doc}`/vasp_runners`).
