# Project and workflow command line in detail

This is the complete command reference, from workspaces and jobs to projects,
signed manifests, and remotes. Installing *httk-workflow* registers lazy
`workflow`, `workspace`, and `job` commands with *httk-core*:

```console
httk workflow --help
```

`httk workflow …` covers workflow execution; workspace and job management are
the top-level trees `httk workspace …` and `httk job …`, and collecting results
is `httk collect`. Every group and command answers `--help`, and a mistyped
action is reported by its group.

## The command tree

```text
httk workspace          init | adopt | list | default | move | forget | delete | status | managers | workflows | settings show | settings set | settings unset | workflow-prelude show | workflow-prelude set | workflow-prelude unset | policy show | policy set | fsck | gc | unlock | seal | unseal | daemon
httk job                 new | submit | request | delete | seal | unseal | detach | eject | adopt | list | show | log | why | debug | transfer
httk collect             [PATH] [--workspace WORKSPACE] [--into PATH] [--dry-run] …
httk workflow list       [--json]
httk workflow describe   TARGET [--json]
httk workflow install    URI... [--json]
httk workflow uninstall  SELECTOR... [--json]
httk workflow runner     publish | describe
httk workflow build      [--workspace WORKSPACE] TARGET...
httk workflow precheck   [--workspace WORKSPACE] [--placement P] [--json]
httk workflow run        [--workspace WORKSPACE]  (the recommended spelling of `manager run`)
httk workflow manager    run
httk workflow launcher   list | add | configure | show | check | remove
httk workflow monitor    [--workspace NAME ...] [--refresh SECONDS]
httk workflow mpi        run -- APPLICATION ARG...
httk workflow postprocess
httk workflow v1         collect
httk workflow remote     list | add | configure | check | import-v1 | show | remove | daemon
httk workflow config     show | set | unset | import-v1
httk workflow seal       verify [PATH] [--json] [--trusted-key KEY] [--shallow]
httk workflow campaign   init | show | submit | collect | start-managers
httk workflow transfer   receive | offer | retire      (hidden protocol only; see `job transfer` for the user-facing verb)
httk init | identity     (core-owned: per-user configuration and named operator identities)
httk project             init | show | import-v1 | export | repair | adopt | manifest create | manifest verify | seal | unseal | verify-seal   (core-owned)
```

### Workspace selection

A `WORKSPACE` is an optional registered local name. When it is omitted,
commands resolve the workspace in this order: the closest enclosing workspace
containing `.httk-workspace/format.json`, the project's recorded default, the
registry's default, then an auto-created per-user default. `workspace init PATH`
creates explicit local names; remote names are written `REMOTE:NAME` at use
time.

The registry is machine-owned and stores only absolute local paths, in
`$XDG_CONFIG_HOME/httk/workspaces.json`. When a command reaches a remote
workspace, the far side resolves the plain name in its own registry. The Python
API keeps `Workspace(path)` for library use; the command line speaks the
registry.

Remote-capable workspace commands, including status and settings, go through
the adapter, as does `job request`. Most job commands stay local-only: jobs are
created in the local default workspace, and `job transfer` moves them to a
remote workspace for execution.

## Workspaces

### `workspace` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init [OPTIONS] PATH...` | create or adopt workspaces, registering each name (basename or `--name`) centrally and recording it in the project's `members.json` | `--name` (one path only), `--setting`, `--no-durable` |
| `workspace daemon WORKSPACE --policy POLICY` | run the confined Slurm file-command broker | `--initialize`, `--reload`, `--export-endpoint`, `--check`, `--once` |
| `workspace list [--json] [REMOTE:]` | list local or owning-machine workspaces | |
| `workspace default [--unset] [NAME]` | read or record this project's default name | |
| `workspace adopt [PATH...] [--name NAME] [--json]` | register copied workspaces on this machine under the names their project's `members.json` records | `--name` (one path only) |
| `workspace move [--no-durable] NAME DEST_DIR` | move a local workspace and update its registry path | |
| `workspace forget [--force] NAME...` | deregister names, leaving workspaces on disk | |
| `workspace delete --force NAME...` | destroy workspaces and deregister them | |
| `workspace status [--json] [NAME...]` | summarize authoritative markers (remote: over the adapter) | |
| `workspace managers [--json] [NAME...]` | list managers serving workspaces, live or stale | |
| `workspace workflows [--json] [NAME...]` | list the runners a workspace publishes, with each directory package's workflow identity | |
| `workspace settings show [--key KEY] [--json] [NAME...]` | print application settings, or one selected key | |
| `workspace settings set --key KEY --value VALUE [NAME...]` | store one application setting in each workspace | |
| `workspace settings unset --key KEY [NAME...]` | remove one application setting from each workspace | |
| `workspace workflow-prelude show [--workflow WORKFLOW] [--json] [NAME...]` | print per-workflow preludes, or one selected workflow | |
| `workspace workflow-prelude set --workflow WORKFLOW --value VALUE [--no-durable] [NAME...]` | store one workflow's prelude in each workspace | `VALUE` may be `@FILE` |
| `workspace workflow-prelude unset --workflow WORKFLOW [--no-durable] [NAME...]` | remove one workflow's prelude from each workspace | |
| `workspace policy show [--json] [NAME...]` | print workspace policies | |
| `workspace policy set --key KEY --value VALUE [--json] [NAME...]` | store one policy member in each workspace | |
| `workspace fsck [OPTIONS] [NAME...]` | check markers against journal frames; repair modes require names | `--repair`, `--quarantine-unrepairable`, `--json` |
| `workspace gc [--dry-run] [--json] [NAME...]` | collect what retention policies allow (remote: over the adapter) | |
| `workspace unlock [--force] [NAME...]` | release maintenance locks | |
| `workspace seal [--force] [--keys REFS] [NAME...]` | record every job's seal digest under one signed workspace seal | `--force` seals still-unsealed jobs first; `--keys` overrides the `seal.keys` setting |
| `workspace unseal [--force] [NAME...]` | remove a workspace's seal, refused while its project is sealed | `--force` skips the confirmation |

### Creating, moving, and removing workspaces

`workspace init` creates and registers an explicit workspace. A canonical path
may have only one registered name. `--setting KEY=VALUE` seeds an application
setting at creation.

```console
httk workspace init --name my-workspace runs/my-workspace
```

A workspace on a cluster is addressed as `REMOTE:NAME`. Its owning machine
chooses the path given to `workspace init REMOTE:PATH`.

`workspace move NAME DEST_DIR` is an atomic same-filesystem rename and refuses
cross-filesystem moves. To move across filesystems, stop managers, copy the tree
manually, forget the old name, and re-register it with
`workspace init --name NAME <newpath>`.

`workspace delete` destroys the workspace, locally or on its remote over the
adapter, and is refused without `--force`. `workspace forget` only removes the
name, and only when there are no unretired outbound transfers. Fetch or retire
those first, or pass `workspace forget --force` to deregister the name anyway.

### Sealing a workspace

Sealing is described in full in {doc}`sealing`. `workspace status` shows a
`sealed` line (and JSON field). `workspace seal` runs inside the maintenance
guard, so the workspace must be quiescent. Without `--force` it lists the
still-unsealed jobs and refuses rather than seal a partial set.

### Application settings

A workspace carries *application settings*: a flat map of small values with
dotted names that a runner resolves when it runs, such as the VASP command or a
pseudopotential library. They are distinct from the engine `policy`, which
tunes scheduling (see [below](#policy-integrity-and-locks)). They are edited by
name:

```console
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" my-workspace
httk workspace settings show my-workspace
httk workspace settings unset --key vasp.command my-workspace
```

A value that parses as JSON is stored as that scalar; a bare word is stored as a
string. `workspace init --setting KEY=VALUE` seeds settings explicitly, and a
workspace bound to a remote is also seeded from that remote definition's
whitelisted remote settings: a cluster's `vasp_command` becomes the new
workspace's `vasp.command`.

A runner reads a setting with `a.setting("vasp.command")`. The value is resolved
in layers, most specific first: the job's own parameters, a real
`HTTK_VASP_COMMAND` deployment override, the workspace setting, then the
runner's default. The manager exports scalar workspace settings into each
attempt environment (`vasp.command` becomes `HTTK_VASP_COMMAND`) and snapshots
them into the `HTTK_WORKFLOW_CONTEXT` JSON value, so a runner sees the values
the workspace held when its job was claimed. See {doc}`/vasp_runners` and
{doc}`/sdks/sdk_parity`.

Workspace settings are non-secret: they are snapshotted into the attempt context
and exported into the runner environment, so never store credentials there.
Remote credentials live elsewhere.

### Workflow preludes

Two layers of shell setup run before a job's runner, both sourced under `set -e`
so that a failing line aborts the job instead of running it in a broken
environment.

- `environment.prelude` is the workspace-wide layer: one shell fragment that
  applies to every job. It is an ordinary application setting:
  `workspace settings set --key environment.prelude --value "…" NAME`.
- `workflow-prelude` is the per-workflow layer, keyed by workflow id (the
  manifest's `[workflow].name`, which equals the job's `workflow`). It applies
  only to jobs of that workflow and runs after the workspace-wide prelude:

  ```console
  httk workspace workflow-prelude set --workflow relax-vasp --value "module load VASP/6.2.1" my-workspace
  httk workspace workflow-prelude set --workflow relax-vasp --value @prelude.sh my-workspace
  httk workspace workflow-prelude show my-workspace
  httk workspace workflow-prelude unset --workflow relax-vasp my-workspace
  ```

  `VALUE` is stored verbatim, never JSON-parsed; `@FILE` reads the shell text
  from a file, such as a multi-line module-load script. Without `--json`, `show`
  prints `WORKFLOW⇥text` lines, so a multi-line prelude's continuation lines
  carry no id prefix; machine consumers should use `--json`.

See {doc}`/running` for how the workspace's launcher delivers each layer, and
why preludes stay behind when a job is transferred.

### Policy, integrity, and locks

Tunables shared by every process attaching a workspace (the visibility deadline,
default lease, journal segment size, and retention limits) are stored in
`format.json` and edited in place:

```console
httk workspace policy show WORKSPACE
httk workspace policy show --json WORKSPACE
httk workspace policy set --key visibility_deadline_seconds --value 60 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
```

`workspace fsck` verifies that every state marker still resolves to a readable
journal frame that agrees with it, and can re-point damaged markers at their
job's last good frame:

```console
httk workspace fsck WORKSPACE
httk workspace fsck --repair --json WORKSPACE
httk workspace fsck --repair --quarantine-unrepairable WORKSPACE
```

It exits `1` while anything remains for an operator to deal with. It also prints
the total and per-category counts of any always-safe leftovers; this line never
changes the exit status. See [the task-manager guide](taskmanager.md) for the
problem codes and what a repair will and will not touch.

The maintenance fence is `.httk-workspace/maintenance.lock`, which holds the
recording process identifier, hostname, and creation time. A lock whose
same-host process is gone, whose content is unreadable, or that is older than
twenty-four hours is reclaimed automatically; any other lock is reported with
its holder. `workspace unlock` clears a lock explicitly; without `--force` it
removes only a stale one:

```console
httk workspace unlock WORKSPACE
httk workspace unlock --force WORKSPACE
```

## Freeing disk

After its durable commit, and after reaping the local process, a committing
manager removes an attempt's control directory when the actual destination is
`ready`, `waiting`, `paused`, or `succeeded`. Failed and cancelled attempts stay
as evidence. Transaction trash is normally removed with that control tree; an
inherited commit is left for `workspace gc`. Managers run the always-safe
categories at startup and the full policy-gated collection at clean exit.
Artefacts orphaned by a crash still require an explicit `workspace gc`, driven
by the workspace's `policy.retention`:

```console
httk workspace gc --dry-run WORKSPACE
httk workspace gc WORKSPACE
httk workspace gc --json WORKSPACE
```

`gc` prints one row per category with the candidates found, what was removed,
and an estimate of the bytes reclaimed; `--json` also lists every entry.
`--dry-run` touches nothing and reports what a real run would remove. A run that
removed anything appends an `httk-workflow-gc` frame with the same counts to the
journal, making the collection part of the workspace's durable history.

### Retention policy

`gc` always prunes the always-safe categories in the table below: what cannot
carry information, and the markers of quiescent, unowned jobs whose payloads the
operator removed. The retention limits configure the other categories:

```console
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
httk workspace policy set --key retention.journal_days --value 90 WORKSPACE
httk workspace policy set --key retention.journal_days --value null WORKSPACE  # keep forever
```

A `null` or `"keep"` retention member means keep. On a fresh workspace,
`journal_days` and `trash_days` default to one day, while
`attempt_control_days` is unlimited.

| Category | Retention limit | What goes |
| --- | --- | --- |
| `attempt_control` | `attempt_control_days` | aged `attempts/*` directories; failed and cancelled jobs retain their newest one, while other quiescent leftovers (including succeeded) also wait one workspace `lease_seconds` grace |
| `transaction_trash` | `trash_days` | trees a replayed transaction moved aside, once the job left `committing` |
| `retired_bundles` | `trash_days` | acknowledged transfer bundles below `transfers/retired/` |
| `transfer_records` | `trash_days` | per-transfer receipts below `transfers/acks/` and `transfers/imported/` |
| `removed_jobs` | always safe | state markers for jobs that are quiescent and unowned by any manager (`succeeded`, `failed`, `cancelled`, `submitted`, or `ready`) whose payload directories are absent, unless a non-terminal parent still references them as join children |
| `journal_segments` | `journal_days` | segments outside every current non-terminal frame chain (and outside terminal current segments), written by a writer no live manager owns |
| `manager_directories` | `journal_days` | directories of dead managers whose segments are gone |
| `placement_directories` | always safe | empty placement mirrors below `state/<kind>/` |
| `tmp_entries` | always safe | staging entries older than 24 hours |
| `retired_requests` | always safe | requests claimed over 30 days ago by a manager now gone, and requests retired over 30 days ago with their `.retirement` records |

### Retired transfers

Completed transfers are an exception to the retention ages. After
acknowledgement, retirement durably records the handover, then immediately
removes the retired payload and its unprotected source journal segments,
ignoring numeric retention ages. The transfer ledger is then pruned; durable
epoch-scoped receipt ranges provide replay protection at the destination.

Set `retention.trash_days` to `"keep"` (or `null`) before retirement to keep
both at retirement; `retention.journal_days: "keep"` independently keeps the
journal. Ordinary GC still applies each category's own limit, so set both
members to `"keep"` to preserve both indefinitely:

```console
httk workspace policy set --key retention.trash_days --value keep WORKSPACE
httk workspace policy set --key retention.journal_days --value keep WORKSPACE
```

Retirement protects current markers, non-terminal chains, sealed transfers, and
live manager writers. It deletes only eligible source-chain segments and empty
writer directories, and creates no replacement journal stream. Shared or
protected segments remain for a later retirement retry or ordinary GC.
Repeating retirement resumes cleanup after an interruption. The reported
`retired_bundle` is an identity path and normally no longer exists.

### Removing jobs

To remove a finished (`succeeded`, `failed`, or `cancelled`), `submitted`, or
`ready` job cleanly, use `httk job delete JOB...`. It removes both the payload
directory and the state marker, asks for confirmation on a terminal, and
protects join children referenced by non-terminal parents. `--force` skips both
the confirmation and the join-parent guard. For a finished job, `rm -r` of
the payload followed by GC or manager cleanup also works. For queued `ready` or
`submitted` jobs prefer `job delete`, because removing the directory first can
race a manager claiming the job. Any other job must first be cancelled with
`job request cancel`.

Containment checks are static: a concurrent replacement of a placement ancestor
by the workspace's own owner is outside the threat model, as it is for
`attempts/` and `logs/`. The hidden `--confirmed` option is a protocol flag like
`--by-path`, not part of the user interface.

### What collection never touches

The collector never touches the quarantine, a sealed transfer bundle, a
persistent workdir, a payload beyond its aged attempt-control directories, a
`claimed`, `running`, `committing`, `cancelling`, `waiting`, or `paused` marker,
a segment in a frame chain protected by a current non-terminal marker, a manager
that is still heartbeating, or the runner store. Removal is bottom-up and
rewrites no state, so a collection killed halfway leaves the workspace as
consistent as it was; running it again finishes the job.

Collecting journal segments has one cost: a current non-terminal marker protects
every segment of its frame chain, but a terminal marker protects only its
current segment. The deep history of a terminal job therefore goes with the aged
segments behind it, and `collect` and `job log` report that timeline with `gaps`
set.

## Workflows, runners, and builds

### `list`: the workflows a name can select

| Command | What it does | Notable options |
| --- | --- | --- |
| `list` | list the workflows `job new --workflow NAME` can resolve: those registered in this process, then those installed plugins bundle, then installed git workflows by short name | `--json` |

Each text line is `WORKFLOW_ID`, alias (or `-`), source (`registered`,
`plugin OWNER`, or `installed URI`), and summary (or `-`). `--json` reports the
same as an array of objects, with `source.kind` `"installed"` and `source.uri`
for installed git workflows. Installed-plugin workflow names also appear in the
`--workflow` help and the unknown-workflow hint, marked `[plugin PLUGIN_NAME]`
for their owner.

A workflow reached only by explicit path (`--workflow-dir DIR` or
`--from-runner FILE`) is not registered, so it is not listed; use
`describe PATH` to report it. To list the runners one workspace has
*published*, rather than the workflows a name resolves to, use
`workspace workflows`.

### `describe`: inspect a workflow without publishing it

| Command | What it does | Notable options |
| --- | --- | --- |
| `describe TARGET` | describe a registered id/alias, git workflow URI, runner file, or package directory | `--json` |

A git URI target is fetched and installed like any explicit reference. It, and
the short name of an installed git workflow, report source kind `installed`,
with `uri` and `commit` in the `--json` `source` object. A plugin workflow name
reports its source as `installed-package`.

Resolving a workflow trusts a directory package's manifest and executes
nothing. As a report, `describe` additionally runs a directory package's runner
entry with `--describe` (stripping any surrounding attempt context). When the
manifest's declared `steps` disagree with what the runner reports, it prints a
prominent `WARNING: step drift` line (`manifest_step_drift` in `--json`), but
still exits `0`.

Directory package authoring, manifest validation, publication, and hook trust
tiers are documented in {doc}`workflow_packages`.

### `install` and `uninstall`: git workflows without a job

| Command | What it does | Notable options |
| --- | --- | --- |
| `install URI...` | fetch each git URI and install its workflow; print the canonical URI and short name | `--json` |
| `uninstall SELECTOR...` | forget installed git workflows by short name or URI; cached checkouts stay | `--json` |

To install a git workflow without creating a job, run
`httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'`;
the workflow is then selectable as `--workflow vasp.relax`. `uninstall` removes
one commit for a pinned URI, and the whole repository-and-subdirectory lineage
for an unpinned URI or a short name. A selector naming only an in-process
registration is an error, and one naming a plugin workflow points to
`httk plugin uninstall`. Each argument is processed independently; any failure
exits `1` after the rest are processed. See {doc}`/details/workflow_uris`.

### `runner`: the shared runners a workspace publishes

| Command | What it does | Notable options |
| --- | --- | --- |
| `runner publish [OPTIONS] FILE_OR_DIRECTORY...` | publish runner files or directories, pinned by digest | `--workspace`, `--name` (one source only), `--replace`, `--json` |
| `runner describe [OPTIONS] [NAME...]` | report published runners and their digests | `--workspace`, `--json` |

### `build`: foreground registration of compiled workflow packages

| Command | What it does | Notable options |
| --- | --- | --- |
| `build [OPTIONS] TARGET...` | build and register packages, store runners, or jobs' workspace runners | `--workspace`, `--list`, `--json` |

```console
httk workflow build [--workspace WORKSPACE] [--json] TARGET...
httk workflow build [--workspace WORKSPACE] [--json] --list
```

`TARGET` may be:

- a workflow package directory;
- a workflow reference, resolved exactly as `job new --workflow` resolves it: a
  registered id or alias, an installed plugin workflow, an installed git
  workflow's short name, or a git URI (fetched and installed);
- a workspace runner-store path;
- a job reference whose runner is a workspace package.

A package directory, or the directory a workflow reference resolves to, is first
published under the digest-pinned store name a job of it would pin; then its
source tree is built and its artifacts are registered for the local platform
tag. A store path or job reference builds the already published source tree. A
plugin-sourced workflow is built by name: `httk workflow build NAME` pins it
into the workspace like any other package.

Targets are classified without guessing: a git URI is a workflow reference; a
path spelling (absolute, `./`, `../`, or containing `/`) is a package directory
(write `./NAME` for one in the current directory); a glob is a job reference;
and a bare name is a runner-store name when that store entry exists, then a
workflow name, then a job reference.

A workflow reference or package directory whose manifest declares
`[workflow.calls]` also builds every workflow it calls, transitively. Each is
reported as its own row carrying `called_by` and `alias`.

`--list` builds nothing and prints the workspace's registrations. Directory
rows are reported as `tree (inferred)`: the store format identifies a tree by
its `run` entry, and although new nested file publishes named `run` are
refused, an older store may hold an ambiguous legacy layout.

`--json` emits machine-readable build or list records. Each build record names
the `target` as written; for a workflow reference also the resolved `workflow`
id and `package` directory (both `null` otherwise); and a `status`:

- `registered`;
- `nothing-to-build`, for a workflow reference whose package has no
  `[workflow.build]` section, which exits 0;
- `failed`, with an `error`, when a called workflow cannot be resolved, which
  makes the command exit 1.

Exit 0 means every registration completed, or the list was read. Malformed
targets, unknown names, workflow references that are not directory packages, a
package directory without `[workflow.build]`, probe or build failures, and
missing artifacts are nonzero failures.

The build vocabulary and engine come from `httk.core.building`; this layer keeps
the workspace runner-build store and the platform-tagged registrations. The
manager passes registered artifacts through `HTTK_WORKFLOW_RUNNER_ARTIFACTS`
without modifying the published source tree.

## Jobs

### `job` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new [OPTIONS]` | scaffold and submit jobs from a workflow, runner file, package directory, or command template | `--workspace`, exactly one of `--workflow`, `--workflow-dir`, `--from-runner`, or `--from-command`, `--parameter`, `--environment`, `--format`, `--input`, `--input-from`, `--file`, `--files`, `--tag`, `--placement`, `--json` |
| `job submit [OPTIONS] SOURCE...` | submit prepared payload directories | `--workspace`, `--placement` (required), `--move` |
| `job request ACTION [OPTIONS] JOB_ID...` | publish one request per job selector (remote: over the adapter) | `--workspace`, optional `--operator` (configured short name or literal `Name <email>`; default identity when omitted), required `--reason`, `--priority`, `--step`, `--force`, `--wait`, `--timeout`, `--adapter-timeout` |
| `job delete [--force] JOB...` | remove selected job payloads and state markers (remote: over the adapter) | `--workspace`, `--force`, `--adapter-timeout` |
| `job seal [--keys REFS] JOB...` | seal the payloads of selected quiescent jobs | `--workspace`, `--keys` overrides the `seal.keys` setting |
| `job unseal [--force] JOB...` | remove the seals of selected jobs, refused while the workspace is sealed | `--workspace`, `--force` skips the confirmation |
| `job detach [OPTIONS] JOB...` | make spawned jobs independent of their parents, permanently | `--workspace`, optional `--operator` (recorded; default identity when omitted) |
| `job eject [OPTIONS] JOB... DEST` | move quiescent jobs out of the workspace to free-standing job directories | `--workspace`; like `mv`, an existing `DEST` directory receives each job as `DEST/<job-key>` |
| `job adopt [OPTIONS] DIR...` | move free-standing job directories into the workspace | `--workspace`, `--placement` (default: where each job was ejected from; refused for a job tree) |
| `job list [OPTIONS]` | list jobs as a cheap table (remote: over the adapter) | `--workspace`, `--kind`, `--placement` (prefix), `--limit`, `--after`, `--tag-contains`, `--counts`, `--json`, `--adapter-timeout` |
| `job show [OPTIONS] JOB...` | describe jobs from their state (remote: over the adapter) | `--workspace`, `--no-children`, `--json`, `--adapter-timeout` |
| `job log [OPTIONS] JOB...` | print transition histories (remote: over the adapter) | `--workspace`, `--limit`, `--json`, `--adapter-timeout` |
| `job why [OPTIONS] JOB...` | explain why jobs are not running (remote: over the adapter) | `--workspace`, `--json`, `--adapter-timeout` |
| `job debug [OPTIONS] JOB` | drive one job to a terminal state in front of you | `--workspace`, `--step`, `--placement`, `--follow-children`, `--timeout`, `--log-level` |
| `job transfer [OPTIONS] SRC DST` | move jobs between two workspaces, each a registered name or else a workspace directory | `--job`, `--state`, `--placement`, `--destination-placement`, `--adapter-timeout`, `--strict-environment`, `--json` |

When giving more than one `JOB_ID`, name the workspace explicitly.

### Selecting jobs

`JOB` is a job UUID, a `tag--uuid` job key, any unique prefix of either, or a
path inside the workspace. A job directory such as `jobs/silicon--UUID` names
one job; a directory such as `jobs` names every live job below it, including
nested placements; and a glob such as `jobs/silicon*` expands to matching
entries. Relative paths are resolved from the command's current working
directory and must remain inside the selected workspace.

Remote `job show`, `job log`, and `job why` accept canonical lowercase job UUIDs
only; resolve keys and prefixes locally first.

### Creating jobs

`job new` scaffolds and submits jobs from a registered or packaged workflow
name, a runner file, a package directory, or a bare CWL, PWD, or jobflow
document, and needs no prepared payload. See {doc}`/quickstart`.

```console
httk job new --workspace WORKSPACE --workflow vasp.relax --input structure=POSCAR --tag silicon
httk job new --workspace WORKSPACE --workflow vasp.relax --input-from structure structures/ --parameter kpoint_density=30.0 --placement project/screening
httk job new --from-runner ./my_runner.py --step characterize --parameter sites=8
httk job new --from-command 'srun --ntasks=10 my_executable {input}' --file input=input_files/a.dat --tag a
```

`--workflow` also accepts a git URI, `git+https://HOST/PATH[@REF][#SUBDIR]`. The
repository is fetched at `REF`, the package in `SUBDIR` is installed, and the
job records the canonical URI with the full commit hash as its workflow id. See
{doc}`/details/workflow_uris`.

A runner supplied with `--from-runner` is run in describe mode to infer its
workflow and initial step, so it must call `httk_workflow_runner WORKFLOW
STEP...` before any work. The runner file is published into the workspace
runner store and pinned by digest, unless `--publish installed` names a packaged
runner where it is installed.

Other options:

- `--parameter NAME=VALUE` supplies an opaque implementation knob.
- `--environment NAME=VALUE` overrides one declared workflow environment entry.
- `--format FORMAT` selects the format of a bare document (see
  [Running workflow documents](#running-workflow-documents)).
- `--data-mode`: the workflows-vasp workflows default to `--data-mode none`, so
  results remain in the persistent workdir and `collect` reads them there, with
  no `data/` copy. `--data-mode transactional` also copies curated outputs into
  `data/`; the explicit option overrides the workflow default. See
  {doc}`/vasp_runners` for single-stage and chained result layouts.

The command prints one tab-separated `job_key<TAB>payload` line per job, or a
report with `--json`. A preparation warning raised by a format realization (for
example a CWL `DockerRequirement`) is printed once on stderr as
`httk workflow: warning: …`.

### Inputs, files, and batches

- `--input NAME=PATH` stages one declared input.
- `--input-from NAME SOURCE...` loads a file or the readable files in a
  directory, realizes the declared payload destination, and creates one job per
  file for a batch. A directory file with no registered reader whose name
  follows a structure convention (`POSCAR*`, `*.vasp`) is read as POSCAR. Any
  remaining unreadable files are skipped and named on one stderr line:
  `httk workflow: skipped N of M files in DIR (no registered reader): …`.
- `--file NAME=PATH` stages anything else.
- `--files DIR` expands every regular file directly inside DIR, sorted by
  basename, into the same staging and placeholder behavior as `--file`, for
  every job-creation form (`--workflow`, `--workflow-dir`, `--from-runner`, and
  `--from-command`). Symlinks to regular files are followed and staged.
  Subdirectories, symlinks to directories, broken symlinks, and other non-file
  entries are skipped with one stderr warning naming up to five entries (plus
  `…`). A DIR with no regular files errors with `no regular files in DIR`.

After a batch, one final stderr line reports `submitted N jobs`. If a batch
fails partway, it instead reports `submitted N of M jobs before failing` and
exits `2`. In a batch, `--tag` becomes a *prefix* combined with each item's
derived tag (`run7-si2o`) rather than replacing it; for a single job, `--tag` is
the whole tag.

### Command templates

With `--from-command TEMPLATE`, `shlex`-style words are turned into a published
one-step Bash runner. The template is an argv-only word list with no shell
syntax. Each `{name}` placeholder must have a matching `--parameter NAME=VALUE`
or `--file NAME=PATH`. Parameter placeholders resolve through
`httk_workflow_parameter` at run time; file placeholders resolve to the absolute
staged path below `HTTK_WORKFLOW_JOB_DIR`. A placeholder name must match
`[A-Za-z_][A-Za-z0-9_.-]*`; `{{` and `}}` emit literal braces, and any other
brace text remains literal.

Each `--file` input is also copied into the job workdir under its basename
before the command runs, so it can be read there by name; existing workdir
entries are never replaced. The copy in the payload remains the immutable
original. For example:

```console
httk job new --from-command 'srun --ntasks=10 --cpus-per-task=1 vasp_std' --files inputs/si --tag si
```

Runner identity is the digest of the rendered text, including the deterministic
sorted workdir staging lines, so staged file names are part of the wrapper
identity even when they are unused. A file name containing `/` is a valid
staging destination, but must be staged under a bare name to be used as a
placeholder. The generated runner can be edited and passed back with
`--from-runner`.

### Running workflow documents

Run a PWD, CWL, or jobflow document directly with
`job new --from-runner DOCUMENT`; the document is resolved as a format
realization:

```console
httk job new --workspace WS --from-runner flow.cwl --input message=echo
httk job new --workspace WS --from-runner workflow.json --parameter pwd_module_path='["."]'
httk job new --workspace WS --from-runner maker.json
```

`--format` accepts `cwl`, `pwd`, `jobflow`, and `httk-v1` for bare document
inputs. Manifest packages and registered ids reject the option because their
format is already declared. See {doc}`/details/workflow_compat` for package
manifests, bare-document rules, the supported CWL subset, PWD security, jobflow
Makers, format default collection, and httk-v1 details.

### Requests

`job request ACTION JOB_ID...` publishes one request per job and requires
`--reason`. `--operator` selects the signing identity; see
[Operator identity](#operator-identity).

An operator `pause` request against `claimed`, `running`, or `committing` is
deferred: the manager records it and pauses the job at the next attempt
boundary, and a terminal outcome supersedes it. An older manager that does not
understand this in-flight pause request quarantines it as invalid.

`--wait` is valid only for `pause` and exits 0 only when each requested job was
observed `paused` at some point during the wait. Jobs are confirmed one by one:
a concurrent operator may already have resumed an earlier job when the command
exits, and any pause, such as a runner-declared step pause, satisfies it.
Terminal, retired, quarantined, or timed-out requests exit 1. `--timeout
SECONDS` requires `--wait`; timed-out requests remain published.

For a remote workspace, `job request ACTION --workspace REMOTE:NAME JOB_ID ...`
asks the owning machine for unsigned envelopes, signs them with the
control-center identity selected by `--operator`, and sends the signed documents
back for verbatim publication. With prefix or tag selectors the remote resolves
the match; use full job UUIDs when precise attribution matters. When both are
given, make `--adapter-timeout` longer than `--timeout`, or the adapter may cut
the session off first. An adapter timeout during publication leaves the outcome
indeterminate: requests may already be published, and retrying creates fresh
request IDs, but generation pinning retires the stale duplicates. An older far
side that cannot parse the additive protocol vectors fails with its own argparse
error; upgrade *httk-workflow* on the remote.

### Inspecting and debugging jobs

`job list`, `job show`, `job log`, and `job why` read one workspace without
writing anything, and `job debug` drives a single job to a terminal state in the
foreground. Each command takes `--json`. See {doc}`taskmanager` for what each
command reports.

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
Each row has `job_key`, `job_id`, `state`, `step`, `placement`, `priority`,
`generation`, and `reason`. To page a large workspace, use `--limit N` and pass
the returned cursor to `--after`. The cursor format is
`<kind>:<placement>/<job_key>`, and `next_after` is null when the page is
exhausted. Paging is weakly consistent: a job that changes kind between pages
can appear twice or be missed, so deduplicate by `job_id`. `--tag-contains TEXT`
filters on the tag portion of the job key. `--counts` adds a `counts` object
with name-based marker totals per selected state kind, including malformed
marker-shaped names (which `fsck` reports); counts need a full directory walk.
The human table without `--limit` materializes all rows.

Besides the per-state claim preconditions, `job why` includes, where they apply:

- a **runner-allowlist refusal** when a live manager's `runner_modules` or
  search paths cannot reach the job's runner, so a claim would fail with
  `runner_unavailable`;
- an **attempt-history** line, `N attempts across M activations at step 'X'; K
  after unclean exits`, summarizing the journal;
- a **flapping** flag when an unlimited-budget job has attempted well past a
  small threshold without progressing;
- any **pending** operator request still in `requests/ready`, or the reason
  recorded for the most recent **retired** one.

### Sealing and detaching jobs

`job seal` and `job unseal` are the per-job half of {doc}`sealing`; a job must
be quiescent to be sealed. `job show` adds a `sealed` line, `yes` with the
signer roles or `no`, and in `--json` a `sealed` boolean plus `seal_roles`.

`job detach` permanently makes a spawned job independent of its parent: it no
longer moves with its parent's tree, it can be transferred on its own, and
`Attempt.parent` reads `None` for it. It stays in any join that references it.
Detaching writes a file in the payload instead of making a state transition, so
it works in any state except `transferring`, sealed or not. Each job prints
`detached` or `already detached`; a job with no parent is refused. For a spawned
job, `job show` adds a `detached` line (`yes` or `no`, with the parent's key),
and `--json` always carries a `detached` boolean.

### Ejecting and adopting jobs

`job eject` and `job adopt` move a job out of a workspace and into one without
either workspace knowing the other, so neither needs to be registered. Reach an
unregistered workspace with `httk -C DIR`.

`job eject` accepts a quiescent job that is not bound to its live parent or in
an unresolved join, and refuses a destination inside any workspace. The job
directory it leaves behind is the whole job: payload, seal, tree metadata, the
state it was in, and any shared workspace runner it pins; the workspace keeps no
copy. A job with bound children leaves as the root of its tree: every bound
descendant, each of which must be paused or terminal, travels inside the same
directory under `.httk-transfer/tree/<placement>/<job_key>/`. Each job prints
`JOB_ID ejected PATH`, and a selected descendant that left inside its root's
directory prints `JOB_ID ejected with its parent`. `httk workflow seal verify
DIR` verifies a sealed one in place. A job whose ejection is still in progress
is not retired by `httk workflow transfer retire`.

`job adopt` verifies a directory, moves it in (a rename on one filesystem;
across filesystems, a copy that is verified before the directory is removed),
restores the job to the state it was ejected in, and installs a runner it
carries. It prints `JOB_ID adopted STATE PAYLOAD`. A tree comes back whole, each
member at the placement it left from, so `--placement` is refused for it, and a
member's directory nested inside it cannot be adopted on its own. Every member
is checked before anything is imported. `job adopt` refuses a directory made by
a transfer to a named workspace (use `httk job transfer`) and a copy of a
directory whose job already passed through this workspace, which it leaves in
place.

Both commands are crash-safe: an interrupted `job eject` is finished by the next
`job eject` or transfer recovery in that workspace, and an interrupted
`job adopt` by adopting the same directory again. Neither works in a sealed
workspace or project; a sealed job keeps its seal.

## Moving jobs between workspaces

`job transfer` takes two workspace endpoints, a source and a destination, and
moves jobs between them in either direction. Each of SRC and DST is tried first
as a registered workspace name, then as a workspace directory (one whose root
directly contains `.httk-workspace/`; there is no upward discovery). A
registered name always wins over a same-named directory, so `./NAME` addresses
the directory unambiguously. A directory endpoint is always local; only a
registered `REMOTE:NAME` binding can point at a remote.

```console
httk job transfer [--job JOB_ID …] [--state STATE …] [--placement P] \
    [--destination-placement P] [--adapter-timeout SECONDS] [--json] SRC DST
```

Where the two endpoints are bound decides which legs run over an adapter and
which stay in this filesystem:

| Direction | What happens | `--job` |
| --- | --- | --- |
| local → remote | each named job is detached, its sealed bundle pushed to the remote, and imported there | UUIDs, prefixes, paths, or globs; required |
| remote → local | the selected jobs are offered, pulled home, imported, and their sources retired | canonical UUIDs only; optional sweep |
| local → local | each named job is detached from the source and imported into the destination directly, in this filesystem | UUIDs, prefixes, paths, or globs; required |
| remote → remote | the client relays the selected offers through local staging and pushes them to the destination (v1; a direct source-to-destination path is deferred) | canonical UUIDs only; optional sweep |

### Selecting what moves

- `--job` names jobs. For a local source it accepts a UUID, tag/key prefix, job
  directory, placement directory, or glob such as `jobs/silicon*`; paths and
  globs are resolved from the current working directory and must be inside the
  source workspace. A remote source accepts only canonical job UUIDs, because
  its selectors are resolved on the remote machine. With `--job`, each named
  job must be eligible before any job is sealed. By-id moves accept any
  quiescent state, and an explicit `--state` remains an additional filter. For
  remote → remote, `--job` values constrain the source offer before the relay
  pulls anything.
- Without `--job`, the move is a skip-tolerant sweep. `--state` (repeatable,
  default `succeeded` and `failed`) chooses which finished kinds it moves, and
  `--placement` restricts it to one subtree.
- `--destination-placement` lands the jobs at a placement other than the one
  they had.
- `--adapter-timeout` bounds every adapter operation the move runs.
- `--strict-environment` blocks the move when the destination environment check
  fails; see [Environment checks](#environment-checks).

Bundles carry sources only for workflows that declare `[workflow.build]`;
compiled artifacts are machine-local and never transferred. After importing such
a bundle, run `httk workflow build --workspace WORKSPACE TARGET` on the
destination before starting its managers; the import repeats this reminder.

### Job trees

A spawned child moves with its parent. Selecting a job selects its whole tree:
the job and every child it spawned that is still in the workspace and not
detached, recursively, whatever `--state` and `--placement` say. The children
that come along are named on standard error. Every child must be `paused` or
finished, and none may be in an unresolved join; otherwise the tree stays where
it is (a sweep skips it with a warning, and an explicit `--job` is refused). A
child named on its own is refused, naming its parent: transfer the parent, or
first make the child independent with `httk job detach`.
`--destination-placement` is refused for a selection containing such a tree,
because its children record their parent's placement.

### Environment checks

Before moving state, a transfer checks each job's declared environment against
job overrides, the destination workspace settings (read through the adapter for
a remote), and declared defaults, never the client process environment. An
unresolved default-less entry produces a warning, and so does an unavailable
remote settings read (once: the environment could not be prechecked remotely).
`--strict-environment` turns both into a block before anything is detached.

### What a transfer guarantees

A transfer fences an explicit quiescent marker, seals it in the payload,
validates the payload digest at import, publishes the preserved UUID and prior
state only at the destination, and retires the source only after an idempotent
acknowledgement. Transfer UUID and digest checks suppress retries, and sealed
and retired bundles are retained for recovery (see
[Retired transfers](#retired-transfers) for when they are reclaimed). Repeating
the same `job transfer SRC DST` resumes the matching sealed transfer, including
across the copy-before-import and lost-acknowledgement boundaries.

The sealed payload digest pins every path, every file's content *and executable
bit*, and the literal target of every symlink. A runner that arrives without its
executable bit, or a link retargeted in transit, is therefore a detected
mismatch rather than a silent corruption. A symlink is carried as its target
string and must stay inside the payload: an absolute target, or a relative one
climbing out with `..`, is refused by name, because it would mean something else
at the destination.

Every step of a fetch is idempotent and the pipeline is resumable: `offer`
reports an already sealed bundle from its ledger instead of sealing it again, a
`pull` onto a matching staged bundle is a no-op, `import` returns the
acknowledgement it already wrote, and a retired source is never offered again.
An interrupted fetch is finished by running the same command again, and a fetch
with nothing to collect does nothing.

### Running on a remote and fetching the results

Add and configure the machine, make sure *httk-workflow* is installed there and
verify it with [`remote check`](#httk-on-the-target-remote-check), create its
workspace, then send and run a job:

```console
httk workflow remote add --template ssh kappa
httk workflow remote configure \
    --set host=kappa.example.org --set username=rar \
    --set check_connectivity=yes kappa
httk workflow remote check kappa
httk workspace init kappa:/scratch/rar/httk/runs
httk workspace settings set --key slurm.partition --value batch kappa:runs
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" kappa:runs
httk job new --workflow vasp.relax --input structure=POSCAR --tag silicon
httk job transfer --job JOB-ID default kappa:runs
httk workflow run --workspace kappa:runs --workers 8
httk workspace status kappa:runs
```

The owning machine chooses the workspace path: remote init sends the path and
registers its basename there. Scheduler settings belong to the workspace.
`job transfer default kappa:runs` imports each selected job on the remote at the
placement it had here, unless `--destination-placement` says otherwise.
`run --workspace kappa:runs` makes the remote run
`httk workflow manager run --workspace runs --detach` on the owning machine,
which uses the workspace's `manager.launch` exactly as a command run on the
cluster or through a `machine_names` alias would; see
[Running managers](#running-managers) for `--count` and `--workers`.

To bring stopped jobs home, use the reverse transfer and then collect:

```console
httk job transfer --state succeeded --state failed --placement project/screening --json \
    kappa:runs default
```

A fetched job arrives as an ordinary job of the local default workspace, in its
offered state and at its remote placement, so `httk collect` reports it like a
job that ran at home.

### Far-side protocol commands

The fetch leg runs two far-side protocol commands over the adapter; they can
also be used on their own on the remote itself. They use literal paths because
they bypass the owning machine's registry:

```console
httk workflow transfer offer --destination-workspace-id UUID [--job JOB_ID …] --json PATH
httk workflow transfer retire --destination-workspace-id UUID PATH JOB_ID ...
```

`offer` detaches every selected job into its sealed bundle and prints one entry
per bundle. It requires `--destination-workspace-id`, because a bundle is sealed
for exactly one destination. It narrows what it seals with the same `--state`
and `--placement` the fetch passes through. `--job` is repeatable, accepts any
quiescent state when no `--state` is supplied, and fails all-or-nothing if an id
is missing or filtered out. A client that sends `--job` requires a new far side:
an older remote rejects the additive flag with its argparse error, which the
client relays.

`retire` first moves the sealed source of an already imported job under
`.httk-workspace/transfers/retired/` and durably records retirement, then
reclaims the bundle and eligible source journal history. A crash leaves the
source wholly live or wholly retired, and a retry finishes any interrupted
cleanup. Set `retention.trash_days` to `"keep"` or `null` to retain recovery
copies. The caller must already hold a destination acknowledgement. For
`retire`, `--destination-workspace-id` is optional and, when given, refuses a
bundle that was sealed for somebody else.

Both print JSON with `--json` and tab-separated lines otherwise. The fetch reads
their answers back over the adapter's `invoke`, so their standard output must
contain only the JSON document: a login banner or profile greeting on stdout
stops the fetch with *remote offer did not return a transfer offer document*
before anything is pulled or imported. On hosts a remote adapter reaches, send
such greetings to stderr or guard them with a non-interactive-shell test.

## Running managers

### `manager` and `run`

| Command | What it does | Notable options |
| --- | --- | --- |
| `run` | run managers through the workspace launcher, or keep one serving with `--idle` | `--workspace`, `--workers`, `--worker-resource`, `--allocation`, `--count`, `--pool`, `--capability`, `--placement-prefix`, `--idle`, `--idle-timeout`, `--time-limit`, `--deadline-margin`, `--inline`, `--launcher`, `--detach`, `--adapter-timeout`, `--log-level` |
| `manager run` | run managers through the workspace launcher, or invoke them on a remote workspace | `--workspace`, `--workers`, `--worker-resource`, `--allocation`, `--count`, `--pool`, `--capability`, `--placement-prefix`, `--idle`, `--idle-timeout`, `--inline`, `--launcher`, `--detach`, `--join-grace-seconds`, `--lease-seconds`, `--drain-timeout`, `--time-limit`, `--deadline-margin`, `--gc-interval`, `--runner-search-path`, `--adapter-timeout`, `--log-level`, `--log-file`, `--json-logs` |

`run` is the recommended spelling and `manager run` the advanced one. Both
follow the binding: a local workspace uses its `manager.launch` setting (the
built-in `process` launcher by default), and a remote workspace invokes the same
command on its owner. Both run until idle by default; `--idle` keeps serving.

The workspace settings `manager.launch`, `manager.count`, `manager.workers`, and
`manager.command` control launch-site behavior:

- `manager.launch` selects the built-in `process` launcher or a named launcher
  bundle; `--launcher` overrides it for one invocation.
- `manager.count` supplies the default number of managers. `--count N`
  overrides it and always counts managers at the launch site, not workers or
  remote adapters.
- `manager.workers` supplies each manager's concurrency; `--workers N`
  overrides it.
- `manager.command` is used after `environment.prelude` to find the manager on
  the resulting `PATH`.

`--inline` forces one in-process manager, and `--detach` returns after starting
the managers. See {doc}`launchers` for launcher bundles and their settings.

`--time-limit DURATION` tells each manager its allocation ends DURATION (Slurm
`--time` syntax such as `12:00:00`) after it starts; without it a manager
inside a Slurm job uses that job's end time (its allocation probe's end). `--deadline-margin SECONDS` (120
by default, raised to `--drain-timeout` when shorter) sets the drain point
before that end: the manager does not start a job whose `mintime` exceeds the
time left until it. When both are known the earlier of `--time-limit` and the
allocation's end wins. An invalid duration, or one not longer than the margin,
is refused before any manager starts. See
{doc}`taskmanager` under "Time requirements".

`--idle-timeout` still ends a manager that makes no progress with nothing
running; while attempts run, a manager with a known end is bounded by its
drain point, where it drains and exits.

`run` also takes `--capability` and `--placement-prefix`, so the quickstart
command can claim a capability-gated job and scope its scan. Both commands print
a startup banner and, on idle exit, a summary line naming any jobs left
unclaimable by the pools, capabilities, resources, or executors this manager
serves, or left committing with an unreadable definition.

`--worker-resource NAME COUNT` is repeatable and advertises per-manager capacity
to the scheduler, overriding the same-named capacity of the manager's
allocation. `--allocation SPEC` selects where a manager learns its nodes,
processors and devices: `auto` (the default: Slurm inside a Slurm job, else
nothing), `none`, `slurm`, `host`, or `exec:PATH` for a site probe; launchers
pass the spec their managers need. See {doc}`taskmanager` under "Allocations". With a local `--count N`, explicit `--worker-resource` pairs are passed
to every manager verbatim. Only auto-detected SLURM capacities are split across
the N managers, using quotient-plus-remainder distribution so that their
aggregate equals the detected allocation. The reserved time labels `maxtime`
and `mintime` are job requirements and are refused as capacities. For a
resource-aware run, advertise
the manager's complete allotment:

```console
httk workflow run --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000 \
  --worker-resource matlab_license_slots 2
```

Pair this with the workflow manifest's per-step resource requirements; see
{doc}`taskmanager` for the packing and dynamic-requirement example.

The default manager log is the append-only workspace file
`.httk-workspace/managers.log`. Each text line is prefixed with the manager id,
and JSON records carry `manager_id`. `--log-file` selects another destination.
The log is rotated when a manager starts, or every 1000 records once the file
exceeds 16 MiB. One backup is kept, and a manager that has not yet reopened the
file keeps appending to that backup.

### `precheck`: readiness before an attempt

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --json
httk workflow precheck --workspace WORKSPACE --runner-search-path PATH --runner-search-path OTHER
```

This read-only, advisory report checks `submitted`, `ready`, `waiting`, and
`paused` jobs; `--placement` restricts the scan. It can become stale, because
the authoritative environment gate is at attempt start. It checks:

- **environment**: each declared entry is shown as `resolved`, `default`, or
  `unresolved`, with its source and setting name. The `HTTK_*` environment layer
  is the current process environment, which may differ on compute nodes; JSON
  carries this caveat once as `environment_variable_caveat`.
- **runner reference**: availability and the pinned digest. Use repeatable
  `--runner-search-path` options to check installed runner references. A plain
  installed reference without one is reported as `indeterminate`, not as a
  broken runner, and does not by itself produce exit status `1`.
- **claimability** against the live managers the workspace publishes: a job no
  live manager can claim is a problem naming the closest manager's unmet
  requirements as `job why` renders them (for example
  `lacks capabilities docker` or `does not allow runner module …`), checked
  against each manager's real `runner_modules` allowlist. With no live manager,
  one non-failing workspace-level `manager_notice` replaces the per-job
  findings.
- **format engine**: for a compat-format job (the collect gate's pair,
  `workflow_realization = language` with a `workflow_language`), each module the
  format needs is checked without importing it, naming the pip extra to install,
  such as `pip install httk-workflow[jobflow]`. Extras belong on the machine
  that runs the job, so a missing module is a problem only when no live manager
  serves the job's executor; otherwise the check is a non-failing
  `indeterminate`, since the serving manager's environment is verified only at
  run time.
- **required inputs**: a declared required input with a staged `destination`
  must still be a member of the payload; a relocated or removed one is a
  problem.
- **step**: a job whose next step is not among the runner's recorded
  `runner_steps` (written into the state frame after its first attempt) is a
  problem. The runner is never executed, so a job with no recorded steps is
  never faulted, and a mutated payload runner may implement a different set by
  the next attempt.

The command exits `1` for an unresolved environment, a broken runner reference,
an unclaimable job, a missing and unserved language engine, a missing required
input, or a step outside the recorded set. The JSON summary carries
`claim_problems`, `language_problems`, `language_indeterminate`,
`input_problems`, and `step_problems` alongside the environment and runner
counts.

### `launcher`: the bundles that start managers

A launcher is to starting workflow managers what a remote is to reaching a
machine. The built-in `process` launcher starts detached local processes; named
launchers are versioned bundles, resolved project-first and then globally. See
{doc}`launchers` and {doc}`launcher_authoring`.

| Command | What it does | Notable options |
| --- | --- | --- |
| `launcher list` | list manager launchers visible to this project | |
| `launcher add [OPTIONS] NAME...` | create launchers from a packaged template | `--template`, `--set`, `--global`, `--non-interactive` |
| `launcher configure --set KEY=VALUE NAME...` | update launcher settings | `--set` |
| `launcher show [--json] NAME...` | describe launchers and their settings | |
| `launcher check [OPTIONS] NAME...` | check a launcher's required binaries | `--launcher-timeout` |
| `launcher remove [--force] NAME...` | remove launcher bundles | |

### `monitor`: inspect and control workspaces interactively

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow monitor` | open the curses workspace monitor | `--workspace NAME` (repeatable), `--refresh SECONDS`, `--adapter-timeout SECONDS`, `--non-interactive` |

See {doc}`monitor` for the panes, keys, and read budget.

### `workflow mpi run`

`httk workflow mpi run -- APPLICATION ARG...` executes one application through
the current daemon MPI allocation. The approved configuration fixes nodes,
ranks, and CPUs; there are no caller-supplied Slurm options. The wrapper
requires an active daemon MPI manager, streams stdout and stderr, uses
`/dev/null` for stdin, and returns the step status. A connection failure
produces an uncertain result without resubmission. See
{doc}`/details/workspace_daemon` for policy, containment, and shared-memory
requirements.

## Collecting results

### `httk collect`: workspaces and calculation trees

`httk collect [PATH]` is a top-level command, no longer a `workflow`
subcommand. What it collects depends on PATH:

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

An option for the other kind of target is refused. A workspace nested in a
calculation tree is collected as a workspace with the default states, not
walked. See {doc}`/details/collecting` for recognized calculations.

For a workspace, `collect` streams one `CollectedJob` summary per finished job
as JSON lines by default; `--raw` streams `JobRecord` records for a data layer.
For a calculation tree, `--dry-run` prints one `httk-collect-claim` line per
claimed, declined, or workspace directory and collects nothing; it cannot be
combined with `--into`. A calculation's summary line names its `"directory"`
relative to the swept root.

With `--into`, a sealed id ledger keeps entry ids stable across rebuilds. It is
on by default at `<into>.ids.sqlite`; `--id-ledger PATH` relocates it, and
`--no-id-ledger` disables it (ids then become unstable across rebuilds). See
{doc}`/details/stable_ids`.

`--upgrade` (only with `--into`) lets an existing store take an additive layout
change: new record kinds appended to a family, or new families, for example a
typed record kind a newer *httk* ships. Without it, such a store is refused with
a message naming `--upgrade`. A change that is not additive still needs a
rebuild. Keep a backup of the store before upgrading.

`--degraded` prints only the degraded per-job lines, while the trailing summary
still counts the whole sweep; it cannot be combined with `--raw`.

Every form except the pure-array `--json` ends with one
`httk-workflow-collect-summary` line counting `collected`, `degraded`,
`unfulfilled_roles`, `storage_errors`, and `skipped_unreadable`, plus
`unclaimed` for a calculation tree and `revised` with `--into`. The command
exits nonzero when any job was degraded, failed to store, or was skipped for an
unreadable `job.json`; unfulfilled roles and unclaimed directories alone keep
the exit at `0`. See {doc}`/details/collecting` for the triage members and the
`--into` partial-state semantics.

### `postprocess`: run a curated script

| Command | What it does | Notable options |
| --- | --- | --- |
| `postprocess [OPTIONS] [JOB...]` | run one declared script for each selected collected job | `--workspace WS`, `--script NAME` (required), `--workflow-dir PKG`, `--state`, `--placement`, `--output-dir DIR`, `--timeout`, `--json` |

```console
httk workflow postprocess --workspace WS --script relaxation-report
httk workflow postprocess --workspace WS --script report --workflow-dir ./my-workflow --json
httk workflow postprocess --script relaxation-plot <job-id>
```

Output is written outside the job payload, so a sealed job, whose seal covers
only the payload, can be postprocessed. The output root is
`<workspace>/postprocess` by default, the `postprocess.directory` workspace
setting when set, or `--output-dir DIR` for one invocation; a relative value
resolves against the workspace root. Each job's directory below it is
`<root>/<placement>/<job_key>/<NAME>/`.

An optional `JOB` selector (UUID, key, unique prefix, or workspace path, the
same forms `job seal` and `job delete` accept) postprocesses exactly those
quiescent jobs and cannot be combined with `--state` or `--placement`.

With `--json`, each result is one JSON object in the `httk-workflow-postprocess`
wire format, version 2, with `workspace_id`, `job_id`, `job_key`, `script`, and
either `returncode` plus `output_dir`, or an `error`. Without `--json`, each
result is the tab-separated line
`job_key<TAB>script<TAB>returncode<TAB>output_dir`, and errors use `ERROR` in
the return-code field. The command exits 0 only when every selected script ran
and returned 0; any resolution error or nonzero script return exits 1.

### `v1 collect`: harvesting finished *httk* v1 trees

| Command | What it does | Notable options |
| --- | --- | --- |
| `v1 collect ROOT` | harvest a pre-existing v1 result tree | `--workflow-dir PKG`, `--into PATH`, `--id-base BASE`, `--id-series SERIES` |

Harvest old v1 results without submitting them:

```console
httk workflow v1 collect --workflow-dir PKG ROOT
httk workflow v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

`v1 collect` ends with one `httk-workflow-v1-collect-summary` line reporting
`finished`, `unfinished_by_status` (tasks the name regex matched that were not
`.finished`, keyed by status), and `skipped_no_rundir` (finished tasks with no
dated run directory). Each collected report carries `identity_stable`: `false`
for a task whose identity is path-derived because it has no `ht.manifest`. A
warning names how many such tasks a harvest saw.

## Remotes

Named after `git remote`, a *remote* is one machine this project can reach,
together with the bundle of adapter operations that reaches it. `REMOTE:NAME`
names a workspace on a remote. The name `local` is reserved for the built-in
remote that every workspace registry resolves as "this machine", so
`remote add local` is refused: a workspace bound to `local` must be
unambiguous.

### `remote` commands

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote list` | list the remotes this project can reach | |
| `remote add [OPTIONS] NAME...` | create remotes from a packaged adapter template | `--template`, `--global`, `--non-interactive` |
| `remote configure [OPTIONS] REMOTE...` | run adapters' `configure` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote check [OPTIONS] REMOTE...` | check that `httk` answers on remotes | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote import-v1 [OPTIONS] SOURCE...` | map legacy *httk* v1 computer bundles | `--name` (one source only), `--global` |
| `remote show [--json] NAME...` | describe remotes and their settings | |
| `remote remove [--force] NAME...` | remove remote bundles | |
| `remote daemon configure REMOTE` | import an approved endpoint catalog | required `--endpoint`, `--mount-root`, `--requests`, `--responses` |
| `remote daemon health REMOTE` | check the confined daemon | `--request-id`, `--wait-seconds` |
| `remote daemon start REMOTE` | start one approved manager configuration | required `--configuration`, `--request-id`; `--wait-seconds` |
| `remote daemon status REMOTE` | inspect a manager | required `--handle`; `--request-id`, `--wait-seconds` |
| `remote daemon cancel REMOTE` | request manager cancellation | required `--handle`, `--request-id`; `--wait-seconds` |

`remote show NAME` reports which file each setting came from, but never a
credential value: a setting stored in the manifest-excluded `credentials.json`
is shown by name only, so pasted output cannot leak a password.

`remote remove` refuses while an unretired transfer still depends on the remote,
which would otherwise have no way home; fetch or retire the transfer first.
`--force` skips only the interactive confirmation, not this refusal.

### Remote definitions and templates

Remote definitions are versioned directories containing `remote.json` and one
executable `adapter`. Project definitions shadow global definitions. The
operation name travels in each versioned JSON request; the dispatcher prints one
JSON result and sends diagnostics to stderr. Commands and remote commands are
always argument arrays. The general maintained templates implement that protocol
through {py:mod}`httk.workflow.adapter_protocol`, the public name of the
packaged implementation. {doc}`adapter_authoring` is the reference for writing
your own: the bundle layout, the request and result documents of the six
operations (`configure`, `install`, `invoke`, `push`, `pull`, and `status`), the
optional `daemon` operation, and the rules for a custom adapter.

The module packages four maintained templates for `remote add --template`:

- `local`: same-machine transport;
- `ssh`: rsync plus command execution over SSH;
- `mount`: a locally mounted remote filesystem plus a command executor;
- `mount-daemon`: typed file requests to a confined destination broker, using a
  separate dispatcher for its restricted file protocol.

The first three use the target workspace's `manager.launch` setting, such as a
packaged `slurm` launcher. `mount-daemon` selects a locally approved serial or
MPI launcher configuration through the daemon and refuses generic `REMOTE:NAME`
operations; transfer jobs using absolute mounted workspace paths. See
{doc}`/details/remotes` for configuration and request-ID retry rules. Any other
`kind` in a `remote.json` is refused rather than executed in the wrong place.

### Remote settings

`remote configure --set KEY=VALUE` persists only the machine-level keys
`check_connectivity`, `check_mount`, `exec_command`, `host`, `httk_command`,
`legacy_settings`, `mount_root`, `port`, `prelude`, `remote_root`, `username`,
`vasp_command`, and `vasp_pseudo_library` in the shareable `remote.json`.
Scheduler profile values are workspace settings: use `slurm.account`,
`slurm.partition`, `slurm.time_limit`, `slurm.nodes`, `slurm.cpus_per_task`,
`slurm.ntasks`, `slurm.ntasks_per_node`, `slurm.mem`, `slurm.gres`,
`slurm.reservation`, and `manager.workers`; those scheduler names are refused as
remote settings. Other non-persistable keys are stored in the remote's
`credentials.json` beside it, with mode `0600`; project manifests exclude that
file. Adapters receive both together as the request's `remote_settings`.

`remote import-v1` maps recognized legacy *httk* v1 computer bundles by reading
assignment-only configuration; the legacy shell executables are never copied or
run. It selects `ssh` when the legacy configuration contains `REMOTE_HOST`,
otherwise `local`. It maps the first legacy submission profile into
`legacy_settings`; if several `config.*` profiles exist, the others are skipped
with a warning, because submission profiles are now workspace settings. Legacy
`SLURM_*` values are not remote settings: add a `slurm` launcher and set the
target workspace's `manager.launch` instead. Review the imported
`legacy_settings` and initialize the workspace path explicitly before
transferring jobs.

### What each kind does

`local` copies files in this filesystem and runs commands as child processes.
Each workspace's `manager.launch` setting selects its manager launch policy.

`ssh` moves files with `rsync` over `ssh` and runs every command on the
configured host. Only `ssh` and `rsync` are required locally.

`mount` moves files through a locally mounted view of the remote filesystem
(sshfs, NFS, any shared mount) and runs every command through an executor
(`exec_command`, an argv prefix such as `ssh -o BatchMode=yes host` or
`/abs/path/bin/hpc run`). `mount_root` is the local mount and `remote_root` the
same tree as the remote spells it; a transfer path outside `remote_root` is
refused rather than copied somewhere local. Neither `ssh` nor `rsync` is
required.

All three kinds implement the same six operations:

| Operation | `local` behaviour | `ssh` behaviour | `mount` behaviour |
| --- | --- | --- | --- |
| `configure` | validates pending machine settings | verifies the host answers with a cheap remote `true` | validates the roots and executor, and (unless disabled) that the mount exists and the executor answers |
| `install` (the `remote check` verb) | checks that local `httk` answers | checks that `httk` answers on the far side and reports its version | checks that `httk` answers through the executor and reports its version |
| `invoke` | runs the argument vector as a child process | runs it on the configured host and returns status, stdout, and stderr | runs it through the executor and returns status, stdout, and stderr |
| `push` / `pull` | copies the requested tree or relative file batch locally | transfers it with `rsync --archive` over SSH | copies it through the mount, mapping the remote path onto `mount_root` |
| `status` | runs the workspace status command locally | runs `httk workspace status --json NAME` remotely | runs `httk workspace status --json NAME` through the executor |

`httk_command` overrides how `httk` is spelled on the far side, for example
`httk_command="/proj/venv/bin/httk"`. Without it, the plain `httk` on the remote
`PATH` is used, and locally a `python3 -m httk.core.cli` fallback applies.

`mount-daemon` supports `configure`, `install` (a health request), and the
optional `daemon` operation, and refuses the generic operations in the table
above. Its eight settings and typed request/result contract are in
{doc}`adapter_authoring`.

### Quoting

Every subprocess an adapter starts is an argument vector, so no shell parses a
value from a request or from settings. The one exception is `ssh`, which joins
its command words for a login shell on the far side to parse, so all remote
command strings are built by a single helper that quotes element-wise. Manager
launcher bundles own any scheduler-script quoting separately. `rsync` transfers
pass `--protect-args`, so even file names travel in the protocol rather than
through the remote shell.

### httk on the target: `remote check`

*httk* is never installed on a remote for you, because every cluster sets up
software differently (modules, venvs, conda, pipx, ...). The requirement is that
the adapter's *non-interactive* shell can run `httk`, with *httk-workflow*
installed beside the core.

Run `remote check` once after configuring a new remote. It confirms that the
host answers, that `httk` is found (also trying `python3 -m httk.core.cli`), and
that the workflow command group exists, and reports the command and version it
found. `--version` alone would only prove *httk-core*.

If the check fails, log in and set up *httk₂* on the remote, for example with
`pipx install httk-workflow`, reachable from a non-interactive shell: a
`module load` or conda activation behind an interactivity test in `.bashrc`
works at login but not over the adapter. If `httk` lives elsewhere (a project
venv, a wrapper script), point the remote at it with
`remote configure --set httk_command="/proj/venv/bin/httk" REMOTE`.

In the adapter protocol this operation keeps its historical name `install`. The
earlier `bootstrap=pip` opt-in that attempted a `pip install --user` is retired.

## Configuration and projects

### Per-user configuration

| Command | What it does | Notable options |
| --- | --- | --- |
| `config show [KEY]` | print the configuration, or one member | |
| `config set KEY VALUE` | store one member | `machine_names` is a comma-separated list of names this machine answers to |
| `config unset KEY` | remove one member | |
| `config import-v1 [SOURCE]` | read a legacy `~/.httk` configuration | |

User configuration follows the XDG base-directory convention, and everything
per-user this package keeps is configuration:

- `$XDG_CONFIG_HOME/httk/config.json` (machine-level settings such as
  `machine_names`);
- operator identity in `$XDG_CONFIG_HOME/httk/identity.json`, managed by
  *httk-core*;
- identity keys in `$XDG_CONFIG_HOME/httk/keys/`;
- global remote definitions in `$XDG_CONFIG_HOME/httk/remotes/`.

`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` can provide explicit deployment or test
overrides.

```console
httk init --name "A User" --email user@example.org
httk workflow config set machine_names "node-a,node-b"
httk workflow config unset machine_names
httk project init --name example .
```

`config set` accepts only the keys the configuration has (`machine_names` is
the only settable one) and names them when it refuses another, so a typo cannot
become a member that nothing reads. `format` and `format_version` describe the
document and are written by *httk* itself; a configuration whose `format` or
`format_version` is missing or different is refused rather than misread.

Operator identity is not a `config` member: the name, email, named identities,
and default live in `identity.json`, managed by the core-owned `httk init` and
`httk identity` commands (see [Operator identity](#operator-identity)). Legacy
`~/.httk` data is read only through `config import-v1`, which imports a legacy
name and email as the first named identity when none is configured, and imports
the legacy public key into `identity.json` through *httk-core*, recording only
`imported_from` in the configuration. The legacy 64-byte private material is not
converted.

### Projects

The project anchor and every project verb (`init`, `show`, `import-v1`,
`export`, `repair`, `adopt`, `manifest create | verify`, `seal`, `unseal`, and
`verify-seal`) belong to *httk-core*, which owns the whole `httk project`
command and documents it. *httk-workflow* mounts no project verbs of its own.
Instead, it registers the **workspace** as a project-*member* kind, so core's
verbs delegate a workspace's internals to it: what to leave out of a manifest,
how to seal it, its seal digest, how to verify it, and its health checks.

A project has `httk_project/project.json` and a standard 32-byte Ed25519 seed
stored with mode `0600`. Commands discover the nearest project in the working
directory's parent chain. The project's default workspace for workflows is
recorded by name and may live outside the project. Create a project with
`httk project init PATH`, then give it a workspace with
`httk workspace init PATH`.

A workspace inside a project is recorded in `httk_project/members.json`:
registered on `httk workspace init`, unregistered on `workspace delete` or
`workspace forget`, and its path followed on `workspace move`. `members.json`
also records each workspace's name, so a project copied to another machine stays
usable. There, `httk workspace adopt` (per workspace) or `httk project adopt`
(every member at once) registers the copied workspaces in the new machine's
per-user registry under their recorded names, joins any that are missing from
`members.json`, and records a name where one was absent. Adoption is idempotent
and never touches a sealed project. Even before adopting, resolving a workspace
by a name the local registry does not know falls back to the enclosing project's
`members.json` for that one invocation, without writing the registry.

To seal a project, run `httk workspace seal` and then `httk project seal`.
Verify the whole tree with `httk project verify-seal` (or `httk workflow seal verify`).

### Describing and checking a project

```console
httk project show
httk project show --json
httk project repair --dry-run
httk project repair .
```

`httk project show` reports the project's metadata and keys;
`httk workspace status` and `httk project manifest verify` report the live
workspace and manifest state.

`project repair` handles conditions that quietly break a project later: a stale
maintenance lock, staging leftovers, a workspace on disk missing from the
registry, and a member not yet adopted on this machine. By default it applies
the safe repairs and adopts every member workspace here, which is what a freshly
copied tree needs. It reports what it did and journals the repair in the
project's workspace. `--dry-run` only reports, and `--no-adopt` skips adopting.
It exits `1` only when a check is broken; a warning, such as a missing manifest,
does not fail it.

### `seal verify`: verify a sealed tree

Seals are written per level (`job seal`, `workspace seal`, `project seal`).
The `seal` group holds the one verb that belongs to no single level: verifying a
whole sealed tree. {doc}`sealing` is the full guide.

| Command | What it does | Notable options |
| --- | --- | --- |
| `seal verify [PATH]` | verify the seal at `PATH` (a project, workspace, or job payload; default `.`) and, unless `--shallow`, every seal it references | `--json`, `--trusted-key KEY_OR_FINGERPRINT` (repeatable), `--shallow` |

Each line is `<level> <subject> <verdict> <reason>`, with indented
`<kind> <path>` discrepancy lines beneath a failing entry. A final status line's
word and exit code mirror `manifest verify`:

- `ok`, exit 0, when every entry is `valid_trusted`;
- `UNTRUSTED`, exit 3, when no entry is invalid but a signer is untrusted;
- `FAILED`, exit 1, on any invalid entry.

By default the project's pinned keys and the local identities' public keys are
trusted, so a tree sealed by its own project or identity verifies as
`valid_trusted` without naming a key.

## Signed manifests and operator identity

### Signed manifests

```console
httk project manifest create .
httk project manifest verify
httk project manifest verify --trusted-key keys/collaborator.pub
```

The *httk₂* manifest is deterministic canonical JSON lines compressed with
bzip2. It records sorted POSIX paths, regular-file sizes and SHA-256 hashes,
empty directories, and symlink targets. Special files are rejected. A
domain-separated body digest is signed with Ed25519. Verification also
recognizes the legacy `ht.project/manifest.bz2` format without changing it.

Creation fences manager launches only when a workspace is co-located with the
project, and refuses active work there. A detached project needs no workspace to
create its manifest.

A directory containing a regular `job.json` that parses as an
`httk-workflow-job`, whose UUID matches the directory's valid job key, is a job
payload. Its direct `attempts`, `logs`, and `.httk-job` children are skipped
during both manifest creation and verification; same-named directories elsewhere
are ordinary project content.

### What a verified manifest proves

The signing key lives in the tree it signs: `httk_project/keys/project.seed`
has mode `0600` and is excluded from the manifest, but not absent. **Anybody who
can write the project directory can re-sign it.** A manifest proves that the
tree is exactly what somebody with the seed described; it is not a tamper seal
against an attacker with write access.

The digests catch accidental damage exactly: a truncated copy, a partial
`rsync`, bit rot on an archive volume, a stray edit. The signature catches a
copy that travelled without the seed, or a *replaced* tree signed by another
key. Verification therefore compares the signing key with a **trust anchor that
did not come from the manifest**: the key pinned in `project.json` at `httk
project init`, plus any key named with `--trusted-key`. A key read from the
manifest's own header would always verify.

Verification has three outcomes:

| Verdict | Exit | Meaning |
| --- | --- | --- |
| `valid_trusted` | `0` | the manifest describes this tree and a pinned key signed it |
| `valid_unknown_key` | `3` | the manifest describes this tree, but nothing here pins the key that signed it |
| `invalid` | `1` | the tree does not match the manifest, the signature does not verify, or the manifest names another project |

`invalid` also covers a manifest whose `project_id` disagrees with
`project.json`: a manifest of a different project dropped into this tree is
refused by name however well it verifies internally.

### Pinning and adopting keys

A project created by `httk project init` pins its own key at creation, so its
manifests verify as `valid_trusted` immediately. A project made before pinning
existed has no `public_key` in `project.json`, so its manifests verify as
`valid_unknown_key` until somebody decides which key to trust. That decision is
explicit, because it is the whole trust model in one act:

```python
from httk.workflow.projects import pin_project_key, trust_project_key

pin_project_key("/path/to/project")  # adopt keys/project.pub
trust_project_key("/path/to/project", "ed25519:…")  # adopt somebody else's key
```

`pin_project_key` adopts the key in the tree *right now*, so use it only on a
tree you believe is the one you left. `trust_project_key` adds a
further anchor to `project.json`'s `trusted_keys`. `httk project import-v1`
fills that list with the legacy identities of an imported *httk* v1 project, so
its old `ht.project/manifest.bz2` verifies as trusted too. `--trusted-key`
accepts either an `ed25519:BASE64` value or the path of a `*.pub` file, and is
the one-off equivalent that writes nothing.

Attribution *between* machines (who published a request, who imported a
transfer) uses the separate operator identity key below.

### Operator identity

Operator identity (the recorded name, email, default, and named identities)
lives in `$XDG_CONFIG_HOME/httk/identity.json`, managed by *httk-core*. The
core-owned root command `httk init` derives a short name from the email's local
part, creates the first named identity and its `identity-SHORT.seed`/`.pub` pair
below `$XDG_CONFIG_HOME/httk/keys/`, and makes it the default.
`httk identity add --name NAME --email EMAIL SHORT` adds named identities, and
`httk identity default SHORT` changes the default. Removing an identity leaves
its key files on disk. Removing the default when exactly one identity remains
selects that identity automatically; with several remaining, select another
default first.

The default signing identity is the `default_identity` recorded in
`identity.json`, or the only configured identity when there is exactly one. With
no configured identity, documents are unsigned; several identities without a
resolvable default are refused.

`job request` records the selected identity's `Name <email>` label and signs the
request with that identity's key. Omitting `--operator` selects the default
identity. A selector containing `<` is a literal `Name <email>` label (the name
may be empty), passed through and signed with the default identity's key; any
other selector must be a configured short name. For remote requests the
control-center identity signs locally, so attribution and signer are the same
identity (see [Requests](#requests)). If a configured identity's key file is
missing or unreadable, the request fails loudly; restore the key file, or run
`httk identity remove SHORT` and then `httk identity add SHORT ...`.

Transfer acknowledgements always use the default identity, with no per-request
selection. Signatures are detached, cover the canonical
JSON of the whole document, and are domain-separated from every other *httk*
signature.

Document signing remains optional for lower-level callers, and a manager or a
transfer source accepts unsigned documents as before. A signature that *is*
present must verify: a request with a broken signature is quarantined with the
reason, and an acknowledgement with a broken signature will not retire a sealed
bundle. A verified request records its `operator_key` in the journalled state
frame beside the operator name and reason.

A signature means attribution, not authorization: it says *which identity
published this document* and permits nothing. Anyone who can write the
workspace's request directory can still publish an unsigned request.

## Campaigns

| Command | What it does | Notable options |
| --- | --- | --- |
| `campaign init` | define the project's partition map and assignment policy | `--partition NAME=WORKSPACE`, `--assignment` |
| `campaign show` | show the partition map | `--json` |
| `campaign submit` | assign one root job to a partition and submit it there | `--workflow` (required), `--key` (required), `--index`, `--input`, `--input-from`, `--parameter`, `--file`, `--tag`, `--placement`, `--priority`, `--name`, `--json` |
| `campaign collect` | collect every partition, one workspace after another | `--partition`, `--state`, `--placement`, `--raw`, `--allow-job-collector`, `--into PATH`, `--id-base BASE`, `--id-series SERIES` |
| `campaign start-<wbr>managers` | start a manager per selected partition | `--partition`, `--workers`, `--worker-resource`, `--count`, `--launcher`, `--idle-timeout`, `--adapter-timeout` |

A campaign is a thin convention over registered workspaces: a partition map,
stored in the project, that spreads a very large body of work across many
workspaces without a new scheduler. Each partition names one registered
workspace, roots are assigned to partitions by policy, and spawned children
always inherit their parent's workspace. See {doc}`/campaigns`. `campaign submit
--workflow` accepts a workflow name, alias, or git URI only; use `job new
--from-runner` or `job new --from-command` for a file or command.

## Protocol spellings and removed commands

`job transfer` shares its machinery with frozen argument vectors that one
machine runs on another over an adapter, addressed as
`httk workflow transfer receive|offer|retire`. They are protocol, not operator
interface: a local → remote move invokes `receive` on the destination, and a
fetch invokes `offer` then `retire` on the source. They are hidden, and spelled
`workflow` rather than `job`, because a remote peer invokes them by exact name.
Their spelling is frozen because the machine that answers may run an *httk*
older or newer than yours:

```text
httk workflow transfer receive --workspace PATH --bundle BUNDLE
httk workflow transfer offer --destination-workspace-id UUID [--job JOB_ID …] --json PATH
httk workflow transfer retire --destination-workspace-id UUID --json PATH JOB_ID …
httk job request-envelopes ACTION --workspace WORKSPACE --operator=LABEL --reason=TEXT [--priority N] [--step S] [--force] --json JOB_ID …
httk job publish-requests --workspace WORKSPACE --document JSON [--document JSON …] [--wait] [--timeout S] [--durable|--no-durable]
httk workspace status --by-path --json PATH
httk workflow manager run --by-path --workspace PATH
```

`receive` is an import half, not an operator command, so `--help` does not
advertise it. Operator-facing vectors use workspace names; the hidden
`--by-path` switch, used after the client probes the owning machine, makes the
workspace argument a literal path with no registry lookup, so one machine can
address another machine's workspace.

### Removed commands

The pre-release `transfer send`, `transfer fetch`, and `transfer status` verbs
are **gone** and no longer parse. Use the single `job transfer SRC DST` verb,
`manager run --workspace NAME` to start managers, and `workspace status NAME` to
read a remote workspace's markers:

| Removed | Now |
| --- | --- |
| `transfer send REMOTE JOB …` | `job transfer --job JOB … LOCAL REMOTE` |
| `transfer fetch --remote REMOTE --workspace LOCAL` | `job transfer REMOTE LOCAL` |
| `transfer status REMOTE` | `workspace status REMOTE` |

An earlier release also renamed two whole groups. `httk workflow computer …`
became `httk workflow remote …` (git's word for the same idea). `httk workflow
tasks …` (once `httk workflow remote send|fetch|…`) became
`httk workflow transfer` and then, when the operator verb moved to the `job`
group, today's `httk job transfer`; the hidden protocol spellings stayed
`httk workflow transfer receive|offer|retire`.

A job whose `runner.path` pins a `pkg:httk.workflow.runners/vasp_*` or
`pkg:httk.workflow.vasp.runners/vasp_*` form breaks too. The VASP workflows left
the module for [workflows-vasp](https://github.com/httk/workflows-vasp), and a
job pinning either path fails with `runner_unavailable` naming the module it
could not resolve. Scaffold the job again from a workflows-vasp URI (see
{doc}`/vasp_runners`).
