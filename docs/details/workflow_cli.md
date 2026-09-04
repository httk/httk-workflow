# Project and workflow command line in detail

*For operators and campaign owners: the whole command tree, including projects, configuration, signed manifests, remotes, and work that travels to them.*

Installing *httk-workflow* registers lazy `workflow`, `workspace`, and `job`
commands with *httk-core*:

```console
httk workflow --help
```

## One command tree

`httk workflow …` remains the spelling for workflow execution and project
commands. Workspace and job management are top-level command trees:
`httk workspace …` and `httk job …`. Each group answers `--help`, each command
answers `--help`, and a mistyped action is reported by the group it was mistyped
in.

## The complete tree

```text
httk workspace          init | list | default | move | forget | delete | status | managers | workflows | settings show | settings set | settings unset | workflow-prelude show | workflow-prelude set | workflow-prelude unset | policy show | policy set | fsck | gc | unlock | seal | unseal
httk workflow runner     publish | describe
httk workflow build      [--workspace WORKSPACE] TARGET...
httk job                 new | submit | request | delete | seal | unseal | list | show | log | why | debug
httk workflow list       [--json]
httk workflow describe   TARGET [--json]
httk workflow seal       verify [PATH] [--json] [--trusted-key KEY] [--shallow]
httk workflow precheck   [--workspace WORKSPACE] [--placement P] [--json]
httk workflow collect
httk workflow postprocess
httk workflow run        [--workspace WORKSPACE]  (the recommended spelling of `manager run`)
httk workflow manager    run
httk workflow campaign   init | show | submit | collect | start-"managers"
httk workflow v1         collect
httk workflow config     show | set | unset | import-v1
httk init | identity     (core-owned: per-user configuration and named operator identities)
httk project             repair | manifest create | manifest verify | seal | unseal   (httk-workflow mounts these beside core init | show | import-v1)
httk workflow remote     list | add | configure | check | import-v1 | show | remove
httk workflow transfer   [OPTIONS] SRC DST      (plus the protocol spellings: receive | offer | retire)
```

### Workspace selection

A `WORKSPACE` is an optional registered local name. When it is omitted, commands
resolve workspaces in this order: the closest enclosing workspace containing
`.httk-workspace/format.json`, the project's recorded default, the registry's
default, then an auto-created per-user default. Explicit local names are created
with `workspace init PATH`, and remote names use `REMOTE:NAME` at use time.

The registry is machine-owned: it stores only absolute local paths in
`$XDG_CONFIG_HOME/httk/workspaces.json`. When a command reaches a remote
workspace, the far side resolves the plain name in its own registry. The Python
API keeps `Workspace(path)` for library use; the registry is what the command
line speaks.

Remote-capable workspace commands use the adapter; this includes status,
settings, and `job request`. Most job commands remain local-only. Jobs are
created in the local default workspace, then `transfer` moves them to a remote
workspace for execution.

`job request ACTION --workspace REMOTE:NAME JOB_ID ...` asks the owning machine for unsigned envelopes,
signs those envelopes with the control-center identity selected by
`--operator`, and sends the signed documents back for verbatim publication.
An older far side that cannot parse the additive protocol vectors fails with
its own argparse error; upgrade `httk-workflow` on the remote. If both are supplied, make
`--adapter-timeout` longer than `--timeout` or the adapter may cut the session
off first. An adapter timeout during publication has an indeterminate outcome:
requests may already be published; retrying creates fresh request IDs, and
generation pinning makes such duplicates harmless because stale requests
retire. With prefix or tag selectors, the remote resolves the match; use full
job UUIDs when precise attribution matters.

### `monitor` — inspect and control workspaces interactively

| Command | What it does | Notable options |
| --- | --- | --- |
| `workflow monitor` | open the curses workspace monitor | `--workspace NAME` (repeatable), `--refresh SECONDS`, `--adapter-timeout SECONDS`, `--non-interactive` |

### `workspace` — the workspace itself, not its jobs

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init [OPTIONS] PATH...` | create or adopt workspaces, registering each name (basename or `--name`) centrally and recording it in the project's `members.json` | `--name` (one path only), `--setting`, `--no-durable` |
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

Sealing is described in full in {doc}`../sealing`. `workspace status` gains a
`sealed` line (and JSON field). `workspace seal` runs inside the maintenance
guard, so it requires a quiescent workspace; without `--force` it lists the
still-unsealed jobs and refuses rather than sealing a partial set.

`workspace init` creates and registers an explicit workspace. A canonical path may
have only one registered name:

```console
httk workspace init --name my-workspace runs/my-workspace
```

A workspace on a cluster is addressed as `REMOTE:NAME`; its owning machine
chooses the path supplied to `workspace init REMOTE:PATH`. `--setting KEY=VALUE`
seeds an application setting at creation. `workspace delete` destroys the
workspace (locally, or on its remote over the adapter) and is refused without
`--force`; `workspace forget` only removes the name when there are no unretired
outbound transfers. Fetch or retire those first, or pass `workspace forget
--force` to deregister the name anyway.

`workspace move NAME DEST_DIR` is an atomic same-filesystem rename. It refuses
cross-filesystem moves; stop managers, copy the tree manually, forget the old
name, and re-register it with `workspace init --name NAME <newpath>` instead.

### `runner` — the shared runners a workspace publishes

| Command | What it does | Notable options |
| --- | --- | --- |
| `runner publish [OPTIONS] FILE_OR_DIRECTORY...` | publish runner files or directories, pinned by digest | `--workspace`, `--name` (one source only), `--replace`, `--json` |
| `runner describe [OPTIONS] [NAME...]` | report published runners and their digests | `--workspace`, `--json` |

### `build` — foreground registration of compiled workflow packages

| Command | What it does | Notable options |
| --- | --- | --- |
| `build [OPTIONS] TARGET...` | build and register packages, store runners, or jobs' workspace runners | `--workspace`, `--list`, `--json` |

Directory rows are reported as `tree (inferred)` because the store format
identifies a tree by its `run` entry. New nested file publishes named `run` are
refused; a pre-existing store may still contain an ambiguous legacy layout, so
the marker is an honest inference rather than provenance metadata.

The build command has two forms:

```console
httk workflow build [--workspace WORKSPACE] [--json] TARGET...
httk workflow build [--workspace WORKSPACE] [--json] --list
```

`TARGET` may be a workflow package directory, a workspace runner-store path,
or a job reference whose runner is a workspace package. A package directory is
published first, then its source tree is built and its artifacts are registered
for the local platform tag. A store path or job reference builds the already
published source tree. `--list` does no build and prints the workspace's
registrations; `--json` emits machine-readable build or list records. Exit 0
means the registration completed (or the list was read); malformed targets,
probe/build failures, and missing artifacts are nonzero failures.

The build vocabulary and engine come from `httk.core.building`; this layer keeps
the workspace runner-build store and platform-tagged registrations. The
manager passes registered artifacts through `HTTK_WORKFLOW_RUNNER_ARTIFACTS`
without modifying the published source tree. A plugin-sourced workflow is first resolved and pinned into
the workspace like any other package, then built with the same command using
its job or store runner target.

### `job` — making jobs, and finding out about them

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new [OPTIONS]` | scaffold and submit jobs from a workflow, runner file, package directory, or command template | `--workspace`, exactly one of `--workflow`, `--workflow-dir`, `--from-runner`, or `--from-command`, `--parameter`, `--environment`, `--format`, `--input`, `--input-from`, `--file`, `--files`, `--tag`, `--placement`, `--json` |
| `job submit [OPTIONS] SOURCE...` | submit prepared payload directories | `--workspace`, `--placement` (required), `--move` |
| `job request ACTION [OPTIONS] JOB_ID...` | publish one request per job selector (remote: over the adapter) | `--workspace`, optional `--operator` (configured short name or literal `Name <email>`; default identity when omitted), required `--reason`, `--priority`, `--step`, `--force`, `--wait`, `--timeout`, `--adapter-timeout` |
| `job delete [--force] JOB...` | remove selected job payloads and state markers (remote: over the adapter) | `--workspace`, `--force`, `--adapter-timeout` |
| `job seal [--keys REFS] JOB...` | seal the payloads of selected quiescent jobs | `--workspace`, `--keys` overrides the `seal.keys` setting |
| `job unseal [--force] JOB...` | remove the seals of selected jobs, refused while the workspace is sealed | `--workspace`, `--force` skips the confirmation |
| `job list [OPTIONS]` | list jobs as a cheap table (remote: over the adapter) | `--workspace`, `--kind`, `--placement` (prefix), `--limit`, `--after`, `--tag-contains`, `--counts`, `--json`, `--adapter-timeout` |
| `job show [OPTIONS] JOB...` | describe jobs from their state (remote: over the adapter) | `--workspace`, `--no-children`, `--json`, `--adapter-timeout` |
| `job log [OPTIONS] JOB...` | print transition histories (remote: over the adapter) | `--workspace`, `--limit`, `--json`, `--adapter-timeout` |
| `job why [OPTIONS] JOB...` | explain why jobs are not running (remote: over the adapter) | `--workspace`, `--json`, `--adapter-timeout` |
| `job debug [OPTIONS] JOB` | drive one job to a terminal state in front of you | `--workspace`, `--step`, `--placement`, `--follow-children`, `--timeout`, `--log-level` |

When giving more than one `JOB_ID`, name the workspace explicitly.

`job show` gains a `sealed` line — `yes` with the signer roles, or `no` — and,
in `--json`, a `sealed` boolean plus `seal_roles`. `job seal` and `job unseal`
are the per-job half of {doc}`../sealing`; a job must be quiescent to be sealed.

An operator `pause` request against `claimed`, `running`, or `committing` is
deferred: the manager records it and pauses the job at the next attempt
boundary; a terminal outcome supersedes it. An older manager that does not
understand this in-flight pause request quarantines it as invalid.

`JOB_ID` is repeatable, so one command publishes one request per job. `--wait`
is valid only for `pause` and exits 0 only when each requested job was observed
`paused` at some point during the wait; jobs are confirmed individually, not
simultaneously. A concurrent operator may already have resumed an earlier job
when the command exits, and any pause (for example, a runner-declared step
pause), not specifically this command's published request, may satisfy it.
Terminal, retired, quarantined, or timed-out requests exit 1. `--timeout
SECONDS` requires `--wait`; timed-out requests remain published.

`JOB` is a job UUID, a `tag--uuid` job key, any unique prefix of either, or a
path inside the workspace. A job directory such as `jobs/silicon--UUID` names
one job; a directory such as `jobs` names every live job below it, including
nested placements; and a glob such as `jobs/silicon*` expands to matching
entries. Relative paths are resolved from the command's current working
directory, and paths must remain inside the selected workspace.

Besides the per-state claim preconditions, `job why` also folds in, where they
apply: a **runner-allowlist refusal** when a live manager's `runner_modules` or
search paths cannot reach the job's runner (so a claim would fail with
`runner_unavailable`); an **attempt-history** line — `N attempts across M
activations at step 'X'; K after unclean exits` — summarizing the journal; a
**flapping** flag when an unlimited-budget job has attempted well past a small
threshold without progressing; and any **pending** operator request still in
`requests/ready`, or the reason recorded for the most recent **retired** one.

Language documents use `job new --from-runner DOCUMENT`; see
{doc}`/workflow_languages` for PWD, CWL, jobflow, and httk-v1 details.

### `collect` — the finished jobs, as summaries

| Command | What it does | Notable options |
| --- | --- | --- |
| `collect WORKSPACE` | stream one collected summary per finished job | `--state`, `--placement`, `--degraded`, `--raw`, `--allow-job-collector`, `--into PATH`, `--id-base BASE`, `--id-series SERIES`, `--no-id-ledger`, `--id-ledger PATH` |

With `--into`, a sealed id ledger keeps entry ids stable across rebuilds. It is
on by default at `<into>.ids.json`; `--id-ledger PATH` relocates it and
`--no-id-ledger` disables it (ids then become unstable across rebuilds). See
{doc}`/stable_ids`.

`--degraded` prints only the degraded per-job lines; the trailing summary still
counts the whole sweep, so a filtered listing never hides how many jobs ran. It
cannot be combined with `--raw`.

Every form except the pure-array `--json` ends with one
`httk-workflow-collect-summary` line counting `collected`, `degraded`,
`unfulfilled_roles`, `storage_errors`, and `skipped_unreadable`. The command
exits nonzero when any job was degraded, failed to store, or was skipped for an
unreadable `job.json`; unfulfilled roles alone keep the exit at `0`. See
{doc}`/collecting` for the triage members and `--into` partial-state semantics.

### postprocess — run a curated script

| Command | What it does | Notable options |
| --- | --- | --- |
| postprocess [OPTIONS] [JOB...] | run one declared script for each selected collected job | --workspace WS, --script NAME (required), --workflow-dir PKG, --state, --placement, --output-dir DIR, --timeout, --json |

~~~console
httk workflow postprocess --workspace WS --script relaxation-report
httk workflow postprocess --workspace WS --script report --workflow-dir ./my-workflow --json
httk workflow postprocess --script relaxation-plot <job-id>
~~~

Output is written outside the job payload, under an output root that is
<workspace>/postprocess by default, the postprocess.directory workspace
setting when set, or --output-dir DIR for one invocation (a relative value
resolves against the workspace root); the per-job directory below it is
<root>/<placement>/<job_key>/<NAME>/. A sealed job can be postprocessed
because its seal covers only the payload.

An optional JOB selector (UUID, key, unique prefix, or a workspace path, the
same forms job seal and job delete accept) postprocesses exactly those
quiescent jobs and cannot be combined with --state or --placement.

With --json, each result is one JSON object in the
httk-workflow-postprocess wire format, version 2, with workspace_id, job_id,
job_key, script, and either returncode plus output_dir, or an error. Without
--json, each result is tab-separated as
job_key<TAB>script<TAB>returncode<TAB>output_dir; errors use ERROR in the
return-code field. The command exits 0 only when every selected script ran
and returned 0; any resolution error or nonzero script return exits 1.

### `list` — the workflows a name can select

| Command | What it does | Notable options |
| --- | --- | --- |
| `list` | list the workflows `job new --workflow NAME` can resolve: those registered in this process, then those installed plugins bundle | `--json` |

Each text line is `WORKFLOW_ID`, alias (or `-`), source (`registered` or `plugin
OWNER`), and summary (or `-`); `--json` reports the same as an array of objects.
A workflow reached only by explicit path — `--workflow-dir DIR` or `--from-runner
FILE` — is not registered, so it is not listed here; use `describe PATH` to
report one such workflow directly. To list the runners one workspace has
*published*, rather than the workflows a name resolves to, use `workspace
workflows`.

### `describe` — inspect a workflow without publishing it

| Command | What it does | Notable options |
| --- | --- | --- |
| `describe TARGET` | describe a registered id/alias, runner file, or package directory | `--json` |

Resolving a workflow trusts a directory package's manifest and never executes
anything. `describe` is a report, so for a directory package it additionally
runs the runner entry's `--describe` (with any surrounding attempt context
stripped) and prints a prominent `WARNING: step drift` line — and a
`manifest_step_drift` field in `--json` — when the manifest's declared `steps`
disagree with what the runner reports. The drift is reported, not gated:
`describe` still exits `0`.

Directory package authoring, manifest validation, publication, and hook trust
tiers are documented in {doc}`workflow_packages`.

Installed-plugin workflow names are included in the registered-workflow
listings — `httk workflow list`, and the `--workflow` help and unknown-workflow
hint entries, which carry `[plugin PLUGIN_NAME]` for their owner; `describe`
resolves a plugin name and reports its source as `installed-package`.

### `precheck` — readiness before an attempt

```console
httk workflow precheck --workspace WORKSPACE
httk workflow precheck --workspace WORKSPACE --json
httk workflow precheck --workspace WORKSPACE --runner-search-path PATH --runner-search-path OTHER
```

This read-only report checks `submitted`, `ready`, `waiting`, and `paused` jobs:
each declared environment entry is shown as `resolved`, `default`, or
`unresolved`, with its source and setting name, and each runner reference is
checked for availability and its pinned digest. `--placement` restricts the
scan. The authoritative environment gate remains at attempt start;
precheck is advisory and can become stale. Its `HTTK_*` environment layer is
the current process environment, which may differ on compute nodes; JSON also
carries this caveat once as `environment_variable_caveat`.
Use repeatable `--runner-search-path` options to check installed runner
references. A plain installed reference without one is reported as
`indeterminate`, not as a broken runner, and does not by itself produce exit
status `1`.

Beyond the environment and runner reference, precheck measures each pending job
against the **live managers** the workspace actually publishes:

- **claimability** — a job no live manager can claim is a problem naming the
  closest manager's unmet requirements exactly as `job why` renders them (for
  example `lacks capabilities docker`, or `does not allow runner module …`).
  Runner modules are validated against each manager's real `runner_modules`
  allowlist, not a fixed default. When no manager is live at all, one
  workspace-level `manager_notice` replaces per-job claim findings, and does not
  fail the run;
- **language engine** — a language job (the collect gate's pair,
  `workflow_realization = language` with a `workflow_language`) has each module
  that language needs checked (without importing it) and names the pip extra to
  install, for example `pip install httk-workflow[jobflow]`. Because the extras
  belong on the machine that runs the job, an absent module is only a problem
  when no live manager serves the job's executor; when one does, the check is
  `indeterminate` (the serving manager's environment may differ, verified only
  at run time) and does not fail the run;
- **required inputs** — a declared required input with a staged `destination`
  must still be a member of the payload; a relocated or removed one is a
  problem.
- **step** — a job whose next step is not one of the runner's recorded
  `runner_steps` (written into the state frame after the runner's first attempt)
  is a problem. This is frame-based only; the runner is never executed, so a job
  that has not recorded its steps yet is never faulted. The frame reflects the
  last attempt's runner, so the check is advisory: a mutated payload runner may
  implement a different set by the next attempt.

The command exits `1` for an unresolved environment, a broken runner reference,
an unclaimable job, a missing-and-unserved language engine, a missing required
input, or a step outside the runner's recorded set; the `indeterminate` cases
stay non-failing. The JSON summary carries `claim_problems`,
`language_problems`, `language_indeterminate`, `input_problems`, and
`step_problems` alongside the environment and runner counts.

### `manager` — the process that runs the jobs

| Command | What it does | Notable options |
| --- | --- | --- |
| `run` | run managers through the workspace launcher, or keep one serving with `--idle` | `--workspace`, `--workers`, `--worker-resource`, `--count`, `--pool`, `--capability`, `--placement-prefix`, `--idle`, `--idle-timeout`, `--inline`, `--launcher`, `--detach`, `--adapter-timeout`, `--log-level` |
| `manager run` | run managers through the workspace launcher, or invoke them on a remote workspace | `--workspace`, `--workers`, `--worker-resource`, `--count`, `--pool`, `--capability`, `--placement-prefix`, `--idle`, `--idle-timeout`, `--inline`, `--launcher`, `--detach`, `--join-grace-seconds`, `--lease-seconds`, `--drain-timeout`, `--gc-interval`, `--runner-search-path`, `--adapter-timeout`, `--log-level`, `--log-file`, `--json-logs` |

`manager run` follows the binding: a local workspace uses its `manager.launch`
setting (the built-in `process` launcher by default), while a remote workspace
invokes the same command on its owner. `--count N` means managers at that launch
site, and `--workers N` means workers each. `--worker-resource NAME COUNT` is
repeatable and advertises per-manager capacity to the scheduler; local
managers also use SLURM allocation variables when present. With a local
`--count N`, explicit `--worker-resource` pairs are passed to every manager
verbatim. Only auto-detected SLURM capacities are split across the N managers,
using quotient-plus-remainder distribution so their aggregate equals the
detected allocation.
Both manager commands run until idle by default; `--idle` keeps serving. The
top-level `run` takes `--capability` and `--placement-prefix` too, so the quickstart command can
claim a capability-gated job and scope its scan; without them a gated job would
stay unclaimable. Both print one startup banner and, on idle exit, one summary
line that names any jobs left not claimable by the pools, capabilities,
resources, or executors this manager serves, or left committing with an
unreadable definition. `--inline` forces one in-process manager, and `--detach`
returns after starting the managers.
The default manager log is the append-only workspace file
`.httk-workspace/managers.log`; each text line is prefixed with the manager id,
and JSON records carry `manager_id`. `--log-file` selects another destination.
The log is rotated when a manager starts or every 1000 records once the file
exceeds 16 MiB; one backup is kept, and a manager that has not yet reopened
the file keeps appending to that backup.

The workspace settings `manager.launch`, `manager.count`, `manager.workers`, and
`manager.command` control launch-site behavior: `manager.launch` selects the
built-in `process` launcher or a named launcher bundle; `manager.count` supplies
the default number of managers; `manager.workers` supplies each manager's
concurrency; and `manager.command` is used after `environment.prelude` to find
the manager on the resulting `PATH`. `--count` and `--workers` override the
corresponding settings for the invocation. `--inline` forces one in-process
manager, while `--detach` returns after launch. `--count` always counts managers
at the launch site, not workers or remote adapters.

For a resource-aware run, advertise the manager's complete allotment:

```console
httk workflow run --workers 4 \
  --worker-resource procs 32 --worker-resource mem 128000 \
  --worker-resource matlab_license_slots 2
```

Pair this with the workflow manifest's per-step resource requirements; see
{doc}`taskmanager` for the packing and dynamic-requirement example.

### `v1` — harvesting finished *httk* v1 trees

| Command | What it does | Notable options |
| --- | --- | --- |
| `v1 collect ROOT` | harvest a pre-existing v1 result tree | `--workflow-dir PKG`, `--into PATH`, `--id-base BASE`, `--id-series SERIES` |

`v1 collect` ends with one `httk-workflow-v1-collect-summary` line reporting
`finished`, `unfinished_by_status` (tasks the name regex matched that were not
`.finished`, keyed by status), and `skipped_no_rundir` (finished tasks with no
dated run directory). Each collected report carries `identity_stable`: `false`
for a task whose identity is path-derived because it has no `ht.manifest`, and a
warning names how many such tasks a harvest saw.

### `config` — the per-user machine configuration

| Command | What it does | Notable options |
| --- | --- | --- |
| `config show [KEY]` | print the configuration, or one member | |
| `config set KEY VALUE` | store one member | `machine_names` is a comma-separated list of names this machine answers to |
| `config unset KEY` | remove one member | |
| `config import-v1 [SOURCE]` | read a legacy `~/.httk` configuration | |

Operator identity is no longer a `config` member. The per-user identity — the
bare name and email and every named identity — is set up and managed by the
core-owned root commands `httk init` and `httk identity`, documented with *httk-core*.
`config import-v1` still imports the legacy name, email, and public key into
`identity.json` through *httk-core* while recording only `imported_from` here.

### `project` — the directory a campaign lives in

The project *anchor* and every project-level verb — `init`, `show`, `import-v1`,
`export`, `repair`, `manifest create | verify`, `seal`, `unseal`, and
`verify-seal` — belong to *httk-core*, which owns the whole `httk project`
command. *httk-workflow* no longer mounts project verbs of its own; instead it
registers the **workspace** as a project-*member* kind, so core's verbs delegate
a workspace's internals to it: what to leave out of a manifest, how to seal it,
its seal digest, how to verify it, and its health checks. A workspace inside a
project is recorded in `httk_project/members.json` — registered on
`httk workspace init`, unregistered on `workspace delete` / `workspace forget`,
and its path followed on `workspace move`. The registered *name* travels too:
`members.json` records each workspace's name, so a project copied to another
machine stays usable. There, `httk workspace adopt` (per workspace) or
`httk project adopt` (every member at once) registers the copied workspaces in
the new machine's per-user registry under their recorded names, joins any that
are missing from `members.json`, and records a name where one was absent —
idempotently, never touching a sealed project. Even before adopting, resolving a
workspace by a name the local registry does not know falls back to the enclosing
project's `members.json` for that one invocation (it never silently writes the
registry). Create a project with `httk project init PATH`, then give it a
workspace with `httk workspace init PATH`; seal it with `httk workspace seal`
and then `httk project seal`, and verify the whole tree with
`httk project verify-seal` (or `httk seal verify`). The project verbs themselves
are documented with *httk-core*.

### `seal` — verify a sealed tree

Writing seals lives beside each level (`job seal`, `workspace seal`, `project
seal`); the top-level `seal` group carries the one verb that belongs to no
single level: verifying a whole sealed tree. {doc}`../sealing` is the full guide.

| Command | What it does | Notable options |
| --- | --- | --- |
| `seal verify [PATH]` | verify the seal at `PATH` (a project, workspace, or job payload; default `.`) and, unless `--shallow`, every seal it references | `--json`, `--trusted-key KEY_OR_FINGERPRINT` (repeatable), `--shallow` |

Each line is `<level> <subject> <verdict> <reason>`, with indented `<kind>
<path>` discrepancy lines beneath a failing entry, then a final status line whose
word and exit code mirror `manifest verify`: `ok` / exit 0 when every entry is
`valid_trusted`, `UNTRUSTED` / exit 3 when none is invalid but a signer is
untrusted, and `FAILED` / exit 1 on any invalid entry. By default the project's
pinned keys and the local identity's public key are trusted, so a tree sealed by
its own project or identity verifies as `valid_trusted` without naming a key.

### `launcher` — the bundles that start managers

A *launcher* is to starting workflow managers what a remote is to reaching a
machine. The built-in `process` launcher starts detached local processes; named
launchers are versioned bundles, resolved project-first and then globally.

| Command | What it does | Notable options |
| --- | --- | --- |
| `launcher list` | list manager launchers visible to this project | |
| `launcher add [OPTIONS] NAME...` | create launchers from a packaged template | `--template`, `--set`, `--global`, `--non-interactive` |
| `launcher configure --set KEY=VALUE NAME...` | update launcher settings | `--set` |
| `launcher show [--json] NAME...` | describe launchers and their settings | |
| `launcher check [OPTIONS] NAME...` | check a launcher's required binaries | `--launcher-timeout` |
| `launcher remove [--force] NAME...` | remove launcher bundles | |

### `remote` — the adapters that reach other machines

Named after `git remote`: a *remote* is one machine this project can reach, and
the bundle of adapter operations that reaches it. The name `local` is reserved
for the built-in remote every workspace registry resolves as "this machine", so
`remote add local` is refused — a workspace bound to `local` must be
unambiguous.

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote list` | list the remotes this project can reach | |
| `remote add [OPTIONS] NAME...` | create remotes from a packaged adapter template | `--template`, `--global`, `--non-interactive` |
| `remote configure [OPTIONS] REMOTE...` | run adapters' `configure` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote check [OPTIONS] REMOTE...` | check that `httk` answers on remotes | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote import-v1 [OPTIONS] SOURCE...` | map legacy *httk* v1 computer bundles | `--name` (one source only), `--global` |
| `remote show [--json] NAME...` | describe remotes and their settings | |
| `remote remove [--force] NAME...` | remove remote bundles | |

`remote add --template` accepts `local` (same-machine transport) or `ssh`
(rsync plus command execution over SSH). These templates describe how to reach
a machine; they do not describe how that machine starts managers. Configure
manager launch separately in the target workspace with `manager.launch`, such
as a packaged `slurm` launcher.

`remote show` never prints a credential *value*: a remote setting stored in
the manifest-excluded `credentials.json` is reported by name only, so a
description an operator pastes into a bug report cannot carry a password.

`remote remove` refuses while an unretired transfer still depends on the
remote, because removing it would leave that transfer with no way home;
`--force` skips the interactive confirmation and **nothing else** — the refusal
stands either way. Fetch or retire the transfer first.

### `transfer` — moving jobs between two workspaces

`transfer` is one verb that takes two registered workspace names — a source and a
destination — and moves jobs between them, whichever way they point:

```console
httk workflow transfer [--job JOB_ID …] [--state STATE …] [--placement P] \
    [--destination-placement P] [--adapter-timeout SECONDS] [--json] SRC DST
```

Both names resolve through the registry, so which legs run over an adapter and
which stay in this filesystem follows entirely from where the two are bound:

| Direction | What happens | `--job` |
| --- | --- | --- |
| local → remote | each named job is detached, its sealed bundle pushed to the remote, and imported there | UUIDs, prefixes, paths, or globs; required |
| remote → local | the selected jobs are offered, pulled home, imported, and their sources retired | canonical UUIDs only; optional sweep |
| local → local | each named job is detached from the source and imported into the destination directly, in this filesystem | UUIDs, prefixes, paths, or globs; required |
| remote → remote | the client relays the selected offers through local staging and pushes them to the destination (v1; a direct source-to-destination path is deferred) | canonical UUIDs only; optional sweep |

`--state` (repeatable, default `succeeded` and `failed`) chooses which finished
kinds a sweep moves, `--placement` restricts it to one subtree,
`--destination-placement` lands the jobs somewhere other than the placement they
had, and `--adapter-timeout` bounds every adapter operation the move runs.
For a local source, `--job` accepts a UUID, tag/key prefix, job directory,
placement directory, or glob such as `jobs/silicon*`; paths and globs are
resolved from the current working directory and must be inside that source
workspace. A remote source accepts only canonical job UUIDs, because its
selectors are resolved on the remote machine.
When `--job` is supplied, each named job must be eligible before any job is
sealed; by-id moves accept any quiescent state, while an explicit `--state`
remains an additional filter. With no `--job`, the sweep remains skip-tolerant
and defaults to `succeeded` and `failed`.
`--strict-environment` blocks before state moves when a checked destination
environment is unresolved or cannot be read. Transfer checks intentionally use
job overrides, destination settings, and declared defaults; they do not use the
client process environment as a destination substitute. A remote settings read
that is unavailable produces one immediate warning in non-strict mode.

For remote → remote, repeated `--job` values constrain the source offer before
the relay pulls anything; omitting them keeps the skip-tolerant terminal-state
sweep.

Bundles carry sources only for workflows that declare `[workflow.build]`;
compiled artifacts are machine-local and are never transferred. After importing
such a bundle, run `httk workflow build --workspace WORKSPACE TARGET` on the destination
before starting its managers; the import operation repeats this reminder.

### The protocol spellings, and what is gone

`transfer` also carries the frozen argument vectors one machine runs on another
over an adapter. Operator-facing vectors use workspace names; the hidden
`--by-path` spelling is the path-only protocol form used after the client probes
the owning machine. They are protocol rather than operator interface — a local→remote move invokes
`receive` on the destination, a fetch invokes `offer` then `retire` on the source.
Their spelling is frozen, because the machine that answers may run an *httk* older
or newer than yours:

```text
httk workflow transfer receive --workspace PATH --bundle BUNDLE
httk workflow transfer offer --destination-workspace-id UUID [--job JOB_ID …] --json PATH
httk workflow transfer retire --destination-workspace-id UUID --json PATH JOB_ID …
httk job request-envelopes ACTION --workspace WORKSPACE --operator=LABEL --reason=TEXT [--priority N] [--step S] [--force] --json JOB_ID …
httk job publish-requests --workspace WORKSPACE --document JSON [--document JSON …] [--wait] [--timeout S] [--durable|--no-durable]
httk workspace status --by-path --json PATH
httk workflow manager run --by-path --workspace PATH
```

`receive` is an import half rather than an operator command, so it is not
advertised in `--help`, but it is its frozen, invocable spelling. The `--by-path`
switch is likewise hidden: it makes the workspace argument a literal path with no
registry lookup, which is exactly what one machine needs when it addresses another
machine's workspace.

The pre-release `transfer send` and `transfer fetch` and
`transfer status` verbs are **gone** — they no longer parse. Move to the single
`transfer SRC DST` verb, and to `manager run --workspace NAME` for starting managers (below)
and `workspace status NAME` for reading a remote workspace's markers.

| Removed | Now |
| --- | --- |
| `transfer send REMOTE JOB …` | `transfer --job JOB … LOCAL REMOTE` |
| `transfer fetch --remote REMOTE --workspace LOCAL` | `transfer REMOTE LOCAL` |
| `transfer status REMOTE` | `workspace status REMOTE` |

An earlier release also renamed two whole groups: `httk workflow computer …`
became `httk workflow remote …` (git's word for the same idea), and
`httk workflow tasks …` (once `httk workflow remote send|fetch|…`) became today's
`httk workflow transfer`. A job whose `runner.path` pins the old
`pkg:httk.workflow.runners/vasp_*` form breaks too: the packaged VASP runners are
now modules of `httk.workflow.vasp.runners`, and a job pinning the old path fails
with `runner_unavailable` naming the module it could not resolve — scaffold the
job again, or edit the one `runner.path` member.

### `campaign` — partitioning a large run across many workspaces

| Command | What it does | Notable options |
| --- | --- | --- |
| `campaign init` | define the project's partition map and assignment policy | `--partition NAME=WORKSPACE`, `--assignment` |
| `campaign show` | show the partition map | `--json` |
| `campaign submit` | assign one root job to a partition and submit it there | `--workflow` (required), `--key` (required), `--index`, `--input`, `--input-from`, `--parameter`, `--file`, `--tag`, `--placement`, `--priority`, `--name`, `--json` |
| `campaign collect` | collect every partition, one workspace after another | `--partition`, `--state`, `--placement`, `--raw`, `--allow-job-collector`, `--into PATH`, `--id-base BASE`, `--id-series SERIES` |
| `campaign start-<wbr>managers` | start a manager per selected partition | `--partition`, `--workers`, `--worker-resource`, `--count`, `--launcher`, `--idle-timeout`, `--adapter-timeout` |

A campaign is a thin convention over the *registered workspaces* above: a
partition map, stored in the project, that spreads a very large body of work
across many workspaces without a new scheduler. Each partition names one
registered workspace, roots are assigned to partitions by policy, and spawned
children always inherit their parent's workspace. See {doc}`/campaigns`.
`campaign submit --workflow` accepts a workflow name or alias only; use
`job new --from-runner` or `job new --from-command` for a file or command.

## Creating jobs

`job new` scaffolds and submits jobs from a registered or packaged workflow name,
a runner file, a package directory, or a bare language document — and needs no
prepared payload:

```console
httk job new --workspace WORKSPACE --workflow vasp-relax --input structure=POSCAR --tag silicon
httk job new --workspace WORKSPACE --workflow vasp-relax --input-from structure structures/ --parameter kpoint_density=30.0 --placement project/screening
httk job new --from-runner ./my_runner.py --step characterize --parameter sites=8
httk job new --from-command 'srun --ntasks=10 my_executable {input}' --file input=input_files/a.dat --tag a
```

`--parameter NAME=VALUE` supplies an opaque implementation knob;
`--environment NAME=VALUE` overrides one declared workflow environment entry;
and `--format LANG` selects the language of a bare document.
With `--from-command TEMPLATE`, `shlex`-style words are turned into a published
one-step Bash runner; each `{name}` placeholder must have a matching
`--parameter NAME=VALUE` or `--file NAME=PATH`. Parameter placeholders resolve
through `httk_workflow_parameter` at run time, while file placeholders resolve
to the staged file below `HTTK_WORKFLOW_JOB_DIR`. Each `--file` input is also
copied into the job workdir under its basename before the command runs, so it
can be read there by name as well; existing workdir entries are never replaced.
`--files DIR` expands every regular file directly inside DIR, sorted by
basename, into the same staging and placeholder behavior as `--file` for every
job-creation form (`--workflow`, `--workflow-dir`, `--from-runner`, and
`--from-command`). Symlinks to regular files are followed and staged;
subdirectories, symlinks to directories, broken symlinks, and other non-file
entries are skipped with one stderr warning naming up to five entries (plus
`…`); a DIR with no regular files errors with `no regular files in DIR`. For
example:

```console
httk job new --from-command 'srun --ntasks=10 --cpus-per-task=1 vasp_std' --files inputs/si --tag si
```

The copy in the payload remains the immutable original, and `{name}` still
resolves to its absolute staged path. The generated runner can be edited and
passed back with `--from-runner`. The template is an argv-only word list: it has no shell syntax. A placeholder name must match
`[A-Za-z_][A-Za-z0-9_.-]*`; `{{` and `}}` emit literal braces, and any other
brace text remains literal.
Runner identity is the digest of the rendered text, including the deterministic
sorted workdir staging lines, so staged file names are part of the wrapper
identity even when they are unused. A file name containing `/` is a valid
staging destination but must be staged under a bare name to use it as a
placeholder.
When a runner is supplied with `--from-runner`, `job new` runs it in describe
mode to infer its workflow and initial step; it must call
`httk_workflow_runner WORKFLOW STEP...` before any work.
`--input-from
NAME SOURCE...` loads a file or the readable files in a directory, realizes the
declared payload destination, and creates one job per file for a batch. A
directory file with no registered reader whose name is a structure convention
(`POSCAR*`, `*.vasp`) is read as POSCAR; any remaining unreadable files are
skipped, and one stderr line names them:
`httk workflow: skipped N of M files in DIR (no registered reader): …`. After a
batch, one final stderr line reports `submitted N jobs`; if a batch fails partway
it instead reports `submitted N of M jobs before failing` and exits `2`. In a
batch, `--tag` becomes a *prefix* combined with each item's derived tag
(`run7-si2o`) rather than replacing it; for a single job `--tag` is the whole
tag. `--file
NAME=PATH` stages anything else, `--input NAME=PATH` stages one declared input,
and the command prints one
tab-separated `job_key<TAB>payload` line per job, or `--json` reports. Any
preparation warning a language raises (for example a CWL `DockerRequirement`) is
printed once as `httk workflow: warning: …` on stderr. The runner
file is published into the workspace runner store and pinned by digest unless
`--publish installed` names a packaged runner where it is installed. See
{doc}`/quickstart`.

## Running language documents

Run a PWD, CWL, or jobflow document directly with `job new --from-runner DOCUMENT`;
the document is resolved as a language realization:

```console
httk job new --workspace WS --from-runner flow.cwl --input message=echo
httk job new --workspace WS --from-runner workflow.json --parameter pwd_module_path='["."]'
httk job new --workspace WS --from-runner maker.json
```

The same `--format` option accepts `cwl`, `pwd`, `jobflow`, and `httk-v1` for
bare document inputs. Manifest packages and registered ids reject the option
because their language is already known.

See {doc}`/workflow_languages` for package manifests, bare-document rules,
the supported CWL subset, PWD security, jobflow Makers, and language collection.

Harvest old v1 results without submitting them:

```console
httk workflow v1 collect --workflow-dir PKG ROOT
httk workflow v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

## Inspecting and debugging jobs

`job list`, `job show`, `job log`, and `job why` read one workspace without
writing anything, and `job debug` drives a single job to a terminal state in the
foreground:

```console
httk job list --workspace WORKSPACE --kind ready
httk job show --workspace WORKSPACE JOB
httk job log --workspace WORKSPACE --limit 20 JOB
httk job why --workspace WORKSPACE JOB
httk job debug --workspace WORKSPACE --follow-children PAYLOAD_OR_JOB
```

`JOB` is a job UUID, a `tag--uuid` job key, any unique prefix of either, or a
path inside the workspace (for example `jobs` or `jobs/silicon*`), and
each command takes `--json`. `job debug` exits `0` on success, `3` on failure, and
`4` when the job stopped without finishing. See
{doc}`taskmanager` for what each command reports.

`job list --json` returns `format`, `format_version`, `jobs`, and `next_after`.
Each row has the existing `job_key`, `job_id`, `state`, `step`, `placement`,
`priority`, `generation`, and `reason` members. Use `--limit N` and pass the
returned cursor to `--after` to page a large workspace; `--tag-contains TEXT`
filters on the tag portion of the job key. `--counts` adds a `counts` object
with name-based marker totals by selected state kind; malformed
marker-shaped names are included, and `fsck` reports them. Counts require a
full directory walk and are not free. The human table without `--limit`
materializes all rows. The cursor format is
`<kind>:<placement>/<job_key>` and `next_after` is null when the page is
exhausted. Paging is weakly consistent during concurrent transitions: a job
can appear twice or be missed if it changes kind between pages, so clients
should deduplicate by `job_id`. Remote `job show`, `job log`, and `job why`
accept canonical lowercase job UUIDs only; keys and prefixes must be resolved
locally first.

`httk workflow collect --workspace WORKSPACE` streams one `CollectedJob` summary per finished
job as JSON lines by default. Use `--raw` to stream `JobRecord` records for a
data layer; see {doc}`/collecting`.

## Configuration and projects

User configuration follows the XDG base-directory convention, and everything
per-user this package keeps is *configuration*:

- `$XDG_CONFIG_HOME/httk/config.json` (machine-level settings such as `machine_names`);
- operator identity in `$XDG_CONFIG_HOME/httk/identity.json`, managed by *httk-core*;
- identity keys in `$XDG_CONFIG_HOME/httk/keys/`;
- global remote definitions in `$XDG_CONFIG_HOME/httk/remotes/`.

`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` can provide explicit deployment or test
overrides. Legacy `~/.httk` data is read only through `config import-v1`; its
64-byte private material is not converted.

```console
httk init --name "A User" --email user@example.org
httk workflow config set machine_names "node-a,node-b"
httk workflow config unset machine_names
httk project init --name example .
```

The project *anchor* is owned by *httk-core*, which provides the umbrella
`httk project` command and its `init`, `show`, and `import-v1` leaves. Create the
anchor with `httk project init PATH` and give it a workspace with
`httk workspace init PATH`. The workflow-aware verbs — `repair`, `manifest`,
`seal`, and `unseal` — are mounted by *httk-workflow* onto the core
`httk project` command.

`config set` accepts only the keys the configuration actually has — `machine_names`
is the sole settable one — and names them when it refuses another, so a typo cannot
become a member that nothing ever reads. Operator name, email, and named identities
are not configuration keys: they live in `identity.json` and are managed by the
core-owned `httk identity` commands and `httk init`. `format` and `format_version` describe
the document and are written by *httk* itself. A configuration whose `format` or
`format_version` is missing or something else is refused rather than read as if
its members meant what *httk* means by them.

A project has `httk_project/project.json` and a standard 32-byte Ed25519 seed
stored with mode `0600`. Its default workspace for workflows is recorded by name and
may live outside the project; commands discover the nearest project in the
working directory's parent chain.

### Describing and checking a project

```console
httk project show
httk project show --json
httk project repair --dry-run
httk project repair .
```

`httk project show` (core) reports the project's metadata and keys. `project
repair` applies the safe repairs by default and adopts every member workspace on
this machine — the exact one-shot a freshly copied tree needs; `--dry-run`
reports without touching anything, and `--no-adopt` repairs without adopting.
`httk workspace status` and `httk project manifest verify` report the live
workspace and manifest state directly.

`project repair` covers the conditions that quietly break a project later — a
stale maintenance lock, staging leftovers, a workspace on disk missing from the
registry, and a member not yet adopted on this machine — reporting or fixing
each.
`--repair` fixes the ones that can be fixed automatically, says exactly what it
did, and journals it in the project's workspace, so the repair is part of that
workspace's durable history. The command exits `1` only when a check is actually
*broken*; a warning, such as a project that has no manifest yet, is something to
know about rather than something to fail a script on.

## Signed manifests

```console
httk project manifest create .
httk project manifest verify
httk project manifest verify --trusted-key keys/collaborator.pub
```

The *httk₂* manifest is deterministic canonical JSON-lines compressed with bzip2.
It records sorted POSIX paths, regular-file sizes and SHA-256 hashes, empty
directories, and symlink targets. Special files are rejected. A
domain-separated body digest is signed with Ed25519. Creation fences manager
launches only when a workspace is co-located with the project, and refuses
active work there. A detached project needs no workspace to create its
manifest. Verification also recognizes the legacy
`ht.project/manifest.bz2` format without changing it.
When a directory contains a regular `job.json` that parses as an
`httk-workflow-job` whose UUID matches the directory's valid job key, it is a
job payload and its direct `attempts`, `logs`, and `.httk-job` children are
skipped during both manifest creation and verification; same-named directories
elsewhere are ordinary project content.

### What a verified manifest actually proves

Be precise about the threat this addresses, because the signing key lives in the
tree it signs. `httk_project/keys/project.seed` is a file of the project, mode
`0600` and excluded from the manifest, but excluded is not absent: **anybody who
can write the project directory can re-sign it**. A manifest therefore proves
that the tree is exactly the tree somebody with the seed described — it does not
prove that nobody changed the tree, and it is not a tamper seal against an
attacker who had write access.

What it does prove is worth having. Against accidental damage — a truncated
copy, a partial `rsync`, bit rot on an archive volume, a stray edit in a
directory nobody meant to touch — the digests are exact. Across a copy that
travelled without the seed, and against a *replaced* tree signed by a different
key, the signature is the check that catches it. That is why verification
compares the signing key with a **trust anchor that did not come from the
manifest**: the key pinned in `project.json` at `httk project init`,
plus any key named with `--trusted-key`. Reading the key out of the manifest
header and checking the manifest against itself would always say *valid*.

Verification is therefore three-way, not a boolean:

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
existed has no `public_key` in `project.json`, so every manifest of it verifies
as `valid_unknown_key` until somebody decides which key to trust. That decision
is explicit, because it is the whole trust model in one act:

```python
from httk.workflow.projects import pin_project_key, trust_project_key

pin_project_key("/path/to/project")  # adopt keys/project.pub
trust_project_key("/path/to/project", "ed25519:…")  # adopt somebody else's key
```

`pin_project_key` adopts the key that is in the tree *right now* — do it only on
a tree you have reason to believe is the one you left. `trust_project_key` adds
a further anchor to `project.json`'s `trusted_keys`; `httk project import-v1` fills
that list with the legacy identities of an imported *httk* v1 project, so its
old `ht.project/manifest.bz2` verifies as trusted too. `--trusted-key` accepts
either an `ed25519:BASE64` value or the path of a `*.pub` file and is the
one-off equivalent that writes nothing.

For attribution *between* machines — who published this request, who imported
this transfer — see the operator identity key below, which is a different key
with a different job.

### Operator identity

Operator identity — the recorded name, email, and default, plus every named
identity — lives in `$XDG_CONFIG_HOME/httk/identity.json`, managed by *httk-core*.
The core-owned root commands drive it: `httk init`
records the bare `name`/`email` and creates the default `identity.seed`/`identity.pub`
pair below `$XDG_CONFIG_HOME/httk/keys/`. Named identities are managed with
`httk identity add --name NAME --email EMAIL SHORT`; each gets
its own `identity-SHORT.seed`/`.pub` pair. The first named identity becomes the
default, and `httk identity default SHORT` changes it. Removing an identity
leaves its key files on disk; removing the default with exactly one identity
remaining selects that identity automatically, while removal with multiple
remaining identities requires selecting another default first.

The default signing identity resolves in this order: the `default_identity`
recorded in `identity.json`, the only configured identity when there is exactly
one, then the bare top-level `name`/`email` and `identity.seed`. A selector containing `<` is a literal
`Name <email>` attribution label (the name may be empty) and uses the resolved
default identity's key; other selectors must be configured short names.

`job request` records the selected identity's `Name <email>` label and signs
the request with that identity's key. Omitting `--operator` selects the
configured default; a short name selects that identity, while a literal label
is passed through and signed with the default identity's key.

For remote requests, the control-center identity builds the signature locally
after the far side returns unsigned envelopes. The remote publishes those
signed documents verbatim, so attribution and signer are the same identity.
If a configured identity's key file is missing or unreadable, the request fails
loudly; remove and re-add it with `httk identity remove SHORT`
then `httk identity add SHORT ...`, or restore the key file.
The two request protocol spellings are additive; when a remote cannot parse
them, upgrade `httk-workflow` on the remote.

The selected key signs operator requests (`httk job request …`).
Transfer acknowledgements always use the DEFAULT identity resolution; they do
not carry a per-request identity selection. Signatures are detached, cover the
canonical JSON of the whole document, and are domain-separated from every
other httk signature.

Document signing remains optional for lower-level callers, and a manager or a
transfer source accepts unsigned documents exactly as before. A signature that
*is* present must verify: a request with a broken signature is quarantined with
the reason, and an acknowledgement with a broken signature will not retire a
sealed bundle. A verified request records its `operator_key` in the journalled
state frame beside the operator name and reason.

The semantics are attribution, not authorization. The key says *which identity
published this document*; it grants nothing, and no operation is permitted
because a document is signed. Anyone who can write the workspace's request
directory can still publish an unsigned request.

The fence is `.httk-workspace/maintenance.lock`, holding the recording process
identifier, hostname, and creation time. A lock whose same-host process is gone,
whose content is unreadable, or that is older than twenty-four hours is
reclaimed automatically; any other lock is reported with its holder. Operators
can also clear one explicitly:

```console
httk workspace unlock WORKSPACE
httk workspace unlock --force WORKSPACE
```

Without `--force` only a stale lock is removed.

## Application settings

A workspace also carries *application settings*: a flat, dotted-name map of small
values a runner resolves when it runs — the VASP command, a pseudopotential
library — distinct from the engine `policy` above, which tunes scheduling. They
are stored in the workspace and edited by name:

```console
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" my-workspace
httk workspace settings show my-workspace
httk workspace settings unset --key vasp.command my-workspace
```

A value that parses as JSON is stored as that scalar; a bare word is stored as a
string. Settings can also be seeded at creation: `workspace init --setting
KEY=VALUE` sets them explicitly, and a workspace bound to a remote is additionally
seeded from that remote definition's whitelisted remote settings, so a cluster's
`vasp_command` becomes the new workspace's `vasp.command` without anyone restating
it.

A runner reads a setting through `a.setting("vasp.command")`, and the value is
resolved in layers, most specific first: the job's own parameters, a real
`HTTK_VASP_COMMAND` deployment override, the workspace setting, then the runner's
default. The manager exports scalar workspace settings into each attempt
environment (`vasp.command` becomes `HTTK_VASP_COMMAND`) and snapshots them into
the `HTTK_WORKFLOW_CONTEXT` JSON value, so a runner sees the values the workspace held when its job was
claimed. See {doc}`/vasp_runners` and {doc}`/sdks/sdk_parity`.
Workspace settings are non-secret configuration: they are snapshotted into the
attempt context and exported into the runner environment, so credentials must
not be stored there; remote credentials already live elsewhere.

### Workflow preludes

Two layers of shell setup run before a job's runner, both sourced under `set -e`
so a failing line aborts the job rather than running the calculation in a broken
environment:

- **`environment.prelude`** — the workspace-wide layer, one shell fragment that
  applies to every job. It is an ordinary application setting: `workspace
  settings set --key environment.prelude --value "…" NAME`.
- **`workflow-prelude`** — the per-workflow layer, keyed by workflow id (the
  `[workflow].id` of the manifest, `=` the job's `workflow`). It applies only to
  jobs of that workflow and runs *after* the workspace-wide prelude:

  ```console
  httk workspace workflow-prelude set --workflow relax-vasp --value "module load VASP/6.2.1" my-workspace
  httk workspace workflow-prelude set --workflow relax-vasp --value @prelude.sh my-workspace
  httk workspace workflow-prelude show my-workspace
  httk workspace workflow-prelude unset --workflow relax-vasp my-workspace
  ```

  `VALUE` is stored verbatim (never JSON-parsed); `@FILE` reads the shell text
  from a file, for a multi-line module-load script kept on disk. Without
  `--json`, `show` is line-oriented (`WORKFLOW⇥text`), so a multi-line prelude's
  continuation lines carry no id prefix — machine consumers should use `--json`.

See {doc}`/taskmanager` for how each layer is delivered by the workspace's
launcher, and why preludes stay behind when a job is transferred.

## Workspace policy and integrity

The tunables a workspace shares with every process attaching it — the
visibility deadline, the default lease, the journal segment size, and the
retention limits — are stored in `format.json` and edited in place:

```console
httk workspace policy show WORKSPACE
httk workspace policy show --json WORKSPACE
httk workspace policy set --key visibility_deadline_seconds --value 60 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
```

`workspace fsck` verifies that every state marker still resolves to a readable
journal frame that agrees with it, and can re-point damaged markers at the last
good frame of their job:

```console
httk workspace fsck WORKSPACE
httk workspace fsck --repair --json WORKSPACE
httk workspace fsck --repair --quarantine-unrepairable WORKSPACE
```

It exits `1` while anything remains for an operator to deal with. See
[the task-manager guide](taskmanager.md) for what each problem code means and
for exactly what a repair will and will not touch. When always-safe leftovers
exist, it also prints their total and per-category counts; this informational
line never changes the exit status.

## Freeing disk

A committing manager removes an attempt's control directory after its durable
commit and after it has reaped the local process when the actual destination is
`ready`, `waiting`, `paused`, or `succeeded`; failed and cancelled attempts stay
as evidence. Transaction trash is normally removed with that control tree. An
inherited commit is left for `workspace gc`. Managers run the always-safe
categories at startup and the full policy-gated collection at clean exit;
artefacts orphaned by a crash still require explicit `workspace gc`, driven by
the workspace's `policy.retention`:

```console
httk workspace gc --dry-run WORKSPACE
httk workspace gc WORKSPACE
httk workspace gc --json WORKSPACE
```

It prints one row per category with the candidates it found, what it removed,
and an estimate of the bytes reclaimed; `--json` lists every individual entry
as well. `--dry-run` touches nothing at all and reports what a real run would
remove. A run that removed anything also appends one `httk-workflow-gc` frame
to the journal summarizing the same counts, so the collection is itself part of
the workspace's durable history.

A `null` or `"keep"` retention member means *keep*. On a fresh workspace,
`journal_days` and `trash_days` default to one day while
`attempt_control_days` stays unlimited. The command always prunes what cannot
carry information, plus removable markers that are quiescent and unowned by any
manager (`succeeded`, `failed`, `cancelled`, `submitted`, and `ready`) whose
payloads the operator removed;
configure the limits to change the policy-gated categories:

```console
httk workspace policy set --key retention.attempt_control_days --value 14 WORKSPACE
httk workspace policy set --key retention.trash_days --value 14 WORKSPACE
httk workspace policy set --key retention.journal_days --value 90 WORKSPACE
httk workspace policy set --key retention.journal_days --value null WORKSPACE  # keep forever
```

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

To remove a finished (`succeeded`, `failed`, or `cancelled`), `submitted`, or
`ready` job cleanly, use `httk job delete JOB...`. It removes both the payload
directory and state marker, confirms on a terminal, and protects join children
referenced by non-terminal parents unless `--force` is given; `--force` skips
both confirmation and the join-parent guard. The manual `rm -r` plus GC or
manager cleanup remains fine for finished jobs. For queued `ready` or
`submitted` jobs, prefer `job delete`, because removing the directory first
can race a manager claiming the job at that instant. A job in any other state
must be cancelled first with `job request cancel`.

Containment checks are static: a concurrent replacement of a placement
ancestor by the workspace's own owner is outside the threat model, as it is
for `attempts/` and `logs/`. The hidden `--confirmed` option is a protocol flag
like `--by-path`, not part of the user interface.

The collector never touches the quarantine, a sealed transfer bundle, a
persistent workdir, a payload beyond its aged attempt-control directories, a
`claimed`, `running`, `committing`, `cancelling`, `waiting`, or `paused` marker,
a segment in a frame chain protected by a current
non-terminal marker, a manager that is still heartbeating, or the runner store.
Removal is bottom-up and rewrites no state, so a collection killed halfway
leaves the workspace exactly as consistent as it was, and running it again
simply finishes the job.

Collecting journal segments has one honest cost. Every segment in a current
non-terminal marker's frame chain is protected, while a terminal marker protects
only its current segment. The deep history of a terminal job therefore goes
with aged segments behind it; `collect` and `job log` report that timeline with
`gaps` set, while a non-terminal job's reachable chain remains intact.

## Remote adapters

Remote definitions are versioned directories containing `remote.json` and one
executable `adapter`. The operation name travels in each versioned JSON request;
the dispatcher prints one JSON result and sends diagnostics to stderr. Commands
and remote commands are always argument arrays. The maintained templates implement that
protocol through {py:mod}`httk.workflow.adapter_protocol`, which is the public
name of the packaged implementation. {doc}`adapter_authoring` is the reference
for writing one of your own: the bundle layout, the exact request and result
document of the six operations (`configure`, `install`, `invoke`, `push`, `pull`,
and `status`), and the rules for a custom adapter.

Maintained `local` and `ssh` templates are packaged with
the module. Project definitions shadow global definitions. `REMOTE:NAME` names
a workspace on a remote. `remote import-v1` maps recognized legacy *httk* v1 computer bundles
by reading assignment-only configuration; legacy shell executables are never
copied or run. Any other `kind` in a `remote.json` is refused rather than
executed in the wrong place.

`remote import-v1` selects `ssh` when the legacy configuration contains
`REMOTE_HOST`, otherwise `local`. It maps the first legacy submission profile
into `legacy_settings`; if several `config.*` profiles exist, the others are
skipped with a warning because submission profiles are now workspace settings.
Legacy `SLURM_*` values are not remote settings: add a `slurm` launcher and set
the target workspace's `manager.launch` instead. The legacy executables are
never copied or executed. Review the imported `legacy_settings` and initialize
the workspace path explicitly before transferring jobs.

`remote configure --set KEY=VALUE` persists only the machine-level keys
`check_connectivity`, `host`, `httk_command`, `legacy_settings`,
`port`, `username`, `vasp_command`, and `vasp_pseudo_library`
in the shareable `remote.json`. Scheduler profile values are workspace
settings: use `slurm.account`, `slurm.partition`, `slurm.time_limit`,
`slurm.nodes`, `slurm.cpus_per_task`, `slurm.ntasks`,
`slurm.ntasks_per_node`, `slurm.mem`, `slurm.gres`, `slurm.reservation`, and
`manager.workers`; those scheduler names are refused as remote settings. Other
non-persistable keys are stored in
the remote's `credentials.json` with mode `0600` beside it, which project
manifests exclude. Adapters receive both together as the request's
`remote_settings`. `remote show NAME` reports which
file each setting came from, and the name — never the value — of every
credential.

### What each kind does

`local` copies files in this filesystem and runs commands as child processes.
Manager launch policy is selected by each workspace's `manager.launch` setting.

`ssh` moves files with `rsync` over `ssh` and runs every command on the
configured host. Only `ssh` and `rsync` are required locally. Both kinds
implement the same six operations:

| Operation | `local` behaviour | `ssh` behaviour |
| --- | --- | --- |
| `configure` | validates pending machine settings | verifies the host answers with a cheap remote `true` |
| `install` (the `remote check` verb) | checks that local `httk` answers | checks that `httk` answers on the far side and reports its version |
| `invoke` | runs the argument vector as a child process | runs it on the configured host and returns status, stdout, and stderr |
| `push` / `pull` | copies the requested tree or relative file batch locally | transfers it with `rsync --archive` over SSH |
| `status` | runs the workspace status command locally | runs `httk workspace status --json NAME` remotely |

`httk_command` overrides how `httk` is spelled on the far side, for example
`httk_command="/proj/venv/bin/httk"`; without it the plain `httk` on the remote
`PATH` is used, and locally a `python3 -m httk.core.cli` fallback applies.

### Quoting

Every subprocess an adapter starts is an argument vector, so no shell ever
parses a value that came from a request or from settings. `ssh` is the one
exception in the protocol, because it always joins its command words and lets a
login shell on the far side parse the result. All remote command strings are
therefore built by a single helper that quotes element-wise. Manager launcher
bundles own any scheduler-script quoting separately. `rsync` transfers pass
`--protect-args` so that even file names travel in the protocol rather than
through the remote shell.

### httk on the target: `remote check`

httk is never installed on a remote for you: setting up software on an HPC
account is yours to do, because every cluster does it differently (modules,
venvs, conda, pipx, ...). The contract is simply that the connection the
adapter opens — a *non-interactive* shell — can run `httk`, with the
*httk-workflow* package installed beside the core.

`remote check` verifies exactly that, and running it once after configuring a
new remote is recommended: it confirms the host answers, that `httk` is found
(also trying `python3 -m httk.core.cli`), that the workflow command group
exists, and reports the command and version it found. `--version` alone would
only prove httk-core.

When the check fails, log in on the remote and make sure *httk₂* is set up and
available there — for example with `pipx install httk-workflow` — and note
that it must be reachable from a *non-interactive* shell: a `module load` or
conda activation guarded by an interactivity test in `.bashrc` works when you
log in but not over the adapter's connection. If `httk` deliberately lives
elsewhere (a project venv, a wrapper script), point the remote at it with
`remote configure --set httk_command="/proj/venv/bin/httk" REMOTE` instead.

In the adapter protocol this operation keeps its historical spelling
`install`; the earlier `bootstrap=pip` opt-in that attempted a
`pip install --user` is retired.

## Detached transfers

A transfer fences an explicit quiescent marker, seals it in the payload,
validates the payload digest at import, publishes the preserved UUID and prior
state only at the destination, and retires the source only after an
idempotent acknowledgement. Transfer UUID and digest checks suppress retries;
sealed and retired bundles are retained for recovery. Repeating the same
`transfer SRC DST` resumes the matching sealed transfer, including the
copy-before-import and lost-acknowledgement boundaries.

The sealed payload digest pins every path, every file's content *and executable
bit*, and the literal target of every symlink, so a runner that arrives without
its executable bit, or a link retargeted in transit, is a detected mismatch
rather than a silent corruption. A symlink is carried as its target string and
must stay inside the payload: an absolute target, or a relative one climbing out
with `..`, is refused by name, because it would mean something else at the
destination.

## Running on a remote and fetching the results

Add and configure the machine, make sure *httk-workflow* is installed there
(log in and set it up, e.g. `pipx install httk-workflow`), verify with
`remote check`, create its workspace, then send and run a job:

```console
httk workflow remote add --template ssh kappa
httk workflow remote configure \
    --set host=kappa.example.org --set username=rar \
    --set check_connectivity=yes kappa
httk workflow remote check kappa
httk workspace init kappa:/scratch/rar/httk/runs
httk workspace settings set --key slurm.partition --value batch kappa:runs
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" kappa:runs
httk job new --workflow vasp-relax --input structure=POSCAR --tag silicon
httk workflow transfer --job JOB-ID default kappa:runs
httk workflow run --workspace kappa:runs --workers 8
httk workspace status kappa:runs
```

The machine that owns a workspace chooses its path; scheduler settings belong
to the workspace instead. Remote init sends the path and registers its basename
on the owning machine.

`transfer default kappa:runs` detaches each selected job from the local default
workspace and imports it on the remote, at the placement it had here unless
`--destination-placement` puts it elsewhere. `run --workspace kappa:runs` submits the
manager invocation via the remote adapter; the remote invokes
`httk workflow manager run --workspace runs --detach` on the owning machine.
That command uses the workspace's `manager.launch`, just as a command run on the
cluster or through a `machine_names` alias would. `--count` means managers at
that launch site, and `--workers` fixes the worker count of each manager.
`manager run` is the advanced spelling for the same operation.

Before a transfer moves state, it checks each job's declared environment against
the destination workspace settings. Unresolved default-less entries produce a
warning; `--strict-environment` blocks the transfer before detaching. Remote
settings are read through the adapter when reachable. If that read cannot be
completed, one warning says the environment could not be prechecked remotely;
strict mode treats that as a block. The client process environment is not used
as a substitute for destination settings.

To bring stopped jobs home, use the reverse transfer and then collect:

```console
httk workflow transfer --state succeeded --state failed --placement project/screening --json \
    kappa:runs default
```

`--state` accepts the kinds a stopped job can be in and defaults to `succeeded`
and `failed`; `--placement` restricts the fetch to one subtree; `--adapter-timeout`
bounds every adapter operation the fetch runs. With `--job`, any quiescent state
is eligible unless an explicit `--state` filters it. A fetched job arrives as an
ordinary job of the local default workspace, in its offered state and at the
placement it had on the remote, so `httk workflow collect` then reports terminal
results exactly like jobs that ran at home.

Under the fetch leg run the two far-side protocol commands, invoked over the
adapter but usable on their own on the remote itself. They use literal paths
because they bypass the owning machine's registry:

```console
httk workflow transfer offer --destination-workspace-id UUID [--job JOB_ID …] --json PATH
httk workflow transfer retire --destination-workspace-id UUID PATH JOB_ID ...
```

`offer` detaches every selected job into its sealed bundle and prints one entry
per bundle; it requires `--destination-workspace-id`, because a bundle is sealed
for exactly one destination. `--job` is repeatable, accepts any quiescent state
when no `--state` is supplied, and fails all-or-nothing if an id is missing or
filtered. `retire` moves the sealed source of an already
imported job under `.httk-workspace/transfers/retired/` — a rename, never a
delete, so a source is only ever whole or moved whole; its
`--destination-workspace-id` is optional and, when given, refuses a bundle that
was sealed for somebody else. `offer` narrows what it seals with the same
`--state` and `--placement` `fetch` passes through; both print their report as
JSON with `--json` and as tab-separated lines otherwise. A client that sends
`--job` requires a new far side: an older remote rejects the additive flag with
its argparse error, which the client relays.

Every step is idempotent and the whole pipeline is resumable: `offer` reports an
already sealed bundle from its ledger instead of sealing it again, a `pull` onto
a matching staged bundle is a no-op, `import` returns the acknowledgement it
already wrote, and a retired source is never offered again. An interrupted fetch
is finished by running the same command again, and a fetch that has nothing to
collect does nothing.

Because `fetch` reads these two commands' answers back over the adapter's
`invoke`, their standard output has to be nothing but the JSON document: a login
banner or a profile's greeting printed on the far side's stdout makes the fetch
stop with *remote offer did not return a transfer offer document* before
anything is pulled or imported. Put such greetings on stderr, or behind a
non-interactive-shell test, on any host a remote adapter reaches.
