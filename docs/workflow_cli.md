# Project and workflow command line

*For operators and campaign owners: the whole command tree, including projects, configuration, signed manifests, computers, and remote work.*

Installing *httk-workflow* registers the lazy `workflow` command with
*httk-core*:

```console
httk workflow --help
```

## Three executables, one tree

**`httk workflow …` is the canonical spelling of every command in this
package.** It is one nested command tree: each group answers `--help`, each
command answers `--help`, and a mistyped action is reported by the group it was
mistyped in.

Two further executables are installed, and both are thin aliases that reuse the
canonical tree's own parsers and handlers rather than a second implementation
of it. They remain supported; prefer the canonical spelling in new work and in
anything you write down.

| Executable | Alias of | Kept for |
| --- | --- | --- |
| `httk workflow` | — | **canonical** |
| `httk-taskmanager` | `httk workflow workspace`/`job`/`manager` leaves | operators and scripts predating `httk workflow` |
| `httk-v1-taskmanager` | `httk workflow v1` | *httk* v1 compatibility operators |

```text
httk-taskmanager init     ->  httk workflow workspace init
httk-taskmanager submit   ->  httk workflow job submit
httk-taskmanager run      ->  httk workflow manager run
httk-taskmanager status   ->  httk workflow workspace status
httk-taskmanager request  ->  httk workflow job request

httk-v1-taskmanager prepare  ->  httk workflow v1 prepare
httk-v1-taskmanager submit   ->  httk workflow v1 submit
httk-v1-taskmanager run      ->  httk workflow v1 run
```

Both aliases keep their own flags, including the `--durable`/`--no-durable`
switch they have always accepted *before* the subcommand. The canonical tree
carries the same switch on the leaf that acts on it, so both spellings work.

## The complete tree

```text
httk workflow workspace  init | status | policy show | policy set | fsck | gc | upgrade | unlock
httk workflow runner     publish | describe
httk workflow job        new | submit | request | list | show | log | why | debug
httk workflow import     pwd | cwl
httk workflow harvest
httk workflow manager    run
httk workflow v1         prepare | submit | run
httk workflow config     init | show | set | unset | import-v1
httk workflow project    init | import-v1 | show | doctor | manifest create | manifest verify
httk workflow computer   list | add | configure | install | import-v1 | show | remove
httk workflow remote     send | fetch | offer | retire | start-manager | status
```

### `workspace` — the workspace itself, not its jobs

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init WORKSPACE` | initialize a workspace, printing its root | `--extension`, `--no-durable` |
| `workspace status WORKSPACE` | summarize the authoritative markers | `--json` |
| `workspace policy show WORKSPACE` | print the shared policy | `--json` |
| `workspace policy set WORKSPACE KEY VALUE` | store one policy member | `--json` |
| `workspace fsck WORKSPACE` | check every marker against its journal frame | `--repair`, `--quarantine-unrepairable`, `--json` |
| `workspace gc WORKSPACE` | collect what the retention policy allows | `--dry-run`, `--json` |
| `workspace upgrade WORKSPACE` | enable an implemented extension | `--extension` (required) |
| `workspace unlock WORKSPACE` | release a maintenance lock | `--force` |

### `runner` — the shared runners a workspace publishes

| Command | What it does | Notable options |
| --- | --- | --- |
| `runner publish FILE` | publish one runner, pinned by digest | `--workspace` (required), `--name`, `--replace` |
| `runner describe [NAME]` | report the published runners and their digests | `--workspace` (required), `--json` |

### `job` — making jobs, and finding out about them

| Command | What it does | Notable options |
| --- | --- | --- |
| `job new WORKSPACE` | scaffold and submit jobs from a template | `--template` (required), `--from`, `--file`, `--input`, `--tag`, `--placement`, `--json` |
| `job submit WORKSPACE SOURCE` | submit one prepared payload directory | `--placement` (required), `--move` |
| `job request WORKSPACE JOB_ID ACTION` | publish an operator request | `--operator`, `--reason` (both required), `--priority`, `--step`, `--force` |
| `job list WORKSPACE` | list the jobs as a cheap table | `--kind`, `--placement`, `--json` |
| `job show WORKSPACE JOB` | describe one job from its state | `--json` |
| `job log WORKSPACE JOB` | print the transition history | `--limit`, `--json` |
| `job why WORKSPACE JOB` | explain why a job is not running | `--json` |
| `job debug WORKSPACE JOB` | drive one job to a terminal state, in front of you | `--step`, `--placement`, `--follow-children`, `--timeout`, `--log-level` |

`JOB` is a job UUID, a `tag--uuid` job key, or any unique prefix of either.

### `import` — workflows written in another language

| Command | What it does | Notable options |
| --- | --- | --- |
| `import pwd WORKSPACE DOCUMENT` | import one Python Workflow Definition document as one job | `--module`, `--module-path`, `--input`, `--allow-module`, `--attempts`, `--allow-unknown-version`, `--placement`, `--tag`, `--name`, `--priority`, `--data-mode`, `--json` |
| `import cwl WORKSPACE WORKFLOW INPUTS` | import one CWL workflow or command-line tool as one job | `--placement`, `--tag`, `--name`, `--priority`, `--data-mode`, `--json` |

Both print one tab-separated `job_key<TAB>payload` line, or a JSON report with
`--json`, exactly as `job new` does. Importing is one way, and neither writes a
runner file: the job references the packaged runner of the format through the
reserved installed form. `import cwl` needs `pip install httk-workflow[cwl]` on
the machine that imports, and nothing extra on the machine that runs the result.
See {doc}`importing_workflows`.

### `harvest` — the finished jobs, as records

| Command | What it does | Notable options |
| --- | --- | --- |
| `harvest WORKSPACE` | stream one record per finished job | `--state`, `--placement`, `--jsonl` (default), `--json` |

### `manager` — the process that runs the jobs

| Command | What it does | Notable options |
| --- | --- | --- |
| `manager run WORKSPACE` | run one task manager | `--workers`, `--pool`, `--capability`, `--until-idle`, `--idle-timeout`, `--lease-seconds`, `--drain-timeout`, `--gc-interval`, `--runner-search-path`, `--log-level`, `--log-file`, `--json-logs` |

### `v1` — *httk* v1 task templates on the v2 engine

| Command | What it does | Notable options |
| --- | --- | --- |
| `v1 prepare SOURCE DESTINATION` | turn an instantiated v1 task into a payload | `--taskset` (default `default`), `--tag`, `--step`, `--priority`, `--attempts` |
| `v1 submit WORKSPACE SOURCE` | prepare and submit one v1 task | `--placement` (required), `--taskset` (default `default`) |
| `v1 run WORKSPACE` | run only the httk-v1 jobs of a workspace | `--taskset` (default `any`), `--wrap`, `--task-timeout`, `--workers`, `--until-idle`, `--idle-timeout` |

`--taskset` deliberately defaults differently between siblings, because the
siblings mean different things by it. `prepare` and `submit` **assign** a task
set to the job they create, so their default is the ordinary `default` set;
`run` **filters** the jobs it will claim, so its default is `any`, which accepts
every set. Unifying them would either strand every submitted job under a manager
filtering for one set, or quietly file every prepared task under a set literally
named `any`.

### `config` — the per-user configuration and identity

| Command | What it does | Notable options |
| --- | --- | --- |
| `config init` | write the configuration and the identity key | `--name`, `--email`, `--non-interactive` |
| `config show [KEY]` | print the configuration, or one member | |
| `config set KEY VALUE` | store one member | |
| `config unset KEY` | remove one member | |
| `config import-v1 [SOURCE]` | read a legacy `~/.httk` configuration | |

### `project` — the directory a campaign lives in

| Command | What it does | Notable options |
| --- | --- | --- |
| `project init [PATH]` | create a project, its key, and its workspace | `--name`, `--description`, `--default-queue`, `--exclude`, `--non-interactive` |
| `project import-v1 [PATH]` | read a legacy `ht.project` | `--source`, `--name` |
| `project show [PATH]` | describe the project, its keys, its workspace, its manifest | `--no-verify`, `--json` |
| `project doctor [PATH]` | check, and optionally repair, the project | `--repair`, `--json` |
| `project manifest create [PROJECT]` | write the signed manifest | `--manifest` |
| `project manifest verify [PROJECT]` | verify the manifest against the tree | `--manifest`, `--trusted-key` |

### `computer` — the adapters that reach other machines

| Command | What it does | Notable options |
| --- | --- | --- |
| `computer list` | list the computers this project can reach | |
| `computer add NAME` | create a computer from a packaged template | `--template`, `--global`, `--non-interactive` |
| `computer configure COMPUTER` | run the adapter's `configure` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `computer install COMPUTER` | run the adapter's `install` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `computer import-v1 SOURCE` | map a legacy computer bundle | `--name`, `--global` |
| `computer show NAME` | describe one computer and its queues | `--json` |
| `computer remove NAME` | remove one computer bundle | `--force` |

`computer show` never prints a credential *value*: a queue setting stored in
the manifest-excluded `credentials.json` is reported by name only, so a
description an operator pastes into a bug report cannot carry a password.

`computer remove` refuses while an unretired transfer still depends on the
computer, because removing it would leave that transfer with no way home;
`--force` skips the interactive confirmation and **nothing else** — the refusal
stands either way. Fetch or retire the transfer first.

### `remote` — work that travels to another computer

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote send COMPUTER JOB_ID …` | detach named jobs and import them on the computer | `--source-workspace`, `--destination-workspace`, `--destination-placement`, `--adapter-timeout` |
| `remote fetch` | bring the jobs that finished on a computer back here | `--computer` (required), `--workspace`, `--remote-workspace`, `--state`, `--placement`, `--adapter-timeout`, `--json` |
| `remote offer WORKSPACE` | seal the finished jobs here for a workspace that will fetch | `--destination-workspace-id` (required), `--state`, `--placement`, `--json` |
| `remote retire WORKSPACE JOB_ID …` | retire the sealed sources another workspace imported | `--destination-workspace-id`, `--json` |
| `remote start-manager COMPUTER` | start managers on the computer | `--count`, `--workers`, `--remote-workspace`, `--adapter-timeout` |
| `remote status COMPUTER` | report the status of the workspace on the computer | `--remote-workspace`, `--adapter-timeout` |

Every workspace option says which side of the transfer it names. `send` leaves
`--source-workspace` here and arrives at `--destination-workspace` there;
`fetch` leaves `--remote-workspace` there and arrives at `--workspace` here;
`start-manager` and `status` only ever mean the far side, and say so with
`--remote-workspace`.

## Migrating from the old spellings

Every spelling below still parses, and none of them appears in `--help` any
more. They are kept for one release; move to the canonical column.

| Deprecated | Canonical | Note |
| --- | --- | --- |
| `httk workflow tasks …` | `httk workflow remote …` | the whole group was renamed |
| `remote send --workspace` | `remote send --source-workspace` | it names the local side |
| `remote start-manager --workspace` | `remote start-manager --remote-workspace` | it names the far side |
| `remote status --workspace` | `remote status --remote-workspace` | it names the far side |
| `remote … --timeout` | `remote … --adapter-timeout` | it bounds adapter calls, not the work |
| `computer configure/install --timeout` | `--adapter-timeout` | as above |
| `manager run --timeout` | `manager run --idle-timeout` | it is the `--until-idle` wait |
| `v1 prepare/submit/run --set` | `--taskset` | `--set` now means `KEY=VALUE` only, on `computer configure` |
| `tasks receive` | `internal receive` | see below; the invoked spelling is protocol |

`remote fetch --workspace` is **not** deprecated: on `fetch` that option has
always named the local destination, which is what it still means.

### The one spelling that is protocol, not interface

`remote send` finishes by asking the far side to import the bundle it pushed,
and `remote fetch` asks the far side to offer and then retire. Those argument
vectors run on a machine whose *httk* may be older or newer than yours, so their
spelling is frozen:

```text
httk workflow tasks receive --workspace WORKSPACE --bundle BUNDLE
httk workflow tasks offer WORKSPACE --destination-workspace-id UUID --json
httk workflow tasks retire WORKSPACE JOB_ID … --destination-workspace-id UUID --json
httk workflow workspace status WORKSPACE --json
httk workflow manager run WORKSPACE
```

They are listed once, as module constants, in
{py:mod}`httk.workflow.workflow_cli`. `receive` is an import half rather than an
operator command, so its canonical home is the unadvertised
`httk workflow internal receive`; `remote receive` and `tasks receive` both
still work, and the *invoked* spelling will not change until every supported
release understands the new one.

## Creating jobs

`job new` scaffolds and submits jobs from a template — a packaged runner name or
the path of a runner file of your own — and needs no prepared payload:

```console
httk workflow job new WORKSPACE --template vasp-relax --from POSCAR --tag silicon
httk workflow job new WORKSPACE --template vasp-relax --from structures/ --placement project/screening
httk workflow job new WORKSPACE --template ./my_runner.py --step characterize --input sites=8
```

`--from` is a structure file, staged as the `files/POSCAR` the packaged runners
read, or a directory of `POSCAR*` and `*.vasp` files, which becomes one job each,
tagged after its file. `--file NAME=PATH` stages anything else, `--input
NAME=VALUE` writes the job's inputs — JSON when the value parses as JSON, a string
otherwise, and `NAME=@FILE` reads a JSON file — and the command prints one
tab-separated `job_key<TAB>payload` line per job, or `--json` reports. The runner
file is published into the workspace runner store and pinned by digest unless
`--publish installed` names a packaged runner where it is installed. See
{doc}`quickstart`.

## Importing workflows written elsewhere

A Python Workflow Definition document or a CWL document becomes one job without
being rewritten:

```console
httk workflow import pwd WORKSPACE workflow.json --module workflow.py --tag arithmetic
httk workflow import cwl WORKSPACE flow.cwl job.yml --tag echo --data-mode transactional
```

The imported job runs on httk's own runner and manager — no other engine is
invoked, and `cwltool` is neither used nor bundled — and it is claimed, retried,
journalled and harvested like every other job. {doc}`importing_workflows`
documents both formats, the supported CWL subset, everything that is refused and
why, and what running a PWD document means for security.

## Inspecting and debugging jobs

`job list`, `job show`, `job log`, and `job why` read one workspace without
writing anything, and `job debug` drives a single job to a terminal state in the
foreground:

```console
httk workflow job list WORKSPACE --kind ready
httk workflow job show WORKSPACE JOB
httk workflow job log WORKSPACE JOB --limit 20
httk workflow job why WORKSPACE JOB
httk workflow job debug WORKSPACE PAYLOAD_OR_JOB --follow-children
```

`JOB` is a job UUID, a `tag--uuid` job key, or any unique prefix of either, and
each command takes `--json`. `job debug` exits `0` on success, `3` on failure, and
`4` when the job stopped without finishing. See
{doc}`taskmanager` for what each command reports.

`httk workflow harvest WORKSPACE` streams one record per finished job for a data
layer to store, as JSON lines by default; see {doc}`harvest`.

## Configuration and projects

User configuration follows the XDG base-directory convention:

- `$XDG_CONFIG_HOME/httk/config.json`;
- keys and global computers below `$XDG_DATA_HOME/httk/`.

`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` can provide explicit deployment or test
overrides. Legacy `~/.httk` data is read only through `config import-v1`; its
64-byte private material is not converted.

```console
httk workflow config init --name "A User" --email user@example.org
httk workflow config set name "Another User"
httk workflow config unset email
httk workflow project init . --name example --default-queue default
```

`config set` accepts only the keys the configuration actually has — `name` and
`email` — and names them when it refuses another, so a typo cannot become a
member that nothing ever reads. `format` and `format_version` describe the
document and are written by *httk* itself. A configuration whose `format` is
something else is refused rather than read as if its members meant what *httk*
means by them; one with no `format_version` at all predates versioning and is
read as version 1.

A project has `.httk-project/project.json`, a standard 32-byte Ed25519 seed
stored with mode `0600`, and a workflow workspace with
`detached-transfer-v1` enabled. Commands discover the nearest project in the
working directory's parent chain.

### Describing and checking a project

```console
httk workflow project show
httk workflow project show --json
httk workflow project doctor
httk workflow project doctor --repair
```

`project show` reports the project's metadata, whether it pins a key and which,
its workspace and job counts, and what its manifest currently verifies as;
`--no-verify` skips the tree walk that last part needs, which is what makes the
command cheap on a very large project.

`project doctor` checks the conditions that quietly break a project later — an
uninitialized workspace, a stale maintenance lock, an unpinned key, staging
leftovers, a legacy identity, an unverifiable manifest — and reports them all.
`--repair` fixes the ones that can be fixed automatically, says exactly what it
did, and journals it in the project's workspace, so the repair is part of that
workspace's durable history. The command exits `1` only when a check is actually
*broken*; a warning, such as a project that has no manifest yet, is something to
know about rather than something to fail a script on.

## Signed manifests

```console
httk workflow project manifest create
httk workflow project manifest verify
httk workflow project manifest verify --trusted-key keys/collaborator.pub
```

The *httk₂* manifest is deterministic canonical JSON-lines compressed with bzip2.
It records sorted POSIX paths, regular-file sizes and SHA-256 hashes, empty
directories, and symlink targets. Special files are rejected. A
domain-separated body digest is signed with Ed25519. Creation fences manager
launches and refuses active work. Verification also recognizes the legacy
`ht.project/manifest.bz2` format without changing it.

### What a verified manifest actually proves

Be precise about the threat this addresses, because the signing key lives in the
tree it signs. `.httk-project/keys/project.seed` is a file of the project, mode
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
manifest**: the key pinned in `project.json` at `httk workflow project init`,
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

A project created by `project init` pins its own key at creation, so its
manifests verify as `valid_trusted` immediately. A project made before pinning
existed has no `public_key` in `project.json`, so every manifest of it verifies
as `valid_unknown_key` until somebody decides which key to trust. That decision
is explicit, because it is the whole trust model in one act:

```python
from httk.workflow.projects import pin_project_key, trust_project_key

pin_project_key("/path/to/project")                 # adopt keys/project.pub
trust_project_key("/path/to/project", "ed25519:…")  # adopt somebody else's key
```

`pin_project_key` adopts the key that is in the tree *right now* — do it only on
a tree you have reason to believe is the one you left. `trust_project_key` adds
a further anchor to `project.json`'s `trusted_keys`; `project import-v1` fills
that list with the legacy identities of an imported *httk* v1 project, so its
old `ht.project/manifest.bz2` verifies as trusted too. `--trusted-key` accepts
either an `ed25519:BASE64` value or the path of a `*.pub` file and is the
one-off equivalent that writes nothing.

For attribution *between* machines — who published this request, who imported
this transfer — see the operator identity key below, which is a different key
with a different job.

### Operator identity

`httk workflow config init` creates `identity.seed`/`identity.pub` below
`$XDG_DATA_HOME/httk/keys/`. That key signs the small documents an operator
publishes: an operator request (`httk workflow job request …`) and a transfer
acknowledgement. The signature is detached, covers the canonical JSON of the
whole document, and is domain-separated from every other httk signature.

It is optional in both directions, deliberately. An installation with no
identity key publishes unsigned documents, and a manager or a transfer source
accepts them exactly as before — so a mixed deployment needs no flag day. A
signature that *is* present must verify: a request with a broken signature is
quarantined with the reason, and an acknowledgement with a broken signature will
not retire a sealed bundle. A verified request records its `operator_key` in the
journalled state frame beside the operator name and reason.

The semantics are attribution, not authorization. The key says *which identity
published this document*; it grants nothing, and no operation is permitted
because a document is signed. Anyone who can write the workspace's request
directory can still publish an unsigned request.

The fence is `.httk-workflow/maintenance.lock`, holding the recording process
identifier, hostname, and creation time. A lock whose same-host process is gone,
whose content is unreadable, or that is older than twenty-four hours is
reclaimed automatically; any other lock is reported with its holder. Operators
can also clear one explicitly:

```console
httk workflow workspace unlock WORKSPACE
httk workflow workspace unlock WORKSPACE --force
```

Without `--force` only a stale lock is removed.

## Workspace policy and integrity

The tunables a workspace shares with every process attaching it — the
visibility deadline, the default lease, the journal segment size, and the
retention limits — are stored in `format.json` and edited in place:

```console
httk workflow workspace policy show WORKSPACE
httk workflow workspace policy show WORKSPACE --json
httk workflow workspace policy set WORKSPACE visibility_deadline_seconds 60
httk workflow workspace policy set WORKSPACE retention.trash_days 14
```

`workspace fsck` verifies that every state marker still resolves to a readable
journal frame that agrees with it, and can re-point damaged markers at the last
good frame of their job:

```console
httk workflow workspace fsck WORKSPACE
httk workflow workspace fsck WORKSPACE --repair --json
httk workflow workspace fsck WORKSPACE --repair --quarantine-unrepairable
```

It exits `1` while anything remains for an operator to deal with. See
[the task-manager guide](taskmanager.md) for what each problem code means and
for exactly what a repair will and will not touch.

## Freeing disk

Nothing in the engine deletes anything on its own: neither a runner nor a
manager is ever required to run cleanup code, so every artefact a crash could
orphan is simply left in place. `workspace gc` is the separate, explicit
collector, driven entirely by the workspace's `policy.retention`:

```console
httk workflow workspace gc WORKSPACE --dry-run
httk workflow workspace gc WORKSPACE
httk workflow workspace gc WORKSPACE --json
```

It prints one row per category with the candidates it found, what it removed,
and an estimate of the bytes reclaimed; `--json` lists every individual entry
as well. `--dry-run` touches nothing at all and reports what a real run would
remove. A run that removed anything also appends one `httk-workflow-gc` frame
to the journal summarizing the same counts, so the collection is itself part of
the workspace's durable history.

A retention limit that is not configured means *keep*, so on a workspace whose
policy is empty the command only prunes what cannot carry information: empty
placement mirrors below the state kinds, staging entries abandoned for a day,
and month-old request leftovers — those claimed by a manager that is gone and
those a manager explicitly retired. Configure the limits to collect the rest:

```console
httk workflow workspace policy set WORKSPACE retention.attempt_control_days 14
httk workflow workspace policy set WORKSPACE retention.trash_days 14
httk workflow workspace policy set WORKSPACE retention.journal_days 90
```

| Category | Retention limit | What goes |
| --- | --- | --- |
| `attempt_control` | `attempt_control_days` | `.httk-attempt.*` directories of terminal jobs, never the newest one of a job |
| `transaction_trash` | `trash_days` | trees a replayed transaction moved aside, once the job left `committing` |
| `retired_bundles` | `trash_days` | acknowledged transfer bundles below `transfers/retired/` |
| `transfer_records` | `trash_days` | per-transfer receipts below `transfers/acks/` and `transfers/imported/` |
| `journal_segments` | `journal_days` | segments no current marker references, written by a writer no live manager owns |
| `manager_directories` | `journal_days` | directories of dead managers whose segments are gone |
| `placement_directories` | always safe | empty placement mirrors below `state/<kind>/` |
| `tmp_entries` | always safe | staging entries older than 24 hours |
| `retired_requests` | always safe | requests claimed over 30 days ago by a manager now gone, and requests retired over 30 days ago with their `.retirement` records |

The collector never touches the quarantine, a sealed transfer bundle, a
persistent workdir, a payload beyond its aged attempt-control directories, any
marker, a segment a current marker references, a manager that is still
heartbeating, or the runner store. Removal is bottom-up and rewrites no state,
so a collection killed halfway leaves the workspace exactly as consistent as it
was, and running it again simply finishes the job.

Collecting journal segments has one honest cost. Only the segment a marker
points into is protected, so the deep history of an old job goes with the
segments behind it; `harvest` and `job log` then report that job's timeline
with `gaps` set. Its state, payload, and outcome are unaffected.

## Computer adapters

Computer definitions are versioned directories containing `computer.json` and
executable `configure`, `install`, `invoke`, `push`, `pull`, `start-manager`,
and `status` operations. Each receives one versioned JSON request filename and
prints one JSON result; diagnostics belong on stderr. Commands and remote
commands are always argument arrays. The maintained templates implement that
protocol through {py:mod}`httk.workflow.adapter_protocol`, which is the public
name of the packaged implementation. {doc}`adapter_authoring` is the reference
for writing one of your own: the bundle layout, the exact request and result
document of each of the seven operations, and a worked skeleton for a cluster
none of the maintained kinds covers.

Maintained `local`, `local-slurm`, and `ssh-slurm` templates are packaged with
the module. Project definitions shadow global definitions. `NAME:QUEUE`
selects an explicit queue; otherwise the project default and then `default`
are tried. `computer import-v1` maps recognized legacy bundles by reading
assignment-only configuration; legacy shell executables are never copied or
run. Any other `kind` in a `computer.json` is refused rather than executed in
the wrong place.

`computer configure --set KEY=VALUE` persists only the non-secret keys
`account`, `bootstrap`, `check_connectivity`, `cpus_per_task`, `host`,
`httk_command`, `legacy_settings`, `nodes`, `partition`, `port`, `reservation`,
`time_limit`, `username`, `workers`, and `workspace` in the shareable
`computer.json`. Every other key is stored per queue in `credentials.json` with
mode `0600` beside it, which project manifests exclude. Adapters receive both
together as the request's `queue_settings`. `computer show NAME` reports which
file each setting came from, and the name — never the value — of every
credential.

### What each kind does

`local` copies files in this filesystem, runs commands as child processes, and
starts the requested `count` of managers as detached local processes.

`local-slurm` keeps the same local copies and local commands, but submits the
manager with a generated batch script through the local `sbatch`. It therefore
requires `sbatch` on the machine that defines the computer, which is checked
when the computer is added.

`ssh-slurm` moves files with `rsync` over `ssh` and runs every command on the
configured host, where the manager is submitted with `sbatch`. Only `ssh` and
`rsync` are required locally. Operation by operation:

| Operation | `ssh-slurm` behaviour | Settings used |
| --- | --- | --- |
| `configure` | verifies the host answers with a cheap remote `true`, so a mistyped host fails immediately instead of at the first transfer | `host`, `username`, `port`, `check_connectivity` |
| `install` | checks that `httk` answers on the far side, reports its version, and creates the queue's workspace directory when it is missing | `host`, `username`, `port`, `workspace`, `httk_command`, `bootstrap` |
| `push` / `pull` | one `rsync --archive` transfer, creating missing destination components; a `pull` is always the whole remote directory, a `push` is the whole tree or the request's explicit relative `files` batch | `host`, `username`, `port` |
| `invoke` | runs the request's argument vector on the host, optionally in the request's directory, and returns its status, stdout and stderr | `host`, `username`, `port`, `httk_command` |
| `status` | the same machinery running `httk workflow workspace status WORKSPACE --json` remotely | as `invoke` |
| `start-manager` | writes a generated batch script into `WORKSPACE/.httk-workflow/batch/`, then submits it with `sbatch` once, or the request's `count` times | `account`, `partition`, `time_limit`, `nodes`, `cpus_per_task`, `reservation`, `workers`, `workspace` |

The generated batch script is a `#!/bin/bash` file carrying one `#SBATCH`
directive per configured setting, `--chdir` set to the workspace, `--output`
and `--error` beside the script, and a single `exec` line that runs the manager
command. The queue's `workers` count is appended only when the request did not
already choose one, so an explicit `--workers` always wins. Both kinds report
the submitted job identifiers.

A `start-manager` request names the workspace outright in its `workspace` field.
When that field is absent the workspace is read back out of the request's
`manager run WORKSPACE` argument vector, and only then from the queue's
`workspace=PATH`; the argv reading is a documented fallback for hand-written
requests, not the normal path. `local` starts `count` detached processes and
reports their `pids`.

`httk_command` overrides how `httk` is spelled on the far side, for example
`httk_command="/proj/venv/bin/httk"`; without it the plain `httk` on the remote
`PATH` is used, and locally a `python3 -m httk.core.cli` fallback applies.

### Quoting

Every subprocess an adapter starts is an argument vector, so no shell ever
parses a value that came from a request or from settings. `ssh` is the one
exception in the protocol, because it always joins its command words and lets a
login shell on the far side parse the result. All remote command strings, and
the one line of the generated batch script that runs the manager, are therefore
built by a single helper that quotes element-wise; nothing else composes a
command string. `rsync` transfers pass `--protect-args` so that even file names
travel in the protocol rather than through the remote shell.

### Installing httk on the target

`computer install` never installs software behind your back. It reports the
`httk` it found and the workspace directory it ensured; when nothing answers it
fails with a message pointing at `pipx install httk-workflow` on the target.
Configuring the queue with `bootstrap=pip` opts into one attempt at
`python3 -m pip install --user httk-workflow` before that check is repeated.

## Detached transfers

Workspaces can enable the implemented migration explicitly:

```console
httk workflow workspace upgrade WORKSPACE --extension detached-transfer-v1
```

A transfer fences an explicit quiescent marker, seals it in the payload,
validates the payload digest at import, publishes the preserved UUID and prior
state only at the destination, and retires the source only after an
idempotent acknowledgement. Transfer UUID and digest checks suppress retries;
sealed and retired bundles are retained for recovery. Repeating `remote send`
resumes the matching sealed transfer, including the copy-before-import and
lost-acknowledgement boundaries.

The sealed payload digest pins every path, every file's content *and executable
bit*, and the literal target of every symlink, so a runner that arrives without
its executable bit, or a link retargeted in transit, is a detected mismatch
rather than a silent corruption. A symlink is carried as its target string and
must stay inside the payload: an absolute target, or a relative one climbing out
with `..`, is refused by name, because it would mean something else at the
destination.

## Running on a computer and fetching the results

The complete loop is four commands. Work is sent to a computer, run there,
fetched back once it has stopped, and harvested locally:

```console
httk workflow remote send CLUSTER JOB_ID ...          # local -> computer
httk workflow remote start-manager CLUSTER --count 2 --workers 4
httk workflow remote fetch --computer CLUSTER --workspace LOCAL_WS
httk workflow harvest LOCAL_WS --state succeeded --state failed
```

`remote send` detaches each named job from the local workspace and imports it on
the computer. `--source-workspace` names the local workspace when it is not the
project's, `--destination-workspace` overrides the queue's `workspace=PATH`, and
`--destination-placement` puts the arriving jobs somewhere other than the
placement they had here. `remote status` reports the remote workspace status
through the adapter, and `internal receive` is the remote half `send` invokes.

`remote start-manager` starts managers on the computer: `--count N` submits the
generated batch script `N` times, `--workers N` fixes the workers per manager,
and leaving `--workers` off lets the queue's configured `workers=N` decide. Both
take `--remote-workspace` and `--adapter-timeout`.

`remote fetch` is the local half. It probes the remote workspace over the
adapter's `status` operation, asks it to `offer` what has stopped, `pull`s each
offered bundle into `.httk-workflow/transfers/incoming/`, imports it, and only
then tells the remote to `retire` the sources it still holds. Both the computer
and its workspace can be named explicitly:

```console
httk workflow remote fetch --computer CLUSTER:large --remote-workspace /scratch/me/runs \
    --state succeeded --state failed --placement project/screening --json
```

`--remote-workspace` defaults to the queue's configured `workspace=PATH`, the
same setting `remote send` uses. `--state` accepts the kinds a stopped job can be
in and defaults to `succeeded` and `failed`; `--placement` restricts the fetch to
one subtree; `--adapter-timeout` bounds every adapter operation the fetch runs, as
it does for `send`, `start-manager`, and `status`. A fetched job arrives as an
ordinary job of the local workspace, in
the terminal state and at the placement it had on the computer, so
`httk workflow harvest` then reports it exactly like a job that ran at home.

The other two commands are the remote half, invoked over the adapter by `remote
fetch` but usable on their own on the computer itself:

```console
httk workflow remote offer WORKSPACE --destination-workspace-id UUID --json
httk workflow remote retire WORKSPACE JOB_ID ... --destination-workspace-id UUID
```

`offer` detaches every finished job into its sealed bundle and prints one entry
per bundle; it requires `--destination-workspace-id`, because a bundle is sealed
for exactly one destination. `retire` moves the sealed source of an already
imported job under `.httk-workflow/transfers/retired/` — a rename, never a
delete, so a source is only ever whole or moved whole; its
`--destination-workspace-id` is optional and, when given, refuses a bundle that
was sealed for somebody else. `offer` narrows what it seals with the same
`--state` and `--placement` `fetch` passes through; both print their report as
JSON with `--json` and as tab-separated lines otherwise.

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
non-interactive-shell test, on any host a computer adapter reaches.
