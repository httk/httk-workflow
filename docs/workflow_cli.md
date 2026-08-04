# Project and workflow command line

*For operators and campaign owners: the whole command tree, including projects, configuration, signed manifests, remotes, and work that travels to them.*

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
httk workflow workspace  init | list | forget | delete | status | settings show | settings set | settings unset | policy show | policy set | fsck | gc | unlock
httk workflow runner     publish | describe
httk workflow job        new | submit | request | list | show | log | why | debug
httk workflow import     pwd | cwl
httk workflow harvest
httk workflow manager    run
httk workflow campaign   init | show | submit | harvest | start-managers
httk workflow v1         prepare | submit | run
httk workflow config     init | show | set | unset | import-v1
httk workflow project    init | import-v1 | show | doctor | manifest create | manifest verify
httk workflow remote     list | add | configure | install | import-v1 | show | remove
httk workflow transfer   SRC DST      (plus the protocol spellings: receive | offer | retire)
```

### Every command names a *registered* workspace

A `WORKSPACE` above is always a **registered name**, never a bare path. A name is
bound once — "init:ed" — to a place: either the built-in `local` remote, meaning
this machine, or exactly one defined remote, plus the path the workspace lives at
there. The binding is always explicit; being local is never implied, because a
name that silently meant "here" would be a different workspace on a different
machine. There is no path notation anywhere in the CLI: a command handed
something that is not a registered name refuses and points you at
`workspace init` or `workspace list`.

A binding lives in one of two scopes — **global**
(`$XDG_CONFIG_HOME/httk/workspaces.json`, shared by every project) or **project**
(the `workspaces` member of `httk_project/project.json`, travelling with the
project). A project binding shadows a global one of the same name, exactly as a
project-local remote shadows a global one.

The registry is the CLI's contract, and a *client-side* concept: it names the
places you address, but a remote never sees it. When a command reaches a remote
workspace, the client resolves the name to its remote path and sends the **path**
(with the hidden `--by-path` switch) to the far side, whose wire protocol has
always been path-based. The Python API keeps `Workspace(path)` for library use;
the registry is what the command line speaks.

Reaching a remote binding depends on the command: the read commands
`workspace status`, `workspace fsck`, and `workspace gc` run over the remote's
adapter and read it exactly as they read a local one, while every command that
only makes sense where the files are — `job …`, `harvest`, `workspace settings`,
`unlock` — refuses a remote binding with a message naming the remote
and pointing at `transfer` and `workspace status`.

### `workspace` — the workspace itself, not its jobs

| Command | What it does | Notable options |
| --- | --- | --- |
| `workspace init NAME` | create a workspace **and** register the name | `--remote` (required), `--path` (required), `--scope`, `--setting`, `--no-durable` |
| `workspace list` | list the registered workspaces and where each resolves | `--json` |
| `workspace forget NAME` | deregister a name, leaving the workspace on disk | |
| `workspace delete NAME` | destroy the workspace and deregister it | `--force` (required) |
| `workspace status NAME` | summarize the authoritative markers (remote: over the adapter) | `--json` |
| `workspace settings show NAME [KEY]` | print the application settings, or one | `--json` |
| `workspace settings set NAME KEY VALUE` | store one application setting | |
| `workspace settings unset NAME KEY` | remove one application setting | |
| `workspace policy show NAME` | print the shared policy | `--json` |
| `workspace policy set NAME KEY VALUE` | store one policy member | `--json` |
| `workspace fsck NAME` | check every marker against its journal frame (remote: over the adapter) | `--repair`, `--quarantine-unrepairable`, `--json` |
| `workspace gc NAME` | collect what the retention policy allows (remote: over the adapter) | `--dry-run`, `--json` |
| `workspace unlock NAME` | release a maintenance lock | `--force` |

`workspace init` both creates the workspace and registers the name for it, so a
first workspace on this machine is one command:

```console
httk workflow workspace init my-workspace --remote local --path runs/my-workspace
```

A workspace on a cluster names the remote it lives on instead of `local`; the
workspace is created there over the adapter, and its name is registered here.
`--setting KEY=VALUE` seeds an application setting at creation, and a
remote-bound workspace is *also* seeded from the remote definition's whitelisted
queue settings — see the *Application settings* section below. `workspace
delete` destroys the workspace (locally, or on its remote over the adapter) and
is refused without `--force`; `workspace forget` only removes the name.

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
| `manager run WORKSPACE` | run a manager locally, or submit managers to a remote workspace's scheduler | `--workers`, `--count`, `--foreground`, `--pool`, `--capability`, `--until-idle`, `--idle-timeout`, `--lease-seconds`, `--drain-timeout`, `--gc-interval`, `--runner-search-path`, `--adapter-timeout`, `--log-level`, `--log-file`, `--json-logs` |

`manager run` follows the binding: a local workspace runs the manager in this
process as before, and a remote workspace submits managers through the remote's
scheduler over its adapter — `--count N` managers, `--workers N` workers each.
`--foreground` asks to run here and so is refused for a remote workspace; this is
the command that subsumed the old `transfer start-manager`.

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

The project *anchor* — the `httk_project` directory, discovery, keys, and pins —
belongs to *httk-core*, which owns the umbrella `httk project` command.
*httk-workflow* keeps its workflow-aware project commands under
`httk workflow project`; the core umbrella owns only the anchor commands.

The anchor's own leaves — `httk project init` and `httk project show` — are
provided by *httk-core*. `httk project init` creates only the anchor, whereas
`httk workflow project init` also creates the workflow workspace:

| Command | What it does | Notable options |
| --- | --- | --- |
| `project init [PATH]` | create a project, its key, and its workspace | `--name`, `--description`, `--default-queue`, `--exclude`, `--non-interactive` |
| `project import-v1 [PATH]` | read a legacy `ht.project` | `--source`, `--name` |
| `project show [PATH]` | describe the project, its keys, its workspace, its manifest | `--no-verify`, `--json` |
| `project doctor [PATH]` | check, and optionally repair, the project | `--repair`, `--json` |
| `project manifest create [PROJECT]` | write the signed manifest | `--manifest` |
| `project manifest verify [PROJECT]` | verify the manifest against the tree | `--manifest`, `--trusted-key` |

### `remote` — the adapters that reach other machines

Named after `git remote`: a *remote* is one machine this project can reach, and
the bundle of adapter operations that reaches it. The name `local` is reserved
for the built-in remote every workspace registry resolves as "this machine", so
`remote add local` is refused — a workspace bound to `local` must be
unambiguous.

| Command | What it does | Notable options |
| --- | --- | --- |
| `remote list` | list the remotes this project can reach | |
| `remote add NAME` | create a remote from a packaged template | `--template`, `--global`, `--non-interactive` |
| `remote configure REMOTE` | run the adapter's `configure` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote install REMOTE` | run the adapter's `install` operation | `--set KEY=VALUE`, `--adapter-timeout` |
| `remote import-v1 SOURCE` | map a legacy *httk* v1 computer bundle | `--name`, `--global` |
| `remote show NAME` | describe one remote and its queues | `--json` |
| `remote remove NAME` | remove one remote bundle | `--force` |

`remote show` never prints a credential *value*: a queue setting stored in
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
httk workflow transfer SRC DST [--job JOB_ID …] [--state STATE …] [--placement P] \
    [--destination-placement P] [--adapter-timeout SECONDS] [--json]
```

Both names resolve through the registry, so which legs run over an adapter and
which stay in this filesystem follows entirely from where the two are bound:

| Direction | What happens | `--job` |
| --- | --- | --- |
| local → remote | each named job is detached, its sealed bundle pushed to the remote, and imported there | at least one required |
| remote → local | the jobs that have finished on the remote are offered, pulled home, imported, and their sources retired | optional; a `--state`/`--placement` filter selects them |
| local → local | each named job is detached from the source and imported into the destination directly, in this filesystem | at least one required |
| remote → remote | the client relays: it fetches from the source into local staging and pushes on to the destination (v1; a direct source-to-destination path is deferred) | optional |

`--state` (repeatable, default `succeeded` and `failed`) chooses which finished
kinds a fetch moves, `--placement` restricts it to one subtree,
`--destination-placement` lands the jobs somewhere other than the placement they
had, and `--adapter-timeout` bounds every adapter operation the move runs.

### The protocol spellings, and what is gone

`transfer` also carries the frozen argument vectors one machine runs on another
over an adapter. They are path-based, because the far side keeps no registry, and
they are protocol rather than operator interface — a local→remote move invokes
`receive` on the destination, a fetch invokes `offer` then `retire` on the source.
Their spelling is frozen, because the machine that answers may run an *httk* older
or newer than yours:

```text
httk workflow transfer receive --workspace PATH --bundle BUNDLE
httk workflow transfer offer PATH --destination-workspace-id UUID --json
httk workflow transfer retire PATH JOB_ID … --destination-workspace-id UUID --json
httk workflow workspace status PATH --by-path --json
httk workflow manager run PATH --by-path
```

`receive` is an import half rather than an operator command, so it is not
advertised in `--help`, but it is its frozen, invocable spelling. The `--by-path`
switch is likewise hidden: it makes the workspace argument a literal path with no
registry lookup, which is exactly what one machine needs when it addresses another
machine's workspace.

The pre-release `transfer send`, `transfer fetch`, `transfer start-manager`, and
`transfer status` verbs are **gone** — they no longer parse. Move to the single
`transfer SRC DST` verb, and to `manager run NAME` for starting managers (below)
and `workspace status NAME` for reading a remote workspace's markers.

| Removed | Now |
| --- | --- |
| `transfer send REMOTE JOB …` | `transfer LOCAL REMOTE --job JOB …` |
| `transfer fetch --remote REMOTE --workspace LOCAL` | `transfer REMOTE LOCAL` |
| `transfer start-manager REMOTE --count N` | `manager run REMOTE --count N` |
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
| `campaign submit` | assign one root job to a partition and submit it there | `--template` (required), `--key` (required), `--index`, `--input`, `--file`, `--tag`, `--placement`, `--priority`, `--name`, `--json` |
| `campaign harvest` | harvest every partition, one workspace after another | `--partition`, `--state`, `--placement`, `--json` |
| `campaign start-managers` | start a manager per selected partition | `--partition`, `--workers`, `--count`, `--adapter-timeout` |

A campaign is a thin convention over the *registered workspaces* above: a
partition map, stored in the project, that spreads a very large body of work
across many workspaces without a new scheduler. Each partition names one
registered workspace, roots are assigned to partitions by policy, and spawned
children always inherit their parent's workspace. See {doc}`campaigns`.

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

User configuration follows the XDG base-directory convention, and everything
per-user this package keeps is *configuration*:

- `$XDG_CONFIG_HOME/httk/config.json`;
- identity keys in `$XDG_CONFIG_HOME/httk/keys/`;
- global remote definitions in `$XDG_CONFIG_HOME/httk/remotes/`.

`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` can provide explicit deployment or test
overrides. Legacy `~/.httk` data is read only through `config import-v1`; its
64-byte private material is not converted.

An earlier release kept the keys and the global definitions below
`$XDG_DATA_HOME/httk/` instead, as `keys/` and `computers/`. The first command
that needs either one moves what is there to its configuration home, preserving
the `0600`/`0700` modes, and says so in one line on stderr. The move happens
once and is idempotent. If both roots somehow exist, the configuration home wins
and the stale legacy copy is reported in the log rather than merged: guessing
which of two definitions of one remote was meant would be worse than saying
nothing was.

```console
httk workflow config init --name "A User" --email user@example.org
httk workflow config set name "Another User"
httk workflow config unset email
httk workflow project init . --name example --default-queue default
```

The project *anchor* is owned by *httk-core*, which provides the umbrella
`httk project` command. `httk project init` creates the anchor alone;
`httk workflow project init` above additionally creates the workflow workspace
(that the two are born together is revisited in a later phase). The manifest,
doctor, and workflow-aware `show` commands below are provided by
*httk-workflow* under `httk workflow project`.

`config set` accepts only the keys the configuration actually has — `name` and
`email` — and names them when it refuses another, so a typo cannot become a
member that nothing ever reads. `format` and `format_version` describe the
document and are written by *httk* itself. A configuration whose `format` is
something else is refused rather than read as if its members meant what *httk*
means by them; one with no `format_version` at all predates versioning and is
read as version 1.

A project has `httk_project/project.json`, a standard 32-byte Ed25519 seed
stored with mode `0600`, and a core-v2 workflow workspace. Commands discover the nearest project in the
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
`--no-verify` skips the tree walk that last part needs; by design, this makes the
command cheap when verification is not required.

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

## Application settings

A workspace also carries *application settings*: a flat, dotted-name map of small
values a runner resolves when it runs — the VASP command, a pseudopotential
library — distinct from the engine `policy` above, which tunes scheduling. They
are stored in the workspace and edited by name:

```console
httk workflow workspace settings set my-workspace vasp.command '"srun -n 32 vasp_std"'
httk workflow workspace settings show my-workspace
httk workflow workspace settings unset my-workspace vasp.command
```

A value that parses as JSON is stored as that scalar; a bare word is stored as a
string. Settings can also be seeded at creation: `workspace init --setting
KEY=VALUE` sets them explicitly, and a workspace bound to a remote is additionally
seeded from that remote definition's whitelisted queue settings, so a cluster's
`vasp_command` becomes the new workspace's `vasp.command` without anyone restating
it.

A runner reads a setting through `a.setting("vasp.command")`, and the value is
resolved in layers, most specific first: the job's own `inputs`, then the
environment (`HTTK_VASP_COMMAND`), then the workspace setting, then the runner's
default. The environment therefore still wins over the workspace setting — a
machine's own `HTTK_VASP_COMMAND` is honoured exactly as before — while the
workspace setting spares an operator from exporting it for every job. The manager
snapshots the settings into each attempt's `context.json`, so a runner sees the
values the workspace held when its job was claimed. See {doc}`vasp_runners` and
{doc}`sdk_parity`.

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

## Remote adapters

Remote definitions are versioned directories containing `remote.json` and
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
are tried. `remote import-v1` maps recognized legacy *httk* v1 computer bundles
by reading assignment-only configuration; legacy shell executables are never
copied or run. Any other `kind` in a `remote.json` is refused rather than
executed in the wrong place.

`remote configure --set KEY=VALUE` persists only the non-secret keys
`account`, `bootstrap`, `check_connectivity`, `cpus_per_task`, `host`,
`httk_command`, `legacy_settings`, `nodes`, `partition`, `port`, `reservation`,
`time_limit`, `username`, `workers`, and `workspace` in the shareable
`remote.json`. Every other key is stored per queue in `credentials.json` with
mode `0600` beside it, which project manifests exclude. Adapters receive both
together as the request's `queue_settings`. `remote show NAME` reports which
file each setting came from, and the name — never the value — of every
credential.

### What each kind does

`local` copies files in this filesystem, runs commands as child processes, and
starts the requested `count` of managers as detached local processes.

`local-slurm` keeps the same local copies and local commands, but submits the
manager with a generated batch script through the local `sbatch`. It therefore
requires `sbatch` on the machine that defines the remote, which is checked
when the remote is added.

`ssh-slurm` moves files with `rsync` over `ssh` and runs every command on the
configured host, where the manager is submitted with `sbatch`. Only `ssh` and
`rsync` are required locally. Operation by operation:

| Operation | `ssh-slurm` behaviour | Settings used |
| --- | --- | --- |
| `configure` | verifies the host answers with a cheap remote `true`, so a mistyped host fails immediately instead of at the first transfer | `host`, `username`, `port`, `check_connectivity` |
| `install` | checks that `httk` answers on the far side, reports its version, and creates the queue's workspace directory when it is missing | `host`, `username`, `port`, `workspace`, `httk_command`, `bootstrap` |
| `push` / `pull` | one `rsync --archive` transfer, creating missing destination components; a `pull` is always the whole remote directory, a `push` is the whole tree or the request's explicit relative `files` batch | `host`, `username`, `port` |
| `invoke` | runs the request's argument vector on the host, optionally in the request's directory, and returns its status, stdout and stderr | `host`, `username`, `port`, `httk_command` |
| `status` | the same machinery running `httk workflow workspace status PATH --by-path --json` remotely | as `invoke` |
| `start-manager` | writes a generated batch script into `WORKSPACE/.httk-workflow/batch/`, then submits it with `sbatch` once, or the request's `count` times | `account`, `partition`, `time_limit`, `nodes`, `cpus_per_task`, `reservation`, `workers`, `workspace` |

The generated batch script is a `#!/bin/bash` file carrying one `#SBATCH`
directive per configured setting, `--chdir` set to the workspace, `--output`
and `--error` beside the script, and a single `exec` line that runs the manager
command. The queue's `workers` count is appended only when the request did not
already choose one, so an explicit `--workers` always wins. Both kinds report
the submitted job identifiers.

A `start-manager` request names the workspace outright in its `workspace` field.
When that field is absent the workspace is read back out of the request's
`manager run PATH --by-path` argument vector, and only then from the queue's
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

`remote install` never installs software behind your back. It reports the
`httk` it found and the workspace directory it ensured; when nothing answers it
fails with a message pointing at `pipx install httk-workflow` on the target.
Configuring the queue with `bootstrap=pip` opts into one attempt at
`python3 -m pip install --user httk-workflow` before that check is repeated.

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

Register the remote workspace once, then the complete loop is four commands. Work
is sent to the remote, run there, fetched back once it has stopped, and harvested
locally:

```console
httk workflow workspace init cluster-runs --remote cluster --path /scratch/me/runs
httk workflow transfer local-runs cluster-runs --job JOB_ID ...     # local -> remote
httk workflow manager run cluster-runs --count 2 --workers 4
httk workflow transfer cluster-runs local-runs                      # remote -> local
httk workflow harvest local-runs --state succeeded --state failed
```

`transfer local-runs cluster-runs` detaches each `--job` from the local workspace
and imports it on the remote, at the placement it had here unless
`--destination-placement` puts it elsewhere. Both names resolve through the
registry, so the source being local and the destination being remote is what
makes this the send leg — no `--source-workspace`/`--remote-workspace` flags, and
no bare paths.

`manager run cluster-runs` starts managers on the remote, because the name is
bound to one: `--count N` submits the generated batch script `N` times, `--workers
N` fixes the workers per manager, and leaving `--workers` off lets the queue's
configured `workers=N` decide.

`transfer cluster-runs local-runs` is the fetch leg. It probes the remote
workspace over the adapter's `status` operation, asks it to `offer` what has
stopped, `pull`s each offered bundle into `.httk-workflow/transfers/incoming/`,
imports it, and only then tells the remote to `retire` the sources it still
holds:

```console
httk workflow transfer cluster-runs local-runs \
    --state succeeded --state failed --placement project/screening --json
```

`--state` accepts the kinds a stopped job can be in and defaults to `succeeded`
and `failed`; `--placement` restricts the fetch to one subtree; `--adapter-timeout`
bounds every adapter operation the fetch runs. A fetched job arrives as an
ordinary job of the local workspace, in the terminal state and at the placement it
had on the remote, so `httk workflow harvest local-runs` then reports it exactly
like a job that ran at home.

Under the fetch leg run the two far-side protocol commands, invoked over the
adapter but usable on their own on the remote itself. They are path-based, because
the far side keeps no registry:

```console
httk workflow transfer offer PATH --destination-workspace-id UUID --json
httk workflow transfer retire PATH JOB_ID ... --destination-workspace-id UUID
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
non-interactive-shell test, on any host a remote adapter reaches.
