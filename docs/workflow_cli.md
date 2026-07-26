# Project and workflow command line

Installing *httk-workflow* registers the lazy `workflow` command with
*httk-core*:

```console
httk workflow --help
```

The complete tree is:

```text
httk workflow workspace init|status|upgrade|unlock
httk workflow runner publish
httk workflow job new|submit|request|list|show|log|why|debug
httk workflow harvest
httk workflow manager run
httk workflow v1 prepare|submit|run
httk workflow config init|show|set|import-v1
httk workflow project init|import-v1|manifest create|manifest verify
httk workflow computer list|add|configure|install|import-v1
httk workflow tasks send|receive|fetch|offer|retire|start-manager|status
```

`httk-taskmanager` and `httk-v1-taskmanager` remain supported and dispatch the
same native and *httk* v1 command functions.

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
httk workflow project init . --name example --default-queue default
```

A project has `.httk-project/project.json`, a standard 32-byte Ed25519 seed
stored with mode `0600`, and a workflow workspace with
`detached-transfer-v1` enabled. Commands discover the nearest project in the
working directory's parent chain.

## Signed manifests

```console
httk workflow project manifest create
httk workflow project manifest verify
```

The *httk₂* manifest is deterministic canonical JSON-lines compressed with bzip2.
It records sorted POSIX paths, regular-file sizes and SHA-256 hashes, empty
directories, and symlink targets. Special files are rejected. A
domain-separated body digest is signed with Ed25519. Creation fences manager
launches and refuses active work. Verification also recognizes the legacy
`ht.project/manifest.bz2` format without changing it.

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

## Computer adapters

Computer definitions are versioned directories containing `computer.json` and
executable `configure`, `install`, `invoke`, `push`, `pull`, `start-manager`,
and `status` operations. Each receives one versioned JSON request filename and
prints one JSON result; diagnostics belong on stderr. Commands and remote
commands are always argument arrays.

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
together as the request's `queue_settings`.

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
sealed and retired bundles are retained for recovery. Repeating `tasks send`
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
httk workflow tasks send CLUSTER JOB_ID ...          # local -> computer
httk workflow tasks start-manager CLUSTER --count 2 --workers 4
httk workflow tasks fetch --computer CLUSTER --workspace LOCAL_WS
httk workflow harvest LOCAL_WS --state succeeded --state failed
```

`tasks send` detaches each named job from the local workspace and imports it on
the computer. `--workspace` names the local workspace when it is not the
project's, `--destination-workspace` overrides the queue's `workspace=PATH`, and
`--destination-placement` puts the arriving jobs somewhere other than the
placement they had here. `tasks status` reports the remote workspace status
through the adapter, and `tasks receive` is the remote half `send` invokes.

`tasks start-manager` starts managers on the computer: `--count N` submits the
generated batch script `N` times, `--workers N` fixes the workers per manager,
and leaving `--workers` off lets the queue's configured `workers=N` decide. Both
take `--workspace` and `--timeout`.

`tasks fetch` is the local half. It probes the remote workspace over the
adapter's `status` operation, asks it to `offer` what has stopped, `pull`s each
offered bundle into `.httk-workflow/transfers/incoming/`, imports it, and only
then tells the remote to `retire` the sources it still holds. Both the computer
and its workspace can be named explicitly:

```console
httk workflow tasks fetch --computer CLUSTER:large --remote-workspace /scratch/me/runs \
    --state succeeded --state failed --placement project/screening --json
```

`--remote-workspace` defaults to the queue's configured `workspace=PATH`, the
same setting `tasks send` uses. `--state` accepts the kinds a stopped job can be
in and defaults to `succeeded` and `failed`; `--placement` restricts the fetch to
one subtree; `--timeout` bounds every adapter operation the fetch runs, as it
does for `send`, `start-manager`, and `status`. A fetched job arrives as an
ordinary job of the local workspace, in
the terminal state and at the placement it had on the computer, so
`httk workflow harvest` then reports it exactly like a job that ran at home.

The other two commands are the remote half, invoked over the adapter by `tasks
fetch` but usable on their own on the computer itself:

```console
httk workflow tasks offer WORKSPACE --destination-workspace-id UUID --json
httk workflow tasks retire WORKSPACE JOB_ID ... --destination-workspace-id UUID
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
