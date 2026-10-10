# Writing a remote adapter in detail

This page is the normative reference for the remote adapter contract, for
reaching a machine that the packaged `local`, `ssh`, `mount` and `mount-daemon`
templates do not cover. It specifies the base and optional operations with
their JSON request and result documents, how settings and credentials reach an
adapter, and the rules an implementation must follow. The operator-facing
description of the maintained adapters, and the command-line options that drive
them, is in {doc}`/details/workflow_cli`.

A *remote adapter* is a versioned directory with one dispatcher executable.
Everything *httk-workflow* does on another machine (push a job bundle, run a
command, pull results back) is one *operation*, and every operation runs the
bundle's single `adapter` program. The engine never opens an ssh connection
itself and parses only the JSON result that program prints. The optional
`daemon` operation relays a typed request to the destination broker, which owns
the protected scheduler policy.

## One executable for all operations

A bundle has one executable, not one per operation. The request JSON names the
operation in its `operation` member, so a single program serves every supported
operation. The operation names are
{py:data}`httk.workflow.adapters.ADAPTER_OPERATIONS`. Generic adapters leave
scheduling to the destination launcher; the `mount-daemon` adapter relays
requests to a protected broker.

## The bundle

```text
my-cluster/
├── remote.json          # the only member the engine reads directly
├── adapter              # the one dispatcher, executable, run for every operation
└── credentials.json     # written by the CLI, never by you, mode 0600
```

Bundles live in one of two places. A project-local definition shadows a global
one with the same name:

| Scope | Location |
| --- | --- |
| project | `PROJECT/httk_project/remotes/NAME/` |
| global | `$XDG_CONFIG_HOME/httk/remotes/NAME/` |

A bundle is read only as `remote.json` below a `remotes/` directory.

### Historical protocol names

The metadata file was renamed, but the format identifiers inside it and in every
request and result document were not. `httk-computer-adapter`,
`httk-computer-request` and `httk-computer-result` are protocol. An adapter
written against an earlier release, or a bundle authored elsewhere, must keep
validating, and renaming an identifier would refuse it for no benefit. Read
them as historical spellings of *remote*; add nothing new under the older word.

### `remote.json`

{py:func}`httk.workflow.adapters.validate_adapter_bundle` validates the document
every time the bundle is resolved, added or run, not once at installation. A
bundle that stops satisfying it stops being usable.

```json
{
  "adapter_version": 2,
  "format": "httk-computer-adapter",
  "format_version": 2,
  "kind": "pbs",
  "settings": {"host": "login.example.org"},
  "required_binaries": ["rsync", "ssh"],
  "timeout_seconds": 300
}
```

| Member | Required | Meaning |
| --- | --- | --- |
| `format` | yes | must be `httk-computer-adapter` (the historical spelling; see above) |
| `format_version` | yes | must be `2` |
| `adapter_version` | yes | must be `2`; the version of the operation contract below |
| `settings` | no | flat machine-level settings; defaults to `{}` |
| `timeout_seconds` | no | positive number, default `60`; the wall-clock bound on one operation |
| `required_binaries` | no | array of program names that must be on `PATH` **of the machine running the adapter**, checked with `shutil.which` at every validation |
| `kind` | no | free-form; see below |

There is no `operations` member. Beside `remote.json` the bundle must contain
one executable file named
{py:data}`httk.workflow.adapters.ADAPTER_EXECUTABLE` (`adapter`). Validation
refuses a bundle whose `adapter` is missing or not executable. The request, not
a per-operation path, selects the operation.

#### `kind`

The generic bundle loader does not interpret `kind`. The dedicated daemon CLI
requires `mount-daemon`, and its separate dispatcher requires that exact kind
on every call.

In the general runtime, `kind` is read only by
{py:mod}`httk.workflow.adapter_protocol`, the packaged implementation that the
maintained templates execute. It dispatches on `kind` and refuses any value
other than `local`, `ssh` and `mount`, rather than running the wrong code in
the wrong place. A custom adapter whose `adapter` executes your own program may
put any value there. A distinctive value is still useful: if `adapter` is
accidentally repointed at the packaged implementation, it then refuses instead
of, for example, copying a cluster job into the local filesystem.

#### `required_binaries`

`required_binaries` is checked locally at validation time. Do not list binaries
that exist only on the far side of a connection. The local `ssh` and `rsync`
clients are local requirements; a program used by a workspace launcher is not a
remote-adapter requirement.

## Running an operation

Every operation runs the same `adapter` executable, started as

```text
adapter  /tmp/httk-adapter-XXXX.json
```

with no shell, no environment contract and no stdin: one argument, the request
file. The program must:

1. read the one JSON request file named by `argv[1]`;
2. read `request["operation"]` to learn which operation to perform;
3. do the work;
4. print **exactly one** JSON result object on stdout;
5. exit `0`.

Diagnostics go to stderr. When the call otherwise succeeds, they are attached
to the result as `diagnostics`.

The maintained template is a one-line dispatcher that executes the packaged
module:

```sh
#!/bin/sh
exec python3 -m httk.workflow.adapter_runtime "$@"
```

The request's `operation` member alone decides which operation runs, and the
module dispatches on it. {py:func}`httk.workflow.adapters.run_adapter` rejects a
result whose `operation` disagrees with the request.

### The request envelope

{py:func}`httk.workflow.adapters.run_adapter` composes every request. The
envelope is always present:

```json
{
  "format": "httk-computer-request",
  "format_version": 2,
  "operation": "invoke",
  "adapter_dir": "/home/me/project/httk_project/remotes/my-cluster",
  "remote_settings": {"host": "login.example.org"}
}
```

- `adapter_dir` is the absolute, resolved bundle directory. It is the only way
  an adapter learns where its own files are.
- `remote_settings` is the merge described under [Settings and credentials](#settings-and-credentials).
- All other members are operation-specific and documented per operation below.

The request file is written with `sort_keys=True` and removed as soon as the
operation returns, whether it succeeded, failed or timed out.

### The result envelope

```json
{"format": "httk-computer-result", "format_version": 2, "operation": "invoke", "ok": true}
```

`run_adapter` rejects a result that is not one JSON object, or whose `format`,
`format_version` or `operation` disagree with the call it made. A *refusal* is
the same envelope with `ok: false` and a human-readable `error`:

```json
{"error": "cannot reach me@login.example.org: Permission denied", "format": "httk-computer-result",
 "format_version": 2, "operation": "configure", "ok": false}
```

## The base operations

### `configure`

Checks that a remote's settings can work, before the command line stores them.

Request members beyond the envelope:

| Member | Type | Meaning |
| --- | --- | --- |
| `settings` | object | the **pending** `--set KEY=VALUE` values, not yet stored anywhere |

Pending settings are passed separately because they are stored only after this
operation succeeds; an adapter that looked only at `remote_settings` could
never validate a host's first configuration. Merge `settings` over
`remote_settings` and check the result.

```json
{"format": "httk-computer-request", "format_version": 2, "operation": "configure",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {},
 "settings": {"host": "login.example.org", "username": "me"}}
```

```json
{"connectivity": "ok", "configured": true, "format": "httk-computer-result",
 "format_version": 2, "operation": "configure", "ok": true}
```

The maintained implementation reports `connectivity` as `ok` when a remote
`true` answered, and as `skipped` when there is no `host` or the remote sets
`check_connectivity=no`.

### `install`

Checks that the target can run *httk-workflow*. The CLI verb is
`httk remote check`; the operation keeps its historical protocol name
`install`, but an adapter never installs software. Setting httk up on the
target is the user's job, done by logging in there.

No request members beyond the envelope.

```json
{"format": "httk-computer-request", "format_version": 2, "operation": "install",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org", "username": "me"}}
```

```json
{"format": "httk-computer-result", "format_version": 2,
 "httk_command": ["httk"], "httk_version": "httk 2.1.0", "installed": true,
 "operation": "install", "ok": true}
```

| Result member | Meaning |
| --- | --- |
| `installed` | `true` once a working `httk` was found |
| `httk_command` | the argument vector that answered, as an array |
| `httk_version` | its `--version` output, stripped |

Finding *httk-core* is not enough. The maintained implementation also runs
`httk workspace --help`, because the `workflow` command group exists only when
this package is installed beside the core. A target without httk is a refusal
that carries the remedy: log in there and make sure *httk₂* is installed and
reachable from a non-interactive shell, or set `httk_command=` to where it
lives.

### `invoke`

Runs one argument vector where this adapter's work belongs and reports what it
did.

| Member | Type | Meaning |
| --- | --- | --- |
| `argv` | array of nonempty strings, required | the command |
| `cwd` | string, optional | the directory to run it in |

```json
{"argv": ["httk", "workflow", "workspace", "status", "/scratch/me/runs", "--json"],
 "cwd": "/scratch/me", "format": "httk-computer-request", "format_version": 2,
 "operation": "invoke", "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 2, "operation": "invoke", "ok": true,
 "returncode": 0, "stdout": "{\"format\": \"httk-workflow-status\", ...}\n", "stderr": ""}
```

**A nonzero `returncode` is still `ok: true`.** The operation succeeded: it ran
the command and reports the outcome. `ok: false` means the adapter could not
run the command at all. Callers check `returncode` themselves; every remote
command in `httk job transfer …` does so and raises on a nonzero value.

If `argv[0]` is the literal `httk`, the adapter must apply the remote's
`httk_command` setting; see
[Spelling `httk` on the target](#spelling-httk-on-the-target).

### `status`

The same contract as `invoke`, byte for byte, except that `cwd` is ignored. It
is a separate operation so that a health probe can have a different
implementation, timeout or credentials from arbitrary command execution.
`httk job transfer REMOTE:NAME default` uses it to check that the far side is a
compatible workspace before anything moves.

```json
{"argv": ["httk", "workflow", "workspace", "status", "/scratch/me/runs", "--json"],
 "format": "httk-computer-request", "format_version": 2, "operation": "status",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 2, "operation": "status", "ok": true,
 "returncode": 0, "stdout": "{\"format\": \"httk-workflow-status\", ...}\n", "stderr": ""}
```

### `push` and `pull`

Move one tree, or one explicit batch of files, to (`push`) or from (`pull`) the
target.

| Member | Type | Meaning |
| --- | --- | --- |
| `source` | nonempty string, required | where the data is now |
| `destination` | nonempty string, required | where it must end up |
| `directory` | boolean, optional | whether the transfer is of a directory's *contents*; inferred from the local side when absent |
| `files` | array of relative paths, optional | transfer only these, relative to `source`; implies a directory transfer |

`files` entries are refused if they are absolute or contain `..`, so a transfer
manifest cannot name anything outside the workspace it came from.

```json
{"destination": "/scratch/me/runs/.httk-workspace/transfers/incoming/6f1c….k4q2…",
 "format": "httk-computer-request", "format_version": 2, "operation": "push",
 "source": "/home/me/ws/.httk-workspace/transfers/outgoing/6f1c…",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 2, "operation": "push", "ok": true,
 "path": "/scratch/me/runs/.httk-workspace/transfers/incoming/6f1c….k4q2…"}
```

`path` is where the data actually landed, and callers use it instead of the
destination they asked for. The maintained implementation reports the requested
destination for remote transfers and the resolved absolute path for local
copies, so the result value is authoritative and the request value is not.

A local copy onto an existing destination is idempotent when both sides carry
the identical `bundle.json`, and an error otherwise. A resumed
transfer therefore does not need to know whether the previous attempt finished.

## The optional `daemon` operation

Protocol version 2 also recognizes `daemon`. It is optional, and existing
adapters may refuse it. The maintained `mount-daemon` bundle uses a separate
`python3 -m httk.workflow._daemon_adapter` dispatcher and refuses the generic
`invoke`, `status`, `push` and `pull` operations. A missing or changed `kind` is
an error; this dispatcher never falls back to local execution.

Beyond the standard request envelope, `daemon` accepts exactly:

| Member | Meaning |
| --- | --- |
| `daemon_request` | complete signed version-4 command object from {doc}`/details/workspace_daemon` |
| `wait_seconds` | optional finite number from 0.05 to 120, default 10 |

Before publication, the client checks the workspace and enrollment ids against
the configured endpoint (the enrollment id only when a daemon is pinned). Requests carry authorized httk identity signatures.
Responses must verify against the pinned daemon public key and match those
identities, the request id and canonical request digest, an allowed operation
outcome, and any requested handle. Paths, commands, argv, cwd, environment and
scheduler arguments are not accepted.

A confirmed response returns adapter process exit 0 with `ok: true`, the
canonical response JSON in `stdout`, an empty `stderr`, and a nested
`returncode`: 0 for `ready`, `submitted`, `status` or `cancel_requested`, and 2
for `refused`, `busy` or `uncertain`. A known negative daemon result thus
survives the adapter boundary. Failure to obtain a validated response is an
adapter error and may leave a live request; callers must keep its id and fields
for retry.

For this kind, `configure` merges pending settings, validates the settings
listed in {doc}`/details/remotes` (`exchange` and `daemon_workspace_id`
required, the daemon pins optional), and checks the mounted `exchange.json`
workspace identity. It publishes nothing. `install` rejects nonempty pending settings and
sends a health request when the daemon is pinned (otherwise it only checks
`exchange.json`); success means the matching daemon answered `ready`. It
does not install software or validate compute-node confinement.

## Settings and credentials

`httk remote configure --set KEY=VALUE NAME` sorts every assignment by
key:

- Keys in {py:data}`httk.workflow.adapters.PERSISTABLE_REMOTE_SETTINGS`
  (`check_connectivity`, `host`, `httk_command`, `legacy_settings`, `port`,
  `username`, the mount settings, `exchange` and the three `daemon_*` identity settings documented in
  {doc}`/details/remotes`, `vasp_command` and `vasp_pseudo_library`) are written
  into the flat `settings` object of the shareable, signable `remote.json`.
- **Every other key** is a credential. It is written into `credentials.json`
  beside it, with mode `0600`, and project manifests exclude that file.

{py:func}`httk.workflow.adapters.remote_settings` merges the two back together,
`remote.json` first and `credentials.json` over it. That single object arrives
as the request's `remote_settings`. **An adapter never sees the split:** it
reads one flat settings object and must not depend on which file a value came
from. `httk remote show NAME` reports which file each setting came
from, and shows only the *name* of each credential, never its value.

Two consequences:

- A credential is never a member of `remote.json`, so a signed project manifest
  covering the bundle covers no secret.
- Adding a persistable key means adding it to `PERSISTABLE_REMOTE_SETTINGS`. A
  key an adapter invents that the engine does not know is treated as a secret,
  which is the safe way to be wrong.

Values in `remote_settings` are strings as the operator typed them, so validate
them. The maintained implementation refuses a non-numeric `port`, a `host` or
`username` containing whitespace, a `workers` value that is not a positive
integer, and any batch directive value containing control characters.

### Spelling `httk` on the target

The `httk_command` setting names how `httk` is run on the target. When an
`invoke` request's `argv[0]` is the literal `httk`, an adapter is expected to
replace that one element with the parsed `httk_command` vector. The `install`
result reports the vector that answered as `httk_command`, and its refusal for
a target without httk suggests setting `httk_command=` to where httk lives.

## No shell

**Every subprocess an adapter starts is an argument vector.** No value from a
request or from settings may be interpolated into a string that a shell will
parse. This is what prevents a workspace path containing a space, or a hostile
job tag, from becoming a command on a cluster login node.

`ssh` is the one unavoidable exception, because it always joins the command
words it is given and lets a login shell on the far side parse the result. The
convention is a *single* helper, used everywhere, that quotes element-wise:

```python
def _shell_command(argv: Sequence[str], *, cwd: str | None = None) -> str:
    quoted = " ".join(shlex.quote(item) for item in argv)
    if cwd is None:
        return quoted
    return f"cd {shlex.quote(cwd)} && {quoted}"
```

Every remote command string is built by that helper and nothing else. A manager
launcher owns any generated scheduler script and its quoting; see
{doc}`/details/launchers` for that separate contract.

`rsync` transfers pass `--protect-args`, so even file names travel inside the
protocol rather than through the remote shell. An explicit `files` batch is
written to a temporary file passed as `--files-from=`, not placed on the command
line.

## Exit codes, refusals, and timeouts

An operation can end in one of these distinct ways:

| Ending | Exit | stdout | What the caller sees |
| --- | --- | --- | --- |
| success | `0` | one result with `ok: true` | the result dictionary, plus `diagnostics` if stderr was written |
| refusal | `0` | one result with `ok: false` and `error` | `RuntimeError(error)` |
| crash | nonzero | ignored | `RuntimeError("adapter OP failed (N): <stderr>")` |
| timeout | — | — | `TimeoutError("adapter OP exceeded N seconds")` |

A **refusal** is a well-formed answer: the adapter understood the request and
will not, or cannot, carry it out. An unreachable host, a target without httk
installed, or an unsupported `kind` are refusals, and the reason reaches the
operator verbatim. A **crash** is for what the adapter could not describe: a
malformed request, an unreadable bundle, an exception. The maintained
implementation exits `2` with one stderr line for a crash and `0` for every
refusal.

Prefer refusals. A message such as `cannot reach me@login.example.org:
Permission denied; set check_connectivity=no to configure the remote anyway`
tells the operator what to do next; a traceback does not.

The timeout is `timeout_seconds` from `remote.json`, overridable per call with
`--adapter-timeout` on the command line. The caller enforces it by killing the
operation. An adapter that may legitimately take minutes, such as an `rsync` of
a large campaign, belongs in a bundle whose `timeout_seconds` allows for it.

### Scheduler sites

For a PBS site, write a custom adapter that implements the six base operations
and uses `qsub` only when a command is explicitly invoked on that site. The
manager launch policy belongs to the target workspace's launcher, not to the
remote adapter. See {doc}`/details/launcher_authoring` for the compact PBS launcher
example, including its batch directives, script lifecycle and
partial-submission rules.

## Reading the maintained implementation

The shipped implementation is the definitive worked example.
{py:mod}`httk.workflow.adapter_protocol` is its public name and carries the
contract in its docstring; {py:mod}`httk.workflow.adapter_runtime` is the
implementation the `adapter` dispatcher executes. Both names refer to the same
objects. Read `_shell_command` and `_rsync` there before writing code that
composes a command for another machine.
