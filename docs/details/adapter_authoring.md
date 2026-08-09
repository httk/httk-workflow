# Writing a remote adapter in detail

*For operators and integrators who need to reach a machine the packaged
`local`, `local-slurm`, and `ssh-slurm` templates do not cover.* This page is the
normative reference for the adapter contract: the bundle layout, the seven
operations and their exact JSON request and result documents, how settings and
credentials reach an adapter, and the rules an implementation must follow. The
operator-facing description of the same adapters — what each maintained kind
does, and which command-line options drive it — is in
{doc}`workflow_cli`.

A *remote adapter* is a versioned directory with one dispatcher executable.
Everything *httk-workflow* does on another machine — push a job bundle, run a
command, submit a manager, pull results back — is one *operation*, and every
operation runs the bundle's single `adapter` program. The engine never opens an
ssh connection, never spells out a scheduler command, and never parses anything
but the one JSON document that program prints.

## One executable, seven operations

There is one executable per bundle, not one per operation. The operation to run
is named inside the request JSON (`"operation": …`), so a single program serves
all seven. Earlier releases carried seven wrapper files —
`configure`, `install`, `invoke`, `push`, `pull`, `start-manager`, `status` —
but they were byte-identical two-line thin execs that differed only in the
operation word they passed along. There was no security boundary between them:
the operation name is already stated, and cross-checked, *inside* the request,
so which file was executed proved nothing the request did not already carry.
Collapsing them to one `adapter` removes a maintenance burden and a false
signal without giving up anything. The operation *names* are unchanged — they
are {py:data}`httk.workflow.adapters.ADAPTER_OPERATIONS` and the value of the
request's `operation` member — only the file layout changed.

## The bundle

```text
my-cluster/
├── remote.json          # the only member the engine reads directly
├── adapter              # the one dispatcher, executable, run for every operation
└── credentials.json     # written by the CLI, never by you, mode 0600
```

Bundles live in one of two places, and a project-local definition shadows a
global one of the same name:

| Scope | Location |
| --- | --- |
| project | `PROJECT/httk_project/remotes/NAME/` |
| global | `$XDG_CONFIG_HOME/httk/remotes/NAME/` |

The metadata file is `remote.json`, below a `remotes/` directory; those are the
only spellings a bundle is read under.

### Historical protocol names

The file was renamed; the *format identifiers* inside it and in every request and
result document were not. `httk-computer-adapter`, `httk-computer-request`, and
`httk-computer-result` are protocol: an adapter written against an earlier
release, or a bundle authored elsewhere, must keep validating and keep being
understood, and renaming an identifier would refuse it for no reason at all.
Read them as historical spellings of *remote*; every side already agrees on
them, and nothing new should be added under the older word.

### `remote.json`

The document is validated by
{py:func}`httk.workflow.adapters.validate_adapter_bundle` every single time the
bundle is resolved, added, or run — not once at installation. A bundle that stops
satisfying it stops being usable, which is the point.

```json
{
  "adapter_version": 1,
  "format": "httk-computer-adapter",
  "format_version": 1,
  "kind": "pbs",
  "settings": {"host": "login.example.org"},
  "required_binaries": ["rsync", "ssh"],
  "timeout_seconds": 300
}
```

There is no `operations` member. The bundle carries one executable named
`adapter`, which validation requires to exist and be executable; the operation
is selected by the request, not by a per-operation path.

| Member | Required | Meaning |
| --- | --- | --- |
| `format` | yes | must be `httk-computer-adapter` (the historical spelling; see above) |
| `format_version` | yes | must be `1` |
| `adapter_version` | yes | must be `1`; the version of the operation contract below |
| `settings` | no | flat machine-level settings; defaults to `{}` |
| `timeout_seconds` | no | positive number, default `60`; the wall-clock bound on one operation |
| `required_binaries` | no | array of program names that must be on `PATH` **of the machine running the adapter**, checked with `shutil.which` at every validation |
| `kind` | no | free-form; see below |

Beside `remote.json` the bundle must contain one executable file named
{py:data}`httk.workflow.adapters.ADAPTER_EXECUTABLE` (`adapter`); validation
refuses a bundle whose `adapter` is missing or not runnable.

`kind` is *not* interpreted by the loader. It is read only by
{py:mod}`httk.workflow.adapter_protocol` — the packaged implementation the
maintained templates execute — which dispatches on it and refuses any value
outside `local`, `local-slurm`, `ssh-slurm` rather than running the wrong code in
the wrong place. A custom adapter whose `adapter` executes your own program may
put whatever it likes there; setting a distinctive value is still worth doing,
because an `adapter` accidentally repointed at the packaged implementation then
refuses instead of, say, copying a cluster job into the local filesystem.

`required_binaries` is what makes `local-slurm` unusable on a machine without
`sbatch`, and it is checked locally, at validation time. Do not list binaries
that only exist on the far side of a connection: the local `ssh` client is a
local requirement, the remote `qsub` is not.

## The seven operations

Every operation runs the same `adapter` executable. It is started as

```text
adapter  /tmp/httk-adapter-XXXX.json
```

with no shell, no environment contract, and no stdin — one argument, the request
file. The program must:

1. read the one JSON request file named by `argv[1]`;
2. read `request["operation"]` to learn which operation to perform;
3. do the work;
4. print **exactly one** JSON result object on stdout;
5. exit `0`.

Diagnostics belong on stderr, where they are attached to the result as
`diagnostics` when the call otherwise succeeds.

The maintained template is a one-line dispatcher that executes the packaged
module:

```sh
#!/bin/sh
exec python3 -m httk.workflow.adapter_runtime "$@"
```

Which operation is running is fixed by the request's `operation` member and
nothing else; the module dispatches on it. A result whose `operation` disagrees
with the request is rejected by {py:func}`httk.workflow.adapters.run_adapter`.

### The request envelope

{py:func}`httk.workflow.adapters.run_adapter` composes every request. The
envelope is always present:

```json
{
  "format": "httk-computer-request",
  "format_version": 1,
  "operation": "invoke",
  "adapter_dir": "/home/me/project/httk_project/remotes/my-cluster",
  "remote_settings": {"host": "login.example.org"}
}
```

- `adapter_dir` is the absolute, resolved bundle directory. It is how an adapter
  finds its own files; nothing else tells it where it lives.
- `remote_settings` is the merge described under [Settings and credentials](#settings-and-credentials).
- Everything else is operation-specific and documented per operation below.

The request file is written with `sort_keys=True` and removed as soon as the
operation returns, whether it succeeded, failed, or timed out.

### The result envelope

```json
{"format": "httk-computer-result", "format_version": 1, "operation": "invoke", "ok": true}
```

`run_adapter` rejects a result that is not one JSON object, or whose `format`,
`format_version`, or `operation` disagree with the call it made. A *refusal* is
the same envelope with `ok: false` and a human-readable `error`:

```json
{"error": "cannot reach me@login.example.org: Permission denied", "format": "httk-computer-result",
 "format_version": 1, "operation": "configure", "ok": false}
```

### `configure`

Verify that a remote's settings can work, before the command line persists them.

Request members beyond the envelope:

| Member | Type | Meaning |
| --- | --- | --- |
| `settings` | object | the **pending** `--set KEY=VALUE` values, not yet stored anywhere |

Pending settings are passed separately because storage happens only after this
operation succeeds; an adapter that only looked at `remote_settings` could never
validate the first configuration of a host. Merge `settings` over
`remote_settings` and check the result.

```json
{"format": "httk-computer-request", "format_version": 1, "operation": "configure",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {},
 "settings": {"host": "login.example.org", "username": "me"}}
```

```json
{"connectivity": "ok", "configured": true, "format": "httk-computer-result",
 "format_version": 1, "operation": "configure", "ok": true}
```

The maintained implementation reports `connectivity` as `ok` when a remote `true`
answered, and `skipped` when there is no `host` or the remote sets
`check_connectivity=no`.

### `install`

Report — and only where explicitly opted into, arrange — that the target can run
*httk-workflow*, and ensure the remote's workspace root exists.

No request members beyond the envelope.

```json
{"format": "httk-computer-request", "format_version": 1, "operation": "install",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org", "username": "me",
                      "bootstrap": "pip"}}
```

```json
{"bootstrapped": false, "format": "httk-computer-result", "format_version": 1,
 "httk_command": ["httk"], "httk_version": "httk 2.0.0", "installed": true,
 "operation": "install", "ok": true, "workspace": "/scratch/me/runs",
 "workspace_created": true}
```

| Result member | Meaning |
| --- | --- |
| `installed` | `true` once a working `httk` was found |
| `httk_command` | the argument vector that answered, as an array |
| `httk_version` | its `--version` output, stripped |
| `bootstrapped` | whether this call installed anything (only ever under `bootstrap=pip`) |

Answering "httk-core is installed" is not enough: the maintained implementation
also runs `httk workflow workspace --help`, because the `workflow` command group
exists exactly when *this* package is installed beside the core. Installing
software on someone else's cluster account is never the default: without
`bootstrap=pip`, a target with no httk is a refusal carrying the instruction to
install it.

### `invoke`

Run one argument vector where this adapter's work belongs, and report what it
did.

| Member | Type | Meaning |
| --- | --- | --- |
| `argv` | array of nonempty strings, required | the command |
| `cwd` | string, optional | the directory to run it in |

```json
{"argv": ["httk", "workflow", "workspace", "status", "/scratch/me/runs", "--json"],
 "cwd": "/scratch/me", "format": "httk-computer-request", "format_version": 1,
 "operation": "invoke", "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 1, "operation": "invoke", "ok": true,
 "returncode": 0, "stdout": "{\"format\": \"httk-workflow-status\", ...}\n", "stderr": ""}
```

**A nonzero `returncode` is still `ok: true`.** The operation succeeded — it ran
the command and is reporting the outcome. `ok: false` means the adapter could not
run it at all. Callers check `returncode` themselves; every remote command in
`httk workflow transfer …` does exactly that and raises on the value.

If `argv[0]` is the literal `httk`, an adapter is expected to honour the remote's
`httk_command` setting by replacing that one element with the parsed vector; see
[Spelling `httk` on the target](#spelling-httk-on-the-target).

### `status`

Byte-for-byte the same contract as `invoke`, except that `cwd` is ignored. It
exists as a separate operation so that a health probe can be given a different
implementation, a different timeout, or different credentials from arbitrary
command execution. `httk workflow transfer REMOTE:NAME default` uses it to check
that the far side is a compatible workspace before anything moves.

```json
{"argv": ["httk", "workflow", "workspace", "status", "/scratch/me/runs", "--json"],
 "format": "httk-computer-request", "format_version": 1, "operation": "status",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 1, "operation": "status", "ok": true,
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

`files` entries are refused if absolute or if they contain `..`: a transfer
manifest must not be able to name anything outside the workspace it came from.

```json
{"destination": "/scratch/me/runs/.httk-workflow/transfers/incoming/6f1c…",
 "format": "httk-computer-request", "format_version": 1, "operation": "push",
 "source": "/home/me/ws/.httk-workflow/transfers/outgoing/6f1c…",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"}}
```

```json
{"format": "httk-computer-result", "format_version": 1, "operation": "push", "ok": true,
 "path": "/scratch/me/runs/.httk-workflow/transfers/incoming/6f1c…"}
```

`path` is where the data actually landed, and callers use it rather than the
destination they asked for. The maintained implementation reports the requested
destination for remote transfers and the *resolved absolute* path for local
copies, which is why the value is authoritative and the request is not.

A local copy onto an existing destination is idempotent when both sides carry the
identical `.httk-transfer/manifest.json`, and an error otherwise, so a resumed
transfer does not have to know whether the previous attempt finished.

### `start-manager`

Start one or more task managers on the target.

| Member | Type | Meaning |
| --- | --- | --- |
| `argv` | array of nonempty strings, required | the manager command, e.g. `["httk", "workflow", "manager", "run", "/scratch/me/runs"]` |
| `workspace` | string, optional | the workspace the managers serve |
| `count` | positive integer, optional | how many to start, default `1` |
| `cwd` | string, optional | honoured by process-starting implementations |

The `workspace` member is stated outright by every caller in this package. When
it is absent the maintained implementation reads it back out of a
`manager run WORKSPACE` argument vector; that fallback is documented for
hand-written requests and is not the normal path.

```json
{"argv": ["httk", "workflow", "manager", "run", "runs"], "count": 2,
 "format": "httk-computer-request", "format_version": 1, "operation": "start-manager",
 "adapter_dir": "/home/me/.config/httk/remotes/my-cluster",
 "remote_settings": {"host": "login.example.org"},
 "workspace": "/scratch/me/runs"}
```

The maintained protocol names the workspace first. A hand-written request may
use a path only when it contains a path separator or already names an existing
workspace directory; registry format and corruption errors are not treated as
paths.

Batch implementations report the submitted identifiers:

```json
{"count": 2, "format": "httk-computer-result", "format_version": 1,
 "job_ids": ["1840271", "1840272"], "operation": "start-manager", "ok": true,
 "script": "/scratch/me/runs/.httk-workflow/batch/manager-9c1f….sbatch"}
```

Process implementations report the process identifiers instead, with `pid`
naming the first, so a caller that only ever started one manager reads the field
it always did:

```json
{"count": 2, "format": "httk-computer-result", "format_version": 1,
 "operation": "start-manager", "ok": true, "pid": 40311, "pids": [40311, 40312]}
```

An adapter should append the workspace's configured `manager.workers` to `argv` only when the
request did not already choose one, so that an explicit `--workers` from the
command line always wins over the workspace's default.

## Settings and credentials

`httk workflow remote configure NAME --set KEY=VALUE` splits every assignment
in two, by name:

- keys in {py:data}`httk.workflow.adapters.PERSISTABLE_REMOTE_SETTINGS` —
  `bootstrap`, `check_connectivity`, `host`, `httk_command`, `legacy_settings`,
  `port`, `username`, `vasp_command`, and `vasp_pseudo_library` —
  are written into the flat `settings` object of the shareable, signable
  `remote.json`;
- **every other key** is a credential. It is written into
  `credentials.json` beside it, with mode `0600`, and project manifests exclude
  that file.

{py:func}`httk.workflow.adapters.remote_settings` merges the two back together —
`remote.json` first, `credentials.json` over it — and that single object is what
arrives as the request's `remote_settings`. **An adapter never sees the split.**
It reads one flat settings object and cannot tell, and must not care, which file
a value came from. `httk workflow remote show NAME` reports which file each
setting came from, and the *name* only — never the value — of every credential.

Two consequences worth stating:

- A credential is never a member of `remote.json`, so a signed project manifest
  covering the bundle covers no secret.
- Adding a persistable key means adding it to `PERSISTABLE_REMOTE_SETTINGS`. A key
  an adapter invents and the engine does not know about is treated as a secret,
  which is the safe direction to be wrong in.

Values arriving in `remote_settings` are strings as the operator typed them.
Validate them: the maintained implementation refuses a non-numeric `port`, a
`host` or `username` containing whitespace, a non-positive-integer `workers`, and
any batch directive value containing control characters.

## No shell, ever

**Every subprocess an adapter starts is an argument vector.** No value that came
from a request or from settings may be interpolated into a string that a shell
will parse. This is not a style rule; it is the reason a workspace path with a
space in it, or a hostile job tag, cannot become a command on a cluster login
node.

`ssh` is the one unavoidable exception in the protocol, because it always joins
the command words it is given and lets a login shell on the far side parse the
result. The convention for that exception is a *single* helper, used everywhere,
that quotes element-wise:

```python
def _shell_command(argv: Sequence[str], *, cwd: str | None = None) -> str:
    quoted = " ".join(shlex.quote(item) for item in argv)
    if cwd is None:
        return quoted
    return f"cd {shlex.quote(cwd)} && {quoted}"
```

Every remote command string, and the one line of a generated batch script that
runs the manager, is built by that helper and by nothing else. A generated batch
script is otherwise a shell program too, so the same rule applies to it: the
`exec` line is composed by the helper, and directive values are checked for
control characters rather than quoted into place.

`rsync` transfers pass `--protect-args`, so even file names travel inside the
protocol rather than through the remote shell. When an explicit `files` batch is
transferred, the list goes into a temporary file passed as `--files-from=` — not
onto the command line.

## Exit codes, refusals, and timeouts

There are three distinct ways an operation can end, and they are not
interchangeable.

| Ending | Exit | stdout | What the caller sees |
| --- | --- | --- | --- |
| success | `0` | one result with `ok: true` | the result dictionary, plus `diagnostics` if stderr was written |
| refusal | `0` | one result with `ok: false` and `error` | `RuntimeError(error)` |
| crash | nonzero | ignored | `RuntimeError("adapter OP failed (N): <stderr>")` |
| timeout | — | — | `TimeoutError("adapter OP exceeded N seconds")` |

A **refusal** is a well-formed answer: *I understood the request and will not, or
cannot, carry it out.* An unreachable host, a target without httk installed, an
unsupported `kind` — these are refusals, and the reason reaches the operator
verbatim. A **crash** is for what the adapter could not describe: a malformed
request, an unreadable bundle, an exception. The maintained implementation exits
`2` with one stderr line for those and `0` for every refusal.

Prefer refusals. An operator reading `cannot reach me@login.example.org:
Permission denied; set check_connectivity=no to configure the remote anyway` is
being told what to do next; an operator reading a traceback is not.

The timeout is `timeout_seconds` from `remote.json`, overridable per call by
`--adapter-timeout` on the command line. It is enforced by the *caller*, which
kills the operation; an adapter that may legitimately take minutes — an `rsync`
of a large campaign — belongs to a bundle whose `timeout_seconds` says so. The
packaged `ssh-slurm` template uses `300` for exactly this reason, against `60`
for `local`.

## A worked example: a PBS cluster

What follows is the *skeleton* of a custom bundle, not a working PBS
implementation. Everything specific to the site — how `qsub` is spelled, which
directives the remote wants, whether files travel by `rsync` or by a staging
service — lives in one Python module of yours, and the bundle is one `adapter`
dispatcher that calls it.

### The dispatcher

The bundle's `adapter` is one trivial script:

```sh
#!/bin/sh
# my-cluster/adapter
exec python3 -m mysite.httk_pbs "$@"
```

Make it executable (`chmod +x`); the bundle validator refuses one that is not.
Nothing else may live in it: keeping it trivial is what makes the "no shell"
rule cheap to hold, because no request value is ever visible to `sh`.

### The metadata

```json
{
  "adapter_version": 1,
  "format": "httk-computer-adapter",
  "format_version": 1,
  "kind": "pbs",
  "settings": {"host": "login.hpc.example.org"},
  "required_binaries": ["rsync", "ssh"],
  "timeout_seconds": 300
}
```

### The module

```python
"""A site PBS adapter for httk-workflow. The bundle's `adapter` executes this module."""

import json
import shlex
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

OPERATIONS = ("configure", "install", "invoke", "push", "pull", "start-manager", "status")


def result(operation: str, **values: object) -> None:
    print(json.dumps({"format": "httk-computer-result", "format_version": 1,
                      "operation": operation, "ok": True, **values}, sort_keys=True))


def refuse(operation: str, message: str) -> None:
    print(json.dumps({"error": message, "format": "httk-computer-result",
                      "format_version": 1, "operation": operation, "ok": False}, sort_keys=True))


def shell_command(argv: Sequence[str], *, cwd: str | None = None) -> str:
    """The one place this module composes a string a shell will parse."""

    quoted = " ".join(shlex.quote(item) for item in argv)
    return quoted if cwd is None else f"cd {shlex.quote(cwd)} && {quoted}"


def ssh(settings: dict, argv: Sequence[str], *, cwd: str | None = None):
    destination = settings["host"] if "username" not in settings \
        else f"{settings['username']}@{settings['host']}"
    return subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "--", destination, shell_command(argv, cwd=cwd)],
        text=True, capture_output=True, stdin=subprocess.DEVNULL, check=False,
    )


def batch_script(argv: Sequence[str], settings: dict, workspace: str) -> str:
    lines = ["#!/bin/bash", "#PBS -N httk-manager"]
    for key, directive in (("account", "-A"), ("partition", "-q")):
        if key in settings:
            lines.append(f"#PBS {directive} {settings[key]}")
        # ... nodes, walltime, and whatever else this site's scheduler needs
    lines += ["", "set -eu", f"cd {shlex.quote(workspace)}", f"exec {shell_command(argv)}", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1:
        print("adapter dispatcher expects one REQUEST.json path", file=sys.stderr)
        return 2
    (request_path,) = arguments
    try:
        request = json.loads(Path(request_path).read_text(encoding="utf-8"))
        operation = request.get("operation") if isinstance(request, dict) else None
        if not isinstance(operation, str) or not operation:
            raise ValueError("request carries no operation")
        settings = request.get("remote_settings") or {}
        if operation == "configure":
            pending = {**settings, **(request.get("settings") or {})}
            probe = ssh(pending, ["true"])
            if probe.returncode != 0:
                refuse(operation, f"cannot reach {pending.get('host')}: {probe.stderr.strip()}")
                return 0
            result(operation, configured=True, connectivity="ok")
        elif operation == "install":
            ...   # probe httk, mkdir -p the workspace, refuse with instructions
        elif operation in {"invoke", "status"}:
            cwd = request.get("cwd") if operation == "invoke" else None
            completed = ssh(settings, request["argv"], cwd=cwd)
            result(operation, returncode=completed.returncode,
                   stdout=completed.stdout, stderr=completed.stderr)
        elif operation in {"push", "pull"}:
            ...   # rsync --archive --protect-args, honouring `files` and `directory`
            result(operation, path=request["destination"])
        elif operation == "start-manager":
            ...   # write batch_script(...) on the far side, qsub it `count` times
            result(operation, job_ids=[...], count=request.get("count", 1))
        else:
            raise ValueError(f"unsupported adapter operation: {operation}")
    except (OSError, RuntimeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"adapter {operation}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

### Installing and using it

The bundle is an ordinary directory; put it where the CLI looks and configure a
remote:

```console
mkdir -p httk_project/remotes/my-cluster
cp -a my-cluster/. httk_project/remotes/my-cluster/
httk workflow remote configure my-cluster --set username=me
httk workflow remote install my-cluster
httk workflow workspace init my-cluster:/scratch/me/runs --name runs
httk workflow manager run my-cluster:runs --workers 8
```

Jobs reach the bundle through `transfer SRC DST`; the same adapter operations
above handle sending, remote execution, and fetching.

## Reading the maintained implementation

The definitive worked example is the shipped one.
{py:mod}`httk.workflow.adapter_protocol` is its public name and carries the
contract in its docstring; {py:mod}`httk.workflow.adapter_runtime` is the
implementation the `adapter` dispatcher executes. Both names refer to the same
objects. Read
`_shell_command`, `_rsync`, and `_batch_script` there before writing any code
that composes a command for another machine.
