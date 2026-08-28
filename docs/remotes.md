# Remotes

*For operators who need to reach another machine.* A remote is a bundle that
combines transport, file movement, and command execution: it can move a job or
workspace tree, invoke `httk` there, and report status. A remote never schedules
managers. Scheduling belongs to the launcher selected by the destination
workspace; see {doc}`launchers` for that side of the workflow.

A remote bundle contains `remote.json` and one executable named `adapter`, with
optional `credentials.json` for values that should not enter the shareable
metadata. Project remotes live at `httk_project/remotes/NAME`; global remotes
live at `~/.config/httk/remotes/NAME`. Project definitions take precedence over
global definitions with the same name.

## Setting one up

Create an SSH remote, configure its connection, and verify that a compatible
`httk` answers on the other machine:

```console
$ httk workflow remote add --template ssh kappa
$ httk workflow remote configure \
      --set host=login.example.org \
      --set username=me \
      --set check_connectivity=yes kappa
$ httk workflow remote check kappa
```

`remote check` invokes the adapter's historical `install` operation; despite
that protocol name, the maintained adapters verify the remote and do not
install anything. Settings that are credentials are stored in
`credentials.json`, which is excluded from signed project manifests. Use
`remote show [--json]` to inspect a definition without printing credential
values; `remote list`, `remote remove`, and `remote import-v1` cover the other
common management tasks.

Initialize a named workspace on the remote by putting the remote name before
the path:

```console
$ httk workspace init --name runs kappa:/scratch/me/httk/runs
$ httk workspace status kappa:runs
```

The `NAME:WORKSPACE` spelling is a binding, not a filesystem path. Transfer a
job into that workspace and run its manager there:

```console
$ httk workflow transfer --job JOB default kappa:runs
$ httk workflow run --workspace kappa:runs --count 4
```

The remote invocation asks the owning machine to run
`httk workflow manager run --workspace runs --detach …`. The target workspace
then applies its own `manager.launch`, `manager.workers`, scheduler settings,
and `environment.prelude`, exactly as if the command had been run on the
login node. Fetch finished jobs back with the reverse transfer:

```console
$ httk workflow transfer kappa:runs default
```

Names listed in `machine_names` are self-addressing: `login:runs` is treated as
a local workspace binding when `login` is configured as one of this machine's
names, so it does not invoke a remote adapter. To use a second tree on the
same host through the adapter contract, create a distinct remote with the
`local` template:

```console
$ httk workflow remote add --template local local-tree
$ httk workspace init --name scratch local-tree:/tmp/me/httk/scratch
```

## From Python

The low-level adapter API is in `httk.workflow.adapters`. This example uses the
`local` template so it can be exercised without an SSH server; use the CLI to
configure an SSH remote's persisted settings, because there is no single
high-level Python equivalent of `remote configure --set`:

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

`add_remote` creates a maintained adapter bundle, `resolve_remote` applies
project-before-global resolution, and `run_adapter` executes one of the six
adapter operations. `probe_remote_workspace` validates the remote status
document and returns the remote workspace UUID and root. The registry's
`resolve_workspace` keeps the `NAME:WORKSPACE` binding in one place; a remote
workspace has no local path until the adapter reports it.

## Writing a remote adapter

A custom remote is a versioned bundle with `remote.json`, one executable named
`adapter`, and optional `credentials.json`. The dispatcher answers six
operations — `configure`, `install` (the operation behind `remote check`),
`invoke`, `push`, `pull`, and `status` — with one JSON result per invocation.
It must implement transport, file movement, and remote command execution while
leaving manager scheduling to the destination workspace's launcher. The engine
refuses malformed metadata, a missing or non-executable dispatcher, unavailable
required binaries, unsupported operations, non-zero dispatcher exits, and
malformed or unsuccessful result documents.

The complete bundle layout, operation request and result documents, settings and
credential handling, and refusal rules are in {doc}`details/adapter_authoring`.
