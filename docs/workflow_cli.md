# Project and workflow command line

*For operators and campaign owners.* Everything is one nested command tree —
`httk workflow …` — where every group and command answers `--help`:

```text
httk workspace           init | list | default | status | managers | workflows | settings | fsck | gc | seal | unseal | ...
httk job                 new | submit | request | delete | seal | unseal | list | show | log | why | debug | transfer
httk workflow runner     publish | describe
httk workflow build      (compiled packages: build and register binaries)
httk workflow list | describe | precheck | collect | postprocess
httk workflow seal       verify
httk workflow manager    run
httk workflow campaign   init | show | submit | collect | start-"managers"
httk workflow remote     list | add | configure | check | show | remove | daemon
httk workflow remote daemon health | start | status | cancel
httk workflow transfer receive | offer | retire   (hidden protocol; remote peers invoke it by exact name)
httk project             init | show | import-v1 | repair | manifest | seal | unseal | verify-seal   (all core-owned; httk-workflow registers the workspace as a project member so these verbs cover it)
httk init | identity     (core-owned: establish/manage per-user named operator identities)
httk workflow config | v1
```

{doc}`quickstart` walks the everyday sequence; {doc}`taskmanager` explains the
operator concepts behind it.

The full reference, {doc}`details/workflow_cli`, documents every command and
option — projects and signed manifests, configuration, remotes and transfers,
and the protocol spellings.

Manager capacities can be advertised with repeatable `--worker-resource NAME
COUNT` options. `--count N` starts multiple managers at the workspace's launch
site. A local workspace uses its `manager.launch` setting; a remote workspace
is reached through its adapter, which invokes the same manager command on the
owning machine with `--detach`.

`httk workspace daemon ABS_WORKSPACE --policy ABS_POLICY` starts the confined
foreground Slurm broker. See {doc}`workspace_daemon` for its protected deployment
layout, enrollment, serial/MPI profiles and current limits. This is a separate path
from ordinary manager launching.

`httk workflow remote daemon` supplies the four typed client controls for a
`mount-daemon` remote. Transfer jobs through absolute mounted workspace paths;
see {doc}`remotes` for configuration, request IDs and retry behavior.


Inside a daemon MPI manager, `httk workflow mpi run -- APPLICATION ARG...` launches
one application across the protected profile's ranks. It streams output and
returns the application status; see {doc}`workspace_daemon` for shared-memory
configuration and site acceptance requirements.
