# Setting up workspaces

A **workspace** is a directory tree that holds jobs and their state. A
**project** groups workspaces together with the remotes, launchers and settings
they share, and is what you copy when work moves to another machine.
**Managers** are the processes that run the jobs in a workspace. This page
sets all of that up; {doc}`running` then creates and runs jobs in it.

## Project and workspace

```console
httk init --name "Your Name" --email you@example.org
httk project init --name campaign .
httk workspace init --name default workspace
httk workspace status
```

`httk init` creates your per-user operator identity, used to attribute and
sign what you publish; it is idempotent. `project init` writes the project
anchor `httk_project/`. `workspace init` creates a workspace and registers it
under a name in the per-user registry, so every later command can address it
with `--workspace NAME`; without `--workspace`, commands use the enclosing
workspace, then the project's default, then the per-user default. A workspace
is a directory of its own (`workspace init` refuses a non-empty directory, so
a project root is never a workspace); a project records its workspaces as
members, and the first workspace initialized inside a project becomes its
default.

```console
httk workspace list
httk workspace default default
httk workspace status --json default
```

State changes are synchronized to storage by default. `workspace init
--no-durable` makes a throwaway workspace faster at the price of recoverability
after a node crash.

## Settings

A workspace carries a flat map of *application settings*, small values a
runner resolves at run time, such as the VASP command:

```console
httk workspace configure default --set vasp.command=vasp_std
httk workspace show default
httk workspace configure default --unset vasp.command
```

The manager exports each scalar setting into the attempt environment under its
`HTTK_` name, so `vasp.command` becomes `HTTK_VASP_COMMAND`. A real
environment variable is a deployment override and wins over the workspace
setting; a job's own parameter of the same name wins over both.

Engine tunables (the visibility deadline of a shared filesystem and the
retention limits of `gc`) are *workspace policy*, kept apart from application
settings and edited with `workspace policy show|set`; see
{doc}`details/taskmanager`.

## Installed workflows

A workspace runs only workflows installed in it, under `workflows/`. Install
once per workspace, locally or on a remote:

```console
httk workflow install --workspace default 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workspace workflows default
```

`job new --install` installs a missing workflow on the fly. Workflows never
travel with jobs: install them in every workspace that runs those jobs.

## Environment preludes

Shell setup an HPC job needs (`module load`, `source activate`) goes in a
prelude, not in the command setting. There are two layers, both run under
`set -e` in a login shell, so a failing line aborts the job instead of running
it in a half-configured environment:

```console
httk workspace settings set --key environment.prelude --value "module load httk" default
httk workspace workflow-prelude set --workflow vasp.relax --value "module load VASP/6.4.2" default
```

`environment.prelude` applies to every job of the workspace and runs before
the manager starts; after it, the manager command is looked up on the resulting
`PATH`. A workflow prelude applies only to that workflow's jobs and runs after
the workspace prelude. Preludes are workspace-local: a job transferred to
another workspace runs under that workspace's preludes.

## Launchers

A launcher decides *where managers start*. The built-in `process` launcher
starts them on the current machine. On a cluster, add the packaged Slurm
launcher once, then select it per workspace together with the scheduler
directives it should use:

```console
httk launcher add --template slurm --global cluster
httk launcher check cluster
httk workspace settings set --key manager.launch --value cluster default
httk workspace settings set --key slurm.partition --value batch default
httk workspace settings set --key slurm.time_limit --value 02:00:00 default
httk workflow run --count 4 --workspace default
```

`httk workflow run --count 4` now submits four managers through Slurm and
prints their scheduler job ids. `--inline` runs one manager in the current
process regardless of the launcher, which is the way to see manager output
while debugging; `--launcher NAME` picks another launcher for one invocation.
Launchers live at `httk_project/launchers/NAME` or
`~/.config/httk/launchers/NAME`, project first.

The full guide, {doc}`details/launchers`, lists every `slurm.*` and `manager.*`
setting, shows how to keep separate CPU and GPU launcher profiles, and gives
the Python API; {doc}`details/launcher_authoring` specifies the launcher bundle
for other schedulers.

## Remotes

A remote reaches another machine: it moves files and runs `httk` there. It
does not schedule anything; the destination workspace's launcher does that.

```console
httk remote add --template ssh kappa
httk remote configure --set host=login.example.org --set username=me kappa
httk remote configure --set prelude='module load Python/3.13' kappa
httk remote check kappa
```

`ssh` runs commands in a non-interactive shell, so the remote's `prelude`
setting is where `httk` is put on `PATH` for those commands. A workspace on the
remote is created and addressed by writing the remote name before it:

```console
httk workspace init --name runs kappa:/scratch/me/httk/runs
httk workspace settings set --key manager.launch --value cluster kappa:runs
httk workspace status kappa:runs
httk workflow run --workspace kappa:runs --count 4
```

`NAME:WORKSPACE` is a binding, not a path. Jobs are created locally and moved
with `httk job transfer`, after `httk workflow install --workspace kappa:runs …`
installed their workflow there; see {doc}`running`. Besides `ssh`, the packaged
templates are `local` (a second tree on this machine), `mount` (files over a
shared mount, commands through a separate executor) and `mount-daemon` (files
as the only channel, through an exchange directory: the workspace's managers serve it, and the confined broker only starts, queries and cancels managers).

The full guide, {doc}`details/remotes`, covers the mount variants, moving job
trees, and the Python adapter API; {doc}`details/adapter_authoring` specifies
the adapter bundle, and {doc}`details/workspace_daemon` the confined broker.

## Moving a project between machines

A project directory is self-describing. Copy the whole tree, including its
workspaces, and run `httk project repair` inside it: every member workspace is
registered under the name recorded in `httk_project/members.json`, and
`httk workflow run`, `--workspace NAME` and the rest work without a new
`init`. It is idempotent and refuses to overwrite a name that already points
elsewhere.

## Further reading

- {doc}`details/taskmanager` covers workspace policy, durability and
  filesystem requirements, resources and scheduling, owners and recovery, and
  maintenance with `fsck` and `gc`.
- {doc}`details/workflow_cli` is the complete command reference, including
  the `workspace`, `launcher`, `remote`, `config` and `project` groups.
- {doc}`campaigns` spreads one very large run across several workspaces.
