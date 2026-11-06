# Workflows by URI

A workflow package kept in a Git repository can be shared and run by its git
URI. The commit-pinned URI is the workflow's id in every workspace it is
installed in, and its manifest `[workflow] name` is its short name. A package
directory installs as `local:<name>` and a runner file as
`adhoc:<name>@<sha12>`.

```text
git+https://github.com/<org>/<repo>[@<ref>][#<subdir>]
```

## Publish a workflow

Commit a workflow package (a directory with `httk_workflow.toml`, see
{doc}`/details/workflow_packages`) to a Git repository reachable over https without
credentials. One repository can hold several workflows, one per
subdirectory:

```text
workflows-vasp/
├── httk_plugin.toml          # optional, for `httk plugin install`
├── vasp-relax/
│   ├── httk_workflow.toml    # [workflow] name = "vasp.relax"
│   └── run
└── vasp-static/
    ├── httk_workflow.toml
    └── run
```

If the repository root holds `httk_workflow.toml`, the URI needs no
`#subdir`.

## Install it

A job runs only a workflow installed in its workspace, so a git URI is
installed first, or with `--install` on first use:

```console
httk workflow install --workspace WS 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workspace WS --workflow vasp.relax --input structure=POSCAR

httk job new --workspace WS --install \
    --workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax' \
    --input structure=POSCAR
```

`httk campaign submit --install`, {py:func}`~httk.workflow.scaffold.new_job`
with `install=True`, and `[workflow.calls]` of an installed package (which
must pin the full commit) install the same way.

`@ref` is a branch, a tag, an abbreviated or full commit hash, or omitted for
the remote default branch. Installing fetches the repository at that ref into
this machine's cache ({py:func}`~httk.workflow.git_workflows.fetch_workflow`)
and copies the package into the workspace's `workflows/` store under the
**canonical URI**: the ref expanded to the full commit hash and the scheme and
host lowercased, for example
`git+https://github.com/httk/workflows-vasp@458aacb2493586faa2c9ac033334457569aeaf75#vasp-relax`.
Every job of it records that id. The repository path is kept verbatim, so
`…/workflows-vasp` and `…/workflows-vasp.git` are different URIs.

Only `git+https://`, `git+http://`, and `git+file://` are accepted;
credentials, queries, and ssh forms are refused. Git runs without global or
system configuration and without credential helpers, so private repositories
are not supported.

A URI without `#subdir` whose repository root carries an `httk_plugin.toml`
(see [below](#the-same-repository-as-a-plugin)) installs every workflow that
file lists ({py:func}`~httk.workflow.git_workflows.fetch_workflows`), each
under its pinned member URI. A call from one listed workflow to another, by
name or alias, binds to the member at the same commit, and the plugin's
`requires` apply to every member:

```console
httk workflow install --workspace WS 'git+https://github.com/httk/workflows-vasp-atomate2@wf-v0.2.0'
```

A root `httk_workflow.toml` is still a single package. A `REMOTE:WS` workspace
gets the pinned repository URI and expands it there. `httk job new
--workflow` and `httk workflow describe` still need `#subdir`.

`httk workflow describe URI` fetches and describes without installing.
`httk workflow uninstall --workspace WS SELECTOR` removes an installation by
id, short name or `NAME@REF` (a selector several installations share is
refused); jobs are not checked unless `--check` is given.

## The machine cache

Without `--workspace`, `httk workflow install URI` only fetches into this
machine's cache and prints each canonical URI and short name, and
`httk workflow uninstall SELECTOR` forgets fetched workflows:

```console
httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow uninstall vasp.relax
```

A pinned URI forgets that commit; an unpinned URI or a short name forgets
every fetched commit of that repository and subdirectory. It refuses
workflows registered in-process and points plugin workflows to
`httk plugin uninstall`. Cached checkouts stay.

## Short names

Within a workspace, `--workflow vasp.relax` selects the installation with that
short name; when several installations share it, the most recently installed
one. `--workflow vasp.relax@REF` selects among them by the tag or branch
the installation was made from (as given to `httk workflow install`; an
install on a remote workspace records none), or by at least seven characters
of its pinned commit, as in `vasp.relax@wf-v0.2.0` or `vasp.relax@561f44b`.
An installation keeps one reference, the tag or branch of its last install
(none when last installed by commit): two tags at one commit are one
installation. A name that an installation itself carries, `@` included,
selects by name first. `httk workflow list --workspace WS` lists the
installations with their ids, which always select exactly one, and shows a
recorded reference as `NAME@REF`. `httk workflow uninstall` refuses a
selector that several installations share, and `httk workflow build` builds
the newest. A `[workflow.calls]` entry written as a short name binds at install
time to the newest installation of that name and is then recorded by its id.

On this machine, outside any workspace, a fetched workflow is also known by
its short name, so `httk workflow install --workspace WS vasp.relax` installs
it. With several commits of one repository and subdirectory fetched, the
short name selects the most recently referenced one; when different
repositories or subdirectories claim the same name, it is refused and the URI
must be used. In-process registrations, installed plugins and
[pip-installed workflow repositories](#install-a-workflow-repository-with-pip)
take precedence, in that order; a fetched workflow they shadow logs a warning
and stays reachable by its URI. `httk workflow list` without `--workspace`
lists them.

## Install a workflow repository with pip

A workflow repository that carries the *httk-workflow* repository build
backend can be installed into a Python environment, which installs its
workflows' Python dependencies and makes all its workflow packages known in
that environment:

```console
git clone https://github.com/httk/workflows-vasp
pip install ./workflows-vasp        # or: pip install -e ./workflows-vasp
httk workflow install --workspace WS vasp.relax
```

The backend is one stdlib-only file, `tools/workflow_repository_build.py`,
whose reference copy is in
[workflows-examples](https://github.com/httk/workflows-examples). The
repository's `pyproject.toml` names it and declares the canonical repository
URI, which is never derived from a git remote:

```toml
[build-system]
requires = []
build-backend = "workflow_repository_build"
backend-path = ["tools"]

[project]
name = "workflows-vasp"
dynamic = ["version"]
requires-python = ">=3.12"
dependencies = ["httk-workflow>=2.1.1"]

[tool.httk.workflow-repository]
repository = "git+https://github.com/httk/workflows-vasp"
```

The installed version is the HEAD commit. Its packages (the top-level
directories holding `httk_workflow.toml`) are known by short name and by
pinned URI, and the packages of every `wf-v*` tag by pinned URI and by a
`@wf-vX.Y.Z` ref, all without the network. Each workflow's id is exactly the
canonical URI a GitHub install of that commit gives, for example
`git+https://github.com/httk/workflows-vasp@<commit>#vasp-relax`, so jobs and
workspaces move freely between a machine that installed it with pip and one
that fetched it from GitHub.

Only committed content is installed; the tree is always taken from git
objects, never from the working tree, so install a package path (or use
`--workflow-dir`) to run a working copy. An editable install follows the
clone's current HEAD and tags; a wheel offers the HEAD and tags of the time it
was built, so after adding a tag at an unchanged HEAD reinstall with
`pip install --force-reinstall --no-deps .`. A running process keeps the pip
state of its first lookup; restart it after `pip install`, `git checkout` or
`git tag`, as for plugins.

Nothing is written to this machine's installed entries
(`data_home()/workflows/installed`), so `pip uninstall` removes the workflows
entirely; only checkout trees stay in the cache. `httk workflow uninstall`
without `--workspace` points to `pip uninstall` for them. Plugin names take
precedence over pip names, and pip names over fetched short names.

## Definition and declaration

The git URI identifies the workflow **definition**, the code that ran. A
workflow may separately publish a **declaration**, a document describing its
inputs and outputs whose `$id` is the manifest's `declaration_uri` (see
{doc}`/details/declarations`). The two are independent: a git workflow without
`declaration_uri` has no declaration `$id`. Collection records both on the
`Run`, as `workflow_definition_uri` (the job's pinned git URI) and
`workflow_declaration_uri`; see {doc}`/details/provenance`.

## The same repository as a plugin

A repository may also carry an `httk_plugin.toml` listing its workflow
directories, so that `httk plugin install git+https://github.com/httk/workflows-vasp`
makes them known on this machine as a plugin. Both routes can coexist, and
plugin names take precedence over fetched short names; either way a workspace
runs a workflow only once it is installed there. `httk workflow install` of the
repository URI without `#subdir` installs every listed workflow into a
workspace ([Install it](#install-it)).

## Cache and trust

Checkouts live under `data_home()/git` (`HTTK_DATA_HOME`, or
`~/.local/share/httk`), one per repository and commit, shared with other git
consumers such as project templates; fetched workflow entries live under
`data_home()/workflows/installed`. A pinned URI with a cached checkout is
reused without running git. Lookups of pip-installed workflow repositories
may run local git against the installed clone or bundle to fill the cache,
never the network; those sources are fixed by what the operator installed,
never by a job payload. A package that could never be installed (for
example one containing a symlink) is refused when it is fetched.

Installing a URI is consent to run its code with the trust of an installed
plugin package. Its runner runs from the workspace's installed copy, and its
instantiate hook from that copy verified against its tree digest; its collect hook and
postprocess scripts run from the cache tree, like those of plugin and
registered-directory packages, or from the verified installed copy with
`--allow-job-collector` (see {doc}`/details/collecting`). Nothing named in a
job payload causes a download: managers and collection only look up what is
already installed, fetched or pip-installed.

The full manifest reference is in {doc}`/details/workflow_packages`. The API
is {py:mod}`httk.workflow.git_workflows`, built on the shared
`httk.core.git_sources`.
