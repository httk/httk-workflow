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

`httk workflow describe URI` fetches and describes without installing.
`httk workflow uninstall --workspace WS SELECTOR` removes an installation by
id or short name; jobs are not checked unless `--check` is given.

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
short name; two installations sharing it are refused, and the id must be
used. `httk workflow list --workspace WS` lists them.

On this machine, outside any workspace, a fetched workflow is also known by
its short name, so `httk workflow install --workspace WS vasp.relax` installs
it. With several commits of one repository and subdirectory fetched, the
short name selects the most recently referenced one; when different
repositories or subdirectories claim the same name, it is refused and the URI
must be used. In-process registrations and installed plugins take precedence;
a fetched workflow they shadow logs a warning and stays reachable by its URI.
`httk workflow list` without `--workspace` lists them.

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
runs a workflow only once it is installed there.

## Cache and trust

Checkouts live under `data_home()/git` (`HTTK_DATA_HOME`, or
`~/.local/share/httk`), one per repository and commit, shared with other git
consumers such as project templates; fetched workflow entries live under
`data_home()/workflows/installed`. A pinned URI with a cached checkout is
reused without running git. A package that could never be installed (for
example one containing a symlink) is refused when it is fetched.

Installing a URI is consent to run its code with the trust of an installed
plugin package. Its runner runs from the workspace's installed copy, and its
instantiate hook from that copy verified against its tree digest; its collect hook and
postprocess scripts run from the cache tree, like those of plugin and
registered-directory packages, or from the verified installed copy with
`--allow-job-collector` (see {doc}`/details/collecting`). Nothing named in a
job payload causes a download: managers and collection only look up what is
already installed or fetched.

The full manifest reference is in {doc}`/details/workflow_packages`. The API
is {py:mod}`httk.workflow.git_workflows`, built on the shared
`httk.core.git_sources`.
