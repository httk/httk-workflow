# Workflows by URI

*For workflow authors who want to share a workflow, and for users who want to
run one somebody else published.* A workflow package kept in a Git repository
is referenced by a git URI, and that URI is the installed workflow's id; its
manifest `[workflow] name` becomes its short name, used once the workflow is
known locally. Built-in and local workflows keep their plain names as ids.

```text
git+https://github.com/<org>/<repo>[@<ref>][#<subdir>]
```

## Publish a workflow

Commit a workflow package — a directory with `httk_workflow.toml`, see
{doc}`workflow_packages` — to a Git repository reachable over https without
credentials. One repository can hold several workflows, one per subdirectory:

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

When the repository root itself holds `httk_workflow.toml`, the URI needs no
`#subdir`.

## Reference it

Any place that takes a workflow takes a git URI: `httk job new --workflow`,
`httk campaign submit --workflow`, `httk workflow describe`,
{py:func}`~httk.workflow.scaffold.new_job`,
{py:func}`~httk.workflow.scaffold.resolve_workflow` and `Attempt.call`:

```console
httk job new --workspace WS \
    --workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax' \
    --input structure=POSCAR
httk workflow describe 'git+https://github.com/httk/workflows-vasp@main#vasp-relax'
```

`@ref` may be a branch, a tag, an abbreviated or full commit hash, or omitted
for the remote default branch. Referencing fetches the repository at that ref
and installs the workflow
({py:func}`~httk.workflow.git_workflows.fetch_workflow` does this directly and
returns the provider). The job records the **canonical URI**, with the ref
expanded to the full commit hash and the scheme and host lowercased, for
example
`git+https://github.com/httk/workflows-vasp@458aacb2493586faa2c9ac033334457569aeaf75#vasp-relax`.
The repository path is kept verbatim, so `…/workflows-vasp` and
`…/workflows-vasp.git` are different URIs. Only `git+https://`, `git+http://`
and `git+file://` are accepted; credentials, queries and ssh forms are refused.
Git runs without global or system configuration and without credential
helpers, so private repositories are not supported.

## Install without creating a job

`httk workflow install` fetches and installs without touching a workspace, and
prints each canonical URI and short name; `httk workflow uninstall` forgets
installed workflows again:

```console
httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workflow uninstall vasp.relax
```

`uninstall` takes a short name or a URI. A pinned URI removes that commit; an
unpinned URI or a short name removes every installed commit of that
repository and subdirectory. It refuses workflows registered in-process, and
for plugin workflows points to `httk plugin uninstall`. Cached checkouts stay.

## Short names

Once installed, the workflow is also reachable by its short name:

```console
httk job new --workspace WS --workflow vasp.relax --input structure=POSCAR
httk workflow list
```

When several commits of the same repository and subdirectory are installed,
the short name means the one referenced most recently, so referencing an older
or newer commit by URI switches what the short name selects. When two
different repositories or subdirectories claim one short name, the name is
refused and the URI must be used. In-process registrations and installed
plugins take precedence; an installed workflow whose short name they shadow
logs a warning and stays reachable by its URI.

## Definition and declaration

The git URI identifies the workflow **definition**: the code that ran. A
workflow may separately publish a **declaration**, a document describing its
inputs and outputs whose `$id` is the manifest's `declaration_uri`; see
{doc}`declarations`. The two are independent: a git workflow without
`declaration_uri` has no declaration `$id`. Collection records both on the
`Run`, as `workflow_definition_uri` (the job's pinned git URI) and
`workflow_declaration_uri`; see {doc}`provenance`.

## The same repository as a plugin

A repository may also carry an `httk_plugin.toml` listing its workflow
directories, so `httk plugin install git+https://github.com/httk/workflows-vasp`
installs them as a plugin. Both routes can coexist; plugin names take
precedence over installed short names.

## Cache and trust

Checkouts live under `data_home()/git` (`HTTK_DATA_HOME`, or
`~/.local/share/httk`), one per repository and commit, shared with other git
consumers such as project templates; installed workflow entries live under
`data_home()/workflows/installed`. A pinned URI whose checkout is cached is
reused without running git, and a package that could never be published (for
example one containing a symlink) is refused before it is installed.

Referencing a URI is consent to run its code with the same trust as an
installed plugin package. Its instantiate hook runs from the digest-pinned tree
published into the workspace; its collect hook and postprocess scripts run from
the installed cache tree, like those of plugin and registered-directory
packages. Collection never fetches: a finished job whose URI is not installed
on this machine is collected as a job without a provider (see
{doc}`collecting`), and nothing named in a job payload ever causes a download.

The full reference, including the grammar and resolution precedence, is in
{doc}`details/workflow_packages`; the API is {py:mod}`httk.workflow.git_workflows`
and the shared `httk.core.git_sources`.
