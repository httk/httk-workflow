# Workflows by IRI

*For workflow authors who want to share a workflow, and for users who want to
run one somebody else published.* A workflow package kept in a Git repository
is referenced by a git IRI, and that IRI is the fetched workflow's id; its
manifest id becomes its short name, used once the workflow is known locally.
Built-in and local workflows keep their plain ids:

```text
git+https://github.com/<org>/<repo>@<ref>#<subdir>
```

## Publish a workflow

Commit a workflow package — a directory with `httk_workflow.toml`, see
{doc}`workflow_packages` — to a Git repository reachable over https. One
repository can hold several workflows, one per subdirectory:

```text
workflows-vasp/
├── httk_plugin.toml          # optional, for `httk plugin install`
├── vasp-relax/
│   ├── httk_workflow.toml    # [workflow] id = "vasp.relax"
│   └── run
└── vasp-bands/
    ├── httk_workflow.toml
    └── run
```

The manifest `[workflow] id` becomes the workflow's short name. When the
repository root itself holds `httk_workflow.toml`, the IRI needs no `#subdir`.

## Reference it

Any place that takes a workflow takes an IRI: `httk job new --workflow`,
`httk workflow describe`, {py:func}`~httk.workflow.scaffold.new_job`,
{py:func}`~httk.workflow.scaffold.resolve_workflow` and `Attempt.call`:

```console
httk job new --workspace WS \
    --workflow 'git+https://github.com/httk/workflows-vasp@468f14262805f0b63f54296e7cb21c81033ed5c2#vasp-relax' \
    --input structure=POSCAR
httk workflow describe 'git+https://github.com/httk/workflows-vasp@main#vasp-relax'
```

`@ref` may be a branch, a tag, an abbreviated or full commit hash, or omitted
for the remote default branch. Referencing fetches the repository at that ref
and installs the workflow ({py:func}`~httk.workflow.git_workflows.fetch_workflow`
does this directly and returns the provider). The job records the **canonical IRI**, with the ref
expanded to the full commit hash and the host lowercased, for example
`git+https://github.com/httk/workflows-vasp@468f14262805f0b63f54296e7cb21c81033ed5c2#vasp-relax`.
The repository path is kept verbatim, so `…/workflows-vasp` and
`…/workflows-vasp.git` are different IRIs. Only `git+https://`, `git+http://`
and `git+file://` are accepted (the scheme is case-insensitive); credentials,
queries and ssh forms are refused. Git runs without global or system
configuration and without credential helpers, so private repositories are not
supported.

## Short names

Once installed, the workflow is also reachable by its short name:

```console
httk job new --workspace WS --workflow vasp.relax --input structure=POSCAR
httk workflow list
```

When several commits of the same repository and subdirectory are installed,
the short name means the one referenced most recently, so referencing an older
or newer commit by IRI switches what the short name selects. When two
different repositories or subdirectories claim one short name, the name is
refused and the IRI must be used. In-process registrations and installed
plugins take precedence; a fetched workflow whose short name they shadow logs a
warning and stays reachable by its IRI.

## The same repository as a plugin

A repository may also carry an `httk_plugin.toml` listing its workflow
directories, so `httk plugin install git+https://github.com/httk/workflows-vasp`
installs them as a plugin. Both routes can coexist; plugin names take
precedence over fetched short names.

## Cache, forgetting, and trust

Fetched trees live under `data_home()/workflows` (`HTTK_DATA_HOME`, or
`~/.local/share/httk`): `git/` holds one checkout per repository and commit,
and `installed/` holds one JSON entry per referenced IRI. Delete an
`installed/*.json` entry to forget that workflow; a pinned IRI whose tree is
cached is reused without running git.

Referencing an IRI is consent to run its code with the same trust as an
installed plugin package. Its instantiate hook runs from the digest-pinned tree
published into the workspace; its collect hook and postprocess scripts run from
the installed cache tree, like those of plugin and registered-directory
packages. Collection never fetches: a finished job whose IRI is not
installed on this machine is collected as a job without a provider (see
{doc}`collecting`), and nothing named in a job payload ever causes a download.

The full reference, including the grammar and resolution precedence, is in
{doc}`details/workflow_packages`; the API is {py:mod}`httk.workflow.git_workflows`.
