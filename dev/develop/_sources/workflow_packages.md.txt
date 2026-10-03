# Workflow packages and URIs

A workflow package is one portable directory: a manifest, `httk_workflow.toml`,
beside a runner written in any language. The whole directory is published into
the workspace content-addressed and pinned by digest in every job made from it.

```text
my-workflow/
├── httk_workflow.toml
└── run                    # the executable entry (any language)
```

```toml
[workflow]
name = "example.relax"

[workflow.runner]
entry = "run"
steps = ["prepare", "relax", "publish"]
initial_step = "prepare"

[workflow.resources]
procs = 4
mem = 4096

[workflow.inputs.structure]
destination = "POSCAR"
entry_type = "structures"

[workflow.parameters.encut]
default = 520
```

`httk job new --workflow-dir my-workflow --input structure=POSCAR` instantiates
a job from it. Beyond runner, inputs and parameters, the manifest can declare
typed workspace settings the runner consumes (`[workflow.environment.*]`),
hooks that run at instantiation and collection (`[workflow.instantiate]`,
`[workflow.collect]`), curated postprocess scripts
(`[workflow.postprocess.NAME]`), a build step for compiled runners
(`[workflow.build]`), the other workflows it calls (`[workflow.calls]`), and
minimum distribution versions (`requires`). Hooks and scripts are executables
in any language that exchange JSON envelopes.

## Sharing a workflow by URI

A package committed to a Git repository is referenced from anywhere by a git
URI, and that URI is the installed workflow's id:

```text
git+https://github.com/<org>/<repo>[@<ref>][#<subdir>]
```

```console
httk job new --workflow 'git+https://github.com/httk/workflows-vasp#vasp-relax' --input structure=POSCAR
httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workflow vasp.relax --input structure=POSCAR
httk workflow uninstall vasp.relax
```

Referencing a URI fetches the repository and installs the workflow; the job
records the canonical URI with the ref expanded to the full commit, so what a
queued job runs cannot change under it. Once installed, the manifest's
`name` is its short name. Only `git+https://`, `git+http://` and `git+file://`
URIs without credentials are accepted. Referencing a URI is consent to run its
code with the trust of an installed plugin. A repository that also carries an
`httk_plugin.toml` installs as a plugin with `httk plugin install`.

## Calling other workflows

A runner can call another workflow as a child job and resume when it finishes,
which is how one workflow is assembled from others without copying their
steps. A package declares what it calls, and a step calls it by alias:

```toml
[workflow.calls]
relax = "vasp.relax"
```

```python
@run.step
def start(a):
    a.call("relax", label="relax", files={"POSCAR": a.payload / "files" / "POSCAR"})
    a.gather("after_relax", when="all_succeeded", on_impossible="triage")
```

Results flow back through `a.children`; input files are copied into the child
or, for large shared files, read from the parent's workdir in place. See
{doc}`details/composing_workflows`.

## Further reading

- {doc}`details/workflow_packages` is the manifest reference: every table and
  key, hook envelopes, output declarations, format realizations, and build
  registration.
- {doc}`details/workflow_uris` covers the URI grammar, short-name
  resolution, caching and trust, and the definition-versus-declaration URIs.
- {doc}`runtime_helpers` and {doc}`sdks/index` describe the runner a package
  wraps.
