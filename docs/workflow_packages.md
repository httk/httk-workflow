# Workflow packages and URIs

A workflow package is one portable directory: a manifest, `httk_workflow.toml`,
beside a runner written in any language. A workflow is installed into a
workspace before jobs of it are created: the whole directory is copied into the
workspace's `workflows/` store, where the manager runs it from.

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

```console
httk workflow install --workspace WS my-workflow
httk job new --workspace WS --workflow example.relax --input structure=POSCAR
```

installs it (id `local:example.relax`) and instantiates a job from it;
`httk job new --workflow-dir my-workflow --install ...` does both in one
command, and installs again when `my-workflow` has changed since. To edit the
runner for a single job instead, unlock that job (`httk job unlock`, see
{doc}`restarting_jobs`). A single runner file (`--from-runner`) or command
(`--from-command`) needs no package: `job new` installs it ad hoc. Beyond runner, inputs and parameters, the manifest can declare
typed workspace settings the runner consumes (`[workflow.environment.*]`)
and the other settings it reads (`settings`),
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
httk workflow install --workspace WS 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk job new --workspace WS --workflow vasp.relax --input structure=POSCAR
httk workflow uninstall --workspace WS vasp.relax
```

Installing a URI fetches the repository and installs the workflow under the
canonical URI with the ref expanded to the full commit; every job records that
id. Once installed, the manifest's `name` is its short name, and
`job new --workflow URI --install` installs on first use. Only `git+https://`, `git+http://` and `git+file://`
URIs without credentials are accepted. Referencing a URI is consent to run its
code with the trust of an installed plugin. A repository that also carries an
`httk_plugin.toml` installs as a plugin with `httk plugin install`.

## Calling other workflows

A runner can call another workflow as a child job and resume when it finishes,
which is how one workflow is assembled from others without copying their
steps. A package declares what it calls, as a workflow name or a git URI pinned
to a full commit; installing the package installs its calls too, and a step
calls one by alias:

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

## Embedded workflows

A job can embed its own workflow instead of naming an installation. With
`job new --embed-workflow` the workflow's package is copied into the job's own
`workflow/` directory and nothing is installed in the workspace store, which
suits one-off jobs (`--from-command`, a runner file, a package directory under
development) and jobs whose code must travel and be sealed with them:

```console
httk job new --workspace WS --embed-workflow --workflow-dir ./my_relax --input structure=POSCAR
httk job new --workspace WS --embed-workflow --from-command 'my_executable {input}' --file input=a.dat
```

`--embed-workflow` works with `--workflow`, `--workflow-dir`, `--from-runner` and
`--from-command`, and is refused together with `--install`. The job records the
id `embedded:<name>@<first 12 hex digits of the tree digest>` and the tree digest
in `job.json`, together with the runner entry or command, the steps, the
fixed calls, the call-slot interfaces and `requires`, all read from the manifest
when the job is created. The manager, the workspace daemon, launch templates
and build steps never read or run anything from `workflow/`: the code runs only
inside the job's own sandbox, and the manager checks the tree against the
recorded digest before every launch. Language realizations, packages with
`[workflow.build]` and calls to workflows that are not installed are refused,
because an embedded tree has no store build and callees are never embedded. The
tree must not contain symlinks, special files, hard-linked files or a
`__pycache__` directory (remove it first).

A job with an embedded workflow is sealed with its tree. To edit the code of one job, unlock it
and change `workflow/` (see {doc}`restarting_jobs`). Full reference, including
what is guaranteed and what is not: {doc}`details/workflow_packages`
(*Embedded workflows*).

## Further reading

- {doc}`details/workflow_packages` is the manifest reference: every table and
  key, hook envelopes, output declarations, format realizations, and build
  registration.
- {doc}`details/workflow_uris` covers the URI grammar, short-name
  resolution, caching and trust, and the definition-versus-declaration URIs.
- {doc}`runtime_helpers` and {doc}`sdks/index` describe the runner a package
  wraps.
