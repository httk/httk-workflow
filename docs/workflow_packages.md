# Workflow packages

*For workflow authors who want one portable directory to describe,
instantiate, run, and collect a workflow.* A package is a directory whose
`httk_workflow.toml` is the strictly *httk*-owned glue around a runner; the
whole directory is published content-addressed and digest-pinned per job:

```text
my-workflow/
├── httk_workflow.toml
└── run                    # the executable entry (any language)
```

```toml
[workflow]
id = "example.relax"

[workflow.runner]
entry = "run"
steps = ["prepare", "relax", "publish"]
initial_step = "prepare"

[workflow.resources]
procs = 4
mem = 4096

[workflow.steps.relax.resources]
procs = 8

[workflow.inputs.structure]
destination = "POSCAR"
entry_type = "structures"

[workflow.parameters.encut]
default = 520
```

`job new --workflow-dir my-workflow --input structure=POSCAR` instantiates a
job from it. Declared inputs are staged objects; parameters are knobs;
`[workflow.environment.*]` consumes typed workspace settings;
`[workflow.instantiate]`/`[workflow.collect]` hooks and
`[workflow.postprocess.NAME]` scripts run in any language; and compiled
workflows declare `[workflow.build]` (sources-only digests, binaries built and
registered per machine with `httk workflow build`).

The `[workflow.build]` vocabulary and engine are shared `httk.core.building`
machinery. *httk-workflow* owns the workspace store layout, platform-tagged
registrations, and manager artifact overlay; the build semantics are unchanged.

An installed *httk₂* plugin may bundle workflow packages. Resolution checks
in-process registrations first, then installed plugins; workflow listings label
plugin entries with their owning plugin, while `workflow describe` reports a
plugin entry as `source: installed-package`.

The full guide, {doc}`details/workflow_packages`, is the manifest reference:
every table and key, hook envelopes, output declarations and provenance,
language realizations, and the build-registration mechanics.
