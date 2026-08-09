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

The full guide, {doc}`details/workflow_packages`, is the manifest reference:
every table and key, hook envelopes, output declarations and provenance,
language realizations, and the build-registration mechanics.
