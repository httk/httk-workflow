# Reference

This is the generated API reference. It documents the deliberate public surface
of the package — the modules grouped by layer below, and the names each declares
in its `__all__` — rather than every source object. The three layers are:

- **Filesystem protocol** — `httk.workflow.protocol` and `httk.workflow.errors`;
  see {doc}`../workflow_filesystem_api`.
- **Execution / authoring** — the package root `httk.workflow` (`Runner`,
  `Attempt`, and the small job and result types), with `httk.workflow.sdk`,
  `httk.workflow.runtime`, `httk.workflow.runtime_utils`, and
  `httk.workflow.hookapi`,
  `httk.workflow.scaffold`, `httk.workflow.executors`, and
  `httk.workflow.shell_bridge`.
- **Orchestration and management** — `httk.workflow.collecting`,
  `httk.workflow.calculations`, `httk.workflow.storing`,
  `httk.workflow.supervision`, `httk.workflow.transfers`,
  `httk.workflow.manifests`, `httk.workflow.hygiene`, `httk.workflow.adapters`,
  `httk.workflow.adapter_protocol`, `httk.workflow.configuration`, and
  `httk.workflow.projects`, plus the `httk.workflow.codes` toolkit for
  simulation-code support distributions (such as *httk-workflow-vasp*, whose
  `httk.codes.vasp` is documented there) and the `httk.workflow.compat` registry and
  consumers (`httk.workflow.compat.cwl`, `httk.workflow.compat.pwd`,
  `httk.workflow.compat.jobflow`, and `httk.workflow.compat.v1` with its
  `httk.workflow.compat.v1.realization`). The registration modules are
  public; their packaged runner modules are internal implementation details.

```{toctree}
:maxdepth: 2

autoapi/httk/workflow/index
autoapi/httk/workflow/compat/index
autoapi/httk/workflow/compat/cwl/index
autoapi/httk/workflow/compat/pwd/index
autoapi/httk/workflow/compat/jobflow/index
autoapi/httk/workflow/compat/v1/index
autoapi/httk/workflow/compat/v1/realization/index
```
