# Reference

This is the generated API reference. It documents the deliberate public surface
of the package — the modules grouped by layer below, and the names each declares
in its `__all__` — rather than every source object. The three layers are:

- **Filesystem protocol** — `httk.workflow.protocol` and `httk.workflow.errors`;
  see {doc}`../workflow_protocol_api`.
- **Execution / authoring** — the package root `httk.workflow` (`Runner`,
  `Attempt`, and the small job and result types), with `httk.workflow.sdk`,
  `httk.workflow.runtime`, `httk.workflow.runtime_utils`,
  `httk.workflow.scaffold`, `httk.workflow.executors`, and
  `httk.workflow.shell_bridge`.
- **Orchestration and management** — `httk.workflow.collecting`,
  `httk.workflow.supervision`, `httk.workflow.transfers`,
  `httk.workflow.manifests`, `httk.workflow.hygiene`, `httk.workflow.adapters`,
  `httk.workflow.adapter_protocol`, `httk.workflow.configuration`, and
  `httk.workflow.projects`, plus the domain and compatibility consumers
  `httk.workflow.vasp` and the `httk.workflow.compat` engines
  (`httk.workflow.compat.v1`, `httk.workflow.languages.cwl`,
  `httk.workflow.languages.pwd`, `httk.workflow.languages.jobflow`, and
  `httk.workflow.languages.httk_v1`). The language registration modules are
  public; their packaged runner modules are internal implementation details.

```{toctree}
:maxdepth: 2

autoapi/httk/workflow/index
autoapi/httk/workflow/languages/index
autoapi/httk/workflow/languages/cwl/index
autoapi/httk/workflow/languages/pwd/index
autoapi/httk/workflow/languages/jobflow/index
autoapi/httk/workflow/languages/httk_v1/index
```
