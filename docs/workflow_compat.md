# Compatibility with other workflow systems

Workflows written for other systems run as ordinary jobs. `httk.workflow.compat`
provides a runner *realization* for each format, selected by name: `cwl`,
`pwd` (Python Workflow Definition), `jobflow` (including atomate2 Makers), and
`httk-v1`. The document or template is not rewritten; it is installed in the
workspace like any workflow, and its jobs are claimed, retried, logged and
collected like any other.

| Format | How a job is made | Needs |
| --- | --- | --- |
| CWL | `job new --from-runner flow.cwl --input message=echo` | `httk-workflow[cwl]` where the job is prepared |
| PWD | `job new --from-runner graph.json` | nothing extra |
| jobflow | `job new --from-runner maker.json`, or a package with `format = "jobflow"` | `httk-workflow[jobflow]` (and `[atomate2]`) where the job runs |
| httk v1 | a package with `format = "httk-v1"` | nothing extra |

`job new --from-runner` installs a bare document ad hoc as a workflow named
`<format>.<stem>`. A workflow package, installed with
`httk workflow install --workspace WS DIR` (or `job new --install`), declares the format in `[workflow.runner]` instead, which
lets it name inputs, outputs and parameters properly:

```toml
[workflow]
name = "example.jobflow"

[workflow.runner]
format = "jobflow"
maker = "atomate2.vasp.flows.core:DoubleRelaxMaker"

[workflow.inputs.structure]
entry_type = "structures"
port = "structure"

[workflow.outputs.result]
entry_type = "structures"
port = "output"
```

A CWL or PWD graph executes as one job. A jobflow flow is driven by a parent
job that creates one child job per jobflow job as dependencies settle, so
independent branches run in parallel across managers, with no MongoDB. A v1
package runs its `ht_steps` or `ht_run` unchanged through the normal manager;
{doc}`details/v1_compatibility` describes that path and the harvest of
finished v1 trees, and {doc}`httk_v1_migration_guide` how to move on from it.

Collection follows the same route: a registered provider's collector first,
otherwise the format's default collector, which maps the document's output
ports to declared roles. The httk v1 format has no default and needs a
`[workflow.collect]` hook.

The full guide, {doc}`details/workflow_compat`, covers the package form of each
format, the supported CWL subset and what is refused, jobflow response
semantics and failure codes, and the Python registry API.
