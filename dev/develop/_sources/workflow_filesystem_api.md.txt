# The filesystem protocol

The engine is built on a language-neutral filesystem protocol: format 3,
profile `core-v3`, layout `jobs-v3`. A job is a directory whose location is
its state and whose name carries its priority and a fresh token, so every move
is one atomic rename that exactly one actor can win. An owner writes only the
jobs it holds, work changes hands only when the owner is proven dead or an
operator attests it, and the workspace uses no file locks, no hard links and no
shared journal. Requests, transactions, children, joins, bundles, holds,
installed workflows, confined launches, seals and the exchange are all defined
as files and atomic filesystem operations, so any implementation that writes
the same trees is a valid peer. Runner authors never need it: the SDKs
({doc}`runtime_helpers`, {doc}`sdks/index`) speak the protocol for you.

The normative specification is {doc}`details/workflow_filesystem_api`. Its
Python expression is {py:mod}`httk.workflow.protocol`, which re-exports what an
independent inspection or verification tool needs to read a workspace it did
not write: the job and state document models (`JobDefinition`, `StateDoc`), the
job-name grammar (`format_job_name`, `parse_job_name`, `JobRef`), the
validators, the attempt context and outcome documents, and the
`WorkflowError` family. The complete list of names is the module's `__all__`
in the {doc}`API reference <reference/index>`.
