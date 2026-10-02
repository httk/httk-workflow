# The filesystem protocol

The engine is built on a language-neutral filesystem protocol, the `core-v2`
profile. Jobs, state markers, attempts, journals, transactional data, detached
transfer and replay-after-stop are all defined as files and atomic filesystem
operations, so any implementation that writes the same trees is a valid peer.
Runner authors never need it: the SDKs ({doc}`runtime_helpers`,
{doc}`sdks/index`) speak the protocol for you.

The normative specification is {doc}`details/workflow_filesystem_api`. Its
Python expression is {py:mod}`httk.workflow.protocol`, which re-exports
everything an independent inspection or verification tool needs to read a
workspace it did not write: the workspace format and policy, immutable job
definitions and their digest, markers and transitions, journal records and
references, attempt context and outcome documents, replayable data
transactions, and the `WorkflowError` family. Nothing in that namespace is
manager bookkeeping or scheduling; those are implementation and live
elsewhere. The complete list of names is the module's `__all__` in the
{doc}`API reference <reference/index>`.
