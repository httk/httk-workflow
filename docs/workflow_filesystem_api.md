# Workflow filesystem API

*For implementers of the protocol, and for anyone who needs to know exactly
what a workspace on disk means.* The *httk-workflow* engine is built around a
language-neutral filesystem protocol — the `core-v2` profile: jobs, markers,
attempts, journals, transactional data, detached transfer, and
replay-after-stop semantics are all defined as files and atomic filesystem
operations, so any implementation that writes the same trees is a valid peer.

The normative specification is {doc}`details/workflow_filesystem_api`. The
Python surface that mirrors it is {doc}`workflow_protocol_api`; runner authors
never need either — the SDKs ({doc}`runtime_helpers`, {doc}`sdks/index`) speak
the protocol for you.
