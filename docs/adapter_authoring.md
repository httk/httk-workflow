# Writing a remote adapter

*For operators and integrators who need to reach a machine the packaged
`local` and `ssh` templates do not cover.* A remote adapter is a versioned
directory with one dispatcher executable: everything *httk-workflow* does on
another machine — push a bundle, run a command, or pull results — is one of six
operations, each a single invocation of that program answering with one JSON
document. Manager launch is owned by the workspace's launcher, not by the
remote adapter.

The normative contract — bundle layout, the six operations and their exact
request/result documents, settings and credential delivery, and the rules an
implementation must follow — is {doc}`details/adapter_authoring`. The
operator-facing view of the packaged adapters is in {doc}`details/workflow_cli`.
