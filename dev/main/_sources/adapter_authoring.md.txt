# Writing a remote adapter

*For operators and integrators who need to reach a machine the packaged
`local`, `local-slurm`, and `ssh-slurm` templates do not cover.* A remote
adapter is a versioned directory with one dispatcher executable: everything
*httk-workflow* does on another machine — push a bundle, run a command,
submit a manager, pull results — is one of seven operations, each a single
invocation of that program answering with one JSON document. The engine never
opens ssh itself and never spells out a scheduler command.

The normative contract — bundle layout, the seven operations and their exact
request/result documents, settings and credential delivery, and the rules an
implementation must follow — is {doc}`details/adapter_authoring`. The
operator-facing view of the packaged adapters is in {doc}`details/workflow_cli`.
