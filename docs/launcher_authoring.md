# Writing a manager launcher

*For operators and integrators whose scheduler the packaged `slurm` launcher
template does not cover.* A launcher is to starting managers what a remote
adapter is to reaching a machine: a versioned directory with one dispatcher
executable that answers two operations — `check` and `start` — each a single
invocation returning one JSON document. The workspace names its launcher in the
`manager.launch` setting, and `httk workflow run` hands that launcher the
manager command to start, however many times `--count` asks for. The engine
never spells out a scheduler command itself.

The normative contract — bundle layout, the two operations and their exact
request/result documents, the workspace settings a launcher receives, and the
rules an implementation must follow — is {doc}`details/launcher_authoring`. The
operator-facing view of launchers is in {doc}`details/workflow_cli`.
