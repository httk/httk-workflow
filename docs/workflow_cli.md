# The command line

Everything is one nested command tree under `httk`, and every group and
command answers `--help`:

```text
httk init | identity     operator identities (core-owned)
httk project             init | show | repair | manifest | seal | unseal | verify-seal | import-v1
httk workspace           init | list | default | status | managers | settings | policy | workflow-prelude | fsck | gc | unlock | seal | unseal | daemon | ...
httk job                 new | submit | request | delete | detach | seal | unseal | list | show | log | why | debug | transfer | eject | adopt
httk collect             workspaces and calculation trees
httk workflow            run | list | describe | install | uninstall | precheck | postprocess | build | monitor
httk workflow runner     publish | describe
httk workflow seal       verify
httk workflow manager    run
httk workflow campaign   init | show | submit | collect | start-managers
httk workflow launcher   list | add | configure | check | show | remove
httk workflow remote     list | add | configure | check | show | remove | import-v1 | daemon
httk workflow config | v1
httk workflow transfer   receive | offer | retire      (hidden protocol; remote peers invoke it by exact name)
```

{doc}`quickstart` walks the everyday sequence, {doc}`workspaces` and
{doc}`running` explain the concepts behind it, and the full reference,
{doc}`details/workflow_cli`, documents every command and option: workspace
selection, job creation, inspection and debugging, projects and signed
manifests, configuration, remotes and transfers, and the frozen protocol
spellings.
