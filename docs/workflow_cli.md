# The command line

Everything is one nested command tree under `httk`, and every group and
command answers `--help`:

```text
httk setup | identity     operator identities (core-owned)
httk project             init | show | check | repair | manifest | verify | seal | unseal | import-v1
httk workspace           init | list | default | show | set | unset | status | owners | attest-dead | fsck | gc | seal | unseal | verify | exchange | daemon | ...
httk job                 new | submit | request | delete | detach | seal | unseal | unlock | list | show | log | why | debug | push | pull | transfer | eject | adopt
httk collect             workspaces and calculation trees
httk workflow            run | list | describe | install | uninstall | precheck | postprocess | build | monitor
httk seal       verify
httk manager    run
httk campaign   init | show | set | unset | remove | submit | collect | start-managers
httk launcher   list | add | set | unset | check | show | remove
httk remote     list | add | set | unset | check | show | remove | import-v1 | daemon
httk config              show | set | unset | import-v1
httk v1                  collect
httk transfer            status
```

`httk job unlock` puts a job in debug mode, and `httk collect --force` collects
such a job; see {doc}`restarting_jobs`.

{doc}`quickstart` walks the everyday sequence, {doc}`workspaces` and
{doc}`running` explain the concepts behind it, and the full reference,
{doc}`details/workflow_cli`, documents every command and option: workspace
selection, job creation, inspection and debugging, projects and signed
manifests, configuration, remotes and transfers, and the frozen protocol
spellings.
