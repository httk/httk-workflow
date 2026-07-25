# httk-workflow

*httk-workflow* is the filesystem-native workflow engine for
[*httk₂*](https://github.com/httk/httk2).

It provides the `httk.workflow` Python API and installs the
`httk-taskmanager` executable. Jobs communicate through atomically published
filesystem state, so interrupted managers and calculations can be recovered
without cleanup hooks.

```console
httk-taskmanager init workflow-store
httk-taskmanager submit workflow-store prepared-job --placement project/00
httk-taskmanager run workflow-store
```

The precise on-disk protocol is documented in
[`docs/workflow_filesystem_api.md`](docs/workflow_filesystem_api.md).
