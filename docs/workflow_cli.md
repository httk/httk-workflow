# Project and workflow command line

*For operators and campaign owners.* Everything is one nested command tree —
`httk workflow …` — where every group and command answers `--help`:

```text
httk workflow workspace  init | list | default | status | managers | settings | fsck | gc | ...
httk workflow job        new | submit | request | list | show | log | why | debug
httk workflow runner     publish | describe
httk workflow build      (compiled packages: build and register binaries)
httk workflow describe | precheck | collect | postprocess
httk workflow manager    run
httk workflow campaign   init | show | submit | collect | start-managers
httk workflow remote     list | add | configure | install | show | remove
httk workflow transfer   SRC DST
httk workflow config | project | v1
```

{doc}`quickstart` walks the everyday sequence; {doc}`taskmanager` explains the
operator concepts behind it.

The full reference, {doc}`details/workflow_cli`, documents every command and
option — projects and signed manifests, configuration, remotes and transfers,
the protocol spellings, and the `httk-taskmanager`/`httk-workflow-*` aliases.
