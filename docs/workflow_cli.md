# Project and workflow command line

*For operators and campaign owners.* Everything is one nested command tree —
`httk workflow …` — where every group and command answers `--help`:

```text
httk workspace           init | list | default | status | managers | settings | fsck | gc | ...
httk job                 new | submit | request | list | show | log | why | debug
httk workflow runner     publish | describe
httk workflow build      (compiled packages: build and register binaries)
httk workflow describe | precheck | collect | postprocess
httk workflow manager    run
httk workflow campaign   init | show | submit | collect | start-managers
httk workflow remote     list | add | configure | check | show | remove
httk workflow transfer   [OPTIONS] SRC DST
httk workflow config | project | v1
```

{doc}`quickstart` walks the everyday sequence; {doc}`taskmanager` explains the
operator concepts behind it.

The full reference, {doc}`details/workflow_cli`, documents every command and
option — projects and signed manifests, configuration, remotes and transfers,
and the protocol spellings.

Manager capacities can be advertised with repeatable `--worker-resource NAME
COUNT` options. `--count N` starts multiple local managers for a local binding
and submits multiple managers through a remote scheduler for a remote binding.
