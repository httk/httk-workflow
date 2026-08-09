# Task-manager usage

*For operators: workspaces, submission, managers, and inspecting what they
leave behind.* The everyday cycle is four commands:

```console
$ httk workflow workspace init . --name default
$ httk workflow job new --workflow vasp-relax --input structure=POSCAR --tag silicon
$ httk workflow run                 # serve jobs until idle (--idle keeps serving)
$ httk workflow workspace status
```

Managers drive every claimable job through its steps and record everything
they do; `job list`, `job show`, `job log`, and `job why` (why is this job
*not* progressing?) read it back, `precheck` reports readiness before a
manager ever starts, and `job debug` drives one job in the foreground while
you author a runner.

The full guide, {doc}`details/taskmanager`, covers workspace naming and
defaults, submission forms, manager scheduling and capabilities, placement,
requests, inspection and repair (`fsck`, `gc`, `unlock`), and the
`httk-taskmanager` compatibility alias.
