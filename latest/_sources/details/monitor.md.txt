# Workflow monitor

`httk workflow monitor` opens a stdlib `curses` view over one or more
registered workspaces:

```console
httk workflow monitor --workspace default --refresh 3
httk workflow monitor --workspace cluster:runs
```

The left pane lists workspaces, per-state counts, and live managers. The centre
pane is a page of jobs with key, state, step, placement, and priority columns.
The right pane displays the selected job's report, diagnosis, recent frames,
and a bounded standard-output tail. Details are loaded only after selection.

Keys are:

`j`/`k` or arrows move; `n`/`p` page forward/back; `Tab` changes pane; `f`
filters by kind, placement prefix, or tag substring (enter space-separated
`KIND path=PREFIX tag=TEXT`); `Enter` loads bounded detail; `w` loads the full
diagnosis; `l` loads full history; `t`
follows `logs/stdio.out`; `c`, `P`, and `C` request cancel, pause, and
continue; `m` starts managers; `x` transfers selected jobs; `D` removes
removable jobs after confirmation; `r` refreshes; `?` shows help; and `q`
quits. Prompts and worker errors appear in the status line.

`--refresh` accepts finite values from 0.5 through 3600 seconds.

The monitor never materializes a workspace's job set. Counts use the marker
names, and the list uses the cursor-stable `job list` page API. A refresh reads
the current page and selected workspace counts; a detail read touches only the
selected job. This keeps the memory and state-read budget bounded even when a
workspace contains 100,000 jobs or more. Page cursors are exclusive and stable
within the weak-consistency guarantees of the workspace protocol.

A flat placement containing 100,000 markers is intentionally handled with one
directory listing and sort; that is the bounded, expected cost for that
placement (roughly the filesystem's directory-listing cost, often about 100 ms)
and the monitor does not build a persistent index. With a finite page limit and
tag filtering, the reader examines at most `max(limit * 100, 10,000)` markers
per page. Therefore a filtered page can be partial even when fewer than `limit`
matches were found; its `next_after` cursor continues the filter scan. A human
table request without a limit scans the complete selected stream. Transfer
parent checks inspect only the `waiting` state subtree, so their cost is
bounded by waiting jobs rather than all jobs.

Remote workspaces use the existing adapter JSON read protocol: page, show,
history, and diagnosis requests are separate bounded adapter calls, and
canonical job IDs are used for detail actions. Remote standard-output following
is unavailable when the adapter does not expose the payload filesystem; the
status line says so. The phase-1 remote read protocol does not expose the
manager manifest, so the managers pane says `unavailable (remote)`; local
manager records remain live. Remote removal is disabled, and remote actions
require canonical job IDs. Actions are dispatched through the existing request,
manager, and transfer command implementations. Remote page refreshes combine
the page and filtered counts in one adapter invocation; show, why, and log are
separate one-invocation reads when explicitly requested.

Removal is local-only and targeted: it preflights every selected marker as
removable, applies the same parent-join guard as garbage collection, then
removes that marker and payload. Use `httk job delete` for remote removal.

The command requires an interactive terminal. On platforms without the stdlib
`curses` module, it exits with a clear diagnostic; `--non-interactive` is an
explicit refusal useful to scripts that need to verify this requirement.
