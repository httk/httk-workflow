# Workflow monitor

`httk workflow monitor` opens a stdlib `curses` view over one or more
registered workspaces:

```console
httk workflow monitor --workspace default --refresh 3
httk workflow monitor --workspace cluster:runs
```

`--refresh` accepts finite values from 0.5 through 3600 seconds. The command
requires an interactive terminal. On platforms without the stdlib `curses`
module it exits with a clear diagnostic; `--non-interactive` is an explicit
refusal for scripts that need to verify this requirement.

## Layout and keys

The left pane lists workspaces, per-state counts, and live managers. The centre
pane is a page of jobs with key, state, step, placement, and priority columns.
The right pane shows the selected job's report, diagnosis, recent frames, and a
bounded standard-output tail. Details are loaded only after selection. Prompts
and worker errors appear in the status line.

| Key | Action |
| --- | --- |
| `j`/`k` or arrows | move |
| `n`/`p` | page forward/back |
| `Tab` | change pane |
| `f` | filter by kind, placement prefix, or tag substring (enter space-separated `KIND path=PREFIX tag=TEXT`) |
| `Enter` | load bounded detail |
| `w` | load the full diagnosis |
| `l` | load full history |
| `t` | follow `logs/stdio.out` |
| `c`, `P`, `C` | request cancel, pause, and continue |
| `m` | start managers |
| `x` | transfer selected jobs |
| `D` | remove removable jobs after confirmation |
| `r` | refresh |
| `?` | show help |
| `q` | quit |

## Bounded reads

The monitor never materializes a workspace's job set. Counts use the marker
names, and the list uses the cursor-stable `job list` page API. A refresh reads
the current page and the selected workspace's counts; a detail read touches only
the selected job. Memory and state reads therefore stay bounded even when a
workspace contains 100,000 jobs or more. Page cursors are exclusive and stable
within the weak-consistency guarantees of the workspace protocol.

A flat placement containing 100,000 markers costs one directory listing and
sort, roughly the filesystem's directory-listing cost (often about 100 ms). This
is the expected cost for that placement, and the monitor builds no persistent
index. With a finite page limit and tag filtering, the reader examines at most
`max(limit * 100, 10,000)` markers per page. A filtered page can therefore be
partial even when fewer than `limit` matches were found; its `next_after`
cursor continues the filter scan. A human table request without a limit scans
the complete selected stream. Transfer parent checks inspect only the `waiting`
state subtree, so their cost is bounded by waiting jobs rather than all jobs.

## Remote workspaces

Remote workspaces use the existing adapter JSON read protocol. Page, show,
history, and diagnosis requests are separate bounded adapter calls: a page
refresh combines the page and filtered counts in one adapter invocation, and
show, why, and log are separate one-invocation reads when explicitly requested.
Detail and other actions require canonical job IDs. Actions are dispatched
through the existing request, manager, and transfer command implementations.

Some features are unavailable remotely:

- Following standard output needs an adapter that exposes the payload
  filesystem; otherwise the status line says it is unavailable.
- The phase-1 remote read protocol does not expose the manager manifest, so the
  managers pane says `unavailable (remote)`. Local manager records remain live.
- Removal is disabled.

## Removing jobs

Removal is local-only and targeted. It preflights every selected marker as
removable, applies the same parent-join guard as garbage collection, then
removes that marker and its payload. Use `httk job delete` for remote removal.
