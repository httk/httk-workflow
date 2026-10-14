# Restarting and resuming jobs

A failed job is not lost. It stays in the workspace with all its files, and an
operator can fix what went wrong and revive it with a request. This page shows
where a failed job is, what you may edit, which request to use, and how to
resume a parent whose child failed. The request mechanics are in
{doc}`/details/taskmanager`; the protocol is in
{doc}`/details/workflow_filesystem_api`.

## Where a failed job is and how to read it

A failed job stays in `jobs/failed/<placement>/...`; `workspace gc` does not
remove it. Three commands tell you what happened:

```console
httk job show silicon        # prints the job directory
httk job why silicon         # explains the failure
httk job log silicon         # prints the run log
```

For a parent that failed at its wait, `job show` also lists its kept join and
children.

The job directory holds:

| Path | Content |
| --- | --- |
| `job.json` | The immutable declaration: workflow, parameters, placement, retry policy. |
| `state.json` | The owner-written state: current step, failure, counters, `join`. |
| `run/` | The persistent workdir, `a.workdir` in the runner. |
| `attempts/<A>/` | The control area of attempt `A`, including `error.json`. |
| `logs/` | The run log. |
| `data/` | The results the job committed. |
| `.httk-job/` | The runner's own state, `a.state`, and the seal. |

## What you may edit

Everything in the job directory may be edited while the job is `failed` or
`paused`, except `job.json`. It is digest-pinned at the first activation, and a
different digest fails the job with `protocol_error`. That includes the retry
policy, the parameters and the placement.

Edit **before** posting the request: a manager may launch the job the moment it
is revived.

A revived step reruns from its top. Whatever the step itself regenerates, such
as copying a template or writing inputs from parameters, it regenerates again
and overwrites a hand edit made at that layer. Edit at the layer the step
reads: a payload file under `files/` that a step copies from, rather than the
copy in `run/`; a file in `run/` (an `INCAR`, say) only when the step uses it as
it is. The runner's documentation says which files a step regenerates.

The retry budgets in `job.json` (`maximum_attempts_per_activation`,
`maximum_total_attempts`, `maximum_activations`; absent means unbounded) bind
manual revival too. A revival past a budget fails right back with
`budget_exhausted` or `retry_exhausted`, and a budget cannot be raised
afterwards because `job.json` cannot be edited.

## The three verbs

```console
httk job request continue --reason "fixed the input" silicon
httk job request restart --reason "redo this step" silicon
httk job request override_step --step relax --reason "redo relax" silicon
```

Each applies to a `failed` or `paused` job. The CLI applies it at once to a job
no manager holds and prints `applied`, `queued`, `refused` or `uncertain`; a
job its owner holds applies it at the next boundary.

| Action | Effect |
| --- | --- |
| `continue` | A job that failed at its wait on children (see below) returns to `waiting` with its retained join: no new attempt, no budget counted. Any other `failed` job, and a `paused` job, returns to `ready` and the current step reruns as a new attempt, counted against the budget. |
| `restart` | Always `ready`: the current step reruns as a new attempt, counted against the budget. The failure is cleared and a retained join is dropped. |
| `override_step --step STEP` | `ready` with a new activation of `STEP`. `STEP` is checked against the steps the runner recorded unless `--force`. A retained join is dropped. |

For a job that did not fail at a wait, `continue` and `restart` are the same.
The attempt reason is `manual_continue` for both. Every rerun is bound by the
retry budgets; only reopening a wait is not. `job why` says which applies.

`--force` applies to all three. It accepts the revival hazard described next,
and publishes an `override_step` outside the recorded steps. Cancelled jobs
cannot be revived by any request.

## Parents and children

A parent that waits on children with
{py:meth}`~httk.workflow.Attempt.gather` fails when its join can no longer be
satisfied, because a child failed or was cancelled. The parent records what it
saw of each child (`observations`) and, when the gather named no
`on_impossible` step, fails with `dependency_failure` at its wait. It keeps its
`join` in `state.json`, and that retained join is what lets you reopen the
wait. The children stay where they failed. A child that runs while its parent
is `failed` still finds it through `a.parent`, since the parent stays at its
placement.

### Fix the child, then continue the parent

```console
httk job why PARENT
httk job request continue --reason "fixed the INCAR" CHILD
httk job show CHILD
httk job log CHILD
httk job request continue --reason "child succeeded" PARENT
```

1. `job why PARENT` says the job failed while waiting on its children, lists
   each child that did not succeed with its failure, and notes that `continue`
   reopens the wait while `restart` reruns the step and spawns new children.
2. Fix the child as described above, then `continue` it. Reviving a child that
   the parent's recorded observations name needs `--force`, unless that parent's
   join still names the child: the parent failed at its wait, or waits on it
   again. This flow needs no `--force`. Continuing the parent first is accepted
   too, but the manager re-decides the reopened wait within one pass and, while
   the child is still `failed`, fails the parent again at once. That is
   harmless: `continue` it again after the child succeeded.
3. Watch the child with `job show` and `job log` until it succeeds.
4. `continue` the parent. It returns to `waiting` with the same join, with no
   new attempt. On its next pass the manager re-evaluates the join from the
   children's current states. If it is satisfied, the gathering step runs with
   the children as `a.children`, as usual. If a child is still `failed` or
   `cancelled`, the parent fails again with `dependency_failure` at once.

Without `--tree`, `continue` on such a parent prints a note on stderr for each
join child that is still `failed` or `cancelled`, or not found at its
placement, so you know the reopened wait will fail again.

### Nested trees

Failures propagate upward: when a child's own wait fails, that child fails with
`dependency_failure`, and its parent's join becomes impossible in turn. Every
level keeps its join. To resume the whole chain at once:

```console
httk job request continue --tree --reason "inputs fixed" PARENT
```

`--tree` also resumes the `failed` descendants reachable through retained
joins, deepest first, before the parent. Each is an ordinary `continue` request,
posted and served one job at a time in that order. If a descendant's request is
not applied (queued behind its owner, refused, or its revival fails at a
budget), the CLI stops, posts nothing for the remaining jobs, prints
`not posted: <job_key> ...` for each and exits `1`; resolve that job and rerun
`continue --tree`. Children in other states (`succeeded`, `paused`, running,
`cancelled`) are left alone. Join children `--tree` leaves alone (not found,
`cancelled`, or held by an owner at that moment) are reported with a note, as
without `--tree`. Fix the leaf failures first: a descendant that fails for its
own reason, not at a wait, returns to `ready` and reruns its step, which may
fail again. `--tree` is valid only with `continue`.

### Restarting the parent instead

`restart` on a parent that failed at its wait reruns the spawning step, which
creates new children. The old children stay where they are. Terminal ones can be
removed with `httk job delete`. The spawning step's attempt counts against the
budget: with `maximum_attempts_per_activation = 1`, `restart` fails right back
with `retry_exhausted`. `httk job request override_step --step <spawning step>`
reruns that step as a new activation instead, bound by `maximum_activations`.
The restarted parent no longer waits on them, so
reviving one of them needs `--force`, and the parent will not notice it.

### Cancelled and deleted children

A cancelled child cannot be revived by any request, so a join naming it stays
impossible: `continue` on the parent fails again at once. `restart` the parent
or `override_step` it instead. A deleted child makes a reopened wait fail with `dependency_failure`
after the join grace, a manager option that defaults to one hour.

### Parents with `on_impossible`

A parent whose gather named an `on_impossible` step advanced to that step
instead of failing, which consumed the observation and cleared the join. Reviving
its child then still needs `--force`, and the parent does not notice the
revived child. The wait cannot be reopened. To have the parent use the revived
child, rerun a step that gathers it again (a runner step that calls
`gather(..., rejoin=[label])`, via `override_step`), or `restart` or
`override_step` the spawning step to spawn new children.

## Paused jobs

A job pauses on a `pause` request or when the runner calls `a.pause(reason)`.
It stays where it is until you `continue` it, which reruns the current step as a
new attempt, or `restart` it, which does the same; edit its files first, as for
a failed job. `override_step` moves it to a named step.

```console
httk job request continue --reason "quota restored" silicon
```

## Remote workspaces

`continue --tree` works on local workspaces only; it is refused for a remote
`--workspace` binding. For a remote workspace, post one `continue` per failed
job yourself, deepest first, then the parent. {doc}`/details/remotes` covers
what a remote binding accepts.

## See also

- {doc}`running`: inspecting and controlling jobs.
- {doc}`/details/taskmanager`: the request table and the revival guard.
- {doc}`/details/workflow_cli`: `job request` options.
- {doc}`/details/composing_workflows`: failure semantics of called children.
- {doc}`/details/workflow_filesystem_api`: `state.json`, joins and requests in
  the protocol.
