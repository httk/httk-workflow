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
`maximum_total_attempts`, `maximum_activations`; absent means unbounded) are
not used up by a revival: each revival gives the job its declared allowance
again from that point, unless you ask it not to with `--keep-budget` (see
[Retry budgets on revival](#retry-budgets-on-revival)).

## The three verbs

```console
httk job request continue --reason "fixed the input" silicon
httk job request restart --reason "redo this step" silicon
httk job request override_step --override-step relax --reason "redo relax" silicon
```

Each applies to a `failed` or `paused` job. The CLI applies it at once to a job
no manager holds and prints `applied`, `queued`, `refused` or `uncertain`; a
job its owner holds applies it at the next boundary.

| Action | Effect |
| --- | --- |
| `continue` | A job that failed at its wait on children (see below) returns to `waiting` with its retained join: no new attempt. Any other `failed` job, and a `paused` job, returns to `ready` and the current step reruns as a new attempt. |
| `restart` | Always `ready`: the current step reruns as a new attempt. The failure is cleared and a retained join is dropped. |
| `override_step --override-step STEP` | `ready` with a new activation of `STEP`. `STEP` is checked against the steps the runner recorded unless `--force`. A retained join is dropped. |

For a job that did not fail at a wait, `continue` and `restart` are the same.
The attempt reason is `manual_continue` for both. Each of the three first
restores the job's declared retry allowance, so the revived job may again run
as many attempts and activations as it was declared with; `--keep-budget`
revives it within the limits it has. `job why` says what the limits are after
the revival.

`--force` applies to all three. It accepts the revival hazard described next,
and publishes an `override_step` outside the recorded steps. Cancelled jobs
cannot be revived by any request.

## Retry budgets on revival

A revival counts what the job has already started and raises each declared
limit so the declared number of units is available again from here. With
`maximum_attempts_per_activation = 3`, a job that failed with
`retry_exhausted` after attempt 3 runs attempts 4, 5 and 6 after a `restart`;
one that failed ordinarily at attempt 1 runs attempts 2, 3 and 4. In detail,
for each limit with a declared value `D`, the new effective limit is the larger
of the current one and `U + D`, where `U` counts the units already started in
the scope the next unit lands in:

| Limit | `U` |
| --- | --- |
| `maximum_attempts_per_activation` | The attempts started in the current activation; `0` for `override_step` and for a `continue` that reopens a wait, whose next attempt opens a new activation. |
| `maximum_total_attempts` | The attempts started in total. |
| `maximum_activations` | The activations that have a started attempt. |

An attempt that was recorded but never started (a retry that settled just
before a `pause`, or a launch refused by a lowered budget) is the next unit,
not a used one, for `continue` and `restart`, which run it. `override_step`
abandons it, and the counters keep its slot, so for `override_step` `U` is the
attempts and activations recorded so far, started or not. So a `continue` that
reopens a wait (after `dependency_failure` or `budget_exhausted`) raises
`maximum_activations` to the activations so far plus `D`, giving the join `D`
more activations, and `override_step` gives the declared attempts in the new
activation and `D` activations counting the one it opens. `job why` on a
parent that failed at its wait lists the reopenable wait and, as
`operator restart`, the limits a `restart` would leave.

The raise is written to `state.json` `budget_override` like a `set_budget`
(see below), and the `request_applied` history entry carries the limits it
changed as `budget`. A limit already higher, or lifted to unlimited, is left
alone; a limit `job.json` declares unlimited is restored to unlimited, which
drops an override that capped it. Automatic retries by the manager never raise
a limit.

`--keep-budget` (on `continue`, `restart` and `override_step`, and on
`continue --tree`, where it applies to every descendant) leaves the limits as
they are. The revival is then bound by them: a rerun past a limit fails right
back with `retry_exhausted` or `budget_exhausted`, and a wait that failed with
`budget_exhausted` re-decides under the same `maximum_activations`.

## Raising a budget

A revival restores the declared allowance by itself. To give a job more than
its declaration allows, or a different budget while it is `ready` or running,
raise the limit with a request instead, since `job.json` cannot be edited:

```console
httk job why silicon
httk job request set_budget --max-attempts-per-activation 3 --reason "flaky node" silicon
```

1. `job why` names the budgets against their effective limits.
2. `set_budget` raises the limit. The job stays where it is, in the state and
   at the priority it had, and no attempt starts.
3. A later revival does not lower it: the raise to the declared allowance only
   ever raises.

`set_budget` takes at least one of `--max-attempts-per-activation N`,
`--max-total-attempts N` and `--max-activations N`. Each is an integer of 1 or
more, or `unlimited` to lift that limit. The options are valid only with
`set_budget`. The request applies to any job that is not `succeeded` or
`cancelled`, including a `ready` or running one; on a running attempt it
applies at the next boundary, like every request. It does not need `--force`.

The values go into `state.json` as `budget_override`. Later requests merge into
it: a key set again takes the later value, and a key not named keeps its
earlier value. A limit with no override falls back to `job.json`. The
**effective** policy is the declared limits with the override applied. The
manager's launch check, automatic retries, the `maximum_activations` checks and
the revival checks of `continue`, `restart` and `override_step` all use it.
`job.json` and the provenance record of the job keep the declared policy.

`job why` and `job show` print the budget counters against the effective
limit, and the declared one where it differs:

```text
attempts this activation  1/3 (declared 1)
```

The JSON output carries the declared values and `budget_override` in its
`budgets` mapping. Each applied `set_budget`, and each revival that raised a
limit, records the limits it set in the job's `state.json` history (the
`request_applied` entry carries `budget`).

A parent that failed at its wait keeps its join, and `continue` reopens it
with no new attempt. A parent that failed with `budget_exhausted` (its join
would advance past `maximum_activations`) is reopened the same way: the
`continue` restores the declared allowance, so the join re-decides with room
for its next activation and advances to its next step, with no new children:

```console
httk job request continue --reason "one more step" PARENT
```

With `--keep-budget`, raise `maximum_activations` with `set_budget` first.
`restart` spawns new children (see
[Restarting the parent instead](#restarting-the-parent-instead)).

A limit can also be lowered. A limit set below what the job has already used
ends the job at its next budget check: a `ready` job fails with
`budget_exhausted` at its next claim. That is a way to stop further automatic
retries. It holds only until the next revival without `--keep-budget`, which
restores the declared allowance.

Do not try the other routes. Editing `job.json` fails the job at the digest
check. Counters are not reset, since that would lose the record of what the job
did; a revival raises the limits instead. `--force` does not skip a budget: it
only accepts the revival hazard, and the manager checks the budget again at
launch.

## Switching the workflow

A job runs the installed workflow its `job.json` names, and `job.json` cannot
be edited. To make existing jobs run another installation, such as a newer
version of the same workflow, post `set_workflow`. To bump every ready job to
the newest installation of a release:

```console
httk workflow install git+https://github.com/ORG/REPO@v1.3.0#relax
httk job request set_workflow --workflow relax@v1.3.0 --state ready --reason "bump to v1.3.0"
```

To repair failed jobs with a fixed version, switch them, then restart them:

```console
httk job request set_workflow --workflow relax@v1.3.1 --state failed --reason "fixed runner"
httk job request restart --state failed --reason "rerun on v1.3.1"
```

`--workflow` takes an installed id, a short name (the newest installation of
that name) or `NAME@REF`. It is resolved once, in the job's workspace, and the
request names the installation's id. Jobs are selected by `JOB_ID` arguments,
by `--state STATE` (repeatable), by `--step STEP` (repeatable) and by
`--placement PREFIX`, as for `job transfer`: `JOB_ID` and
`--state` are joined, `--step` narrows both and alone selects from every
unowned state, and `--placement` narrows `--state` or a `--step` given without
`JOB_ID`. Every action of `job request` takes them, so
`--state ready --step relax` bumps only the ready jobs that will run `relax`.

Every selected job is checked before any request is posted, and the job's owner
checks again when it applies the request. The candidate must:

1. be installed in the job's workspace;
2. carry the same declaration `$id` as the job's stored declaration, or, when
   the job's declaration has no `$id`, have the same short name;
3. declare the same inputs as the job's `declared.inputs`: the same names,
   payload destinations and hook-consumed inputs;
4. declare the same output names as the job's stored declaration;
5. declare no parameter that the job has no value for, unless the job's own
   declaration also had no value for it, and keep every parameter the job's
   declaration lists, each value matching the declared type;
6. declare the same environment names and types;
7. declare the step the job runs next; for a parent waiting on its children
   (or failed at its wait), the steps its join advances to.

A job made from an installed workflow stores its declaration and records
empty parameter, input and environment declarations by leaving them out. A job
that stores no declaration, such as a spawned child, is compared with its
current installation instead; when that is gone too, the switch is refused as
unverifiable. A refusal names the rule that failed. Switching a job back to the
workflow its `job.json` names removes the override. A job that runs a
language realization (CWL, PWD, jobflow, httk v1) is never switched, since its
executed document lives in the payload.

The request applies to any job that is not `succeeded` or `cancelled`. It does
not need `--force`. An unowned job is switched at once; a job a manager holds
is switched at its next boundary, and a running attempt finishes on the code it
started with. A waiting parent can be switched; its children are separate jobs
that keep their own workflows. Children that the job spawns after the switch run
the new installation, and `a.call` uses the new installation's
`[workflow.calls]`.

What changes is the runner code and its calls. What stays as created is the
payload, the parameters, the declarations, `resources` and `step_resources`
(a larger `maxtime` in the new version is not used; `job show` shows the job's
resources). An instantiate hook of the new version is not run again; the
command warns when the candidate has one. Implementations that share one
declaration `$id` can differ in language, so a switch can move a job, for
example, from a Python runner to a Bash one.

The choice is written to `state.json` as `workflow_override` `{id, name}`, and
the recorded `runner_steps` of the old runner are cleared. `job show` prints
the declared workflow and the one the job runs. Collection uses the
installation that produced the outputs, and the collected job record keeps the
declared `workflow` with `workflow_override` beside it. A job that carries an
override cannot be ejected into a workspace daemon's exchange, whose adoption
rebuilds `state.json`; the workspace daemon protocol does not carry
`set_workflow`.

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
A parent can also fail at its wait with `budget_exhausted`: its join is
satisfied (or rescued), but the advance would pass `maximum_activations`. It
keeps its `join` then too; see [Raising a budget](#raising-a-budget).

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
`continue --tree`. A budget refusal happens only with `--keep-budget`, which
`--tree` forwards to every `continue` it posts. Children in other states (`succeeded`, `paused`, running,
`cancelled`) are left alone. Join children `--tree` leaves alone (not found,
`cancelled`, or held by an owner at that moment) are reported with a note, as
without `--tree`. Fix the leaf failures first: a descendant that fails for its
own reason, not at a wait, returns to `ready` and reruns its step, which may
fail again. `--tree` is valid only with `continue`.

### Restarting the parent instead

`restart` on a parent that failed at its wait reruns the spawning step, which
creates new children. The old children stay where they are. Terminal ones can be
removed with `httk job delete`. The restart restores the declared allowance, so
it runs even with `maximum_attempts_per_activation = 1`; with `--keep-budget`
the spawning step's attempt counts against the current limit, and such a
`restart` fails right back with `retry_exhausted`. Alternatively,
`httk job request override_step --override-step <spawning step>` reruns that step as a
new activation.
The restarted parent no longer waits on them, so
reviving one of them needs `--force`, and the parent will not notice it.

### Cancelled and deleted children

A cancelled child cannot be revived by any request, so a join naming it stays
impossible: `continue` on the parent fails again at once. `restart` the parent
or `override_step` it instead. A deleted child makes a reopened wait fail with `dependency_failure`
after the join grace, a manager option that defaults to one hour.

### Parents with `on_impossible`

A parent whose gather named an `on_impossible` step advanced to that step
instead of failing, which consumed the observation and cleared the join
(unless that advance failed with `budget_exhausted`, which keeps it). Reviving
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
job yourself, deepest first, then the parent. `--state`, `--step` and `--placement`
also select jobs of local workspaces only; give job ids for a remote one.
`set_workflow` resolves `--workflow` on the remote side, against the remote
workspace's installations; it is refused for a workspace daemon remote.
{doc}`/details/remotes` covers what a remote binding accepts.

## See also

- {doc}`running`: inspecting and controlling jobs.
- {doc}`/details/taskmanager`: the request table and the revival guard.
- {doc}`/details/workflow_cli`: `job request` options.
- {doc}`/details/composing_workflows`: failure semantics of called children.
- {doc}`/details/workflow_filesystem_api`: `state.json`, joins and requests in
  the protocol.
