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
| `workflow/` | Only in an unlocked job: its own copy of the workflow's runner (see [Debug mode](#debug-mode-unlocked-jobs)). |

## What you may edit

Everything in the job directory may be edited while the job is `failed` or
`paused`, except `job.json`. It is digest-pinned at the first activation, and a
different digest fails the job with `protocol_error`. That includes the retry
policy, the parameters and the placement. The exception is an unlocked job (see
[Debug mode](#debug-mode-unlocked-jobs)), whose `job.json` may be edited.

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
`restart` reruns the spawning step, which reuses its existing children (see
[Restarting the parent instead](#restarting-the-parent-instead)).

A limit can also be lowered. A limit set below what the job has already used
ends the job at its next budget check: a `ready` job fails with
`budget_exhausted` at its next claim. That is a way to stop further automatic
retries. It holds only until the next revival without `--keep-budget`, which
restores the declared allowance.

Do not try the other routes. Editing `job.json` fails the job at the digest
check, unless the job is unlocked (see
[Debug mode](#debug-mode-unlocked-jobs)). Counters are not reset, since that would lose the record of what the job
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
the new installation (a restarted step reuses its existing children, which keep
their workflow; delete them to get children of the new one), and `a.call` uses the new installation's
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

## Debug mode: unlocked jobs

The rules that keep a job's provenance intact (a digest-pinned `job.json`, a
succeeded job that is never changed, the `set_workflow` compatibility rules, a
pinned workflow tree) get in the way while you develop or debug a workflow.
*Unlocking* a job lifts them for that job and marks it, so that collection does
not store it by accident:

```console
httk job unlock --reason "debugging relax" silicon   # marker and a runner copy
httk job unlock --no-copy silicon                   # marker only
httk job request unlock --reason "debugging relax" silicon   # the same, ancestors included
```

`job unlock` posts an `unlock` request per selected job and applies it at once
to a job no manager holds; it prints and exits as `job detach` does, and takes
`--reason TEXT` (default `job unlock`). `job request unlock` posts the same
request, and on a local workspace unlocks the ancestors too, with the same
lines and exit status. A remote workspace is refused by `job unlock` as by
`job detach`; `job request unlock` reaches an ssh remote through the signed
request envelope, but is refused there for a spawned job (unlock spawned jobs
on the workspace host with `httk job unlock`, which also unlocks their
ancestors). A workspace daemon does not carry `unlock` (run it on the daemon
host). A cancelled job cannot be unlocked, and a
succeeded job with a live seal must be released with `job unseal` first. A
second `unlock` of a job that already has its copy, or was unlocked with
`--no-copy`, is dropped as already unlocked; an unlock with the copy missing
makes a fresh copy.

**The marker is one-way.** `state.json` records `unlocked` with the time, the
operator and the reason. No request clears it, and it survives the repair of a
damaged `state.json`. `job show` prints an `unlocked` row (since and by, or inherited from, whether
it runs its own runner copy, and that collection needs `--force`), `job why`
has a "debug mode" check, and the JSON report carries `unlocked` and
`paths.runner_copy` (`<payload>/workflow`). To get an ordinary job
again, make a new one.

**Ancestors and children.** `job unlock` also posts a marker-only request to
every ancestor of each selected job, nearest first, following the parent
recorded in `job.json` (also for a detached job). Ancestor lines end with `
(ancestor)`; an ancestor already unlocked is skipped (`already unlocked
(ancestor)`), and a parent that cannot be found is reported without changing the
exit status. Each refusal is printed per job, and the command exits `1` if any
request was refused. Children that an unlocked parent publishes later start out
unlocked too (`unlocked.inherited_from` names the parent); they get no runner
copy.

**Jobs with an embedded workflow.** A job created with `job new --embed-workflow` (see
{doc}`details/workflow_packages`) already has its workflow in `workflow/`, so
`job unlock` keeps that tree (`existing runner copy kept`; `--no-copy` is the
same) and never looks anything up in the store; with the tree missing, a copying
unlock is dropped (`the embedded workflow/ is missing; nothing to copy`). Edit `workflow/` and restart.
From then on the job launches the tree unverified and the manager re-pins its
digest with `source` `"copy"`, like any unlocked copy. `set_workflow` is refused
for an embedded-workflow job, with or without the unlock: change `workflow/` instead.

### What it relaxes

- `job.json` may be edited at any time. The next reconcile pins the new digest
  and records `job_digest_repinned` (`{from, to}`) in the history. A `job.json` that cannot be
  read still fails the job.
- `continue`, `restart` and `override_step`, and also `set_budget` and
  `set_workflow`, apply to a `succeeded` job. The revival hazard described
  above still needs `--force`.
- `set_workflow` skips every compatibility check beyond the installation of
  the candidate: the ones listed above and the check of the call slots. The
  candidate must still be installed, and a job that runs a language
  realization is still refused.
- A succeeded unlocked job is not sealed (`seal` is `{"disabled": true}`), and
  `job seal` is refused. `job delete` of it needs no `job unseal`.

### Editing the runner

The default `unlock` copies the job's installed `package/` to `workflow/` at
the top of the job directory, next to `job.json`, `run/` and `data/`, so that a
listing shows that the job carries its own code. `workflow` is a reserved
payload name: inputs, `staged_files` and `--input-move-from-job` paths cannot
target it, and a v1 template package with a top-level `workflow` member is
refused at job preparation. An existing copy is never
overwritten (a first unlock that finds one keeps it and reports `job unlocked;
existing runner copy kept`, so you may place your own runner tree there before
unlocking), and a language realization (CWL, PWD, jobflow, httk v1) gets none.
From then on, while the job is unlocked, the manager launches the runner from
that copy (execution from `workflow/` is not automatic: only an unlocked job
that has the directory runs from it, and a `workflow/` in a locked job is
inert), and `workflow_pin`
records the digest of the copy at launch (with `source` `"copy"`, against
`"installed"` for the installation), without the installation's declared
build artifacts. Only the runner itself comes from the copy: the build spec,
platform probe and build artifacts of a compiled workflow still come from the
installation in the store, so editing `[workflow.build]` in the copy has no
effect; rebuild through the installation. Edit the copy in place and revive
the job:

```console
httk job unlock silicon
$EDITOR "$(httk job show silicon)/workflow/run.py"
httk job request restart --reason "try the fix" silicon
```

In a confined attempt and its ranks `workflow/` is a read-only overlay, like
`job.json` and `logs/`: job-local code runs only inside the job's own sandbox, and nothing a
running attempt writes there is trusted afterwards. Place or edit the copy
between attempts; you may also place your own tree before unlocking. An attempt
that leaves a `workflow/` that did not exist at its launch (`workflow_pin`
`workflow_present` false) fails with `protocol_error` ("the attempt created
workflow/; it was removed"), its outcome is voided as for an attempt that rewrites
`job.json`, and the created `workflow/` is deleted. The same check runs when the
manager recovers a crashed `running` attempt, and when it rebuilds a damaged
`state.json` it also deletes `workflow/` unless the salvaged pin says it was
present at launch. A tree present at launch is never touched; it was read-only
for the attempt. A spawned
child staged with a top-level `workflow/` is refused.

The installed workflow is untouched, so other jobs of it are not affected, and
`set_workflow` is refused while the copy exists, by the CLI before anything is
posted: edit or remove the copy instead. The copy is not fsynced, so after a
power loss it may be torn: remove `workflow/` and run `job unlock`
again. `--no-copy` is for debugging with data and `job.json` only.

### What stays yours

Nothing clears `data/`, `run/` or the children a job already published when a
step reruns; delete what must not survive. Still enforced: that only the owner
writes `state.json`, that requests apply once, the refusal of symlinks and
other unsafe entries, the placement and schema rules, the sandbox overlays,
`cancelled` as a final state, the retry budgets and the digests of staged
inputs. `job new --input-move-from-job` out of an unlocked job is refused (copy
the file instead), and so is ejecting it into a workspace daemon's exchange.

### Collecting an unlocked job

`httk collect` stores nothing for an unlocked job. It reports the job as
degraded, `job is unlocked (debug mode); pass --force to collect it`, and exits
nonzero; collection with `--postprocess` reports the same text as an error, and
`--force` lets both collect the job.
`httk collect --force` collects it through its workflow's registered collector
as usual; the collect hook of the runner copy is used only with
`--allow-job-collector`, unverified, only when no registered provider
supplies a collector, and only after the job has run again from the copy (`workflow_pin` `source` `"copy"`) with the copy unchanged since that launch. Unlocking a finished job and collecting straight away degrades the
job with `the job's last attempt did not run from its runner copy; restart it before collecting`. Other reasons read `runner copy modified since
the job ran: ... digest X does not match pinned Y` and `... could not be
verified`.
An unlocked job without a copy uses the installed tree as before. With `--into --force` the run is stored **without** any mark
that the job was unlocked: you take responsibility for it. Every per-job line
of an unlocked job carries `"unlocked": true`, forced or not. `--raw` and campaign collection are readouts and do
not check the marker. See {doc}`/collecting`.

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
   reopens the wait while `restart` reruns the step, which reuses the children
   that still exist.
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

`restart` on a parent that failed at its wait reruns the spawning step. A child
label is unique per parent job, and a spawn or call whose label names an
existing child reuses it: nothing new is created and the new specification is
ignored. A failed or cancelled child would be reused too, so the parent would
fail at its wait again. `restart` therefore refuses, and posts nothing, when the
parent recorded such a child: it prints one `error:` line per failed or
cancelled child (its label, job key, state and failure code) and the remedy,
then exits non-zero (`1`; `2` over ssh, where the far side's lines are relayed):

```console
$ httk job request restart --reason "redo" PARENT
error: x x--<uuid> is failed dependency_failure; a restart reruns the spawning step, which reuses it
resume failed descendants and the parent with 'httk job request continue --tree --reason TEXT PARENT'; ...
```

Revive the child first (`continue` it, or `continue --tree` the parent), or
delete it with `httk job delete` so the restarted step creates a fresh one.
`--force` restarts anyway; the runtime then logs a warning for each failed or
cancelled child it reuses. The check also applies to jobs selected with
`--state` or `--step`, and to a remote workspace over ssh. A child that no
longer exists is created anew. The restart restores the declared allowance, so
it runs even with `maximum_attempts_per_activation = 1`; with `--keep-budget`
the spawning step's attempt counts against the current limit, and such a
`restart` fails right back with `retry_exhausted`. Alternatively,
`httk job request override_step --override-step <spawning step>` reruns that step as a
new activation.
The restarted step waits on the reused children again. Between the
`restart` and the publication of the new wait, reviving one of them still needs
`--force`; revive the child before restarting the parent and no `--force` is
needed.

To get several children that share a name, number the labels. A restart of this
step reuses `site-0`, `site-1`, ... that still exist:

```python
for index, site in enumerate(sites):
    a.spawn(ChildSpec(step="relax", parameters={"site": site}), label=f"site-{index}")
a.gather("collect")
```

### Cancelled and deleted children

A cancelled child cannot be revived by any request, so a join naming it stays
impossible: `continue` on the parent fails again at once. A restart or `override_step` of
the spawning step reuses the cancelled child too (`restart` refuses unless
`--force`): delete it first, then restart the parent or `override_step` it. A deleted child makes a reopened wait fail with `dependency_failure`
after the join grace, a manager option that defaults to one hour.

### Parents with `on_impossible`

A parent whose gather named an `on_impossible` step advanced to that step
instead of failing, which consumed the observation and cleared the join
(unless that advance failed with `budget_exhausted`, which keeps it). Reviving
its child then still needs `--force`, and the parent does not notice the
revived child. The wait cannot be reopened. To have the parent use the revived
child, rerun a step that gathers it again (a runner step that calls
`gather(..., rejoin=[label])`, via `override_step`), or delete the child and
`restart` or `override_step` the spawning step to create a new one (without the
delete the step reuses it).

## Paused jobs

A job pauses on a `pause` request or when the runner calls `a.pause(reason)`.
It stays where it is until you `continue` it, which reruns the current step as a
new attempt, or `restart` it, which does the same; edit its files first, as for
a failed job. `override_step` moves it to a named step.

```console
httk job request continue --reason "quota restored" silicon
```

## Remote workspaces

`continue --tree` works on a remote workspace reached over ssh as it does
locally: the far side expands the tree, your machine signs only `continue`
requests (the selected jobs and their failed descendants, each noted on
stderr), and the far side serves them one at a time, deepest first, stopping
at the first that does not resume. A workspace daemon remote refuses `--tree`;
there, post one `continue` per failed job yourself, deepest first, then the
parent, or run it on the daemon host. The `restart` refusal above runs on the
far side of an ssh remote too. `--state`, `--step` and `--placement`
also select jobs of local workspaces only; give job ids for a remote one.
`set_workflow` resolves `--workflow` on the remote side, against the remote
workspace's installations; it is refused for a workspace daemon remote.
{doc}`/details/remotes` covers what a remote binding accepts. A job lost on a
remote after `job push` is returned from its `away` copy with
`httk job recover JOB` and pushed again.

## See also

- {doc}`running`: inspecting and controlling jobs.
- {doc}`/details/taskmanager`: the request table and the revival guard.
- {doc}`/details/workflow_cli`: `job request` options.
- {doc}`/details/composing_workflows`: failure semantics of called children.
- {doc}`/details/workflow_filesystem_api`: `state.json`, joins and requests in
  the protocol.
