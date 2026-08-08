# Native Bash runner API

*For authors writing a workflow runner in Bash.* The complete authoring surface,
each function beside its Python equivalent, is the table in {doc}`sdk_parity`,
which is normative and enforced by the test suite; this page teaches it.

A native Bash runner is one script that implements the steps of one workflow, the
same shape as a Python runner: `httk_workflow_runner` declares the workflow and
its steps, one `step_<name>` function implements each of them, and
`httk_workflow_main` dispatches the step the manager asked for.

Directory-package hooks are language-independent too. A Bash executable can be
the manifest's instantiate or collect hook; it consumes the JSON contract in
{doc}`workflow_packages` rather than the runner SDK. For example, an
instantiate hook can use `jq` to read the envelope, stage a file from the
payload-relative descriptor, and return the required response:

```bash
#!/usr/bin/env bash
set -euo pipefail

request=$(cat)
input_path=$(jq -r '.inputs.structure.path' <<<"$request")
cp -- "$input_path" files/structure-from-hook
parameters=$(jq '.parameters + {"prepared_by": "bash"}' <<<"$request")
jq -n --argjson parameters "$parameters" '{parameters: $parameters}'
```

This example is deliberately honest about JSON-in-Bash: general JSON needs a
parser such as `jq`; any language with JSON support can implement the same
contract. Python authors can use `httk.workflow.hookapi`'s `instantiate_main()`
and `collect_main()` helpers.

Nothing declares the shape of the workflow up front. A step decides *at run time*
which children to spawn and which step runs next, so the graph of a job is
whatever its steps published. Every Bash function below performs its work through
exactly the same implementation as the Python
{doc}`authoring SDK <runtime_helpers>`, so a Bash runner and a Python runner
publish the same bytes for the same campaign.

The manager exports absolute paths to the two packaged, sourced libraries, so a
runner never has to locate Python package data itself. Each function of those
libraries is one invocation of one subcommand of
{py:mod}`httk.workflow.shell_bridge`, the public name of the command bridge that
publishes through the same implementation the Python SDK does; read that module
when writing a runner in a third language.

## A complete runner

This is a full defect campaign: it characterizes a structure, spawns one child
job per candidate site, gathers them, and either reports or triages.

```bash
#!/usr/bin/env bash
# Defect campaign: characterize, relax every site, aggregate, triage.
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
httk_workflow_runner defects characterize relax aggregate triage

step_characterize() {
    local site
    while read -r site; do
        httk_workflow_spawn "site-$site" \
            --step relax \
            --parameter site="$site" \
            --parameter structure=@"defect-$site.json" \
            --data-mode transactional >/dev/null
    done <sites.txt
    httk_workflow_gather aggregate --when all_terminal --on-impossible triage
}

step_relax() {
    if ! httk_workflow_run --timeout 3600 -- relax --site "$(httk_workflow_parameter site)"; then
        httk_workflow_retry "the relaxation did not finish"
        return
    fi
    httk_workflow_put energy.json results/energy.json >/dev/null
    httk_workflow_succeed
}

step_aggregate() {
    local label state job_key workdir data failed=
    : >campaign.tsv
    while IFS=$'\t' read -r label state job_key workdir data; do
        printf '%s\t%s\n' "$label" "$(cat "$data/results/energy.json")" >>campaign.tsv
    done < <(httk_workflow_children --succeeded)
    while IFS=$'\t' read -r label state job_key workdir data; do
        failed=${failed:+$failed,}$label
    done < <(httk_workflow_children --failed)
    if [ -n "$failed" ]; then
        httk_workflow_advance triage --state failed="$failed"
    else
        httk_workflow_succeed
    fi
}

step_triage() {
    local failed
    failed=$(httk_workflow_state_get failed || true)
    httk_workflow_runlog_note "sites that did not relax: $failed"
    httk_workflow_fail defects.child_failed "failed: $failed"
}

httk_workflow_main
```

The runner is a single file, and every child job runs the same file at a
different step. Publish it once in the workspace and every job — parent and
child — references those bytes by digest:

```console
httk workflow runner publish defects.sh --workspace workflow-workspace --name defects/run.sh
```

```python
from httk.workflow import JobSpec, Workspace, prepare_job_payload

workspace = Workspace("workflow-workspace")
reference = workspace.publish_runner("defects.sh", name="defects/run.sh")
prepare_job_payload(
    "prepared-job",
    JobSpec(
        name="Si vacancies",
        workflow="defects",
        runner_source="workspace",
        runner_path=str(reference["path"]),
        runner_sha256=str(reference["sha256"]),
        initial_step="characterize",
        parameters={"encut": 520},
    ),
)
workspace.submit("prepared-job", "project/si-vacancies")
```

A Bash step can also prepare a payload itself, with
`httk_workflow_job_prepare DESTINATION SPEC.json`, where the spec object holds
the members of `JobSpec` — including `runner_executor`, `runner_source`,
`runner_sha256`, and `parameters` — and the created job's identity is printed back as
JSON.

The libraries require Bash 4.2 or newer and work with `set -euo pipefail`. They
call the manager's `HTTK_WORKFLOW_PYTHON` with argv arrays; they never evaluate a
command string, and they need neither `jq` nor `eval`.

## Registration and dispatch

`httk_workflow_runner WORKFLOW STEP...` declares the complete step set before any
work happens. Declaring the same step twice is refused, and so is a step name
that cannot spell a Bash function suffix.

`httk_workflow_main` checks the registration against the functions that exist —
in both directions, because a declared step with no `step_` function would fail
one activation later and a `step_` function nobody declared would never be
dispatched — replays any workdir batch an interrupted attempt sealed but did not
apply, and then dispatches. It turns every ending of a step into an outcome:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one | that outcome |
| the handler returns without publishing | `fail("no_outcome", ...)` |
| the step is not registered | `fail("unknown_step", "... registered steps: ...")` |
| the handler aborts | an `error.json` breadcrumb, then a nonzero exit the manager records as `process_failure` |

`HTTK_WORKFLOW_DESCRIBE=1` makes `httk_workflow_runner` print
`{"format": "httk-workflow-runner-description", "format_version": 1, "workflow":
..., "steps": [...]}` and exit before any step function is even defined, so a
tool can enumerate the steps of a runner it is not running. It is produced by the
shell itself: describing a runner reads no attempt context, needs no interpreter,
and touches nothing on disk. `httk_workflow_main --describe` does the same. The
step set is also recorded in the job's state frame as `runner_steps`, from the
first outcome the job publishes.

### Returning, not exiting

The outcome functions **return**; they do not exit. `httk_workflow_main` owns the
process exit status, because it is the one place that knows whether the step
published anything. A handler that has said how it ended should simply return:

```bash
step_relax() {
    if [ ! -f OUTCAR ]; then
        httk_workflow_fail relax.no_output "the relaxation produced no OUTCAR"
        return
    fi
    httk_workflow_succeed
}
```

The handler runs in a subshell whose exit is where every ending is turned into an
outcome, so a `set -e` abort in the middle of a step is reported as the failure it
is: the unpublished draft is discarded, `<control>/error.json` records the step,
the failing command, and the exit status, and the runner exits nonzero. Nothing a
step composed lives in shell state, so the subshell costs a step nothing.

## What a step reads

| Call | What it returns |
| --- | --- |
| `httk_workflow_parameter NAME [DEFAULT]` | one member of the job's opaque `parameters` object |
| `httk_workflow_setting NAME [DEFAULT]` | one application setting: job parameter, `HTTK_*`, workspace setting, then the call default |
| `httk_workflow_environment NAME [DEFAULT]` | one declared workflow environment value: job override, declared setting `HTTK_*`, workspace setting, declaration default, then the call default |
| `httk_workflow_context [FIELD]` | the attempt context, or one field of it |
| `httk_workflow_state_get NAME` | one key of the job's JSON state |
| `httk_workflow_declaration NAME` | one workflow declaration: the observed document, else the declared one; 1 when neither exists |
| `httk_workflow_children [--all\|--succeeded\|--failed]` | one tab-separated row per observed child |
| `httk_workflow_child LABEL FIELD` | one field of one observed child |
| `$HTTK_WORKFLOW_STEP` | the step this attempt runs |
| `$HTTK_WORKFLOW_WORKDIR`, `$HTTK_WORKFLOW_JOB_DIR`, `$HTTK_WORKFLOW_DATA_DIR` | absolute paths; the data directory is set only for a transactional job |
| `$HTTK_WORKFLOW_DURABLE` | `1` on a storage-durable workspace, `0` otherwise |

A step starts in its workdir, so ordinary relative paths are workdir paths.

`httk_workflow_environment` only reads names declared by the workflow and
returns exit status 1 for an undeclared or unresolved name without a default.
The declared environment metadata and per-job overrides are described in
{doc}`workflow_packages`.

`$HTTK_WORKFLOW_DURABLE` is the durability contract for a Bash runner. There is
no new function: every outcome, transaction, spawn, and workdir batch you publish
through `httk_workflow_*` already synchronizes itself before it becomes
authoritative exactly when this is `1`, because the bridge reads the same
workspace mode from the attempt context. A step that writes its own protocol
bytes by hand should honour the same flag; a step that only calls the packaged
functions never has to look at it.

Job state is stored inside the payload, below `.httk-job/`, so it survives
retries, step advances, and isolated workdirs, and it travels with a transferred
job. `httk_workflow_state_set NAME VALUE` stores one value,
`httk_workflow_state_merge NAME=VALUE ...` several in one atomic replace, and
`httk_workflow_state_delete NAME` removes one. A value that parses as JSON is
stored as that JSON value, and anything else as the string it is, so
`state_set answer 42` stores a number and `state_set stage relaxing` a string.
`NAME=@file.json` reads the value from a file.

`httk_workflow_declare NAME FILE.json` records what this job *observed* about its
own workflow declaration, and `httk_workflow_declaration NAME` prints one back —
the observed document when the job wrote one, the document `job.json` declared
otherwise, and exit status 1 when there is neither. The document is passed as a
file because a whole JSON object is what a command line cannot quote, and it is
carried verbatim: nothing in *httk-workflow* looks inside it. It is stored at
`.httk-job/declarations/NAME.json`, digest-excluded like the rest of `.httk-job/`,
and the last write of a name wins:

```bash
httk_workflow_declaration workflow >declared.json
jq '.outputs = {"structures": 3}' declared.json >refined.json
httk_workflow_declare workflow refined.json
```

See {doc}`declarations` for the declared/observed contract and what a collect
reports.

`httk_workflow_children` prints `label`, terminal state, job key, workdir, and
data directory, separated by tabs; the workdir and data columns are absolute
paths and are empty when the child has none. That is one row per child, so the
whole join reads with `read`:

```bash
while IFS=$'\t' read -r label state job_key workdir data; do
    ...
done < <(httk_workflow_children --succeeded)
```

`httk_workflow_child LABEL FIELD` reads one field of one child, where `FIELD` is
`label`, `state`, `job_id`, `job_key`, `failure_code`, `failure_message`,
`payload`, `workdir`, `data`, or `data_generation`. The observation is empty
unless this activation followed a gather, which is why `aggregate` above hands
what it learned to `triage` through job state.

## What a step publishes

Exactly one outcome per attempt. `httk_workflow_spawn`, `httk_workflow_put`, and
`httk_workflow_remove` accumulate in one implicit draft below the attempt control
directory, and the draft has no effect until a terminal call publishes it with a
single atomic rename. A second terminal call is refused, and a draft left by an
aborted handler is discarded, so no half-outcome ever reaches the manager.

The draft lives in the control directory rather than in shell state, which is
what lets a Bash step compose it across as many bridge calls as it likes: the
spawned children, the staged data transaction, and the operation counter are all
read back from the draft by whichever process asks next.

| Call | Meaning |
| --- | --- |
| `httk_workflow_advance STEP [--state NAME=VALUE ...] [--priority N]` | run `STEP` next; the state is written before publication |
| `httk_workflow_gather STEP [--when C] [--count N] [--on-impossible STEP] [--priority N]` | wait for the children spawned on this attempt, then run `STEP` at the optional priority |
| `httk_workflow_succeed` | the job is done |
| `httk_workflow_retry REASON` | repeat this activation within the job's attempt budget |
| `httk_workflow_pause REASON` | stop until an operator resumes the job |
| `httk_workflow_fail CODE MESSAGE [--details @FILE.json] [--retryable] [--priority N]` | the canonical failure record at the optional terminal priority |

`fail` publishes the one canonical failure object,
`{"code", "message", "details", "retryable"}`, which is exactly what a Python
runner and the manager itself publish. `CODE` is the string a job lists in
`retry_on`; `--retryable` declares that repeating the attempt could help, which
the manager honours within the job's budgets; `--priority` changes the terminal
marker priority.

Every step name these calls accept is checked against the registered set at the
call that names it, so `httk_workflow_advance colect` is refused immediately,
listing the registered steps, instead of failing the job one activation later.
The same check covers a gather target, its `--on-impossible` step, and the
`--step` of a spawned child that inherits this runner.

### Spawning children

`httk_workflow_spawn LABEL ...` registers one child job, to be created when the
outcome is published, and prints its job key. The label is mandatory and unique
within one attempt: it is how `gather` and `httk_workflow_children` name that
child later, and it becomes the child's job tag by default, so its payload
directory is readable at a glance.

A spawned child needs no prepared payload at all. `--step` and `--parameter`
synthesize a complete `job.json`, and everything not given follows the spawning
job: its workflow, its claim pool, its priority, its resources, and its runner.

| Option | Meaning |
| --- | --- |
| `--step STEP` | the step the child starts at; required unless `--payload` is given |
| `--parameter NAME=VALUE`, `--parameter NAME=@FILE.json` | one member of the child's `parameters` object |
| `--payload DIRECTORY` | spawn a prepared payload directory instead of synthesizing a job |
| `--runner inherit\|ws:PATH@SHA256\|installed:PATH@SHA256` | which runner the child executes; `inherit` is the default |
| `--placement PATH` | where the child is created; the placement of this job by default |
| `--tag TAG`, `--name NAME`, `--workflow WORKFLOW` | override what the child is called |
| `--priority N`, `--claim-pool POOL`, `--capability NAME` | override how the child is scheduled |
| `--workdir-mode persistent\|isolated`, `--workdir-path PATH` | the child's workdir |
| `--data-mode none\|transactional` | whether the child owns durable data |
| `--retry-on CODE`, `--max-attempts-per-activation N`, `--max-total-attempts N`, `--max-activations N` | the child's retry policy |
| `--resources @FILE.json` | the child's requested resources |

`inherit` copies this job's own `(source, path, sha256)`, which is what a campaign
whose steps all live in one published runner wants. A payload runner cannot be
inherited, because a synthesized child has no payload to copy it into: publish it
with `httk workflow runner publish` and name it with `--runner ws:PATH@SHA256`, or
prepare a payload directory and spawn that with `--payload`.

### Gathering them

`httk_workflow_gather STEP` joins exactly the children spawned on this attempt —
the same bundle that creates them, which is what makes the join resolvable — and
runs `STEP` when the condition holds. `--when` is `all_succeeded` (the default),
`all_terminal`, `any_succeeded`, `any_terminal`, or `at_least` with `--count`.
`--priority` changes the priority of the activation that resumes after the join.
When the condition can no longer be met the job advances to `--on-impossible` if
one is named, and fails with `dependency_failure` otherwise.

### Data and workdir changes

For a job with `data.mode` `transactional`, `httk_workflow_put SOURCE DESTINATION`
stages a file or a directory and `httk_workflow_remove DESTINATION [--missing-ok]`
stages a removal. The manager applies them exactly once when it commits the
outcome. Operation identifiers are generated in call order and printed, and the
counter lives in the draft rather than in one process, so `op-0001`, `op-0002`, …
come out in the same sequence however many bridge calls a step makes:

```bash
httk_workflow_put energy.json results/energy.json      # op-0001
httk_workflow_put bands results/bands                  # op-0002
httk_workflow_remove scratch --missing-ok              # op-0003
httk_workflow_succeed
```

A job with `data.mode` `none` has no data transaction, and these calls say so
rather than staging something that could never be committed.

`httk_workflow_workdir_apply SPEC.json` applies a replayable batch of changes to
the *workdir*, which `httk_workflow_main` completes on the next attempt if this
one is interrupted after sealing it. A spec uses the protocol operation names
`make-dir`, `put-file`, `put-tree`, `replace-tree`, and `remove`. Sources are
copied into a sealed bundle before publication, and symlinks and special files
are rejected.

## Many calls, one interpreter

Each Bash function above is one short-lived Python process. A step that composes
many operations can send them as one group instead, one command per line, sharing
that process's attempt and its draft:

```bash
httk_workflow_batch <<'EOF'
# lines are the bridge commands behind the functions, with shell quoting
state-set converged true
state-set energy -12.5
put energy.json results/energy.json
remove scratch --missing-ok
EOF
```

A batch stops at its first failing line, names that line on stderr, and reports
its exit status. Blank lines and `#` comments are ignored, and a batch cannot
contain another batch. There is no long-lived coprocess: a batch removes the
interpreter starts that matter without a second process to keep alive and reap.

## Exit codes

Every function reports one of three statuses, so an absent answer never looks
like a broken call:

| Status | Meaning |
| --- | --- |
| `0` | the call succeeded |
| `1` | the answer is legitimately absent: an unset state key, a missing parameter without a default, a null child field, a child that was not observed |
| `2` | the call is refused: bad usage, a protocol violation such as a second terminal call, or a corrupt attempt context |

Reading something that may not be there is therefore an ordinary conditional:

```bash
if energy=$(httk_workflow_state_get energy); then
    printf 'resuming from %s\n' "$energy"
fi
failed=$(httk_workflow_state_get failed || true)
```

The functions that run a program report the classified outcome of that program
instead:

| Status | Meaning |
| --- | --- |
| `22` | `httk_workflow_run` and `httk_vasp_run`: the program exited nonzero |
| `124` | the program timed out and its process group was terminated |
| `125` | a checker or a diagnostic stopped the program; also what the manager's launcher reports for a runner it could not start at all |
| `20` | `httk_vasp_run` and `httk_vasp_diagnose`: a structured diagnostic stop |
| `21` | `httk_vasp_run`: the calculation completed without converging |
| `3` | `httk_vasp_remedy_plan`: the reviewed policy has no safe remaining action |

## Supervised commands

`httk_workflow_run [--timeout S] [--grace S] [--report FILE] [--stdout FILE]
[--stderr FILE] [--checker SPEC.json] -- ARGV...` runs an argv array under
supervision and terminates its whole process group on timeout. The versioned JSON
report it writes is authoritative.

A checker spec has format `httk-workflow-checker-spec`, format version 1, an
`argv` string array, and optional `required` and `sources` fields. Each source
contains `path` and may contain `name` and a positive `inactivity_timeout`. The
checker receives one `httk-workflow-checker-event` JSON object per line on stdin
and emits `httk-workflow-checker-result` version 1 objects on stdout. Diagnostics
belong on stderr.

`httk_workflow_runlog_note`, `httk_workflow_runlog_headline`, and
`httk_workflow_runlog_append MESSAGE FILE...` retain structured evidence in the
workdir's run log; `httk_workflow_log LEVEL MESSAGE...` writes a timestamped line
to stderr, which the manager retains too. `httk_calc`, `httk_template_render`,
`httk_compress`, and `httk_decompress` are safe replacements for commonly used
*httk* v1 conveniences: templates use `string.Template` and an explicit JSON
values object, never `eval`.

## VASP functions

The `httk_vasp_*` surface corresponds directly to functions in
`httk.workflow.vasp`:

- `prepare`, `prepare_kpoints`, `prepare_potcar`, `get_tag`, `set_tag`, and
  `nbands`;
- `run` and `diagnose`;
- `remedy_plan` and `remedy_apply`;
- `preclean`, `clean_outcar`, `normalize_poscar`, `scale_poscar`,
  `rattle_poscar`, and the energy, volume, POTIM, plane-wave, and POTCAR
  summary extractors.

Source `$HTTK_WORKFLOW_VASP_BASH_API` after `$HTTK_WORKFLOW_BASH_API`. Every option
of the underlying subcommand is available, because the arguments are passed through
untouched.

Remedies are never automatic. Applying a decision uses a replayable workdir batch
and records before/after input digests and policy history. Preparation also
enforces the conservative 240-byte VASP workdir-path limit.

Version 2 of this library keeps the function surface of version 1 and changes three
defaults, in Bash and in Python at once:

- k-point centering starts at `Monkhorst-Pack`, because the reviewed ladder promotes
  `Gamma` as the fix for two k-point failure classes;
- `httk_vasp_preclean` keeps `CONTCAR` and `vasp-run-report.json`, which are what a
  remedy and a restart read; `--also-remove NAME` deletes them anyway;
- `httk_vasp_rattle_poscar` requires `--seed` or `--entropy`, because an unseeded
  retry rattles a structure exactly like the attempt before it.

`httk_vasp_remedy_plan` prints the diagnosed problem on stdout, takes `--directory`
(the calculation the remedy is validated against) and `--policy`, and records the
escalation ladder in the job state directory rather than in the workdir, so a job
with an isolated workdir keeps climbing it. A complete Bash VASP runner built on
these functions ships with the module: see {doc}`vasp_runners`.

## Mapping from *httk* v1

| *httk* v1 capability | Native *httk₂* replacement |
| --- | --- |
| `HT_TASK_INIT`, next/finished/broken | `httk_workflow_runner`, `step_` functions, and the outcome functions |
| `HT_TASK_SUBTASKS`, `HT_TASK_CREATE` | `httk_workflow_spawn` and `httk_workflow_gather` |
| `HT_TASK_ATOMIC_*` | `httk_workflow_put` / `remove`, or `httk_workflow_workdir_apply` |
| `HT_TASK_STORE_VAR` | `httk_workflow_state_set` and `state_get`, which belong to the job |
| `HT_TASK_RUN_CONTROLLED`, follow-file checkers | `httk_workflow_run` and the checker JSON-lines protocol |
| priority file | `--priority` on the published outcome |
| run log helpers | `httk_workflow_runlog_*` |
| math, template, compression | `httk_calc`, `httk_template_render`, `httk_compress`; no shell evaluation |
| node probing through `mpirun` | requested resources in the attempt context |
| POTCAR/KPOINTS/INCAR preparation | `httk_vasp_prepare*` |
| stdout/OSZICAR/OUTCAR checkers | VASP 5/6 structured diagnostics |
| `VASP_INPUTS_ADJUST/FIX_ERROR` | explicit `httk_vasp_remedy_plan` and `httk_vasp_remedy_apply` |
| energy/volume/POTIM/plane waves/cleanup | native VASP extraction and cleanup functions |

Trivial path matching and field splitting use normal quoted Bash constructs.
Native code does not source or expose the legacy `HT_TASK_*` or `VASP_*`
function names. Unchanged *httk* v1 workflows continue to use
[*httk* v1 task compatibility](v1_compatibility.md). For a step-by-step
conversion, see
[*httk* v1 migration guide](httk_v1_migration_guide.md).
