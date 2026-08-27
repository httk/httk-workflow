# Python and Bash authoring parity

*For runner authors in either language.* One table, below, is the **normative**
list of the authoring surface: every operation a step can perform, spelled in
Python and in Bash, with the protocol artifact it produces. The prose guides —
{doc}`../runtime_helpers` for Python and {doc}`native_bash_api` for Bash — teach the
two languages; this page is what they must both agree with.

The table is enforced. `tests/test_docs_parity.py` parses it and fails the build
when a listed Python member does not exist, when a listed Bash function is not
defined in the packaged library, or when a public member of `Runner` or
`Attempt`, or a function of `shell/httk-workflow.sh`, is missing a row. An
authoring feature that is not in this table does not exist as far as the
documentation is concerned, and adding one to the code means adding a row here.

A third language SDK, {doc}`in C <native_c_api>`, is a bridge client of this same
surface: like the Bash functions, each `httk_workflow_*` C function is one
invocation of one {py:mod}`httk.workflow.shell_bridge` subcommand, so a C runner
publishes the same bytes too. Its own function-by-function mapping to the Python
and Bash columns below lives in {doc}`native_c_api`, which is the reference and
the foundation the Fortran bindings build on.

A fourth SDK, {doc}`in modern Fortran <native_fortran_api>`, adds no new bridge
protocol at all: it is `iso_c_binding` bindings over that C library plus an
idiomatic Fortran module, so the C `httk_workflow_main` owns dispatch and every
verb reaches the same subcommand. A Fortran runner therefore publishes the same
bytes as the C, Bash, and Python runners; its Fortran-to-C mapping lives in
{doc}`native_fortran_api`.

A fifth SDK, {doc}`in safe Rust <native_rust_api>`, is a bridge client of this
same surface, but — unlike the Fortran one — it is not FFI over the C library: it
is a std-only, dependency-free reimplementation of the same thin pattern in safe
Rust, so `cargo build --offline` needs no network. Each `Attempt` method is one
invocation of one {py:mod}`httk.workflow.shell_bridge` subcommand, so a Rust
runner publishes the same bytes too; its Rust-to-C mapping lives in
{doc}`native_rust_api`.

A sixth SDK, {doc}`in pure Perl <native_perl_api>`, is another thin bridge client:
its object-shaped `Runner` and `Attempt` surface uses core Perl only, and each
bridge-backed method invokes the same {py:mod}`httk.workflow.shell_bridge`
subcommand. Its Perl-to-Python/Bash mapping lives in {doc}`native_perl_api`.

A seventh SDK, {doc}`in Ada <native_ada_api>`, is another thin binding client:
its `Interfaces.C` package wraps the existing C library, whose bridge-backed
methods invoke the same {py:mod}`httk.workflow.shell_bridge` subcommand. The C
library owns Ada registration and dispatch, so its Ada-to-C mapping lives in
{doc}`native_ada_api` and no Ada protocol implementation is introduced.

An eighth SDK, {doc}`in C++ <native_cpp_api>`, is a header-only C++17 binding
client over the same C library. Its RAII string wrapper and `Runner` builder add
no bridge protocol or dispatch implementation: the C library owns registration
and `httk_workflow_main`, while every verb reaches the same
{py:mod}`httk.workflow.shell_bridge` subcommand. Its C++-to-C mapping lives in
{doc}`native_cpp_api`.

A ninth SDK, {doc}`in Java <native_java_api>`, is a standalone `java.base`-only
reimplementation of the same thin bridge-client pattern. Its `ProcessBuilder`
argv is list-form and never uses JNI, C linkage, or a shell; only `--describe`
is native. Its Java-to-Python/Bash mapping lives in {doc}`native_java_api`.

One thing the SDKs do *not* share is the `error.json` breadcrumb's `exception`
label for a handler that ends abnormally: Bash records `ShellError`, C records
`CError`, Rust records `RustError`, Perl records `PerlError`, Java records
`JavaError`, and the Fortran, Ada, and C++ bindings inherit `CError` because
they end through the C library. The label names
the language a handler died in; the outcome the manager acts on is identical.

Both languages perform their work through exactly one implementation — the Bash
functions are thin calls into {py:mod}`httk.workflow.shell_bridge`, which drives
the same `Attempt` object the Python SDK exposes — so a Bash runner and a Python
runner publish the same bytes for the same campaign. That is a tested property,
not an aspiration: `tests/test_parity.py` runs one campaign twice, once in each
language, and compares everything both left behind.

## How to read the table

- **Python** names a member of `Runner`, `Attempt`, or a type reached from
  `httk.workflow` — either exported at its root or from one of its named
  submodules, in which case the cell spells the submodule (for example
  `protocol.JobSpec` or `runtime_utils.render_template`). `—` means the operation
  has no Python member of its own, because Python reaches it some other way (an
  attribute, an exception, the standard library).
- **Bash** names a function of the packaged `shell/httk-workflow.sh`, sometimes
  with the option that selects the behaviour of the row; only the function name
  is normative. `—` means the operation has no Bash function, because Bash
  reaches it through an exported environment variable instead — named in bold in
  that case.
- **Protocol artifact** is what the operation writes or reads on disk. `none`
  means the operation is entirely in-process.

## The table

| Python | Bash | Semantics | Protocol artifact |
| --- | --- | --- | --- |
| `Runner` | `httk_workflow_runner` | Declare the workflow, and in Bash its complete step set, before any step runs. | none; registration is in-process |
| `InstantiateHandler` | — | The callable type accepted by `Runner.instantiate`. | none |
| `Runner.workflow` | — | The name of the workflow this runner implements. | the `workflow` member of the description |
| `Runner.inputs` | — | Immutable creation-time staged-input declarations. | the optional `inputs` member of the description |
| `Runner.has_instantiate` | — | Whether a creation-time instantiate hook is registered. | none |
| `Runner.step` | **step_&lt;name&gt;** function | Register one handler for one step; the name is the function's unless overridden. | none |
| `Runner.instantiate` | — | Register the in-process Python creation-time hook receiving `scaffold.InstantiateContext`; directory packages may use the language-neutral executable hook contract instead. | none for the Python form; JSON stdin/stdout for the executable form |
| `Runner.steps` | — | Every registered step name, against which every step name an outcome publishes is checked. | `.httk-job/runner-steps.json`, rewritten when the set changes |
| `Runner.description` | `httk_workflow_main --describe` | Print this runner's own description and touch nothing else. | one `httk-workflow-runner-description` version 2 object on stdout |
| `Runner.main` | `httk_workflow_main` | Dispatch the step the manager asked for, and turn every ending of it into exactly one outcome. | the published `outcome.ready/` of one attempt |
| `Runner.main` | `httk_workflow_main` | A handler that returns without publishing an outcome is reported as such rather than left ambiguous. | `fail` outcome with `failure.code` `no_outcome` |
| `Runner.main` | `httk_workflow_main` | A step this runner does not implement is reported, with the registered steps named. | `fail` outcome with `failure.code` `unknown_step` |
| `Attempt` | `httk_workflow_main` | A handler that raises, or dies under set -e, discards its unpublished draft, leaves a breadcrumb, and lets the manager's retry policy own what happens next. | `error.json` (`httk-workflow-runner-error` version 2) in the attempt control directory |
| `Attempt.initialize` | `httk_workflow_main` | Bind this process to its attempt and replay every workdir batch an earlier attempt sealed but did not apply. | reads the `HTTK_WORKFLOW_*` variables; replays `.httk-runner/workdir-ready/` |
| `Attempt.context` | `httk_workflow_context` | The manager-written identity and restart evidence of this attempt; one field, or the whole object. | `attempt/context.json` (`httk-workflow-attempt-context` version 2) |
| `Attempt.step` | `httk_workflow_context step` | The step this attempt runs. Bash also exports it for the handler. | `context.json` → `step`; **$HTTK_WORKFLOW_STEP** |
| `Attempt.control` | — | The attempt control directory, where the outcome is published. | **$HTTK_WORKFLOW_CONTROL_DIR** |
| `Attempt.payload` | — | The immutable job payload directory. | **$HTTK_WORKFLOW_JOB_DIR** |
| `Attempt.workdir` | — | The directory the step does its work in. | **$HTTK_WORKFLOW_WORKDIR** |
| `Attempt.workspace` | — | The workspace root this job belongs to. | **$HTTK_WORKFLOW_WORKSPACE_DIR** |
| `Attempt.data` | — | The job's published transactional data, or `None` when the job has `data.mode` `none`. | **$HTTK_WORKFLOW_DATA_DIR** |
| `runtime.AttemptContext.durable` | `httk_workflow_context durable` | The workspace durability mode, threaded into every artifact the attempt publishes so an outcome, transaction, or child is synchronized before it is renamed authoritative. Neither SDK needs a call to act on it. | `context.json` → `durable`; **$HTTK_WORKFLOW_DURABLE** |
| `Attempt.job` | — | The immutable job definition as a typed value. | `job.json` (read-only) |
| `Attempt.parameters` | `httk_workflow_parameter` | The opaque implementation `parameters` object of the job. | `job.json` → `parameters` |
| `Attempt.parameter` | `httk_workflow_parameter` | One parameter, with an optional default; without one, a missing parameter raises `KeyError` in Python and exits 1 in Bash. | `job.json` → `parameters` |
| `Attempt.setting` | `httk_workflow_setting` | One application setting, resolved most-specific first: the job's `parameters[name]`, then the environment variable `HTTK_` + the dotted name upper-cased with dots as underscores (`vasp.command` → `HTTK_VASP_COMMAND`), then the workspace settings, then the default; without one, an absent setting returns `None` in Python and exits 1 in Bash. | `job.json` → `parameters`, manager-built attempt environment, `context.json` → `settings` |
| `Attempt.environment` | `httk_workflow_environment` | One declared workflow environment value. The start gate resolves every entry before a handler runs and snapshots the result for the attempt; direct reads retain the override → environment variable → workspace setting → manifest default → call-default order. An undeclared or unresolved value raises `KeyError` in Python and exits 1 in Bash. | `job.json` → `environment`, manager-built attempt environment, `context.json` → `settings` |
| `Runner.main` | `httk_workflow_main` | Before dispatch, a declared environment is resolved eagerly. Missing or ill-typed values publish non-retryable `environment_unresolved`; successful changes are recorded with their source and logged once after the handler completes. Describe mode and jobs without declarations are unchanged. | `fail` outcome, `.httk-job/declarations/environment.json`, `.httk-runner/runlog.jsonl` |
| `Attempt.state` | — | The job's private JSON state mapping, surviving every advance, every retry, and every isolated workdir. | `.httk-job/state.json` |
| `JobState.read` | `httk_workflow_state_get` | Read the whole state document, or in Bash one key; an unset key exits 1. | `.httk-job/state.json` |
| `JobState.set` | `httk_workflow_state_set` | Store one JSON value in one atomic replace. | `.httk-job/state.json` |
| `JobState.delete` | `httk_workflow_state_delete` | Remove one key, reporting whether it was present. | `.httk-job/state.json` |
| `JobState.merge` | `httk_workflow_state_merge` | Write several keys in one atomic replace. | `.httk-job/state.json` |
| `Attempt.log` | — | The append-only structured run log of this workdir. | `.httk-runner/runlog.jsonl` |
| `protocol.RunLog.append` | `httk_workflow_runlog_note` | Append one ordinary evidence event. | one `httk-workflow-runlog-event` line, kind `note` |
| `protocol.RunLog.append` | `httk_workflow_runlog_headline` | Append one event meant to be read first when the job is inspected. | one `httk-workflow-runlog-event` line, kind `headline` |
| `protocol.RunLog.append` | `httk_workflow_runlog_append` | Append one event with whole files attached by content and digest. | one `httk-workflow-runlog-event` line, kind `files` |
| — | `httk_workflow_log` | Write one timestamped `LEVEL MESSAGE` line to stderr, which the manager retains with the attempt. The Python side uses `logging`. | the attempt's captured stderr |
| `Attempt.declare` | `httk_workflow_declare` | Record the workflow declaration this job *observed*, carried verbatim; repeating it overwrites. | `.httk-job/declarations/<name>.json` |
| `Attempt.declaration` | `httk_workflow_declaration` | Read one declaration back: observed first, then the one `job.json` declared, else absent. | `.httk-job/declarations/<name>.json`, or `job.json` → `declarations` |
| `Attempt.run` | — | Run an argv array in the workdir and terminate its whole process group on timeout, returning a `CommandResult`. | none; the result is in memory |
| `supervision.ProcessSupervisor` | `httk_workflow_run` | Run an argv array under checkers and inactivity watches, writing the authoritative report of what it did. | `process-report.json` (`httk-workflow-process-report` version 2) |
| `Attempt.put` | `httk_workflow_put` | Stage one file or tree into the job's transactional data; the manager applies it exactly once when the outcome commits. | an operation of `outcome.tmp.<uuid>/transaction/` |
| `Attempt.remove` | `httk_workflow_remove` | Stage one removal from the job's transactional data. | an operation of `outcome.tmp.<uuid>/transaction/` |
| `Attempt.workdir_batch` | `httk_workflow_workdir_apply` | Group workdir changes so that an interrupted apply is replayed on the next attempt instead of being left half-done. | `.httk-runner/workdir-ready/<uuid>/`, then `workdir-applied/` |
| `ChildSpec` | `httk_workflow_spawn --step` | Describe a synthesized child job by its step and parameters; everything else follows the spawning job. | one entry of `outcome.tmp.<uuid>/children/spawn.json` and the child's `job.json` |
| `RunnerRef` | `httk_workflow_spawn --runner` | Which runner a synthesized child runs: `inherit` (Bash `inherit`), `workspace` (`ws:PATH@SHA256`), or `installed` (`installed:PATH@SHA256`). | `runner` of the child's `job.json` |
| `Attempt.spawn` | `httk_workflow_spawn` | Register one child under a mandatory unique label — a `ChildSpec`, or the path of a prepared payload directory — created when the outcome is published. | `outcome.tmp.<uuid>/children/spawn.json` |
| `Attempt.children` | `httk_workflow_children` | The children observed by the join that started this activation; empty when no join did, so it can be read unconditionally. | `context.json` → `children` |
| `ChildResult` | `httk_workflow_child` | One observed child by label: its state, identity, failure, and absolute payload, workdir and data paths. | `context.json` → `children[]` |
| `Attempt.advance` | `httk_workflow_advance` | Publish a new activation of this job at another step, optionally merging state first so the next step finds what decided to run it. `resources=` / `--resource NAME=INT` optionally sets its requirement. | outcome action `advance` |
| `Attempt.gather` | `httk_workflow_gather` | Python `a.gather(step, *, when="all_succeeded", count=None, on_impossible=None, rejoin=(), priority=None, resources=None)` waits for children spawned on this attempt plus earlier-activation labels named by `rejoin`; Bash waits for children spawned on this attempt. `when` is `all_succeeded`, `all_terminal`, `any_succeeded`, `any_terminal`, or `at_least` with `count`; when the condition can no longer be met the job advances to `on_impossible` if one is named, and fails with `dependency_failure` otherwise. `priority` and `resources=` optionally change the join activation. | outcome action `wait` with a `join` |
| `Attempt.succeed` | `httk_workflow_succeed` | Publish the successful completion of this job. | outcome action `succeed` |
| `Attempt.fail` | `httk_workflow_fail` | Publish a structured terminal failure: `code` is the token `retry_on` lists, `retryable` declares that repeating could help, and `priority` optionally changes the terminal priority. | outcome action `fail` with a `failure` |
| `Attempt.retry` | `httk_workflow_retry` | Ask for another attempt of this same activation. | outcome action `retry` |
| `Attempt.pause` | `httk_workflow_pause` | Pause this job until an operator resumes it. | outcome action `pause` |
| `Attempt.published` | — | Whether this attempt already published; a second outcome is refused before anything of the first is disturbed. | the existence of `outcome.ready/` |
| `protocol.JobSpec` | `httk_workflow_job_prepare` | The complete member set of a job definition, including the runner reference and the parameters. | `job.json` |
| `protocol.prepare_job_payload` | `httk_workflow_job_prepare` | Write `job.json` into a prepared payload directory from that specification. | `job.json` of a new payload |
| — | `httk_workflow_batch` | Run several bridge commands, one per line, in one interpreter start; a batch stops at its first failing line. | none |
| `runtime_utils.evaluate_expression` | `httk_calc` | Evaluate one arithmetic expression without a shell. | none |
| `runtime_utils.render_template` | `httk_template_render` | Render one template file with one JSON value document. | the named output file |
| `runtime_utils.compress_files` | `httk_compress` | Compress named files with `bz2`, `gz`, or `xz`, optionally removing the sources. | the compressed files |
| `runtime_utils.decompress_files` | `httk_decompress` | Decompress named files, optionally removing the sources. | the decompressed files |

Every scalar workspace setting is exported into each attempt's environment as
**`HTTK_<DOTTED_NAME_UPPERCASED>`**, with dots replaced by underscores (for
example, `vasp.command` becomes **`HTTK_VASP_COMMAND`**). The export uses
setdefault semantics: a variable already present in the manager's environment
wins. Boolean and non-scalar settings are not exported. The
**`HTTK_WORKFLOW_*`** namespace is reserved and no setting may derive a
variable in it.

## Exit-code discipline

Python raises and returns typed values; Bash cannot, so every Bash function
reports one of three statuses, and an absent answer never looks like a broken
call:

| Status | Meaning | Python equivalent |
| --- | --- | --- |
| `0` | the call succeeded | the return value |
| `1` | the answer is legitimately absent: an unset state key, a missing parameter without a default, a null child field, a child that was not observed | `KeyError`, or `None` |
| `2` | the call is refused: bad usage, a protocol violation such as a second terminal call, or a corrupt attempt context | `ValueError` or `RuntimeError` |

The functions that run a program report the classified outcome of *that program*
instead: `22` for a nonzero exit, `124` for a timeout whose process group was
terminated, `125` when a checker or diagnostic stopped it — which is also what
the manager's launcher reports for a runner it could not start at all. The VASP
functions add `20`, `21`, and `3`; see {doc}`native_bash_api`.

`httk_workflow_main` owns the exit status of a Bash runner, which is why the
outcome functions return rather than exit. `Runner.main` owns it in Python, and
returns `0` for every ending it turned into an outcome.

## Every ending is an outcome

The three rows above about `no_outcome`, `unknown_step`, and `error.json` are the
same guarantee stated three ways, and it is the reason the two SDKs can be
compared byte for byte at all:

- A handler that **returns without publishing** is not silently successful. The
  attempt is failed with code `no_outcome` naming the step.
- A step the runner **does not implement** is not a crash. The attempt is failed
  with code `unknown_step`, naming the steps that are registered.
- A handler that **raises** — or, in Bash, dies at a failing command under
  `set -e` — publishes nothing at all. Its unpublished draft is removed, so no
  half-outcome survives, and `error.json` records the exception, the message, and
  the traceback (in Bash, the file, line, and command the handler died on). The
  exception then reaches the manager, whose retry policy decides what happens
  next.

Because the draft is removed rather than published, a step that spawned three
children and then crashed spawned nothing: the children exist only if the outcome
that creates them was published.
