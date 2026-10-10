# Rust runner API

*For authors writing a workflow runner in Rust.* The Rust SDK is the same
authoring surface as the {doc}`Python <../runtime_helpers>`, {doc}`Bash
<bash_api>`, {doc}`C <c_api>`, and {doc}`Fortran
<fortran_api>` ones, in idiomatic, dependency-free Rust. Like the Bash and
C libraries it is a **bridge client**: every verb spawns `$HTTK_WORKFLOW_PYTHON
-m httk.workflow._shell_bridge <verb> …`, which drives the same `Attempt` object
the Python SDK exposes, so a Rust runner and a Python, Bash, C, or Fortran runner
publish the same bytes for the same campaign. Only the `--describe` handshake is
native. The normative cross-language semantics are the table in {doc}`sdk_parity`;
the method-by-method Rust mapping is the table below.

Unlike the Fortran SDK — which is `iso_c_binding` bindings *over* the C library —
this crate is **not** an FFI layer over `httk_workflow.c`. It is a self-contained
reimplementation of the same thin pattern in safe Rust: `#![forbid(unsafe_code)]`,
**zero crates.io dependencies** (std only), so `cargo build --offline` and plain
`rustc` work with no network at all. The crate is `languages/rust/`
(`Cargo.toml` and one `src/lib.rs`), packaged under `httk.workflow`, designed to
be **path-depended** from a runner crate or **vendored** into one.

## A complete runner

A runner declares its workflow and its complete step set once, registers one
handler per step, and hands control to `Runner::main`:

```rust
use httk_workflow::{Attempt, Runner, StepError};

fn prepare(attempt: &Attempt) -> Result<(), StepError> {
    attempt.advance("run", &[])?;
    Ok(())
}
fn run(attempt: &Attempt) -> Result<(), StepError> {
    attempt.succeed()?;
    Ok(())
}

fn main() {
    Runner::new("my.workflow", &["prepare", "run"])
        .step("prepare", prepare)
        .step("run", run)
        .main();
}
```

A handler is any `Fn(&Attempt) -> Result<(), StepError>` — a free function or a
closure. Build the runner crate against the packaged SDK with an ordinary path
dependency, so nothing is fetched:

```toml
[dependencies]
httk_workflow = { path = ".../httk/workflow/languages/rust" }
```

```console
cargo build --release --offline
```

The compiled binary is an executable runner file: `httk job new --from-runner`
describes it by running it and installs it as an `adhoc:` workflow whatever
language wrote it, and the manager runs the installed copy directly.

A Cargo path cannot name an environment variable, so a workflow package depends
on `httk_workflow = { path = "target/sdk" }` and its `[workflow.build]` command
first copies the installed SDK crate there from `HTTK_WORKFLOW_LANGUAGES_DIR`:

```console
mkdir -p target && cp -R "$HTTK_WORKFLOW_LANGUAGES_DIR/rust" target/sdk && cargo build --release --offline
```

Its manifest then runs the registered binary (copied out of `target/release`) directly, with no `run` bridge
script (see {doc}`../details/workflow_packages`):

```toml
[workflow.runner]
command = ["{artifacts}/runner"]
```

Declare `target` and `Cargo.lock` as build `artifacts`, so the copied SDK and
the build outputs are stripped before publication and never enter the source
digest.

A complete VASP relaxation authored this way is described at the end of this
page.

## Registration and dispatch

`Runner::new(workflow, &["prepare", "run", …])` declares the complete step set
before any work happens; one `Runner::step(name, handler)` per declared step
attaches its handler, and the registrations chain. A name that is empty, contains
a character outside `[A-Za-z0-9._-]`, is duplicated, is declared without a
handler, or has a handler for a name that was not declared is refused with a
diagnostic on stderr and exit status `2`.

`Runner::main` reads the step the manager asked for, dispatches its handler, and
**owns the process exit status** (it calls `std::process::exit`) — which is why
the handlers *return* rather than exit. It turns every ending of a step into
exactly one outcome, the same guarantee the other SDKs give:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one, then returns `Ok(())` | that outcome |
| the handler returns `Ok(())` without publishing | `fail("no_outcome", …)` |
| the step is not registered | `fail("unknown_step", "… registered steps: …")` |
| the handler returns `Err(e)` | an `error.json` breadcrumb (exception `RustError`), then the nonzero exit `e.code()` the manager records as `process_failure` |

A handler returns `Ok(())` when it ended — whether or not it published — and
`Err(StepError)` when it could not complete. The `Err` case is the Rust analogue
of a C handler returning nonzero or a Bash handler dying under `set -e`: the
unpublished draft is discarded, `error.json` records the step and the message,
and the process exits with `StepError::code()`. `StepError::new(code)` names an
exit status and takes the default breadcrumb message `"<step> exited with status
<code>"`; `StepError::with_message(code, text)` sets an explicit one. A
[`BridgeError`](#error-semantics) propagated with `?` becomes a `StepError` with
code `2` (`REFUSED`) — never `1`, which is the `ABSENT` convention for an
ordinary answer — so a handler can `?` its way out of an unexpected bridge failure.

Ordinary host failures propagate the same way. `StepError` implements `From` for
`std::io::Error`, `std::fmt::Error`, `std::num::ParseIntError`,
`std::num::ParseFloatError`, `std::str::Utf8Error`, and
`std::string::FromUtf8Error`, so `?` works directly on a file operation or a
parse. Such an error aborts with code `1` (`httk_workflow::HOST_FAILURE`, the
status an uncaught Python exception exits with) and a breadcrumb message that
names the kind of failure and carries the error's own text:

```rust
fn collect(attempt: &Attempt) -> Result<(), StepError> {
    // A missing file aborts with "I/O error: No such file or directory (os error 2)".
    let energy: f64 = std::fs::read_to_string("energy.txt")?.trim().parse()?;
    attempt.state_set("energy", &energy.to_string())?;
    attempt.succeed()?;
    Ok(())
}
```

A `std::io::Error` does not know which path failed; when the breadcrumb should
name it, or for an error type without a conversion, map it explicitly with
`map_err(|error| StepError::with_message(1, format!("cannot read energy.txt: {error}")))`.
The conversions are the std-only set a handler routinely meets; there is no
blanket `From<E: Error>`, which would collide with the reflexive `From<StepError>` since `StepError` is itself an `Error`.

`HTTK_WORKFLOW_DESCRIBE=1` and a `--describe` argument each make `Runner::main`
print the runner description and exit `0` before any step runs. The description is
produced natively, byte-for-byte what a Python, Bash, C, or Fortran runner prints
for the same workflow and steps:

```json
{"format": "httk-workflow-runner-description", "format_version": 2, "steps": ["prepare", "run"], "workflow": "my.workflow"}
```

The step names are byte-sorted; the `workflow` follows.

## Values, ownership, and error semantics

A Rust caller never sees a raw byte buffer or a status integer to interpret; the
three-status discipline of the bridge is expressed in the return types:

- **Reads** return `Result<Option<String>, BridgeError>`. `Ok(Some(value))` is a
  present answer, `Ok(None)` is a legitimately absent one (an unset state key, a
  missing parameter without a default, an unobserved child — the bridge's status
  `1`), and `Err(BridgeError)` is a call the bridge refused (status `2`) or a
  bridge that could not be reached. A single trailing run of newlines is stripped
  from a captured value, as command substitution does in the shell.
- **Command verbs** return `Result<i32, BridgeError>` — the bridge exit status of
  a call that ran (`0`, or the classified `22`/`124`/`125` from `run`), or
  `Err(BridgeError)` when the bridge could not be started at all.
- **`BridgeError`** distinguishes `PythonUnset` (the manager did not export
  `HTTK_WORKFLOW_PYTHON`, printed with the same stderr diagnostic as the Bash and
  C SDKs), `Spawn(io::Error)` (the subprocess could not be started), and `Refused`
  (the bridge ran and refused the call). It implements `std::error::Error`.

Reading a value is therefore an ordinary `match`:

```rust
match attempt.state_get("energy")? {
    Some(energy) => { /* resume from energy */ }
    None => { /* nothing recorded yet */ }
}
```

`Attempt::invoke(&["verb", "arg", …])` is the escape hatch for any bridge
subcommand without a dedicated method; stdin and stdout are inherited, so a
streaming verb streams, and it returns the bridge exit status.

## The Rust method table

Each method is one bridge subcommand — the same subcommand the paired Bash
function calls — so this table's C column is the {doc}`c_api` row this SDK
realizes, and through it the {doc}`sdk_parity` row. `args`/`files`/`assignments`
are `&[&str]` option arrays; a `fallback` is an `Option<&str>` default.

| Rust | C |
| --- | --- |
| `Runner::new(workflow, steps)` | `httk_workflow_runner` |
| `Runner::step(name, handler)` | (one `httk_workflow_step` entry) |
| `Runner::main(self)` | `httk_workflow_main` |
| `Runner::description(&self)` | `httk_workflow_describe` |
| `Attempt::invoke(argv)` | `httk_workflow_invoke` |
| `Attempt::context(field)` | `httk_workflow_context` |
| `Attempt::parent(field)` | `httk_workflow_parent` |
| `Attempt::parameter(name, fallback)` | `httk_workflow_parameter` |
| `Attempt::setting(name, fallback)` | `httk_workflow_setting` |
| `Attempt::environment(name, fallback)` | `httk_workflow_environment` |
| `Attempt::stage_input(name, destination, fallback)` | `httk_workflow_stage_input` |
| `Attempt::state_get(name)` | `httk_workflow_state_get` |
| `Attempt::state_set(name, value)` | `httk_workflow_state_set` |
| `Attempt::state_delete(name)` | `httk_workflow_state_delete` |
| `Attempt::state_merge(assignments)` | `httk_workflow_state_merge` |
| `Attempt::declare(name, document_file)` | `httk_workflow_declare` |
| `Attempt::declaration(name)` | `httk_workflow_declaration` |
| `Attempt::runlog_note(message)` | `httk_workflow_runlog_note` |
| `Attempt::runlog_headline(message)` | `httk_workflow_runlog_headline` |
| `Attempt::runlog_append(message, files)` | `httk_workflow_runlog_append` |
| `Attempt::log(level, message)` | `httk_workflow_log` |
| `Attempt::put(source, destination)` | `httk_workflow_put` |
| `Attempt::spawn(label, args)` | `httk_workflow_spawn` |
| `Attempt::call(label, workflow, args)` | `httk_workflow_call` |
| `Attempt::children(selection)` | `httk_workflow_children` |
| `Attempt::child(label, field)` | `httk_workflow_child` |
| `Attempt::advance(next_step, args)` | `httk_workflow_advance` |
| `Attempt::gather(next_step, options)` | `httk_workflow_gather` |
| `Attempt::succeed()` | `httk_workflow_succeed` |
| `Attempt::fail(code, message, retryable)` | `httk_workflow_fail` |
| `Attempt::retry(reason)` | `httk_workflow_retry` |
| `Attempt::pause(reason)` | `httk_workflow_pause` |
| `Attempt::batch()` | `httk_workflow_batch` |
| `Attempt::job_prepare(destination, spec_file)` | `httk_workflow_job_prepare` |
| `Attempt::workdir_apply(spec_file)` | `httk_workflow_workdir_apply` |
| `Attempt::run(args)` | `httk_workflow_run` |
| `Attempt::calc(expression)` | `httk_calc` |
| `Attempt::template_render(template_file, output, values_file)` | `httk_template_render` |
| `Attempt::compress(args)` | `httk_compress` |
| `Attempt::decompress(args)` | `httk_decompress` |

Booleans are Rust `bool`, as `Attempt::fail`'s `retryable`. `Attempt::put`
returns the staged path `data/<destination>`; explicit transactions
(`transaction begin|put|commit`) go through `Attempt::invoke`. `Attempt::stage_input` returns `Ok(true)` when staged and
`Ok(false)` when the payload has no such file. `Attempt::parent` returns
`Ok(None)` when the job has no reachable parent. `Attempt::call` spawns a job
of another installed workflow, named by a `[workflow.calls]` alias of this
job's workflow or the installed id it names, as a child and returns its job key; `args` carries the
`call` options (`--file NAME=PATH`, `--input NAME=PATH`, `--parameter K=V`, …). `Attempt::gather` takes a `Gather` options struct with `when`,
`count`, `on_impossible`, and `priority` fields, each `Option`, defaulting to the
bridge's own default (`Gather::default()`). As in C, a code's Bash API, such as
*httk-workflow-vasp*'s `httk_vasp_*`, has no dedicated methods; reach a `<code>-*`
verb such as `vasp-*` through
`Attempt::invoke`, which is why the example below runs the configured command
through `Attempt::run` and classifies its result. The `--details` and `--priority`
options of `fail` are likewise reachable through `Attempt::invoke`.

## Exit codes

The three-status discipline is re-exported as crate constants, identical to the C
and Fortran SDKs':

| Status | Meaning |
| --- | --- |
| `httk_workflow::OK` (`0`) | the call succeeded |
| `httk_workflow::ABSENT` (`1`) | the answer is legitimately absent: an unset state key, a missing parameter without a default, a child that was not observed |
| `httk_workflow::REFUSED` (`2`) | the call is refused: bad usage, a protocol violation, a corrupt attempt context — also the exit status when `HTTK_WORKFLOW_PYTHON` is unset |

`httk_workflow::HOST_FAILURE` (`1`) is not a bridge status: it is the exit
status of a handler aborted by a host error propagated with `?` (see
[Registration and dispatch](#registration-and-dispatch)).

The read return type folds `OK`/`ABSENT` into `Ok(Some)`/`Ok(None)` and `REFUSED`
into `Err(BridgeError::Refused)`, so a read is an ordinary `match` and never a
status comparison. `Attempt::run` returns the classified outcome of the program it
ran instead: `0`, `22` for a nonzero exit, `124` for a timeout whose process group
was terminated, and `125` when a checker or diagnostic stopped it.

## A VASP relaxation package

The `vasp-relax-rust` package of
[workflows-vasp-other-languages](https://github.com/httk/workflows-vasp-other-languages) is a complete
`prepare`/`run`/`publish` relaxation built with this SDK, mock-VASP compatible;
results stay in the workdir, and its `publish_data` parameter also copies them
to `data/`. Its workflow is `vasp.relax-rust`; run it
as `git+https://github.com/httk/workflows-vasp-other-languages#vasp-relax-rust`.
