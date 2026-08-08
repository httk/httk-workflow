# Native Rust runner API

*For authors writing a workflow runner in Rust.* The Rust SDK is the same
authoring surface as the {doc}`Python <runtime_helpers>`, {doc}`Bash
<native_bash_api>`, {doc}`C <native_c_api>`, and {doc}`Fortran
<native_fortran_api>` ones, in idiomatic, dependency-free Rust. Like the Bash and
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
`rustc` work with no network at all. The crate is `native/rust/`
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
httk_workflow = { path = ".../httk/workflow/native/rust" }
```

```console
cargo build --release --offline
```

The compiled binary is the runner file a job references. It is executable, so the
manager runs it directly and the scaffolder describes it by running it — a job
that starts from a runner file of your own is resolved the same way whatever
language wrote it.

A complete VASP relaxation authored this way — `prepare`, `run`, `publish`, the
`vasp.command` setting, mock-VASP compatible, publishing to transactional data —
ships as `examples/relax_rust/`; see the walkthrough at the end of this page.

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

`HTTK_WORKFLOW_DESCRIBE=1` and a `--describe` argument each make `Runner::main`
print the runner description and exit `0` before any step runs. The description is
produced natively, byte-for-byte what a Python, Bash, C, or Fortran runner prints
for the same workflow and steps:

```json
{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ["prepare", "run"], "workflow": "my.workflow"}
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
function calls — so this table's C column is the {doc}`native_c_api` row this SDK
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
| `Attempt::parameter(name, fallback)` | `httk_workflow_parameter` |
| `Attempt::setting(name, fallback)` | `httk_workflow_setting` |
| `Attempt::environment(name, fallback)` | `httk_workflow_environment` |
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
| `Attempt::remove(destination, missing_ok)` | `httk_workflow_remove` |
| `Attempt::spawn(label, args)` | `httk_workflow_spawn` |
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

Booleans are Rust `bool`: `Attempt::remove`'s `missing_ok`, and `Attempt::fail`'s
`retryable`. `Attempt::gather` takes a `Gather` options struct with `when`,
`count`, `on_impossible`, and `priority` fields, each `Option`, defaulting to the
bridge's own default (`Gather::default()`). As in C, the `httk_vasp_*` surface of
the Bash SDK has no dedicated methods; reach a `vasp-*` verb through
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

The read return type folds `OK`/`ABSENT` into `Ok(Some)`/`Ok(None)` and `REFUSED`
into `Err(BridgeError::Refused)`, so a read is an ordinary `match` and never a
status comparison. `Attempt::run` returns the classified outcome of the program it
ran instead: `0`, `22` for a nonzero exit, `124` for a timeout whose process group
was terminated, and `125` when a checker or diagnostic stopped it.

## The `examples/relax_rust` walkthrough

`examples/relax_rust/src/main.rs` has the same three-step shape as
`examples/relax_c`, in three step functions built entirely on the methods above.
It is a minimal example: it stages a POSCAR and an optional INCAR and runs one
command, and deliberately omits what the packaged `vasp-relax` runner adds — INCAR
and KPOINTS generation, restart and back-off logic, and the supervision
diagnostics and remedies.

- **`prepare`** stages the payload POSCAR into the workdir with
  `attempt.parameter("poscar", Some("files/POSCAR"))` and `std::fs::copy`, fails
  by name (`attempt.fail("vasp.input_missing", …, false)`) when it is absent,
  copies an optional INCAR, notes progress with `attempt.runlog_note`, and
  `attempt.advance("run", &[])`.
- **`run`** resolves the VASP command with `attempt.setting("vasp.command", …)` —
  falling back to a `vasp_command` parameter, and failing `vasp.command_missing`
  when neither is set — splits it on whitespace, runs it under supervision with
  `attempt.run`, records `state_set("classification", "completed")` and advances
  to `publish` on success, and `attempt.fail("vasp.failed", …, false)` otherwise.
- **`publish`** stages the finished files into the job's transactional data with
  `attempt.put` when the job has a data directory, and `attempt.succeed`.

Build and describe it:

```console
cd examples/relax_rust
make
./relax --describe
```

Then drive it exactly like `docs/quickstart.md`, naming the compiled binary as the
runner and the mock VASP as the command:

```console
httk project init --name relax-rust
httk workflow workspace init . --name default
httk workflow job new --workflow ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

The runner file declares no inputs, so its POSCAR is staged with `--file` and its
first step is named with `--step`. The finished calculation lands in
`jobs/*/data/vasp/`, and for the same input it publishes the same collected files
as the other relaxation examples.
