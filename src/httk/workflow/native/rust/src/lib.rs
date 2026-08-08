//! Native httk-workflow authoring SDK, in safe Rust.
//!
//! This crate is the same authoring surface as the Python, Bash, C, and Fortran
//! SDKs, in idiomatic, dependency-free Rust. Like the Bash and C libraries it is
//! a **bridge client**: every verb spawns
//! `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge <verb> …` with
//! [`std::process::Command`] and reports what it did, so a Rust runner and a
//! Python, Bash, C, or Fortran runner publish the same protocol bytes for the
//! same campaign. Only the `--describe` handshake is native, so a runner can be
//! enumerated without an interpreter.
//!
//! Unlike the Fortran SDK — which is `iso_c_binding` bindings over the C library
//! — this crate is **not** an FFI layer over `httk_workflow.c`. It is a
//! self-contained reimplementation of the same thin pattern in safe Rust
//! (`#![forbid(unsafe_code)]`), with **zero crates.io dependencies**, so
//! `cargo build --offline` and plain `rustc` work with no network at all.
//!
//! # A complete runner
//!
//! A runner declares its workflow and its complete step set once, registers one
//! handler per step, and hands control to [`Runner::main`], which owns the
//! process exit status:
//!
//! ```no_run
//! use httk_workflow::{Attempt, Runner, StepError};
//!
//! fn prepare(attempt: &Attempt) -> Result<(), StepError> {
//!     attempt.advance("run", &[])?;
//!     Ok(())
//! }
//! fn run(attempt: &Attempt) -> Result<(), StepError> {
//!     attempt.succeed()?;
//!     Ok(())
//! }
//!
//! fn main() {
//!     Runner::new("my.workflow", &["prepare", "run"])
//!         .step("prepare", prepare)
//!         .step("run", run)
//!         .main();
//! }
//! ```
//!
//! A handler returns `Ok(())` when the step ended — whether or not it published
//! an outcome — and `Err(`[`StepError`]`)` when it could not complete, which
//! [`Runner::main`] turns into an aborted-attempt breadcrumb. Every ending
//! becomes exactly one outcome; see [`Runner::main`].

#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsStr;
use std::fmt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

/// The version of this native SDK, mirroring `HTTK_WORKFLOW_C_API_VERSION`.
pub const API_VERSION: u32 = 2;

/// The call succeeded.
pub const OK: i32 = 0;
/// The answer is legitimately absent (an unset key, a missing default-less read).
pub const ABSENT: i32 = 1;
/// The call was refused (bad usage, a protocol violation, a corrupt context).
pub const REFUSED: i32 = 2;

const PYTHON_VAR: &str = "HTTK_WORKFLOW_PYTHON";
const BRIDGE_MODULE: &str = "httk.workflow._shell_bridge";
const RUNNER_WORKFLOW_VAR: &str = "HTTK_WORKFLOW_RUNNER_WORKFLOW";
const RUNNER_STEPS_VAR: &str = "HTTK_WORKFLOW_RUNNER_STEPS";
const STEP_VAR: &str = "HTTK_WORKFLOW_STEP";
const CONTROL_VAR: &str = "HTTK_WORKFLOW_CONTROL_DIR";
/// The exception label an aborted Rust handler records, the analogue of the C
/// SDK's `CError` and the Fortran SDK's inherited `CError`.
const ABORT_EXCEPTION: &str = "RustError";

/// A failure to reach or run the Python bridge itself, distinct from a bridge
/// call that ran and reported an absent or refused answer.
#[derive(Debug)]
pub enum BridgeError {
    /// `HTTK_WORKFLOW_PYTHON` is unset or empty; the manager did not set it.
    PythonUnset,
    /// The bridge subprocess could not be started.
    Spawn(std::io::Error),
    /// The bridge ran and refused the call (exit status other than `0`/`1`).
    Refused,
}

impl fmt::Display for BridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeError::PythonUnset => {
                write!(f, "HTTK_WORKFLOW_PYTHON is not set by the workflow manager")
            }
            BridgeError::Spawn(error) => write!(f, "could not start the httk-workflow bridge: {error}"),
            BridgeError::Refused => write!(f, "the httk-workflow bridge refused the call"),
        }
    }
}

impl std::error::Error for BridgeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BridgeError::Spawn(error) => Some(error),
            _ => None,
        }
    }
}

/// The error a step handler returns when it could not complete.
///
/// Its `code` becomes the process exit status, and its optional message the
/// breadcrumb text; without one the breadcrumb reads
/// `"<step> exited with status <code>"`, exactly as the C SDK phrases it. A
/// [`BridgeError`] propagated with `?` becomes a `StepError` with code
/// [`REFUSED`] (`2`).
#[derive(Debug)]
pub struct StepError {
    code: i32,
    message: Option<String>,
}

impl StepError {
    /// Abort with an exit status and the default `"<step> exited with status N"`
    /// breadcrumb message.
    pub fn new(code: i32) -> Self {
        StepError { code, message: None }
    }

    /// Abort with an exit status and an explicit breadcrumb message.
    pub fn with_message(code: i32, message: impl Into<String>) -> Self {
        StepError {
            code,
            message: Some(message.into()),
        }
    }

    /// The exit status this abort carries.
    pub fn code(&self) -> i32 {
        self.code
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.message {
            Some(message) => write!(f, "{message}"),
            None => write!(f, "step handler failed with status {}", self.code),
        }
    }
}

impl std::error::Error for StepError {}

impl From<BridgeError> for StepError {
    /// A bridge failure a handler propagated with `?` aborts with [`REFUSED`]
    /// (`2`), never `1`: `1` is the [`ABSENT`] convention, an ordinary answer, so
    /// an abort must not reuse it.
    fn from(error: BridgeError) -> Self {
        StepError {
            code: REFUSED,
            message: Some(error.to_string()),
        }
    }
}

/// One declared step: its name and the handler that implements it.
type Handler = Box<dyn Fn(&Attempt) -> Result<(), StepError>>;

/// A workflow runner: its declared step set and their handlers.
///
/// Built with [`Runner::new`] and one [`Runner::step`] per declared step, then
/// handed to [`Runner::main`].
pub struct Runner {
    workflow: String,
    step_names: Vec<String>,
    handlers: Vec<(String, Handler)>,
}

impl Runner {
    /// Declare a runner for `workflow` with the complete set of `steps` it
    /// implements. The names are the describe order and the set an outcome's
    /// step is checked against; register one handler per name with
    /// [`Runner::step`] before calling [`Runner::main`].
    pub fn new(workflow: &str, steps: &[&str]) -> Self {
        Runner {
            workflow: workflow.to_string(),
            step_names: steps.iter().map(|name| name.to_string()).collect(),
            handlers: Vec::new(),
        }
    }

    /// Register the handler for one declared step. Consumes and returns `self`
    /// so registrations chain.
    pub fn step<F>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(&Attempt) -> Result<(), StepError> + 'static,
    {
        self.handlers.push((name.to_string(), Box::new(handler)));
        self
    }

    /// This runner's machine-readable description, byte-for-byte what a Python,
    /// Bash, C, or Fortran runner prints for the same workflow and step set,
    /// with one trailing newline. The step names are byte-sorted.
    pub fn description(&self) -> String {
        let mut names: Vec<&str> = self.step_names.iter().map(String::as_str).collect();
        names.sort_unstable();
        let steps: Vec<String> = names.iter().map(|name| format!("\"{name}\"")).collect();
        format!(
            "{{\"format\": \"httk-workflow-runner-description\", \"format_version\": 1, \"steps\": [{}], \"workflow\": \"{}\"}}\n",
            steps.join(", "),
            self.workflow,
        )
    }

    /// Dispatch the step the manager asked for and turn its ending into exactly
    /// one outcome, then exit the process with the status the ending implies.
    ///
    /// | Ending | Published outcome |
    /// | --- | --- |
    /// | the handler publishes one | that outcome, exit `0` |
    /// | the handler returns `Ok(())` without publishing | `fail("no_outcome", …)`, exit `0` |
    /// | the step is not registered | `fail("unknown_step", …)`, exit `0` |
    /// | the handler returns `Err(e)` | an `error.json` breadcrumb, exit `e.code()` |
    ///
    /// Recognizes `--describe` in argv and `HTTK_WORKFLOW_DESCRIBE=1`, printing
    /// the description and exiting `0` without dispatching anything.
    pub fn main(self) -> ! {
        std::process::exit(self.dispatch());
    }

    fn dispatch(self) -> i32 {
        // An invalid registration is refused before anything else, including
        // --describe, the way the C and Bash SDKs refuse first.
        if let Err(status) = self.validate() {
            return status;
        }

        // The native handshake needs no interpreter and never dispatches a step.
        let described = env::args_os().skip(1).any(|argument| argument == "--describe")
            || env::var(DESCRIBE_VAR).map(|value| value == "1").unwrap_or(false);
        if described {
            print!("{}", self.description());
            return OK;
        }

        // What the Python bridge splits to reconstruct the step set an outcome is
        // checked against, and to name the registered steps of an unknown one.
        env::set_var(RUNNER_WORKFLOW_VAR, &self.workflow);
        env::set_var(RUNNER_STEPS_VAR, self.step_names.join("\n"));

        let attempt = Attempt::new();
        let step = match attempt.capture(&["begin"]) {
            Ok((OK, output)) => output.unwrap_or_default(),
            _ => return REFUSED,
        };
        env::set_var(STEP_VAR, &step);

        // The start gate may already have published a fail outcome.
        if outcome_published() {
            return OK;
        }

        let handler = self.handlers.iter().find(|(name, _)| *name == step);
        let handler = match handler {
            Some((_, handler)) => handler,
            None => {
                return match attempt.command(&["fail-unknown-step"]) {
                    Ok(OK) => OK,
                    _ => REFUSED,
                };
            }
        };

        match handler(&attempt) {
            Ok(()) => {
                if !outcome_published() {
                    match attempt.command(&["fail-no-outcome"]) {
                        Ok(OK) => {}
                        _ => return REFUSED,
                    }
                }
                match attempt.command(&["environment-log"]) {
                    Ok(OK) => OK,
                    _ => REFUSED,
                }
            }
            Err(error) => {
                // An aborted handler discards its unpublished draft and leaves a
                // breadcrumb, the way a Python traceback names the failing line.
                let message = error
                    .message
                    .unwrap_or_else(|| format!("{step} exited with status {}", error.code));
                let _ = attempt.command(&["abort", "--exception", ABORT_EXCEPTION, "--message", &message]);
                error.code
            }
        }
    }

    fn validate(&self) -> Result<(), i32> {
        if self.workflow.is_empty() || self.step_names.is_empty() {
            eprintln!("httk-workflow: a runner needs a workflow name and at least one step");
            return Err(REFUSED);
        }
        // The same charset the step names use: an id with a `"` would otherwise
        // emit invalid describe JSON.
        if !valid_step_name(&self.workflow) {
            eprintln!("httk-workflow: workflow id {} is not a valid runner id", self.workflow);
            return Err(REFUSED);
        }
        for (index, name) in self.step_names.iter().enumerate() {
            if !valid_step_name(name) {
                eprintln!("httk-workflow: step name {name} cannot name a step handler");
                return Err(REFUSED);
            }
            if self.step_names[..index].contains(name) {
                eprintln!("httk-workflow: step {name} is already registered on the {} runner", self.workflow);
                return Err(REFUSED);
            }
        }
        for name in &self.step_names {
            if !self.handlers.iter().any(|(handled, _)| handled == name) {
                eprintln!(
                    "httk-workflow: the {} runner declares step {name} but registers no handler",
                    self.workflow
                );
                return Err(REFUSED);
            }
        }
        for (name, _) in &self.handlers {
            if !self.step_names.contains(name) {
                eprintln!(
                    "httk-workflow: step {name} has a handler but is not declared on the {} runner",
                    self.workflow
                );
                return Err(REFUSED);
            }
        }
        Ok(())
    }
}

const DESCRIBE_VAR: &str = "HTTK_WORKFLOW_DESCRIBE";

/// Optional join conditions for [`Attempt::gather`], mirroring the bridge's
/// optional arguments. Every field defaults to unset (the bridge's own default).
#[derive(Default, Clone)]
pub struct Gather<'a> {
    /// `all_succeeded`, `all_terminal`, `any_succeeded`, `any_terminal`, or `at_least`.
    pub when: Option<&'a str>,
    /// The count required by `when = "at_least"`.
    pub count: Option<i64>,
    /// The step to advance to when the join condition can no longer be met.
    pub on_impossible: Option<&'a str>,
    /// A join-activation priority override.
    pub priority: Option<i64>,
}

/// The attempt a step handler acts through. Every method spawns the Python
/// bridge; reads distinguish an absent answer (`Ok(None)`) from a refused call
/// (`Err(`[`BridgeError::Refused`]`)`), and command verbs return the bridge exit
/// status.
pub struct Attempt {
    _private: (),
}

impl Attempt {
    fn new() -> Self {
        Attempt { _private: () }
    }

    // --- The bridge exec ----------------------------------------------------

    fn spawn_bridge(&self, argv: &[&str], capture: bool) -> Result<(i32, Option<String>), BridgeError> {
        let python = match env::var_os(PYTHON_VAR) {
            Some(value) if !value.is_empty() => value,
            _ => {
                eprintln!("httk-workflow: {PYTHON_VAR} is not set by the workflow manager");
                return Err(BridgeError::PythonUnset);
            }
        };
        let mut command = Command::new(python);
        command.arg("-m").arg(BRIDGE_MODULE).args(argv.iter().map(OsStr::new));
        if capture {
            // Capture stdout only; stderr and stdin stay inherited, as command
            // substitution leaves them in the shell.
            let child = command.stdout(Stdio::piped()).spawn().map_err(BridgeError::Spawn)?;
            let output = child.wait_with_output().map_err(BridgeError::Spawn)?;
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            // Strip trailing newlines, as command substitution does in a shell.
            while text.ends_with('\n') {
                text.pop();
            }
            Ok((output.status.code().unwrap_or(REFUSED), Some(text)))
        } else {
            // Inherit stdin and stdout, so `run` and `batch` stream normally.
            let status = command.status().map_err(BridgeError::Spawn)?;
            Ok((status.code().unwrap_or(REFUSED), None))
        }
    }

    fn command(&self, argv: &[&str]) -> Result<i32, BridgeError> {
        let (status, _) = self.spawn_bridge(argv, false)?;
        Ok(status)
    }

    fn capture(&self, argv: &[&str]) -> Result<(i32, Option<String>), BridgeError> {
        self.spawn_bridge(argv, true)
    }

    fn read(&self, argv: &[&str]) -> Result<Option<String>, BridgeError> {
        let (status, output) = self.spawn_bridge(argv, true)?;
        match status {
            OK => Ok(Some(output.unwrap_or_default())),
            ABSENT => Ok(None),
            _ => Err(BridgeError::Refused),
        }
    }

    /// The foundational bridge call and the escape hatch for any subcommand
    /// without a dedicated wrapper. `argv` names the subcommand and its
    /// arguments; the SDK prepends the interpreter invocation. stdin and stdout
    /// are inherited, so a streaming verb such as `run` or `batch` streams;
    /// returns the bridge exit status.
    pub fn invoke(&self, argv: &[&str]) -> Result<i32, BridgeError> {
        self.command(argv)
    }

    // --- What a step reads --------------------------------------------------

    /// The attempt context, or one `field` of it.
    pub fn context(&self, field: Option<&str>) -> Result<Option<String>, BridgeError> {
        match field {
            Some(field) => self.read(&["context", field]),
            None => self.read(&["context"]),
        }
    }

    /// One member of the job's opaque parameters object, with an optional default.
    pub fn parameter(&self, name: &str, fallback: Option<&str>) -> Result<Option<String>, BridgeError> {
        self.read_named("parameter", name, fallback)
    }

    /// One resolved application setting, with an optional default.
    pub fn setting(&self, name: &str, fallback: Option<&str>) -> Result<Option<String>, BridgeError> {
        self.read_named("setting", name, fallback)
    }

    /// One declared workflow environment value, with an optional default.
    pub fn environment(&self, name: &str, fallback: Option<&str>) -> Result<Option<String>, BridgeError> {
        self.read_named("environment", name, fallback)
    }

    fn read_named(&self, verb: &str, name: &str, fallback: Option<&str>) -> Result<Option<String>, BridgeError> {
        match fallback {
            Some(default) => self.read(&[verb, name, "--default", default]),
            None => self.read(&[verb, name]),
        }
    }

    /// One key of the job's JSON state; `Ok(None)` when it is unset.
    pub fn state_get(&self, name: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["state-get", name])
    }

    /// One workflow declaration: the observed document, else the declared one.
    pub fn declaration(&self, name: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["declaration", name])
    }

    /// The observed children as tab-separated rows, one per line. `selection` is
    /// `None`, `"--all"`, `"--succeeded"`, or `"--failed"`.
    pub fn children(&self, selection: Option<&str>) -> Result<Option<String>, BridgeError> {
        match selection {
            Some(selection) => self.read(&["children", selection]),
            None => self.read(&["children"]),
        }
    }

    /// One `field` of one observed child by `label`.
    pub fn child(&self, label: &str, field: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["child", label, field])
    }

    // --- Job state ----------------------------------------------------------

    /// Store one JSON value (a bare scalar, valid JSON, or `@file.json`) in one
    /// atomic replace.
    pub fn state_set(&self, name: &str, value: &str) -> Result<i32, BridgeError> {
        self.command(&["state-set", name, value])
    }

    /// Remove one key, returning [`ABSENT`] when it was not present.
    pub fn state_delete(&self, name: &str) -> Result<i32, BridgeError> {
        self.command(&["state-delete", name])
    }

    /// Write several `NAME=VALUE` assignments in one atomic replace.
    pub fn state_merge(&self, assignments: &[&str]) -> Result<i32, BridgeError> {
        self.command(&prepend("state-merge", assignments))
    }

    // --- Declarations and the run log ---------------------------------------

    /// Record the workflow declaration this job observed, from a JSON document file.
    pub fn declare(&self, name: &str, document_file: &str) -> Result<i32, BridgeError> {
        self.command(&["declare", name, document_file])
    }

    /// Append one ordinary evidence event to this workdir's run log.
    pub fn runlog_note(&self, message: &str) -> Result<i32, BridgeError> {
        self.command(&["runlog", "note", message])
    }

    /// Append one event meant to be read first when the job is inspected.
    pub fn runlog_headline(&self, message: &str) -> Result<i32, BridgeError> {
        self.command(&["runlog", "headline", message])
    }

    /// Append one event with whole files attached by content.
    pub fn runlog_append(&self, message: &str, files: &[&str]) -> Result<i32, BridgeError> {
        let mut argv = vec!["runlog", "files", message];
        argv.extend_from_slice(files);
        self.command(&argv)
    }

    /// Write one timestamped `LEVEL MESSAGE` line to stderr, which the manager
    /// retains with the attempt. Unlike every other verb this is local (no bridge).
    pub fn log(&self, level: &str, message: &str) {
        eprintln!("{} [{level}] {message}", utc_timestamp());
    }

    // --- Transactional data -------------------------------------------------

    /// Stage one file or tree into the job's data; returns the operation id.
    pub fn put(&self, source: &str, destination: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["put", source, destination])
    }

    /// Stage one removal from the job's data; returns the operation id.
    pub fn remove(&self, destination: &str, missing_ok: bool) -> Result<Option<String>, BridgeError> {
        if missing_ok {
            self.read(&["remove", destination, "--missing-ok"])
        } else {
            self.read(&["remove", destination])
        }
    }

    // --- Children -----------------------------------------------------------

    /// Register one child under a mandatory unique `label`, created when the
    /// outcome is published; returns the child's job key. `args` carries the
    /// child options (`--step`, `--parameter NAME=VALUE`, `--payload`,
    /// `--runner`, …).
    pub fn spawn(&self, label: &str, args: &[&str]) -> Result<Option<String>, BridgeError> {
        let mut argv = vec!["spawn", label];
        argv.extend_from_slice(args);
        self.read(&argv)
    }

    // --- What a step publishes (exactly one per attempt) --------------------

    /// Run `next_step` next; `args` may carry `--state NAME=VALUE` and `--priority`.
    pub fn advance(&self, next_step: &str, args: &[&str]) -> Result<i32, BridgeError> {
        let mut argv = vec!["advance", next_step];
        argv.extend_from_slice(args);
        self.command(&argv)
    }

    /// Wait for the children spawned on this attempt, then run `next_step`.
    pub fn gather(&self, next_step: &str, options: &Gather<'_>) -> Result<i32, BridgeError> {
        let count = options.count.map(|count| count.to_string());
        let priority = options.priority.map(|priority| priority.to_string());
        let mut argv = vec!["gather", next_step];
        if let Some(when) = options.when {
            argv.push("--when");
            argv.push(when);
        }
        if let Some(count) = &count {
            argv.push("--count");
            argv.push(count);
        }
        if let Some(on_impossible) = options.on_impossible {
            argv.push("--on-impossible");
            argv.push(on_impossible);
        }
        if let Some(priority) = &priority {
            argv.push("--priority");
            argv.push(priority);
        }
        self.command(&argv)
    }

    /// Publish the successful completion of this job.
    pub fn succeed(&self) -> Result<i32, BridgeError> {
        self.command(&["succeed"])
    }

    /// Publish a structured terminal failure. `retryable` declares that repeating
    /// could help; `--details` and `--priority` are reachable through [`Attempt::invoke`].
    pub fn fail(&self, code: &str, message: &str, retryable: bool) -> Result<i32, BridgeError> {
        if retryable {
            self.command(&["fail", code, message, "--retryable"])
        } else {
            self.command(&["fail", code, message])
        }
    }

    /// Ask for another attempt of this same activation.
    pub fn retry(&self, reason: &str) -> Result<i32, BridgeError> {
        self.command(&["retry", reason])
    }

    /// Pause this job until an operator resumes it.
    pub fn pause(&self, reason: &str) -> Result<i32, BridgeError> {
        self.command(&["pause", reason])
    }

    // --- Many calls, one interpreter; payload and workdir; utilities --------

    /// Run several bridge commands, one per line read from this process's stdin,
    /// in one interpreter start. Stops at the first failing line.
    pub fn batch(&self) -> Result<i32, BridgeError> {
        self.command(&["batch"])
    }

    /// Write `job.json` into a prepared payload from a JobSpec file; returns its JSON.
    pub fn job_prepare(&self, destination: &str, spec_file: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["job-prepare", destination, spec_file])
    }

    /// Apply a replayable batch of workdir changes from a spec file; returns its id.
    pub fn workdir_apply(&self, spec_file: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["workdir-apply", spec_file])
    }

    /// Run an argv array under supervision, terminating its process group on
    /// timeout. `args` is the whole tail the bridge forwards: options, then
    /// `--`, then the argv (`["--timeout", "3600", "--", "vasp"]`). Returns the
    /// classified status (`0`, `22`, `124`, or `125`); stdout is inherited.
    pub fn run(&self, args: &[&str]) -> Result<i32, BridgeError> {
        self.command(&prepend("run", args))
    }

    /// Evaluate one arithmetic expression without a shell; returns its value.
    pub fn calc(&self, expression: &str) -> Result<Option<String>, BridgeError> {
        self.read(&["calc", expression])
    }

    /// Render one template file with one JSON value document.
    pub fn template_render(&self, template_file: &str, output: &str, values_file: &str) -> Result<i32, BridgeError> {
        self.command(&["template", template_file, output, values_file])
    }

    /// Compress named files; `args` carries `--method`/`--remove-source` and the paths.
    pub fn compress(&self, args: &[&str]) -> Result<i32, BridgeError> {
        self.command(&prepend("compress", args))
    }

    /// Decompress named files; `args` carries `--remove-source` and the paths.
    pub fn decompress(&self, args: &[&str]) -> Result<i32, BridgeError> {
        self.command(&prepend("decompress", args))
    }
}

/// Build one bridge argv from a verb and a tail.
fn prepend<'a>(verb: &'a str, tail: &[&'a str]) -> Vec<&'a str> {
    let mut argv = vec![verb];
    argv.extend_from_slice(tail);
    argv
}

fn valid_step_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

fn outcome_published() -> bool {
    let control = match env::var_os(CONTROL_VAR) {
        Some(value) if !value.is_empty() => PathBuf::from(value),
        _ => PathBuf::from("."),
    };
    control.join("outcome.ready").is_dir()
}

/// The current UTC time as `YYYY-MM-DDTHH:MM:SSZ`, without a date-time crate.
fn utc_timestamp() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0) as i64;
    let days = seconds.div_euclid(86_400);
    let time = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time / 3_600,
        (time % 3_600) / 60,
        time % 60,
    )
}

/// Gregorian year/month/day for a count of days since 1970-01-01 (Hinnant).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_from_days_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn description_is_byte_sorted_with_one_trailing_newline() {
        let runner = Runner::new("my.workflow", &["run", "collect", "prepare"]);
        assert_eq!(
            runner.description(),
            "{\"format\": \"httk-workflow-runner-description\", \"format_version\": 1, \
             \"steps\": [\"collect\", \"prepare\", \"run\"], \"workflow\": \"my.workflow\"}\n"
        );
    }
}
