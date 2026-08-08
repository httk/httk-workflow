//! One VASP relaxation, authored in safe Rust: the same three-step shape as
//! `examples/relax_c/relax.c`, built on the native Rust SDK (`native/rust`).
//!
//!   prepare  stage the payload POSCAR (and INCAR if present) into the workdir
//!   run      run the configured VASP command and classify what it did
//!   publish  copy the finished calculation into the job's transactional data
//!
//! Every `Attempt` method reaches the same `$HTTK_WORKFLOW_PYTHON -m
//! httk.workflow._shell_bridge` implementation the Python, Bash, C, and Fortran
//! SDKs do, so this runner is mock-vasp compatible and publishes the same bytes.
//! Build it with the Makefile beside this file, or:
//!
//!     cargo build --release --offline
//!
//! See ../mock_vasp.py for a stand-in VASP, and README.md for the whole flow.

use std::env;
use std::fs;
use std::path::Path;

use httk_workflow::{Attempt, Runner, StepError};

/// The files a finished relaxation publishes, if the run produced them.
const COLLECT: &[&str] = &[
    "INCAR",
    "KPOINTS",
    "OUTCAR",
    "CONTCAR",
    "OSZICAR",
    "vasprun.xml",
    "vasp-run-report.json",
];

/// One environment variable, or a default when it is unset or empty.
fn env_or(name: &str, fallback: &str) -> String {
    match env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => fallback.to_string(),
    }
}

/// Stage a payload-relative file named by one parameter into the workdir.
/// Returns 1 when staged, 0 when the source is absent, -1 on failure.
fn stage_input(attempt: &Attempt, job_dir: &str, parameter: &str, fallback: &str, destination: &str) -> i32 {
    let relative = match attempt.parameter(parameter, Some(fallback)) {
        Ok(Some(relative)) => relative,
        _ => return -1,
    };
    let source = Path::new(job_dir).join(&relative);
    if !source.is_file() {
        return 0;
    }
    match fs::copy(&source, destination) {
        Ok(_) => 1,
        Err(_) => -1,
    }
}

fn step_prepare(attempt: &Attempt) -> Result<(), StepError> {
    let job_dir = env_or("HTTK_WORKFLOW_JOB_DIR", ".");
    if stage_input(attempt, &job_dir, "poscar", "files/POSCAR", "POSCAR") <= 0 {
        let _ = attempt.fail("vasp.input_missing", "the starting structure is not in this payload", false);
        return Ok(());
    }
    // An INCAR is optional; the mock VASP reads only the POSCAR.
    let _ = stage_input(attempt, &job_dir, "incar", "files/INCAR", "INCAR");
    let _ = attempt.runlog_note("prepared a relaxation");
    let _ = attempt.advance("run", &[]);
    Ok(())
}

fn step_run(attempt: &Attempt) -> Result<(), StepError> {
    let from_parameter = attempt.parameter("vasp_command", Some(""))?.unwrap_or_default();
    let command = attempt.setting("vasp.command", Some(&from_parameter))?.unwrap_or_default();
    if command.trim().is_empty() {
        let _ = attempt.fail(
            "vasp.command_missing",
            "no VASP command is configured: set it with \
             httk workflow workspace settings set vasp.command '...', or set \
             HTTK_VASP_COMMAND, or give the job a vasp_command parameter",
            false,
        );
        return Ok(());
    }

    let timeout = attempt
        .parameter("timeout", Some("86400"))?
        .unwrap_or_else(|| "86400".to_string());

    // The resolved command is one argv string; split it on whitespace, the way
    // the Bash runner leaves it unquoted for the shell to word-split.
    let tokens: Vec<&str> = command.split_whitespace().collect();
    let mut args: Vec<&str> = vec!["--timeout", &timeout, "--report", "vasp-run-report.json", "--"];
    args.extend_from_slice(&tokens);

    let status = attempt.run(&args)?;
    if status == 0 {
        let _ = attempt.state_set("classification", "completed");
        let _ = attempt.runlog_note("VASP completed");
        let _ = attempt.advance("publish", &[]);
    } else {
        let _ = attempt.fail("vasp.failed", &format!("VASP did not complete (status {status})"), false);
    }
    Ok(())
}

fn step_publish(attempt: &Attempt) -> Result<(), StepError> {
    let prefix = attempt
        .parameter("data_prefix", Some("vasp"))?
        .unwrap_or_else(|| "vasp".to_string());
    let data_dir = env::var("HTTK_WORKFLOW_DATA_DIR").unwrap_or_default();
    let to_data = !data_dir.is_empty();
    for &name in COLLECT {
        if !Path::new(name).is_file() {
            continue;
        }
        if to_data {
            let _ = attempt.put(name, &format!("{prefix}/{name}"));
        }
    }
    let _ = attempt.runlog_note(if to_data {
        "published to transactional data"
    } else {
        "kept the result in the workdir"
    });
    let _ = attempt.succeed();
    Ok(())
}

fn main() {
    Runner::new("httk.vasp.relax-rust", &["prepare", "run", "publish"])
        .step("prepare", step_prepare)
        .step("run", step_run)
        .step("publish", step_publish)
        .main();
}
