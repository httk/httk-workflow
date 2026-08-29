# A VASP relaxation runner in Rust

`src/main.rs` has the same three-step shape as `examples/relax_c/relax.c`,
authored against the native Rust SDK in `src/httk/workflow/native/rust/`. It
declares the workflow `httk.vasp.relax-rust` with three steps — `prepare`, `run`,
`publish` — and is a *bridge client*: every `Attempt` method execs
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`, so a published outcome is
the same protocol bytes a Python, Bash, C, or Fortran runner writes, and it works
with the mock VASP beside `examples/mock_vasp.py`. It is a minimal example and
deliberately omits what the packaged `vasp-relax` runner adds — INCAR and KPOINTS
generation, restarts, and supervision diagnostics.

The Rust SDK is a std-only crate with **zero crates.io dependencies** and no
`unsafe`, path-depended here, so the build needs no network at all. The Makefile
runs the offline release build and copies out the binary:

```console
make            # cargo build --release --offline ; cp target/release/relax relax
```

Enumerate it without running it — every native runner answers `--describe`:

```console
./relax --describe
{"format": "httk-workflow-runner-description", "format_version": 2, "steps": ["prepare", "publish", "run"], "workflow": "httk.vasp.relax-rust"}
```

Drive one relaxation with the compiled binary as the runner, exactly the flow of
`docs/quickstart.md`:

```console
httk project init --name relax-rust .
httk workspace init --name default .
httk job new --from-runner ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workspace settings set --key vasp.command --value "$PWD/../mock_vasp.py" default
httk workflow run
httk workflow collect
```

The runner file declares no inputs, so its POSCAR is staged with `--file` and its
first step is named with `--step`. The finished calculation lands in
`jobs/*/data/vasp/`, the same collected files the other relaxation examples
produce for this input.

See `docs/sdks/native_rust_api.md` for the full method table, the error semantics
(`Result<Option<String>>` reads and `StepError` aborts), the offline build, and
how a Rust step handler is dispatched.
