# A VASP relaxation runner in Perl

`relax.pl` has the same three-step shape as `examples/relax_rust/src/main.rs`
and `examples/relax_c/relax.c`, authored against the native Perl SDK in
`src/httk/workflow/native/perl/`. It declares `httk.vasp.relax-perl` with
three steps — `prepare`, `run`, `publish` — and is a *bridge client*: every
bridge-backed `Attempt` method execs `$HTTK_WORKFLOW_PYTHON -m
httk.workflow._shell_bridge`, so it publishes the same protocol bytes as a
Python, Bash, C, Fortran, or Rust runner and works with the mock VASP beside
`examples/mock_vasp.py`.

Perl is interpreted, so there is no build step. Under a manager, the runner
resolves the SDK through the exported `HTTK_WORKFLOW_PERL_API` directory; for a
bare in-tree invocation it falls back to the `use lib` path relative to the
script:

```console
perl relax.pl --describe
```

Drive one relaxation with the script as the runner, exactly the documented job
sequence:

```console
httk project init --name relax-perl .
httk workflow workspace init --name default .
httk workflow job new --workflow ./relax.pl --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set --key vasp.command --value "$PWD/../mock_vasp.py" default
httk workflow run
httk workflow collect
```

The runner file declares no inputs, so its POSCAR is staged with `--file` and
its first step is named with `--step`. The finished calculation lands in
`jobs/*/data/vasp/`.
