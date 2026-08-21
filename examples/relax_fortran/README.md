# A VASP relaxation runner in modern Fortran

`relax.f90` has the same three-step shape as `examples/relax_c/relax.c` (not a
line-for-line port), authored against the native Fortran SDK in
`src/httk/workflow/native/fortran/`. It declares the workflow
`httk.vasp.relax-fortran` with three steps — `prepare`, `run`, `publish` — and is
a *bridge client*: the Fortran SDK is `iso_c_binding` bindings over the native C
SDK, so every `httk_workflow_*` call reaches the same `$HTTK_WORKFLOW_PYTHON -m
httk.workflow._shell_bridge` implementation as a Python, Bash, or C runner. It
therefore publishes the same protocol bytes and works with the mock VASP beside
`examples/mock_vasp.py`. Two SDK-level divergences from the C example: a value
with significant trailing whitespace is trimmed at the C boundary, and a command
token wider than the fixed `ARG_WIDTH` (4096) makes `run` fail loudly rather than
truncate — neither affects an ordinary VASP command.

The Fortran SDK is one `.f90` module with no dependency beyond `iso_c_binding`
and the C SDK it wraps. The two languages are compiled separately (the Fortran
standard flag is not valid for C), which the Makefile does for you:

```console
make            # cc -std=c99 -c .../httk_workflow.c ; gfortran -std=f2008 ...
```

Enumerate it without running it — every native runner answers `--describe`:

```console
./relax --describe
{"format": "httk-workflow-runner-description", "format_version": 2, "steps": ["prepare", "publish", "run"], "workflow": "httk.vasp.relax-fortran"}
```

Drive one relaxation with the compiled binary as the runner, exactly the flow of
`docs/quickstart.md`:

```console
httk project init --name relax-fortran .
httk workflow workspace init --name default .
httk workflow job new --workflow ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set --key vasp.command --value "$PWD/../mock_vasp.py" default
httk workflow run
httk workflow collect
```

The runner reads the `poscar` parameter (default `files/POSCAR`), so the POSCAR is
staged with `--file POSCAR=POSCAR` and the first step is `--step prepare`. The
finished calculation lands in `jobs/*/data/vasp/`; because every language SDK
publishes through the one bridge, those files are the same bytes the Python, Bash,
and C relaxation runners publish.

See `docs/sdks/native_fortran_api.md` for the module surface, the string-ownership
rules across the C boundary, and how a Fortran step handler is dispatched.
