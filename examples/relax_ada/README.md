# A VASP relaxation runner in Ada

`relax.adb` has the same three-step shape as `examples/relax_c/relax.c` (not a
line-for-line port), authored against the native Ada SDK in
`src/httk/workflow/native/ada/`. It declares the workflow
`httk.vasp.relax-ada` with three steps — `prepare`, `run`, `publish` — and is a
bridge client: the Ada package is `Interfaces.C` bindings over the native C
SDK, so every `Httk_Workflow_*` call reaches the same
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge` implementation as a
Python, Bash, C, or Fortran runner.

The package has no dependency beyond Ada 2012 and the C SDK. Handler functions
must be library-level functions with `Convention => C`; nested handlers require
executable-stack trampolines and are not supported. Build the two
languages separately (the C standard flag is not an Ada flag), which the
Makefile does for you:

```console
make            # cc -std=c99 -c .../httk_workflow.c ; gnatmake -gnat2012 ...
```

Enumerate it without running it:

```console
./relax --describe
{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ["prepare", "publish", "run"], "workflow": "httk.vasp.relax-ada"}
```

Drive one relaxation with the compiled binary as the runner:

```console
httk project init --name relax-ada
httk workflow workspace init . --name default
httk workflow job new --workflow ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

The runner reads the `poscar` parameter (default `files/POSCAR`), optionally
stages `INCAR`, runs the configured command, and publishes the VASP result into
transactional data. See `docs/sdks/native_ada_api.md` for the binding surface and
the NULL/empty string ownership rules.
