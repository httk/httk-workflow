# A VASP relaxation runner in C

`relax.c` has the same three-step shape as the packaged Bash runner
`httk.workflow.vasp.runners/vasp_relax.sh` (not a line-for-line port), authored against the native C SDK in
`src/httk/workflow/native/c/`. It declares the workflow `httk.vasp.relax-c` with
three steps — `prepare`, `run`, `publish` — and is a *bridge client*: every
`httk_workflow_*` call execs `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`,
so it publishes the same protocol bytes as a Python or Bash runner and works with
the mock VASP beside `examples/mock_vasp.py`.

The SDK is one `.h`/`.c` pair with no dependency beyond libc and POSIX. Vendor it
into your runner's tree, or compile it directly beside your runner:

```console
make            # cc -std=c99 -Wall -Wextra relax.c ../../src/.../httk_workflow.c
```

Enumerate it without running it — every native runner answers `--describe`:

```console
./relax --describe
{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ["prepare", "publish", "run"], "workflow": "httk.vasp.relax-c"}
```

Drive one relaxation with the compiled binary as the runner. The runner starts at
`prepare` and reads its structure from `files/POSCAR` (not a declared input), so
the job names the step and stages the file explicitly:

```console
httk project init --name relax-c
httk workflow workspace init . --name default
httk workflow job new --workflow ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

The finished calculation lands in `jobs/*/data/vasp/`.

See `docs/native_c_api.md` for the full function table, the memory-ownership
rules, and how the C surface maps to the Python and Bash SDKs.
