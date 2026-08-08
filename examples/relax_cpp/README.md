# A VASP relaxation runner in C++17

`relax.cpp` has the same three-step shape as the Ada and Fortran examples:
`prepare`, `run`, and `publish`. It declares `httk.vasp.relax-cpp` and uses the
header-only C++17 SDK in `src/httk/workflow/native/cpp/`, a RAII wrapper over the
native C SDK. Every bridge call reaches the same
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge` implementation as the
other runner SDKs, so it works with `examples/mock_vasp.py`.

Build it with the Makefile:

```console
make            # cc -std=c99 -c .../httk_workflow.c ; c++ -std=c++17 ...
```

Enumerate it without running it:

```console
./relax --describe
{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ["prepare", "publish", "run"], "workflow": "httk.vasp.relax-cpp"}
```

Drive one relaxation with the compiled binary:

```console
httk project init --name relax-cpp
httk workflow workspace init . --name default
httk workflow job new --workflow ./relax --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

See `docs/sdks/native_cpp_api.md` for the C++ surface, C string ownership, and the
plain-function-pointer handler contract.
