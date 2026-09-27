# Runner SDKs

Every SDK is a bridge client that spawns `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`; only `--describe` is native and byte-identical, and the normative surface is {doc}`sdk_parity`.

- {doc}`../runtime_helpers` — Python, the original authoring SDK
- {doc}`bash_api` — the same surface in Bash
- {doc}`c_api` — the same surface in C, and the foundation for Fortran bindings
- {doc}`fortran_api` — the same surface in modern Fortran, over the C bindings
- {doc}`rust_api` — the same surface in safe, std-only Rust
- {doc}`perl_api` — the same surface in pure, core-only Perl
- {doc}`ada_api` — the same surface in Ada 2012, over the C bindings
- {doc}`cpp_api` — the same surface in C++17, over the C bindings
- {doc}`java_api` — the same surface in Java 17, over the Python bridge

The breadcrumb labels summarize the errors as ShellError; CError for C, Fortran, Ada, and C++; RustError; PerlError; and JavaError.

Single-file compiled runners are architecture-bound and should transfer only
between matching machines. A self-contained package with a `[workflow.build]`
declaration is the portable alternative: transfer its sources, then build once
per platform class. Build commands and runners find the installed SDKs under
`$HTTK_WORKFLOW_LANGUAGES_DIR`, one subdirectory per language (`bash`, `c`, `cpp`,
`fortran`, `rust`, `ada`, `java`, `perl`); see {doc}`../details/workflow_packages`.
A Bash runner still sources `$HTTK_WORKFLOW_BASH_API`, which names the
`bash/httk-workflow.sh` file there.
Such a package names what the manager runs in `[workflow.runner]`, for example
`command = ["{artifacts}/relax"]`, instead of carrying a `run` bridge script.
Outside a manager, `python -c 'import httk.workflow, pathlib;
print(pathlib.Path(httk.workflow.__file__).with_name("languages"))'` prints the
same directory.

A VASP relaxation written with each language SDK is the package `vasp-relax-<lang>`
(workflow `vasp.relax-<lang>`) of
[workflows-vasp-other-languages](https://github.com/httk/workflows-vasp-other-languages),
run as `git+https://github.com/httk/workflows-vasp-other-languages#vasp-relax-<lang>`.

```{toctree}
:maxdepth: 1

sdk_parity
bash_api
c_api
fortran_api
rust_api
perl_api
ada_api
cpp_api
java_api
```
