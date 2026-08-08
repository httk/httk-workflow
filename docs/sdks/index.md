# Runner SDKs

Every SDK is a bridge client that spawns `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`; only `--describe` is native and byte-identical, and the normative surface is {doc}`sdk_parity`.

- {doc}`../runtime_helpers` — Python, the original authoring SDK
- {doc}`native_bash_api` — the same surface in Bash
- {doc}`native_c_api` — the same surface in C, and the foundation for Fortran bindings
- {doc}`native_fortran_api` — the same surface in modern Fortran, over the C bindings
- {doc}`native_rust_api` — the same surface in safe, std-only Rust
- {doc}`native_perl_api` — the same surface in pure, core-only Perl
- {doc}`native_ada_api` — the same surface in Ada 2012, over the C bindings
- {doc}`native_cpp_api` — the same surface in C++17, over the C bindings
- {doc}`native_java_api` — the same surface in Java 17, over the Python bridge

The breadcrumb labels summarize the errors as ShellError; CError for C, Fortran, Ada, and C++; RustError; PerlError; and JavaError.

```{toctree}
:maxdepth: 1

sdk_parity
native_bash_api
native_c_api
native_fortran_api
native_rust_api
native_perl_api
native_ada_api
native_cpp_api
native_java_api
```
