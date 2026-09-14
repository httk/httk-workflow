# Native C++ runner API

*For authors writing a workflow runner in C++17.* The C++ SDK is one
header-only RAII wrapper over the native C SDK in `native/c/`. It does not
reimplement the bridge protocol: every verb reaches
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`, while only
`--describe` is native. The C library owns registration, dispatch, and process
exit status, so C++ runners publish the same protocol bytes as C, Bash,
Fortran, Ada, Rust, Perl, and Python runners.

The header is `native/cpp/httk_workflow.hpp`. Compile the C source separately,
then compile and link the C++ runner with C++17:

```console
cc -std=c99 -Wall -Wextra -c .../native/c/httk_workflow.c -o httk_workflow_c.o
c++ -std=c++17 -Wall -Wextra -I.../native/cpp \
  -o runner runner.cpp httk_workflow_c.o
```

## Registration and dispatch

`httk::workflow::Runner` is a builder for one workflow and its complete step
set:

```cpp
#include "httk_workflow.hpp"

int prepare() { return httk::workflow::Attempt::advance("run"); }

int main(int argc, char** argv) {
    httk::workflow::Runner runner("my.workflow");
    runner.add_step("prepare", httk::workflow::guarded<&prepare>);
    return runner.main(argc, argv);
}
```

`add_step` accepts a plain `int (*)()` handler. Handlers must be
C-function-pointer-compatible and must not capture state: a capturing lambda
or `std::function` cannot be converted to the C ABI. Keep state in the normal
attempt context and job state, or use a non-capturing named function. The
wrapper builds the C registration view from the builder's strings and hands it
to `httk_workflow_runner`; `Runner::main` then calls
`httk_workflow_main`.

Exceptions must never cross into the C dispatch frame. Register handlers through
`guarded<&handler>`, which catches `BridgeError`, other `std::exception` values,
and unknown exceptions, reports one line to `stderr`, and returns nonzero. The C
dispatcher then records the structured abort breadcrumb with the inherited
`CError` label. A handler may instead catch exceptions manually and return
nonzero; raw handlers that let a C++ exception escape are unsupported.

Every ending has the C semantics:

| Handler ending | Result |
| --- | --- |
| publishes and returns `0` | the published outcome |
| returns `0` without publishing | `no_outcome` |
| requested step is absent | `unknown_step` |
| returns nonzero | `error.json`, then that nonzero status |

The last row inherits the C layer's breadcrumb label, **`CError`**. There is no
`CppError`: dispatch and abnormal-handler classification happen in the C
library.

## Attempt-to-C mapping

`httk::workflow::Attempt` keeps the C verb groups as static methods. It owns no
protocol state; the current attempt is selected by the manager's environment.

| C++ | C |
| --- | --- |
| `Runner::main`, `Runner::describe`, `Runner::add_step` | `httk_workflow_runner`, `httk_workflow_main`, `httk_workflow_describe` |
| `Attempt::invoke`, `invoke_capture` | `httk_workflow_invoke` |
| `Attempt::context`, `parameter`, `setting`, `environment` | corresponding read functions |
| `Attempt::state_get`, `state_set`, `state_delete`, `state_merge` | corresponding `httk_workflow_state_*` functions |
| `Attempt::declaration`, `declare` | `httk_workflow_declaration`, `httk_workflow_declare` |
| `Attempt::runlog_note`, `runlog_headline`, `runlog_append`, `log` | corresponding `httk_workflow_*` functions |
| `Attempt::put`, `remove`, `spawn` | corresponding transactional/child C functions |
| `Attempt::children`, `child` | `httk_workflow_children`, `httk_workflow_child` |
| `Attempt::advance`, `gather`, `succeed`, `fail`, `retry`, `pause` | corresponding outcome C functions |
| `Attempt::batch`, `job_prepare`, `workdir_apply` | corresponding C functions |
| `Attempt::run`, `calc`, `template_render` | `httk_workflow_run`, `httk_calc`, `httk_template_render` |
| `Attempt::compress`, `decompress` | `httk_compress`, `httk_decompress` |

Methods taking tail arguments use `Attempt::Arguments`, an alias for
`std::vector<std::string>`, and pass a temporary NULL-terminated C array. An
empty vector is passed as a C NULL pointer. Methods with a fallback have an
overload with and without that fallback.

## Strings, ownership, and absent reads

The C SDK returns freshly `malloc`'d NUL-terminated strings. The wrapper puts
each result in a `std::unique_ptr` with a libc `free` deleter before copying it
into a `std::string`, so the C allocation is released exactly once even when a
C++ exception is raised. Inputs remain ordinary C++ strings and their
NUL-terminated views live through the C call.

Read methods return `std::optional<std::string>`:

```cpp
const auto value = httk::workflow::Attempt::state_get("energy");
if (value) {
    // An engaged optional may contain an empty string.
}
```

An allocated empty C string is an engaged optional with `value->empty() ==
true`. A NULL answer with status `HTTK_WORKFLOW_ABSENT` (`1`) is `std::nullopt`.
A refused read with status `HTTK_WORKFLOW_REFUSED` (`2`) throws
`httk::workflow::BridgeError`; `error.status()` preserves the C status.
`spawn`, which must produce a child key, also throws `BridgeError` for a
refused or missing result. The other result-returning operations retain the
optional result shape. Command verbs return the C bridge status directly;
`Attempt::run` returns the supervised classification (`0`, `22`, `124`, or
`125`) rather than throwing for the program's result.

## The relaxation example

`examples/relax_cpp/relax.cpp` declares `httk.vasp.relax-cpp` with `prepare`,
`run`, and `publish` steps. It stages `files/POSCAR`, resolves
`vasp.command`, invokes `Attempt::run`, records the completion state, and
stages VASP outputs into transactional data. Build and enumerate it with:

```console
cd examples/relax_cpp
make
./relax --describe
```

The Makefile uses `cc -std=c99` for the C object and
`c++ -std=c++17 -Wall -Wextra` for the C++ compile and link. No C++ protocol
implementation, third-party dependency, CMake project, or executable-stack
linker flag is needed.
