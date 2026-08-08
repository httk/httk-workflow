# Native Ada runner API

*For authors writing a workflow runner in Ada 2012.* The Ada SDK is a thin
`Interfaces.C` binding over the native C SDK in `native/c/`. It does not
reimplement the bridge protocol: every verb reaches
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`, while only
`--describe` is native. The C library owns registration, dispatch, and process
exit status, so Ada runners publish the same protocol bytes as C, Bash,
Fortran, and Python runners.

The package is `httk_workflow.ads` plus `httk_workflow.adb` under
`native/ada/`. It uses only Ada 2012 standard packages and GNAT's normal
compiler/runtime. Build the C source separately, then link it through
`gnatmake`:

```console
cc -std=c99 -c .../native/c/httk_workflow.c -o httk_workflow_c.o
gnatmake -gnat2012 -gnatwa -gnatwe -I.../native/ada \
  -o runner runner.adb -largs httk_workflow_c.o
```

## Registration and dispatch

`Httk_Workflow_Runner` takes a workflow name, an array of
`Unbounded_String` step names, and an array of `Step_Handler` values. A handler
is a library-level C-convention function returning `Interfaces.C.int`:

```ada
function Step_Prepare return Interfaces.C.int with Convention => C;

function Step_Prepare return Interfaces.C.int is
begin
   if Httk_Workflow_Advance ("run") /= 0 then null; end if;
   return 0;
end Step_Prepare;
```

Handlers must be declared at library level. A nested handler's `'Access` would
require a GNAT stack trampoline and an executable stack, so nested handlers are
not supported.

The C struct contains an `int (*)(void)`, so handlers must return an integer;
procedures are undefined behavior. `Httk_Workflow_Main` forwards the Ada
command line to `httk_workflow_main`, which dispatches the registered handler
and owns the final status. Call `Httk_Workflow_Exit` with that status because
Ada's normal program termination would otherwise return zero.

Every ending has the C semantics:

| Handler ending | Result |
| --- | --- |
| publishes and returns `0` | the published outcome |
| returns `0` without publishing | `no_outcome` |
| requested step is absent | `unknown_step` |
| returns nonzero | `error.json`, then that nonzero status |

The last row inherits the C layer's breadcrumb label, **`CError`**. There is no
`AdaError`: dispatch and abnormal-handler classification happen in the C
library.

`Httk_Workflow_Describe` prints the native description. Passing `--describe`
to `Httk_Workflow_Main`, or setting `HTTK_WORKFLOW_DESCRIBE=1`, produces the
same bytes as the Bash SDK for the same workflow and steps.

## Ada-to-C mapping

The public package keeps the C names and groups, with Ada strings copied into
`Interfaces.C.Strings.chars_ptr` values for each call:

| Ada | C |
| --- | --- |
| `Httk_Workflow_Runner`, `Httk_Workflow_Main` | `httk_workflow_runner`, `httk_workflow_main` |
| `Httk_Workflow_Invoke` | `httk_workflow_invoke` |
| `Httk_Workflow_Context`, `Parameter`, `Setting`, `Environment` | `httk_workflow_context`, `httk_workflow_parameter`, `httk_workflow_setting`, `httk_workflow_environment` |
| `Httk_Workflow_State_Get`, `State_Set`, `State_Delete`, `State_Merge` | corresponding `httk_workflow_state_*` functions |
| `Httk_Workflow_Declaration`, `Declare` | `httk_workflow_declaration`, `httk_workflow_declare` |
| `Httk_Workflow_Runlog_Note`, `Runlog_Headline`, `Runlog_Append`, `Log` | corresponding `httk_workflow_*` functions |
| `Httk_Workflow_Put`, `Remove`, `Spawn` | corresponding transactional/child C functions |
| `Httk_Workflow_Children`, `Child` | `httk_workflow_children`, `httk_workflow_child` |
| `Httk_Workflow_Advance`, `Gather`, `Succeed`, `Fail`, `Retry`, `Pause` | corresponding outcome C functions |
| `Httk_Workflow_Batch`, `Job_Prepare`, `Workdir_Apply` | corresponding C functions |
| `Httk_Workflow_Run`, `Httk_Calc`, `Httk_Template_Render` | `httk_workflow_run`, `httk_calc`, `httk_template_render` |
| `Httk_Compress`, `Httk_Decompress` | `httk_compress`, `httk_decompress` |

Tail arguments use `String_List`, an array of `Unbounded_String`; `No_Arguments`
passes a C NULL array. `Httk_Workflow_Parameter`, `Setting`, and `Environment`
have overloads with and without a fallback. `Httk_Workflow_Exit` is the Ada
counterpart of returning from a C `main`.

## Strings, ownership, and absent reads

Input `String` values are copied to temporary NUL-terminated C strings and
released after the call. C string results are returned as freshly `malloc`'d
strings under the C contract. The binding copies each result into an Ada
`Unbounded_String` and releases the original with libc `free`, exactly once.

Read procedures return both `Present : Boolean` and `Status : C.int`:

```ada
Value : Ada.Strings.Unbounded.Unbounded_String;
Present : Boolean;
Status : Interfaces.C.int;
Httk_Workflow_State_Get ("energy", Value, Present, Status);
```

An allocated empty C string means `Present = True` and `Status = 0`. A NULL
answer means `Present = False`; status `1` is a legitimate absent answer and
status `2` is a refused call. Thus absent and present-empty values do not
collapse. The same rule applies to operation ids, child keys, declarations,
context values, calculated values, and other C string returns.

The status constants are `HTTK_WORKFLOW_OK` (`0`),
`HTTK_WORKFLOW_ABSENT` (`1`), and `HTTK_WORKFLOW_REFUSED` (`2`).
`Httk_Workflow_Run` instead returns the supervised program classification:
`0`, `22` for a nonzero program exit, `124` for timeout, and `125` for a
checker or diagnostic stop.

## The relaxation example

`examples/relax_ada/relax.adb` registers the library-level handlers from
`relax_steps.ads`/`relax_steps.adb` and declares `httk.vasp.relax-ada` with
`prepare`, `run`, and `publish` steps. It stages `files/POSCAR`, resolves
`vasp.command`, invokes `Httk_Workflow_Run`, records the completion state, and
stages VASP outputs into transactional data. It is driven by the same mock VASP
flow as the other native SDK examples:

```console
cd examples/relax_ada
make
./relax --describe
```

The Makefile uses `cc -std=c99` for the C object and
`gnatmake -gnat2012 -gnatwa -gnatwe` for Ada, with no executable-stack linker
flag.
