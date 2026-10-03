# Modern-Fortran runner API

*For authors writing a workflow runner in Fortran.* The Fortran SDK is the same
authoring surface as the {doc}`Python <../runtime_helpers>`, {doc}`Bash
<bash_api>`, and {doc}`C <c_api>` ones. It adds **no new bridge
protocol**: it is `iso_c_binding` bindings over the C library
(`languages/c/httk_workflow.{h,c}`) plus one idiomatic Fortran module, so the C
`httk_workflow_main` still owns registration, dispatch, and the process exit
status, and every verb still execs `$HTTK_WORKFLOW_PYTHON -m
httk.workflow._shell_bridge <verb> …`. A Fortran runner therefore publishes the
same bytes as a Python, Bash, or C runner for the same campaign. Only the
`--describe` handshake is native. The normative cross-language semantics are the
table in {doc}`sdk_parity`; the function-by-function Fortran mapping is the table
below.

The SDK is one module source, `languages/fortran/httk_workflow.f90`, packaged under
`httk.workflow`. It is modern Fortran (2008), warning-clean under `gfortran
-std=f2008 -Wall -Wextra -Werror`, with no dependency beyond `iso_c_binding` and
the C SDK it wraps. It is designed to be **compiled beside a runner** together
with the C source.

## A complete runner

A runner declares its workflow and its complete step set once, implements one
handler per step, and hands control to `httk_workflow_main`. Each handler is a
`bind(c)` function of no arguments returning `integer(c_int)`:

```fortran
module my_steps
  use, intrinsic :: iso_c_binding, only: c_int
  use httk_workflow
  implicit none
contains
  function step_prepare() result(code) bind(c)
    integer(c_int) :: code
    call ignore(httk_workflow_advance("run"))
    code = 0
  end function
  function step_run() result(code) bind(c)
    integer(c_int) :: code
    call ignore(httk_workflow_succeed())
    code = 0
  end function
end module

program main
  use, intrinsic :: iso_c_binding, only: c_funloc
  use httk_workflow
  use my_steps
  implicit none
  if (httk_workflow_runner("my.workflow", &
        [character(len=8) :: "prepare", "run"], &
        [c_funloc(step_prepare), c_funloc(step_run)]) /= HTTK_WORKFLOW_OK) &
      call httk_workflow_exit(2)
  call httk_workflow_exit(httk_workflow_main())
end program
```

Build it against the packaged SDK, compiling the two languages separately (the
Fortran standard flag is not valid for C, and the C source is compiled with a C
compiler, then linked into the Fortran build):

```console
cc       -std=c99   -c .../httk/workflow/languages/c/httk_workflow.c -o httk_workflow_c.o
gfortran -std=f2008    .../httk/workflow/languages/fortran/httk_workflow.f90 \
         runner.f90 httk_workflow_c.o -o runner
```

The compiled binary is the runner file a job references. It is executable, so the
manager runs it directly and the scaffolder describes it by running it — a job
that starts from a runner file of your own is resolved the same way whatever
language wrote it.

From a workflow package, build against the installed SDK through
`HTTK_WORKFLOW_LANGUAGES_DIR`, which `[workflow.build]` commands and attempts both
see:

```console
cc       -std=c99   -c "$HTTK_WORKFLOW_LANGUAGES_DIR/c/httk_workflow.c" -o httk_workflow_c.o
gfortran -std=f2008    "$HTTK_WORKFLOW_LANGUAGES_DIR/fortran/httk_workflow.f90" \
         runner.f90 httk_workflow_c.o -o runner
```

Its manifest then runs the registered binary directly, with no `run` bridge
script (see {doc}`../details/workflow_packages`):

```toml
[workflow.runner]
command = ["{artifacts}/runner"]
```

A complete VASP relaxation authored this way is described at the end of this
page.

## Step handlers and dispatch

`httk_workflow_runner(workflow, names, handlers)` declares the complete step set
before any work happens. `names` is a character array of the step names, and
`handlers` is the array of their addresses, obtained with `c_funloc`, in the same
order. A name that is empty, contains a character outside `[A-Za-z0-9._-]`, is
duplicated, or is paired with a null handler is refused by the C validator with a
diagnostic on stderr and the return value `HTTK_WORKFLOW_REFUSED`. The
registration is saved module state the C library keeps a pointer into; it
outlives the process, so nothing is copied on dispatch.

A step handler is a **`bind(c)` function returning `integer(c_int)`**, not the
plainer Fortran subroutine, and this is the one place the Fortran surface is
shaped by the C ABI it sits on. The C dispatcher calls each handler through an
`int (*)(void)` pointer: `c_funloc` requires an interoperable (that is,
`bind(c)`) target, and reading the return value of a `void`-returning procedure
through an `int (*)(void)` pointer would be undefined behaviour. The return value
carries the same meaning as a C handler's:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one, then `code = 0` | that outcome |
| the handler sets `code = 0` without publishing | `fail("no_outcome", …)` |
| the step is not registered | `fail("unknown_step", "… registered steps: …")` |
| the handler sets `code` nonzero | an `error.json` breadcrumb (exception `CError`), then that nonzero exit the manager records as `process_failure` |

`httk_workflow_main()` forwards this process's command line to the C
`httk_workflow_main`, which reads the step the manager asked for, dispatches its
handler, and **owns the exit status**. Because a Fortran program end always exits
`0`, the runner propagates that status with `httk_workflow_exit(status)` — the
Fortran counterpart of a C runner's `return` from `main` (it calls libc `exit`,
so the code is exact and carries no `STOP` diagnostic, and the SDK stays within
Fortran 2008).

Because dispatch lives in the C library, the breadcrumb an aborted handler leaves
carries the exception label **`CError`** (not a Fortran-specific name). A Fortran
author who set `code` nonzero recognizes their handler in `httk job why`
output by that `CError` exception together with the `"<step> exited with status
N"` message.

`HTTK_WORKFLOW_DESCRIBE=1` makes `httk_workflow_runner` print the runner
description and exit `0` before any step runs, and `httk_workflow_main` honours
`--describe` in argv the same way. The description is produced natively,
byte-for-byte what a Python, Bash, or C runner prints for the same workflow and
steps:

```json
{"format": "httk-workflow-runner-description", "format_version": 2, "steps": ["prepare", "run"], "workflow": "my.workflow"}
```

## Strings and ownership across the C boundary

Fortran callers never see a C pointer. The module marshals strings both ways and
frees every C allocation exactly once:

- **Arguments in** are ordinary `character(len=*)` values, each copied to a
  NUL-terminated C string for the call. **A trailing run of blanks is trimmed at
  the C boundary.** This is a real divergence to be aware of, not a free lunch: a
  deferred-length string *can* hold meaningful trailing spaces, and they will not
  round-trip through this SDK. A value whose trailing whitespace is significant
  (a rare case for the bridge's tokens and paths) must be passed through the
  {doc}`C <c_api>` SDK instead, which copies bytes verbatim.
- **Reads out** are **subroutines** with a `character(len=:), allocatable,
  intent(out)` result argument — deliberately not functions. The C side returns a
  freshly `malloc`'d string; the module copies it into the argument and calls the
  C `free`, leaving a plain owned Fortran string with no cleanup for the caller.
  When the answer is **absent** (the C side returned `NULL`), the argument is
  left **unallocated**, which `allocated(value)` and the `status` argument both
  report; a legitimate **empty** string arrives **allocated with length zero**.
  That absent-versus-empty distinction is why the reads cannot be functions: an
  unallocated allocatable function result is undefined to assign from, and
  gfortran collapses it into a zero-length string, erasing the difference.
- **Optional arguments** carry the C tail-arguments and defaults. A read with an
  optional default (`httk_workflow_parameter(name, value, fallback, status)`)
  omits `fallback` to pass C `NULL`; an absent optional `status` is simply not
  written. The NUL-terminated `char *[]` tail each verb forwards to the bridge is
  an optional `character(len=*), dimension(:)` argument (`args`, `files`,
  `assignments`): each element becomes one bridge argument, trailing blanks
  trimmed, and omitting the argument passes C `NULL`.

Every integer-returning verb returns the bridge exit status directly as a plain
`integer`. Every read takes an optional `integer, intent(out) :: status` that
carries the bridge exit status distinguishing an absent answer from a refused
call (the constants below). `call ignore(verb(...))` discards a status a step
body does not inspect, the frequent publish-and-move-on case.

## The Fortran function table

Each Fortran procedure is one C function, which is one bridge subcommand — the
same subcommand the paired Bash function calls — so this table's C column is the
{doc}`c_api` row this SDK realizes, and through it the {doc}`sdk_parity`
row. `args`/`files`/`assignments` are optional `character(len=*)` arrays;
`status` is the optional bridge-status out-argument; `fallback` is an optional
default. **Reads are subroutines** (marked *`sub`*): the string arrives in the
`intent(out), allocatable` argument named in the signature
(`value`/`operation`/`job_key`/`job`/`id`), which is left unallocated when the
answer is absent. Every other verb is an `integer` function returning the bridge
status.

| Fortran | C |
| --- | --- |
| `httk_workflow_runner(workflow, names, handlers)` | `httk_workflow_runner` |
| `httk_workflow_main()` | `httk_workflow_main` |
| `httk_workflow_exit(status)` | (`return` from `main`) |
| `httk_workflow_describe()` | `httk_workflow_describe` |
| `httk_workflow_invoke(argv, output)` | `httk_workflow_invoke` |
| *`sub`* `httk_workflow_context(value, field, status)` | `httk_workflow_context` |
| *`sub`* `httk_workflow_parent(value, field, status)` | `httk_workflow_parent` |
| *`sub`* `httk_workflow_parameter(name, value, fallback, status)` | `httk_workflow_parameter` |
| *`sub`* `httk_workflow_setting(name, value, fallback, status)` | `httk_workflow_setting` |
| *`sub`* `httk_workflow_environment(name, value, fallback, status)` | `httk_workflow_environment` |
| `httk_workflow_stage_input(name, destination, fallback)` | `httk_workflow_stage_input` |
| *`sub`* `httk_workflow_state_get(name, value, status)` | `httk_workflow_state_get` |
| `httk_workflow_state_set(name, value)` | `httk_workflow_state_set` |
| `httk_workflow_state_delete(name)` | `httk_workflow_state_delete` |
| `httk_workflow_state_merge(assignments)` | `httk_workflow_state_merge` |
| `httk_workflow_declare(name, document_file)` | `httk_workflow_declare` |
| *`sub`* `httk_workflow_declaration(name, value, status)` | `httk_workflow_declaration` |
| `httk_workflow_runlog_note(message)` | `httk_workflow_runlog_note` |
| `httk_workflow_runlog_headline(message)` | `httk_workflow_runlog_headline` |
| `httk_workflow_runlog_append(message, files)` | `httk_workflow_runlog_append` |
| `httk_workflow_log(level, message)` | `httk_workflow_log` |
| *`sub`* `httk_workflow_put(source, destination, operation, status)` | `httk_workflow_put` |
| *`sub`* `httk_workflow_remove(destination, operation, missing_ok, status)` | `httk_workflow_remove` |
| *`sub`* `httk_workflow_spawn(label, job_key, args, status)` | `httk_workflow_spawn` |
| *`sub`* `httk_workflow_call(label, workflow, job_key, args, status)` | `httk_workflow_call` |
| *`sub`* `httk_workflow_children(value, selection, status)` | `httk_workflow_children` |
| *`sub`* `httk_workflow_child(label, field, value, status)` | `httk_workflow_child` |
| `httk_workflow_advance(next_step, args)` | `httk_workflow_advance` |
| `httk_workflow_gather(next_step, args)` | `httk_workflow_gather` |
| `httk_workflow_succeed()` | `httk_workflow_succeed` |
| `httk_workflow_fail(code, message, args)` | `httk_workflow_fail` |
| `httk_workflow_retry(reason)` | `httk_workflow_retry` |
| `httk_workflow_pause(reason)` | `httk_workflow_pause` |
| `httk_workflow_batch()` | `httk_workflow_batch` |
| *`sub`* `httk_workflow_job_prepare(destination, spec_file, job, status)` | `httk_workflow_job_prepare` |
| *`sub`* `httk_workflow_workdir_apply(spec_file, id, status)` | `httk_workflow_workdir_apply` |
| `httk_workflow_run(args)` | `httk_workflow_run` |
| *`sub`* `httk_calc(expression, value, status)` | `httk_calc` |
| `httk_template_render(template_file, output, values_file)` | `httk_template_render` |
| `httk_compress(args)` | `httk_compress` |
| `httk_decompress(args)` | `httk_decompress` |
| `httk_copy_file(source, destination)` | `httk_copy_file` |
| `httk_getenv(name, fallback)` | (pure Fortran, `get_environment_variable`) |

`httk_workflow_parent` leaves `value` unallocated with status `1` when the job has
no reachable parent, and for `field="workdir"` when the parent uses isolated
workdirs.

`httk_getenv` is a function returning an always-allocated string: the variable's
value, or `fallback` (default `""`) when it is unset or empty.

Booleans are Fortran `logical`: `httk_workflow_remove`'s `missing_ok` is an
optional `logical`, marshalled to the C `int` flag. As in C, a code's Bash API,
such as *httk-workflow-vasp*'s `httk_vasp_*`, has no dedicated wrappers; reach a
`<code>-*` verb such as `vasp-*` through
`httk_workflow_invoke`, which is why the example below runs the configured
command through `httk_workflow_run` and classifies its result.

## Exit codes

The three-status discipline is re-exported as module parameters, identical to the
C SDK's:

| Status | Meaning |
| --- | --- |
| `HTTK_WORKFLOW_OK` (`0`) | the call succeeded |
| `HTTK_WORKFLOW_ABSENT` (`1`) | the answer is legitimately absent: an unset state key, a missing parameter without a default, a child that was not observed |
| `HTTK_WORKFLOW_REFUSED` (`2`) | the call is refused: bad usage, a protocol violation, a corrupt attempt context — also what is returned when `HTTK_WORKFLOW_PYTHON` is unset |

Reading a value is therefore an ordinary conditional:

```fortran
integer :: status
character(len=:), allocatable :: energy
call httk_workflow_state_get("energy", energy, status)
if (status == HTTK_WORKFLOW_OK) then  ! equivalently: if (allocated(energy))
  ! resume from energy
end if
```

`httk_workflow_run` returns the classified outcome of the program it ran instead:
`0`, `22` for a nonzero exit, `124` for a timeout whose process group was
terminated, and `125` when a checker or diagnostic stopped it.

## A VASP relaxation package

The `vasp-relax-fortran` package of
[workflows-vasp-other-languages](https://github.com/httk/workflows-vasp-other-languages) is a complete
`prepare`/`run`/`publish` relaxation built with this SDK, mock-VASP compatible
and publishing to transactional data. Its workflow is `vasp.relax-fortran`; run it
as `git+https://github.com/httk/workflows-vasp-other-languages#vasp-relax-fortran`.
