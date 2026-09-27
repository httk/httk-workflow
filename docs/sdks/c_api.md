# C runner API

*For authors writing a workflow runner in C.* The C SDK is the same authoring
surface as the {doc}`Python <../runtime_helpers>` and {doc}`Bash <bash_api>`
ones, in a clean C-idiomatic ABI. Like the Bash library it is a **bridge
client**: every verb execs `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge
<verb> …`, which drives the same `Attempt` object the Python SDK exposes, so a C
runner and a Python or Bash runner publish the same bytes for the same campaign.
Only the `--describe` handshake is native. The normative cross-language semantics
are the table in {doc}`sdk_parity`; the function-by-function C mapping is the
table below.

The SDK is one header/source pair, `httk_workflow.h` and `httk_workflow.c`,
packaged under `httk.workflow` at `languages/c/`. It is C99 with no dependency
beyond libc and POSIX, and it is designed to be **vendored** into a runner's own
tree or **compiled directly** beside it. It is also the foundation the later
Fortran bindings build on: the exported symbols are all prefixed
`httk_workflow_`, take opaque scalars and NUL-terminated strings, and never a
macro where a function works.

## A complete runner

A C runner declares its workflow and its complete step set once, implements one
handler per step, and hands control to `httk_workflow_main`:

```c
#include "httk_workflow.h"

static int step_prepare(void) { /* … */ httk_workflow_advance("run", NULL); return 0; }
static int step_run(void)     { /* … */ httk_workflow_succeed();          return 0; }

int main(int argc, char **argv) {
    static const httk_workflow_step steps[] = {
        {"prepare", step_prepare},
        {"run",     step_run},
    };
    if (httk_workflow_runner("my.workflow", steps, 2) != 0)
        return 2;
    return httk_workflow_main(argc, argv);
}
```

Build it against the packaged SDK:

```console
cc -std=c99 -Wall -Wextra runner.c \
   .../httk/workflow/languages/c/httk_workflow.c \
   -I.../httk/workflow/languages/c -o runner
```

The compiled binary is the runner file a job references. It is executable, so the
manager runs it directly and the scaffolder describes it by running it — a job
that starts from a runner file of your own is resolved the same way whatever
language wrote it.

From a workflow package, build against the installed SDK through
`HTTK_WORKFLOW_LANGUAGES_DIR`, which `[workflow.build]` commands and attempts both
see:

```console
cc -std=c99 -Wall -Wextra -I"$HTTK_WORKFLOW_LANGUAGES_DIR/c" runner.c \
   "$HTTK_WORKFLOW_LANGUAGES_DIR/c/httk_workflow.c" -o runner
```

Its manifest then runs the registered binary directly, with no `run` bridge
script (see {doc}`../details/workflow_packages`):

```toml
[workflow.runner]
command = ["{artifacts}/runner"]
```

A complete VASP relaxation authored this way is described at the end of this
page.

## Registration and dispatch

`httk_workflow_runner(workflow_id, steps, count)` declares the complete step set
before any work happens. Each `httk_workflow_step` pairs a step name with the
`int (*)(void)` handler that implements it. A name that is empty, contains a
character outside `[A-Za-z0-9._-]`, is duplicated, or has a `NULL` handler is
refused with a diagnostic on stderr and the return value `HTTK_WORKFLOW_REFUSED`.
The registration is the one piece of state the library keeps, mirroring the Bash
library's private globals; the `steps` array must outlive the process, so a
`static const` array is idiomatic.

`httk_workflow_main(argc, argv)` reads the step the manager asked for, dispatches
its handler, and **owns the process exit status** — which is why the handlers and
the outcome functions *return* rather than exit. It turns every ending of a step
into exactly one outcome, the same guarantee the other SDKs give:

| Ending | Published outcome |
| --- | --- |
| the handler publishes one | that outcome |
| the handler returns 0 without publishing | `fail("no_outcome", …)` |
| the step is not registered | `fail("unknown_step", "… registered steps: …")` |
| the handler returns nonzero | an `error.json` breadcrumb (exception `CError`), then that nonzero exit the manager records as `process_failure` |

A handler returns `0` when it ended — whether or not it published — and a nonzero
value when it could not complete. The nonzero case is the C analogue of a Bash
handler dying under `set -e`: the unpublished draft is discarded, `error.json`
records the step, the message, and the status, and the process exits nonzero.
There is no per-line breadcrumb, because C has no shell `ERR` trap; the message
names the step and its status.

`HTTK_WORKFLOW_DESCRIBE=1` makes `httk_workflow_runner` print the runner
description and exit `0` before any step runs, and `httk_workflow_main --describe`
does the same. The description is produced natively, byte-for-byte what a Python
or Bash runner prints for the same workflow and steps:

```json
{"format": "httk-workflow-runner-description", "format_version": 2, "steps": ["prepare", "run"], "workflow": "my.workflow"}
```

The step names are byte-sorted; the `workflow` follows.

## Memory ownership

Every function that returns a `char *` returns a freshly `malloc`'d,
NUL-terminated string the caller must `free()`, **or `NULL`**. `NULL` means the
answer is absent or the call was refused; the `int *status` out-parameter (when
present) carries the bridge exit status that distinguishes the two — see the exit
codes below. Trailing newlines are stripped from a captured value, as command
substitution does in the shell.

Functions that return `int` return the bridge exit status directly. The
variadic-tail parameters spelled `const char *const *args` are NULL-terminated
arrays of extra bridge arguments the caller owns; pass `NULL` for none. They carry
the same options the corresponding Bash function forwards untouched (for example
`--step`, `--parameter NAME=VALUE`, `--when`, `--priority`).

`httk_workflow_invoke(out, argv)` is the foundation every verb is built on and the
escape hatch for any bridge subcommand or option without a dedicated wrapper.
`argv` names the subcommand and its arguments; the SDK prepends the interpreter
invocation. When `out` is non-`NULL` the subcommand's stdout is captured into
`*out`; when it is `NULL`, stdin and stdout are inherited so `run` and `batch`
stream normally.

## The C function table

Each C function is one bridge subcommand, the same subcommand the paired Bash
function calls, so this table's Bash and Python columns are the {doc}`sdk_parity`
rows this SDK realizes. `args` is a NULL-terminated options array; `status` is the
bridge exit status out-parameter.

| C | Bash | Python |
| --- | --- | --- |
| `httk_workflow_runner(id, steps, n)` | `httk_workflow_runner` | `Runner` |
| `httk_workflow_main(argc, argv)` | `httk_workflow_main` | `Runner.main` |
| `httk_workflow_describe()` | `httk_workflow_main --describe` | `Runner.description` |
| `httk_workflow_invoke(out, argv)` | `_httk_workflow_bridge` | `shell_bridge.main` |
| `httk_workflow_context(field, status)` | `httk_workflow_context` | `Attempt.context` |
| `httk_workflow_parameter(name, fallback, status)` | `httk_workflow_parameter` | `Attempt.parameter` |
| `httk_workflow_setting(name, fallback, status)` | `httk_workflow_setting` | `Attempt.setting` |
| `httk_workflow_environment(name, fallback, status)` | `httk_workflow_environment` | `Attempt.environment` |
| `httk_workflow_state_get(name, status)` | `httk_workflow_state_get` | `JobState.read` |
| `httk_workflow_state_set(name, value)` | `httk_workflow_state_set` | `JobState.set` |
| `httk_workflow_state_delete(name)` | `httk_workflow_state_delete` | `JobState.delete` |
| `httk_workflow_state_merge(assignments)` | `httk_workflow_state_merge` | `JobState.merge` |
| `httk_workflow_declare(name, file)` | `httk_workflow_declare` | `Attempt.declare` |
| `httk_workflow_declaration(name, status)` | `httk_workflow_declaration` | `Attempt.declaration` |
| `httk_workflow_runlog_note(message)` | `httk_workflow_runlog_note` | `RunLog.append` |
| `httk_workflow_runlog_headline(message)` | `httk_workflow_runlog_headline` | `RunLog.append` |
| `httk_workflow_runlog_append(message, files)` | `httk_workflow_runlog_append` | `RunLog.append` |
| `httk_workflow_log(level, message)` | `httk_workflow_log` | — (`logging`) |
| `httk_workflow_put(source, dest, status)` | `httk_workflow_put` | `Attempt.put` |
| `httk_workflow_remove(dest, missing_ok, status)` | `httk_workflow_remove` | `Attempt.remove` |
| `httk_workflow_spawn(label, args, status)` | `httk_workflow_spawn` | `Attempt.spawn` |
| `httk_workflow_children(selection, status)` | `httk_workflow_children` | `Attempt.children` |
| `httk_workflow_child(label, field, status)` | `httk_workflow_child` | `ChildResult` |
| `httk_workflow_advance(next_step, args)` | `httk_workflow_advance` | `Attempt.advance` |
| `httk_workflow_gather(next_step, args)` | `httk_workflow_gather` | `Attempt.gather` |
| `httk_workflow_succeed()` | `httk_workflow_succeed` | `Attempt.succeed` |
| `httk_workflow_fail(code, message, args)` | `httk_workflow_fail` | `Attempt.fail` |
| `httk_workflow_retry(reason)` | `httk_workflow_retry` | `Attempt.retry` |
| `httk_workflow_pause(reason)` | `httk_workflow_pause` | `Attempt.pause` |
| `httk_workflow_batch()` | `httk_workflow_batch` | — |
| `httk_workflow_job_prepare(dest, spec, status)` | `httk_workflow_job_prepare` | `prepare_job_payload` |
| `httk_workflow_workdir_apply(spec, status)` | `httk_workflow_workdir_apply` | `Attempt.workdir_batch` |
| `httk_workflow_run(args)` | `httk_workflow_run` | `ProcessSupervisor` |
| `httk_calc(expression, status)` | `httk_calc` | `evaluate_expression` |
| `httk_template_render(template, output, values)` | `httk_template_render` | `render_template` |
| `httk_compress(args)` | `httk_compress` | `compress_files` |
| `httk_decompress(args)` | `httk_decompress` | `decompress_files` |

The `httk_vasp_*` surface of the Bash SDK has no dedicated C wrappers; a C runner
that needs a VASP subcommand reaches it through `httk_workflow_invoke` with the
same `vasp-*` verb, which is why the example below runs the configured command
through `httk_workflow_run` and classifies its result.

## Exit codes

Every function reports the same three-status discipline as Bash, so an absent
answer never looks like a broken call:

| Status | Meaning |
| --- | --- |
| `HTTK_WORKFLOW_OK` (`0`) | the call succeeded |
| `HTTK_WORKFLOW_ABSENT` (`1`) | the answer is legitimately absent: an unset state key, a missing parameter without a default, a child that was not observed |
| `HTTK_WORKFLOW_REFUSED` (`2`) | the call is refused: bad usage, a protocol violation, a corrupt attempt context — also what is returned when `HTTK_WORKFLOW_PYTHON` is unset |

Reading a value is therefore an ordinary conditional:

```c
int status;
char *energy = httk_workflow_state_get("energy", &status);
if (status == HTTK_WORKFLOW_OK) {
    /* resume from *energy */
}
free(energy);
```

`httk_workflow_run` returns the classified outcome of the program it ran instead:
`0`, `22` for a nonzero exit, `124` for a timeout whose process group was
terminated, and `125` when a checker or diagnostic stopped it.

## A VASP relaxation package

The `vasp-relax-c` package of
[workflows-vasp-other-languages](https://github.com/httk/workflows-vasp-other-languages) is a complete
`prepare`/`run`/`publish` relaxation built with this SDK, mock-VASP compatible
and publishing to transactional data. Its workflow is `vasp.relax-c`; run it
as `git+https://github.com/httk/workflows-vasp-other-languages#vasp-relax-c`.
