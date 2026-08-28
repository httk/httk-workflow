/*
 * Native httk-workflow C authoring SDK.
 *
 * This header and httk_workflow.c are one self-contained pair, designed to be
 * vendored into a runner's own source tree or compiled directly beside it:
 *
 *     cc -std=c99 -Wall -Wextra runner.c httk_workflow.c -o runner
 *
 * A C runner declares its workflow and its steps once, implements one handler
 * per declared step, and ends by handing control to httk_workflow_main, exactly
 * like the Bash SDK in shell/httk-workflow.sh:
 *
 *     #include "httk_workflow.h"
 *
 *     static int step_prepare(void) { ...; return 0; }
 *     static int step_run(void)     { ...; return 0; }
 *     static int step_publish(void) { ...; return 0; }
 *
 *     int main(int argc, char **argv) {
 *         static const httk_workflow_step steps[] = {
 *             {"prepare", step_prepare},
 *             {"run",     step_run},
 *             {"publish", step_publish},
 *         };
 *         if (httk_workflow_runner("my.workflow", steps, 3) != 0)
 *             return 2;
 *         return httk_workflow_main(argc, argv);
 *     }
 *
 * This SDK is a *bridge client*: every verb below execs
 * `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge <verb> ...`, so a C
 * runner and a Python or Bash runner publish the same bytes because they publish
 * through one implementation. Only the `--describe` handshake is native, so a
 * runner can be enumerated without an interpreter.
 *
 * The step handlers *return*; they do not exit. httk_workflow_main owns the
 * process exit status, because it is what turns every ending of a step into
 * exactly one published outcome: a handler that returns 0 without publishing is
 * reported as `no_outcome`, a step this runner does not implement is reported as
 * `unknown_step`, and a handler that returns nonzero discards its unpublished
 * draft and leaves an `error.json` breadcrumb.
 *
 * ---------------------------------------------------------------------------
 * Memory ownership
 * ---------------------------------------------------------------------------
 * Every function that returns a `char *` returns a freshly malloc'd,
 * NUL-terminated string that the caller must free(), or NULL. A NULL return
 * means the answer is absent or the call was refused; the accompanying `status`
 * out-parameter (when present) distinguishes the two — see the exit-status
 * constants below. Trailing newlines are stripped from a captured value, as
 * command substitution does in the shell.
 *
 * The `steps` array passed to httk_workflow_runner must outlive the process
 * (a `static const` array is idiomatic); the SDK stores a pointer to it and
 * copies nothing. It is the only mutable state the SDK keeps.
 *
 * The variadic-tail parameters (`const char *const *args`) are NULL-terminated
 * arrays of extra bridge arguments the caller owns; pass NULL for none. They
 * mirror the options each Bash verb forwards untouched to the bridge.
 */

#ifndef HTTK_WORKFLOW_H
#define HTTK_WORKFLOW_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The version of this native library, mirroring HTTK_WORKFLOW_BASH_API_VERSION. */
#define HTTK_WORKFLOW_C_API_VERSION 2

/*
 * Bridge exit-status discipline, identical to the Bash SDK: a call succeeds (0),
 * its answer is legitimately absent (1), or it is refused (2). The program-running
 * verbs (httk_workflow_run) additionally report the classified outcome of the
 * program they ran (22 nonzero, 124 timeout, 125 stopped by a checker), returned
 * verbatim from the bridge.
 */
#define HTTK_WORKFLOW_OK 0
#define HTTK_WORKFLOW_ABSENT 1
#define HTTK_WORKFLOW_REFUSED 2

/*
 * A step handler. It returns 0 when the step ended (whether or not it published
 * an outcome) and nonzero when it could not complete, which is turned into an
 * aborted-attempt breadcrumb the way a Python traceback is.
 */
typedef int (*httk_workflow_step_fn)(void);

/* One declared step: its name and the handler that implements it. */
typedef struct {
    const char *name;
    httk_workflow_step_fn handler;
} httk_workflow_step;

/*
 * Declare the workflow and the complete step set of this runner, before any step
 * runs. Every step name must be non-empty, contain only [A-Za-z0-9._-], be
 * unique, and have a non-NULL handler; otherwise a diagnostic is written to
 * stderr and HTTK_WORKFLOW_REFUSED is returned.
 *
 * When HTTK_WORKFLOW_DESCRIBE=1 is set, this prints the runner description (see
 * httk_workflow_describe) and exits the process with status 0, so describing a
 * runner needs no interpreter and never dispatches a step.
 *
 * Returns HTTK_WORKFLOW_OK on success or HTTK_WORKFLOW_REFUSED on bad usage.
 */
int httk_workflow_runner(const char *workflow_id, const httk_workflow_step *steps, size_t count);

/*
 * Dispatch the step the manager asked for and turn its ending into exactly one
 * outcome. Recognizes `--describe` in argv (equivalent to HTTK_WORKFLOW_DESCRIBE)
 * and otherwise reads the step from the attempt, dispatches its handler, and
 * owns the process exit status. Returns the exit status the process should use.
 */
int httk_workflow_main(int argc, char **argv);

/*
 * Print this runner's machine-readable description to stdout, byte-for-byte the
 * same JSON a Python or Bash runner prints for the same workflow and step set:
 *
 *   {"format": "httk-workflow-runner-description", "format_version": 2,
 *    "steps": [<byte-sorted names>], "workflow": "<workflow>"}
 *
 * followed by one newline. httk_workflow_runner must have been called first.
 */
void httk_workflow_describe(void);

/*
 * The foundational bridge call every verb below is built on, and the escape
 * hatch for any bridge subcommand or option without a dedicated wrapper.
 * `argv` is a NULL-terminated array naming the bridge subcommand and its
 * arguments (for example {"parameter", "encut", "--default", "520", NULL}); the
 * SDK prepends `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`.
 *
 * When `out` is non-NULL, the subcommand's stdout is captured into `*out` (a
 * malloc'd string the caller frees, with trailing newlines stripped) and
 * stdout is not inherited; when `out` is NULL, stdout and stdin are inherited so
 * a verb such as `run` or `batch` streams normally.
 *
 * Returns the bridge exit status, or HTTK_WORKFLOW_REFUSED when
 * HTTK_WORKFLOW_PYTHON is unset (the same diagnostic the Bash SDK prints) or the
 * subprocess could not be started.
 */
int httk_workflow_invoke(char **out, const char *const *argv);

/* --- What a step reads ------------------------------------------------------ */

/* The attempt context, or one field of it when `field` is non-NULL. */
char *httk_workflow_context(const char *field, int *status);
/* One member of the job's opaque parameters object, with an optional default. */
char *httk_workflow_parameter(const char *name, const char *fallback, int *status);
/* One resolved application setting, with an optional default. */
char *httk_workflow_setting(const char *name, const char *fallback, int *status);
/* One declared workflow environment value, with an optional default. */
char *httk_workflow_environment(const char *name, const char *fallback, int *status);
/* One key of the job's JSON state; absent (status 1) when unset. */
char *httk_workflow_state_get(const char *name, int *status);
/* One workflow declaration: the observed document, else the declared one. */
char *httk_workflow_declaration(const char *name, int *status);
/*
 * The observed children as tab-separated rows, one per line (the whole block is
 * returned verbatim). `selection` is NULL, "--all", "--succeeded", or "--failed".
 */
char *httk_workflow_children(const char *selection, int *status);
/* One field of one observed child by label. */
char *httk_workflow_child(const char *label, const char *field, int *status);

/* --- Job state -------------------------------------------------------------- */

/* Store one JSON value (a bare scalar, valid JSON, or @file.json) in one replace. */
int httk_workflow_state_set(const char *name, const char *value);
/* Remove one key, returning HTTK_WORKFLOW_ABSENT when it was not present. */
int httk_workflow_state_delete(const char *name);
/* Write several NAME=VALUE assignments (NULL-terminated) in one atomic replace. */
int httk_workflow_state_merge(const char *const *assignments);

/* --- Declarations and the run log ------------------------------------------ */

/* Record the workflow declaration this job observed, from a JSON document file. */
int httk_workflow_declare(const char *name, const char *document_file);
/* Append one ordinary evidence event to this job's logs/runlog.jsonl. */
int httk_workflow_runlog_note(const char *message);
/* Append one event meant to be read first when the job is inspected. */
int httk_workflow_runlog_headline(const char *message);
/* Append one event with whole files (NULL-terminated) attached by content. */
int httk_workflow_runlog_append(const char *message, const char *const *files);
/*
 * Write one timestamped "LEVEL MESSAGE" line to stderr, which the manager
 * retains with the attempt. Unlike every other verb this is local (no bridge).
 */
int httk_workflow_log(const char *level, const char *message);

/* --- Transactional data ----------------------------------------------------- */

/* Stage one file or tree into the job's data; returns the operation id. */
char *httk_workflow_put(const char *source, const char *destination, int *status);
/* Stage one removal from the job's data; returns the operation id. */
char *httk_workflow_remove(const char *destination, int missing_ok, int *status);

/* --- Children --------------------------------------------------------------- */

/*
 * Register one child under a mandatory unique `label`, created when the outcome
 * is published; returns the child's job key. `args` carries the child options
 * (--step, --parameter NAME=VALUE, --payload, --runner, ...) untouched.
 */
char *httk_workflow_spawn(const char *label, const char *const *args, int *status);

/* --- What a step publishes (exactly one per attempt) ------------------------ */

/* Run `next_step` next; `args` may carry --state NAME=VALUE and --priority. */
int httk_workflow_advance(const char *next_step, const char *const *args);
/* Wait for the children spawned on this attempt, then run `next_step`; `args`
 * may carry --when, --count, --on-impossible, and --priority. */
int httk_workflow_gather(const char *next_step, const char *const *args);
/* Publish the successful completion of this job. */
int httk_workflow_succeed(void);
/* Publish a structured terminal failure; `args` may carry --details, --retryable,
 * and --priority. */
int httk_workflow_fail(const char *code, const char *message, const char *const *args);
/* Ask for another attempt of this same activation. */
int httk_workflow_retry(const char *reason);
/* Pause this job until an operator resumes it. */
int httk_workflow_pause(const char *reason);

/* --- Many calls, one interpreter ------------------------------------------- */

/* Run several bridge commands, one per line read from this process's stdin, in
 * one interpreter start. Stops at the first failing line. */
int httk_workflow_batch(void);

/* --- Payload and workdir ---------------------------------------------------- */

/* Write job.json into a prepared payload from a JobSpec file; returns its JSON. */
char *httk_workflow_job_prepare(const char *destination, const char *spec_file, int *status);
/* Apply a replayable batch of workdir changes from a spec file; returns its id. */
char *httk_workflow_workdir_apply(const char *spec_file, int *status);

/* --- Utilities -------------------------------------------------------------- */

/*
 * Run an argv array under supervision, terminating its process group on timeout.
 * `args` is the whole tail the Bash function forwards: options, then `--`, then
 * the argv, for example {"--timeout", "3600", "--", "vasp", NULL}. Returns the
 * classified status (0, 22, 124, or 125). stdout is inherited.
 */
int httk_workflow_run(const char *const *args);
/* Evaluate one arithmetic expression without a shell; returns its value. */
char *httk_calc(const char *expression, int *status);
/* Render one template file with one JSON value document. */
int httk_template_render(const char *template_file, const char *output, const char *values_file);
/* Compress named files; `args` carries --method/--remove-source and the paths. */
int httk_compress(const char *const *args);
/* Decompress named files; `args` carries --remove-source and the paths. */
int httk_decompress(const char *const *args);

#ifdef __cplusplus
}
#endif

#endif /* HTTK_WORKFLOW_H */
