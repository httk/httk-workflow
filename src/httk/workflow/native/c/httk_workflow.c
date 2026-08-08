/*
 * Native httk-workflow C authoring SDK. See httk_workflow.h for the contract.
 *
 * This is a bridge client: every verb execs
 * `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge <verb> ...` and reports
 * what it did, so a C runner publishes the same protocol bytes as a Python or
 * Bash runner. Only `--describe` is native. The one piece of state the library
 * keeps is the registration table, mirroring the Bash SDK's private globals.
 *
 * C99 + POSIX only; no dependency beyond libc.
 */

#define _POSIX_C_SOURCE 200809L

#include "httk_workflow.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

/* The declared step set of this process; the only state the library keeps. */
static const char *g_workflow = NULL;
static const httk_workflow_step *g_steps = NULL;
static size_t g_step_count = 0;

/* ------------------------------------------------------------------------- */
/* The bridge exec: fork/exec the Python bridge with argv built safely (no    */
/* shell), capturing stdout when a caller wants a value back.                 */
/* ------------------------------------------------------------------------- */

static char *capture(int fd) {
    size_t cap = 256, len = 0;
    char *buf = malloc(cap);
    if (buf == NULL) {
        return NULL;
    }
    for (;;) {
        char chunk[4096];
        ssize_t got = read(fd, chunk, sizeof chunk);
        if (got < 0) {
            if (errno == EINTR) {
                continue; /* a delivered signal is not end of output */
            }
            free(buf);
            return NULL;
        }
        if (got == 0) {
            break;
        }
        if (len + (size_t)got + 1 > cap) {
            while (len + (size_t)got + 1 > cap) {
                cap *= 2;
            }
            char *grown = realloc(buf, cap);
            if (grown == NULL) {
                free(buf);
                return NULL;
            }
            buf = grown;
        }
        memcpy(buf + len, chunk, (size_t)got);
        len += (size_t)got;
    }
    /* Strip trailing newlines, as command substitution does in a shell. */
    while (len > 0 && buf[len - 1] == '\n') {
        len--;
    }
    buf[len] = '\0';
    return buf;
}

int httk_workflow_invoke(char **out, const char *const *argv) {
    const char *python = getenv("HTTK_WORKFLOW_PYTHON");
    if (python == NULL || *python == '\0') {
        fputs("httk-workflow: HTTK_WORKFLOW_PYTHON is not set by the workflow manager\n", stderr);
        return HTTK_WORKFLOW_REFUSED;
    }

    size_t count = 0;
    while (argv[count] != NULL) {
        count++;
    }
    /* python, -m, module, argv..., NULL */
    char **command = malloc((count + 4) * sizeof *command);
    if (command == NULL) {
        return HTTK_WORKFLOW_REFUSED;
    }
    command[0] = (char *)python;
    command[1] = (char *)"-m";
    command[2] = (char *)"httk.workflow._shell_bridge";
    for (size_t i = 0; i < count; i++) {
        command[3 + i] = (char *)argv[i];
    }
    command[3 + count] = NULL;

    int pipe_fd[2] = {-1, -1};
    if (out != NULL && pipe(pipe_fd) != 0) {
        free(command);
        return HTTK_WORKFLOW_REFUSED;
    }

    pid_t pid = fork();
    if (pid < 0) {
        if (out != NULL) {
            close(pipe_fd[0]);
            close(pipe_fd[1]);
        }
        free(command);
        return HTTK_WORKFLOW_REFUSED;
    }
    if (pid == 0) {
        if (out != NULL) {
            close(pipe_fd[0]);
            if (dup2(pipe_fd[1], STDOUT_FILENO) < 0) {
                _exit(127);
            }
            close(pipe_fd[1]);
        }
        execvp(command[0], command);
        _exit(127);
    }

    char *captured = NULL;
    if (out != NULL) {
        close(pipe_fd[1]);
        captured = capture(pipe_fd[0]);
        close(pipe_fd[0]);
    }

    int wait_status = 0;
    int reaped = 0;
    while (waitpid(pid, &wait_status, 0) < 0) {
        if (errno != EINTR) {
            /* SIGCHLD SIG_IGN reaps the child for us (ECHILD): the command ran,
             * but its status is gone, so do not spin. */
            reaped = -1;
            break;
        }
    }
    free(command);

    int status;
    if (reaped < 0) {
        status = HTTK_WORKFLOW_REFUSED;
    } else if (WIFEXITED(wait_status)) {
        status = WEXITSTATUS(wait_status);
    } else {
        status = HTTK_WORKFLOW_REFUSED;
    }

    if (out != NULL) {
        *out = captured != NULL ? captured : calloc(1, 1);
    }
    return status;
}

/* Build one bridge argv from a NULL-terminated prefix and optional tail. */
static int call(char **out, const char *const *prefix, const char *const *tail) {
    size_t pn = 0, tn = 0;
    while (prefix[pn] != NULL) {
        pn++;
    }
    if (tail != NULL) {
        while (tail[tn] != NULL) {
            tn++;
        }
    }
    const char **argv = malloc((pn + tn + 1) * sizeof *argv);
    if (argv == NULL) {
        if (out != NULL) {
            *out = NULL;
        }
        return HTTK_WORKFLOW_REFUSED;
    }
    size_t i = 0;
    for (size_t j = 0; j < pn; j++) {
        argv[i++] = prefix[j];
    }
    for (size_t j = 0; j < tn; j++) {
        argv[i++] = tail[j];
    }
    argv[i] = NULL;
    int status = httk_workflow_invoke(out, argv);
    free(argv);
    return status;
}

/* A read verb: capture stdout, and return NULL (freeing it) unless it succeeded. */
static char *read_value(const char *const *prefix, const char *const *tail, int *status) {
    char *out = NULL;
    int rc = call(&out, prefix, tail);
    if (status != NULL) {
        *status = rc;
    }
    if (rc != HTTK_WORKFLOW_OK) {
        free(out);
        return NULL;
    }
    return out;
}

/* ------------------------------------------------------------------------- */
/* Registration, description, and dispatch.                                   */
/* ------------------------------------------------------------------------- */

static int valid_step_name(const char *name) {
    if (name == NULL || *name == '\0') {
        return 0;
    }
    for (const char *p = name; *p != '\0'; p++) {
        char c = *p;
        int ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' ||
                 c == '_' || c == '-';
        if (!ok) {
            return 0;
        }
    }
    return 1;
}

static int compare_names(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

void httk_workflow_describe(void) {
    fputs("{\"format\": \"httk-workflow-runner-description\", \"format_version\": 1, \"steps\": [", stdout);
    if (g_step_count > 0) {
        const char **names = malloc(g_step_count * sizeof *names);
        if (names != NULL) {
            for (size_t i = 0; i < g_step_count; i++) {
                names[i] = g_steps[i].name;
            }
            qsort(names, g_step_count, sizeof *names, compare_names);
            for (size_t i = 0; i < g_step_count; i++) {
                if (i > 0) {
                    fputs(", ", stdout);
                }
                putchar('"');
                fputs(names[i], stdout);
                putchar('"');
            }
            free(names);
        }
    }
    printf("], \"workflow\": \"%s\"}\n", g_workflow != NULL ? g_workflow : "");
}

int httk_workflow_runner(const char *workflow_id, const httk_workflow_step *steps, size_t count) {
    if (workflow_id == NULL || *workflow_id == '\0' || steps == NULL || count == 0) {
        fputs("httk-workflow: httk_workflow_runner needs a workflow name and at least one step name\n", stderr);
        return HTTK_WORKFLOW_REFUSED;
    }
    /* The workflow id is emitted verbatim into the description JSON, so it obeys
     * the same charset as a step name: a stray quote would print invalid JSON. */
    if (!valid_step_name(workflow_id)) {
        fprintf(stderr, "httk-workflow: workflow name %s cannot name a runner\n", workflow_id);
        return HTTK_WORKFLOW_REFUSED;
    }
    for (size_t i = 0; i < count; i++) {
        if (!valid_step_name(steps[i].name)) {
            fprintf(stderr, "httk-workflow: step name %s cannot name a C step handler\n",
                    steps[i].name != NULL ? steps[i].name : "");
            return HTTK_WORKFLOW_REFUSED;
        }
        if (steps[i].handler == NULL) {
            fprintf(stderr, "httk-workflow: the %s runner declares step %s but registers no handler\n", workflow_id,
                    steps[i].name);
            return HTTK_WORKFLOW_REFUSED;
        }
        for (size_t j = 0; j < i; j++) {
            if (strcmp(steps[i].name, steps[j].name) == 0) {
                fprintf(stderr, "httk-workflow: step %s is already registered on the %s runner\n", steps[i].name,
                        workflow_id);
                return HTTK_WORKFLOW_REFUSED;
            }
        }
    }

    g_workflow = workflow_id;
    g_steps = steps;
    g_step_count = count;

    setenv("HTTK_WORKFLOW_RUNNER_WORKFLOW", workflow_id, 1);
    /* Newline-joined in declaration order: what the Python bridge splits to
     * reconstruct the step set an outcome is checked against. */
    size_t total = 0;
    for (size_t i = 0; i < count; i++) {
        total += strlen(steps[i].name) + 1;
    }
    char *joined = malloc(total + 1);
    if (joined == NULL) {
        /* Without the exported step set the bridge cannot reconstruct the runner,
         * so every outcome would check against nothing: refuse rather than lie. */
        return HTTK_WORKFLOW_REFUSED;
    }
    size_t at = 0;
    for (size_t i = 0; i < count; i++) {
        if (i > 0) {
            joined[at++] = '\n';
        }
        size_t n = strlen(steps[i].name);
        memcpy(joined + at, steps[i].name, n);
        at += n;
    }
    joined[at] = '\0';
    setenv("HTTK_WORKFLOW_RUNNER_STEPS", joined, 1);
    free(joined);

    const char *describe = getenv("HTTK_WORKFLOW_DESCRIBE");
    if (describe != NULL && strcmp(describe, "1") == 0) {
        httk_workflow_describe();
        exit(0);
    }
    return HTTK_WORKFLOW_OK;
}

static int outcome_published(void) {
    const char *control = getenv("HTTK_WORKFLOW_CONTROL_DIR");
    if (control == NULL || *control == '\0') {
        control = ".";
    }
    size_t need = strlen(control) + strlen("/outcome.ready") + 1;
    char *path = malloc(need);
    if (path == NULL) {
        return 0;
    }
    snprintf(path, need, "%s/outcome.ready", control);
    struct stat info;
    int published = stat(path, &info) == 0 && S_ISDIR(info.st_mode);
    free(path);
    return published;
}

static int invoke_simple(const char *verb) {
    const char *argv[] = {verb, NULL};
    return httk_workflow_invoke(NULL, argv);
}

int httk_workflow_main(int argc, char **argv) {
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--describe") == 0) {
            httk_workflow_describe();
            return 0;
        }
    }
    if (g_workflow == NULL) {
        fputs("httk-workflow: call httk_workflow_runner WORKFLOW STEP... before httk_workflow_main\n", stderr);
        return HTTK_WORKFLOW_REFUSED;
    }

    char *step = NULL;
    const char *begin[] = {"begin", NULL};
    if (httk_workflow_invoke(&step, begin) != HTTK_WORKFLOW_OK || step == NULL) {
        /* step == NULL only on a capture allocation failure; treat it as refused
         * rather than dereference it in the dispatch loop below. */
        free(step);
        return HTTK_WORKFLOW_REFUSED;
    }
    setenv("HTTK_WORKFLOW_STEP", step, 1);

    /* The environment gate may already have published a fail outcome. */
    if (outcome_published()) {
        free(step);
        return 0;
    }

    httk_workflow_step_fn handler = NULL;
    for (size_t i = 0; i < g_step_count; i++) {
        if (strcmp(g_steps[i].name, step) == 0) {
            handler = g_steps[i].handler;
            break;
        }
    }
    if (handler == NULL) {
        int rc = invoke_simple("fail-unknown-step");
        free(step);
        return rc == HTTK_WORKFLOW_OK ? 0 : HTTK_WORKFLOW_REFUSED;
    }

    int code = handler();
    if (code != 0) {
        /* An aborted handler discards its unpublished draft and leaves a
         * breadcrumb, the way a Python traceback names the failing line. */
        size_t need = strlen(step) + 48;
        char *message = malloc(need);
        if (message != NULL) {
            snprintf(message, need, "%s exited with status %d", step, code);
            const char *abort_argv[] = {"abort", "--exception", "CError", "--message", message, NULL};
            httk_workflow_invoke(NULL, abort_argv);
            free(message);
        }
        free(step);
        return code;
    }

    if (!outcome_published()) {
        if (invoke_simple("fail-no-outcome") != HTTK_WORKFLOW_OK) {
            free(step);
            return HTTK_WORKFLOW_REFUSED;
        }
    }
    if (invoke_simple("environment-log") != HTTK_WORKFLOW_OK) {
        free(step);
        return HTTK_WORKFLOW_REFUSED;
    }
    free(step);
    return 0;
}

/* ------------------------------------------------------------------------- */
/* What a step reads.                                                         */
/* ------------------------------------------------------------------------- */

char *httk_workflow_context(const char *field, int *status) {
    if (field != NULL) {
        const char *prefix[] = {"context", field, NULL};
        return read_value(prefix, NULL, status);
    }
    const char *prefix[] = {"context", NULL};
    return read_value(prefix, NULL, status);
}

static char *read_named(const char *verb, const char *name, const char *fallback, int *status) {
    const char *prefix[] = {verb, name, NULL};
    const char *tail[] = {"--default", fallback, NULL};
    return read_value(prefix, fallback != NULL ? tail : NULL, status);
}

char *httk_workflow_parameter(const char *name, const char *fallback, int *status) {
    return read_named("parameter", name, fallback, status);
}

char *httk_workflow_setting(const char *name, const char *fallback, int *status) {
    return read_named("setting", name, fallback, status);
}

char *httk_workflow_environment(const char *name, const char *fallback, int *status) {
    return read_named("environment", name, fallback, status);
}

char *httk_workflow_state_get(const char *name, int *status) {
    const char *prefix[] = {"state-get", name, NULL};
    return read_value(prefix, NULL, status);
}

char *httk_workflow_declaration(const char *name, int *status) {
    const char *prefix[] = {"declaration", name, NULL};
    return read_value(prefix, NULL, status);
}

char *httk_workflow_children(const char *selection, int *status) {
    const char *prefix[] = {"children", NULL};
    const char *tail[] = {selection, NULL};
    return read_value(prefix, selection != NULL ? tail : NULL, status);
}

char *httk_workflow_child(const char *label, const char *field, int *status) {
    const char *prefix[] = {"child", label, field, NULL};
    return read_value(prefix, NULL, status);
}

/* ------------------------------------------------------------------------- */
/* Job state.                                                                 */
/* ------------------------------------------------------------------------- */

int httk_workflow_state_set(const char *name, const char *value) {
    const char *prefix[] = {"state-set", name, value, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_state_delete(const char *name) {
    const char *prefix[] = {"state-delete", name, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_state_merge(const char *const *assignments) {
    const char *prefix[] = {"state-merge", NULL};
    return call(NULL, prefix, assignments);
}

/* ------------------------------------------------------------------------- */
/* Declarations and the run log.                                              */
/* ------------------------------------------------------------------------- */

int httk_workflow_declare(const char *name, const char *document_file) {
    const char *prefix[] = {"declare", name, document_file, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_runlog_note(const char *message) {
    const char *prefix[] = {"runlog", "note", message, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_runlog_headline(const char *message) {
    const char *prefix[] = {"runlog", "headline", message, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_runlog_append(const char *message, const char *const *files) {
    const char *prefix[] = {"runlog", "files", message, NULL};
    return call(NULL, prefix, files);
}

int httk_workflow_log(const char *level, const char *message) {
    time_t now = time(NULL);
    struct tm utc;
    char stamp[32] = "";
    if (gmtime_r(&now, &utc) != NULL) {
        strftime(stamp, sizeof stamp, "%Y-%m-%dT%H:%M:%SZ", &utc);
    }
    fprintf(stderr, "%s [%s] %s\n", stamp, level != NULL ? level : "", message != NULL ? message : "");
    return HTTK_WORKFLOW_OK;
}

/* ------------------------------------------------------------------------- */
/* Transactional data.                                                        */
/* ------------------------------------------------------------------------- */

char *httk_workflow_put(const char *source, const char *destination, int *status) {
    const char *prefix[] = {"put", source, destination, NULL};
    return read_value(prefix, NULL, status);
}

char *httk_workflow_remove(const char *destination, int missing_ok, int *status) {
    const char *prefix[] = {"remove", destination, NULL};
    const char *tail[] = {"--missing-ok", NULL};
    return read_value(prefix, missing_ok ? tail : NULL, status);
}

/* ------------------------------------------------------------------------- */
/* Children, and what a step publishes.                                       */
/* ------------------------------------------------------------------------- */

char *httk_workflow_spawn(const char *label, const char *const *args, int *status) {
    const char *prefix[] = {"spawn", label, NULL};
    return read_value(prefix, args, status);
}

int httk_workflow_advance(const char *next_step, const char *const *args) {
    const char *prefix[] = {"advance", next_step, NULL};
    return call(NULL, prefix, args);
}

int httk_workflow_gather(const char *next_step, const char *const *args) {
    const char *prefix[] = {"gather", next_step, NULL};
    return call(NULL, prefix, args);
}

int httk_workflow_succeed(void) {
    const char *prefix[] = {"succeed", NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_fail(const char *code, const char *message, const char *const *args) {
    const char *prefix[] = {"fail", code, message, NULL};
    return call(NULL, prefix, args);
}

int httk_workflow_retry(const char *reason) {
    const char *prefix[] = {"retry", reason, NULL};
    return call(NULL, prefix, NULL);
}

int httk_workflow_pause(const char *reason) {
    const char *prefix[] = {"pause", reason, NULL};
    return call(NULL, prefix, NULL);
}

/* ------------------------------------------------------------------------- */
/* Many calls, one interpreter; payload and workdir; utilities.               */
/* ------------------------------------------------------------------------- */

int httk_workflow_batch(void) {
    const char *argv[] = {"batch", NULL};
    /* out == NULL leaves stdin and stdout inherited, so the batch reads its
     * commands from this process's stdin and streams normally. */
    return httk_workflow_invoke(NULL, argv);
}

char *httk_workflow_job_prepare(const char *destination, const char *spec_file, int *status) {
    const char *prefix[] = {"job-prepare", destination, spec_file, NULL};
    return read_value(prefix, NULL, status);
}

char *httk_workflow_workdir_apply(const char *spec_file, int *status) {
    const char *prefix[] = {"workdir-apply", spec_file, NULL};
    return read_value(prefix, NULL, status);
}

int httk_workflow_run(const char *const *args) {
    const char *prefix[] = {"run", NULL};
    return call(NULL, prefix, args);
}

char *httk_calc(const char *expression, int *status) {
    const char *prefix[] = {"calc", expression, NULL};
    return read_value(prefix, NULL, status);
}

int httk_template_render(const char *template_file, const char *output, const char *values_file) {
    const char *prefix[] = {"template", template_file, output, values_file, NULL};
    return call(NULL, prefix, NULL);
}

int httk_compress(const char *const *args) {
    const char *prefix[] = {"compress", NULL};
    return call(NULL, prefix, args);
}

int httk_decompress(const char *const *args) {
    const char *prefix[] = {"decompress", NULL};
    return call(NULL, prefix, args);
}
