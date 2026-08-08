#!/usr/bin/env bash

# Native httk workflow Bash API, version 2. This file is sourced, not executed.
#
# A Bash runner declares its workflow and its steps once, implements one
# step_<name> function per declared step, and ends with httk_workflow_main:
#
#     source "$HTTK_WORKFLOW_BASH_API"
#     httk_workflow_runner defects characterize relax aggregate triage
#     step_characterize() { ...; httk_workflow_gather aggregate; }
#     ...
#     httk_workflow_main
#
# The outcome functions return rather than exit: httk_workflow_main owns the
# process exit status, because it is what turns every ending of a step into
# exactly one published outcome.
HTTK_WORKFLOW_BASH_API_VERSION=2

# The declared step set of this process, the handler being dispatched, and the
# file the step subshell records its failing command in. All three are private.
_httk_workflow_steps=()
_httk_workflow_step_name=
_httk_workflow_trace_file=

_httk_workflow_bridge() {
    if [ -z "${HTTK_WORKFLOW_PYTHON:-}" ]; then
        printf 'httk-workflow: HTTK_WORKFLOW_PYTHON is not set by the workflow manager\n' >&2
        return 2
    fi
    "$HTTK_WORKFLOW_PYTHON" -m httk.workflow._shell_bridge "$@"
}

# The machine-readable description of this runner, byte for byte what a Python
# runner prints for the same workflow and step set. It is produced here, in the
# shell, so describing a runner needs no interpreter and touches nothing.
_httk_workflow_describe() {
    local item first=1
    printf '{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ['
    if [ "${#_httk_workflow_steps[@]}" -gt 0 ]; then
        while IFS= read -r item; do
            if [ -z "$item" ]; then
                continue
            fi
            if [ "$first" -eq 1 ]; then
                first=0
            else
                printf ', '
            fi
            printf '"%s"' "$item"
        done < <(printf '%s\n' "${_httk_workflow_steps[@]}" | LC_ALL=C sort)
    fi
    printf '], "workflow": "%s"}\n' "${HTTK_WORKFLOW_RUNNER_WORKFLOW:-}"
}

_httk_workflow_has_step() {
    local item
    for item in ${_httk_workflow_steps[@]:+"${_httk_workflow_steps[@]}"}; do
        if [ "$item" = "$1" ]; then
            return 0
        fi
    done
    return 1
}

# Declare the workflow and the complete step set of this runner. Registration is
# complete before any step runs, which is what lets every step name a step
# publishes be checked against the steps that really exist.
httk_workflow_runner() {
    if [ "$#" -lt 2 ]; then
        printf 'httk-workflow: httk_workflow_runner needs a workflow name and at least one step name\n' >&2
        return 2
    fi
    HTTK_WORKFLOW_RUNNER_WORKFLOW=$1
    shift
    local outer inner index=0 position
    for outer in "$@"; do
        case $outer in
            '' | *[!A-Za-z0-9._-]*)
                printf 'httk-workflow: step name %s cannot name a Bash step_ function\n' "$outer" >&2
                return 2
                ;;
        esac
        index=$((index + 1))
        position=0
        for inner in "$@"; do
            position=$((position + 1))
            if [ "$position" -gt "$index" ] && [ "$inner" = "$outer" ]; then
                printf 'httk-workflow: step %s is already registered on the %s runner\n' \
                    "$outer" "$HTTK_WORKFLOW_RUNNER_WORKFLOW" >&2
                return 2
            fi
        done
    done
    _httk_workflow_steps=("$@")
    HTTK_WORKFLOW_RUNNER_STEPS=$(printf '%s\n' "$@")
    export HTTK_WORKFLOW_RUNNER_WORKFLOW HTTK_WORKFLOW_RUNNER_STEPS
    if [ "${HTTK_WORKFLOW_DESCRIBE:-}" = 1 ]; then
        _httk_workflow_describe
        exit 0
    fi
}

# Every declared step must have a handler, and every handler must be declared:
# an undeclared step_ function would never be dispatched, and its step name
# would never be checked.
_httk_workflow_check_registration() {
    local item name
    for item in ${_httk_workflow_steps[@]:+"${_httk_workflow_steps[@]}"}; do
        if ! declare -F "step_$item" >/dev/null 2>&1; then
            printf 'httk-workflow: the %s runner declares step %s but defines no step_%s function\n' \
                "$HTTK_WORKFLOW_RUNNER_WORKFLOW" "$item" "$item" >&2
            return 2
        fi
    done
    while read -r _ _ name; do
        case $name in
            step_*) ;;
            *) continue ;;
        esac
        if ! _httk_workflow_has_step "${name#step_}"; then
            printf 'httk-workflow: the %s runner defines %s but does not declare step %s\n' \
                "$HTTK_WORKFLOW_RUNNER_WORKFLOW" "$name" "${name#step_}" >&2
            return 2
        fi
    done < <(declare -F)
    return 0
}

# Record the command a handler died on, so the breadcrumb of an aborted attempt
# names it the way a Python traceback names the failing line.
_httk_workflow_trace() {
    if [ -n "$_httk_workflow_trace_file" ]; then
        printf '%s:%s: %s\n' "${BASH_SOURCE[1]:-?}" "${BASH_LINENO[0]:-?}" "${BASH_COMMAND:-?}" \
            >>"$_httk_workflow_trace_file" 2>/dev/null || true
    fi
}

# Turn the ending of one handler into exactly one outcome. This runs inside the
# step subshell, on every way out of it, which is what makes an abort under
# `set -e` reportable: the subshell dies at the failing command, and its very
# last act is to discard the unpublished draft and leave the breadcrumb.
_httk_workflow_step_exit() {
    local code=$?
    trap - EXIT
    if [ "$code" -ne 0 ]; then
        _httk_workflow_bridge abort \
            --exception ShellError \
            --message "$_httk_workflow_step_name exited with status $code" \
            --traceback-file "$_httk_workflow_trace_file" || true
        exit "$code"
    fi
    if [ ! -d "${HTTK_WORKFLOW_CONTROL_DIR:-.}/outcome.ready" ]; then
        _httk_workflow_bridge fail-no-outcome || exit 2
    fi
    _httk_workflow_bridge environment-log || exit 2
    exit 0
}

# Run the step this attempt was launched for. The handler runs in a subshell
# whose exit is the single place every ending is turned into an outcome; nothing
# it composed lives in shell state, so the subshell costs it nothing.
httk_workflow_main() {
    local argument
    for argument in "$@"; do
        if [ "$argument" = "--describe" ]; then
            _httk_workflow_describe
            return 0
        fi
    done
    if [ -z "${HTTK_WORKFLOW_RUNNER_WORKFLOW:-}" ]; then
        printf 'httk-workflow: call httk_workflow_runner WORKFLOW STEP... before httk_workflow_main\n' >&2
        return 2
    fi
    _httk_workflow_check_registration || return 2
    local step
    step=$(_httk_workflow_bridge begin) || return 2
    HTTK_WORKFLOW_STEP=$step
    export HTTK_WORKFLOW_STEP
    if [ -d "${HTTK_WORKFLOW_CONTROL_DIR:-.}/outcome.ready" ]; then
        return 0
    fi
    if ! _httk_workflow_has_step "$step"; then
        _httk_workflow_bridge fail-unknown-step || return 2
        return 0
    fi
    _httk_workflow_step_name=step_$step
    _httk_workflow_trace_file=${HTTK_WORKFLOW_CONTROL_DIR:-.}/runner-trace.log
    rm -f "$_httk_workflow_trace_file"
    (
        trap '_httk_workflow_step_exit' EXIT
        set -E
        trap '_httk_workflow_trace' ERR
        "$_httk_workflow_step_name"
    )
}

httk_workflow_context() {
    _httk_workflow_bridge context "$@"
}

httk_workflow_parameter() {
    if [ "$#" -ge 2 ]; then
        _httk_workflow_bridge parameter "$1" --default "$2"
    else
        _httk_workflow_bridge parameter "$1"
    fi
}

httk_workflow_setting() {
    if [ "$#" -ge 2 ]; then
        _httk_workflow_bridge setting "$1" --default "$2"
    else
        _httk_workflow_bridge setting "$1"
    fi
}

httk_workflow_environment() {
    if [ "$#" -ge 2 ]; then
        _httk_workflow_bridge environment "$1" --default "$2"
    else
        _httk_workflow_bridge environment "$1"
    fi
}

httk_workflow_state_get() {
    _httk_workflow_bridge state-get "$1"
}

httk_workflow_state_set() {
    _httk_workflow_bridge state-set "$1" "$2"
}

httk_workflow_state_delete() {
    _httk_workflow_bridge state-delete "$1"
}

httk_workflow_state_merge() {
    _httk_workflow_bridge state-merge "$@"
}

# Record the observed workflow declaration NAME from a JSON document file, and
# read one back. The document is carried verbatim; reading prints the observed
# document when the job wrote one, the declared one from job.json otherwise, and
# returns 1 when there is neither.
httk_workflow_declare() {
    _httk_workflow_bridge declare "$1" "$2"
}

httk_workflow_declaration() {
    _httk_workflow_bridge declaration "$1"
}

httk_workflow_log() {
    local level=$1
    shift
    printf '%s [%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$level" "$*" >&2
}

httk_workflow_runlog_note() {
    _httk_workflow_bridge runlog note "$1"
}

httk_workflow_runlog_headline() {
    _httk_workflow_bridge runlog headline "$1"
}

httk_workflow_runlog_append() {
    local message=$1
    shift
    _httk_workflow_bridge runlog files "$message" "$@"
}

httk_workflow_put() {
    _httk_workflow_bridge put "$1" "$2"
}

httk_workflow_remove() {
    _httk_workflow_bridge remove "$@"
}

httk_workflow_spawn() {
    _httk_workflow_bridge spawn "$@"
}

httk_workflow_children() {
    _httk_workflow_bridge children "$@"
}

httk_workflow_child() {
    _httk_workflow_bridge child "$1" "$2"
}

httk_workflow_advance() {
    _httk_workflow_bridge advance "$@"
}

httk_workflow_gather() {
    _httk_workflow_bridge gather "$@"
}

httk_workflow_succeed() {
    _httk_workflow_bridge succeed
}

httk_workflow_fail() {
    _httk_workflow_bridge fail "$@"
}

httk_workflow_retry() {
    _httk_workflow_bridge retry "$1"
}

httk_workflow_pause() {
    _httk_workflow_bridge pause "$1"
}

# Run several bridge commands, one per line, in one interpreter start.
httk_workflow_batch() {
    _httk_workflow_bridge batch
}

httk_workflow_job_prepare() {
    _httk_workflow_bridge job-prepare "$1" "$2"
}

httk_workflow_workdir_apply() {
    _httk_workflow_bridge workdir-apply "$1"
}

httk_workflow_run() {
    _httk_workflow_bridge run "$@"
}

httk_calc() {
    _httk_workflow_bridge calc "$1"
}

httk_template_render() {
    _httk_workflow_bridge template "$1" "$2" "$3"
}

httk_compress() {
    _httk_workflow_bridge compress "$@"
}

httk_decompress() {
    _httk_workflow_bridge decompress "$@"
}
