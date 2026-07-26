#!/usr/bin/env bash

# Native httk workflow Bash API, version 1. This file is sourced, not executed.
HTTK_WORKFLOW_BASH_API_VERSION=1

_httk_workflow_bridge() {
    if [ -z "${HTTK_WORKFLOW_PYTHON:-}" ]; then
        printf '%s\n' "HTTK_WORKFLOW_PYTHON is not set by the workflow manager" >&2
        return 1
    fi
    "$HTTK_WORKFLOW_PYTHON" -m httk.workflow._shell_bridge "$@"
}

httk_workflow_init() {
    local step
    step=$(_httk_workflow_bridge init) || return
    HTTK_WORKFLOW_STEP=$step
    export HTTK_WORKFLOW_STEP
}

httk_workflow_context() {
    _httk_workflow_bridge context "$@"
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

httk_workflow_outcome_begin() {
    HTTK_WORKFLOW_OUTCOME_DRAFT=$(_httk_workflow_bridge outcome-begin) || return
    export HTTK_WORKFLOW_OUTCOME_DRAFT
    printf '%s\n' "$HTTK_WORKFLOW_OUTCOME_DRAFT"
}

_httk_workflow_require_draft() {
    if [ -z "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        printf '%s\n' "call httk_workflow_outcome_begin first" >&2
        return 1
    fi
}

httk_workflow_transaction_mkdir() {
    _httk_workflow_require_draft || return
    _httk_workflow_bridge tx-mkdir "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2"
}

httk_workflow_transaction_put_file() {
    _httk_workflow_require_draft || return
    _httk_workflow_bridge tx-put-file "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2" "$3"
}

httk_workflow_transaction_put_tree() {
    _httk_workflow_require_draft || return
    _httk_workflow_bridge tx-put-tree "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2" "$3"
}

httk_workflow_transaction_replace_tree() {
    _httk_workflow_require_draft || return
    _httk_workflow_bridge tx-replace-tree "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2" "$3"
}

httk_workflow_transaction_remove() {
    _httk_workflow_require_draft || return
    if [ "${3:-}" = "--missing-ok" ]; then
        _httk_workflow_bridge tx-remove "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2" --missing-ok
    else
        _httk_workflow_bridge tx-remove "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2"
    fi
}

httk_workflow_child_add() {
    _httk_workflow_require_draft || return
    _httk_workflow_bridge child-add "$HTTK_WORKFLOW_OUTCOME_DRAFT" "$1" "$2"
}

httk_workflow_job_prepare() {
    _httk_workflow_bridge job-prepare "$1" "$2"
}

httk_workflow_workdir_apply() {
    _httk_workflow_bridge workdir-apply "$1"
}

httk_workflow_advance() {
    local next_step=$1
    shift
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge advance "$next_step" "${draft_args[@]}" "$@" || return
    exit 0
}

httk_workflow_wait() {
    local next_step=$1
    shift
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge wait "$next_step" "${draft_args[@]}" "$@" || return
    exit 0
}

httk_workflow_succeed() {
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge succeed "${draft_args[@]}" || return
    exit 0
}

httk_workflow_fail() {
    local code=$1
    local message=$2
    shift 2
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge fail "$code" "$message" "${draft_args[@]}" "$@" || return
    exit 0
}

httk_workflow_retry() {
    local reason=$1
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge retry "$reason" "${draft_args[@]}" || return
    exit 0
}

httk_workflow_pause() {
    local reason=$1
    local draft_args=()
    if [ -n "${HTTK_WORKFLOW_OUTCOME_DRAFT:-}" ]; then
        draft_args=(--draft "$HTTK_WORKFLOW_OUTCOME_DRAFT")
    fi
    _httk_workflow_bridge pause "$reason" "${draft_args[@]}" || return
    exit 0
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
