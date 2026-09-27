#!/usr/bin/env bash

# Native httk VASP Bash API, version 2. Source httk-workflow.sh first.
#
# Every function is one bridge subcommand, and every option of that subcommand is
# available here: the arguments are passed through untouched. Version 2 keeps that
# surface and changes three defaults, in both languages at once:
#
#   * k-point centering starts at Monkhorst-Pack, because the reviewed remedy
#     ladder promotes Gamma as the fix for two k-point failure classes;
#   * httk_vasp_preclean keeps CONTCAR and vasp-run-report.json, which are what a
#     remedy and a restart read; pass --also-remove NAME to delete them anyway;
#   * httk_vasp_rattle_poscar requires --seed or --entropy, because an unseeded
#     retry rattles a structure exactly like the attempt before it.
#
# httk_vasp_remedy_plan and httk_vasp_remedy_apply record the escalation ladder in
# the job state directory by default, so a job with an isolated workdir keeps
# climbing it; httk_vasp_remedy_plan prints the diagnosed problem on stdout and
# takes --directory (the calculation it validates the remedy against) and --policy.
HTTK_VASP_BASH_API_VERSION=2

_httk_vasp_require_workflow_api() {
    if ! declare -F _httk_workflow_bridge >/dev/null 2>&1; then
        printf 'httk-workflow: source HTTK_WORKFLOW_BASH_API before HTTK_WORKFLOW_VASP_BASH_API\n' >&2
        return 2
    fi
}

httk_vasp_prepare() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-prepare "$@"
}

httk_vasp_get_tag() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-get-tag "$@"
}

httk_vasp_set_tag() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-set-tag "$@"
}

httk_vasp_prepare_kpoints() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-kpoints "$@"
}

httk_vasp_prepare_potcar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-potcar "$@"
}

httk_vasp_nbands() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-nbands "$@"
}

httk_vasp_energy() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-energy "$@"
}

httk_vasp_volume() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-volume "$@"
}

httk_vasp_potim() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-potim "$@"
}

httk_vasp_plane_waves() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-plane-waves "$@"
}

httk_vasp_promote_contcar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-promote-contcar "$@"
}

httk_vasp_potcar_summary() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-potcar-summary "$@"
}

httk_vasp_clean_outcar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-clean-outcar "$@"
}

httk_vasp_preclean() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-preclean "$@"
}

httk_vasp_normalize_poscar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-normalize-poscar "$@"
}

httk_vasp_scale_poscar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-scale-poscar "$@"
}

httk_vasp_rattle_poscar() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-rattle-poscar "$@"
}

httk_vasp_run() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-run "$@"
}

httk_vasp_diagnose() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-diagnose "$@"
}

httk_vasp_remedy_plan() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-remedy-plan "$@"
}

httk_vasp_remedy_apply() {
    _httk_vasp_require_workflow_api || return
    _httk_workflow_bridge vasp-remedy-apply "$@"
}
