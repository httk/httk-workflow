#!/usr/bin/env bash
# The campaign of examples/defect_campaign.py, authored in Bash.
#
# The same four steps, the same job inputs, the same job state, the same failure
# codes, and the same published outcomes: one workflow protocol, two authoring
# SDKs. Every child job runs this same file at the "relax" step.
#
#     httk-taskmanager init campaign-workspace --extension transactional-data-v1
#     httk workflow job new campaign-workspace \
#         --template examples/defect_campaign.sh --step characterize \
#         --input sites=3 --input diverging=1 --tag campaign
#     httk-taskmanager run campaign-workspace --until-idle
#
# Job inputs:
#   sites      -- how many sites to relax, one child job each
#   diverging  -- a comma-separated list of site numbers that fail (default none)
set -euo pipefail

# The manager exports HTTK_WORKFLOW_BASH_API; sourcing it is what makes the
# httk_workflow_* functions of the native Bash API available.
source "$HTTK_WORKFLOW_BASH_API"

# The workflow name and every step this runner implements, declared before any
# work happens, exactly as Runner("...") and @run.step do in Python.
httk_workflow_runner examples.defects characterize relax aggregate triage

# Spawn one child job per site and wait for every one of them.
step_characterize() {
    local sites diverging site diverge
    sites=$(httk_workflow_input sites)
    diverging=$(httk_workflow_input diverging '')
    httk_workflow_state_set sites "$sites"
    site=0
    while [ "$site" -lt "$sites" ]; do
        diverge=false
        case ",$diverging," in
            *",$site,"*) diverge=true ;;
        esac
        httk_workflow_spawn "site-$site" \
            --step relax \
            --input site="$site" \
            --input diverge="$diverge" \
            --data-mode transactional \
            --max-attempts-per-activation 1 \
            --placement project/children >/dev/null
        site=$((site + 1))
    done
    httk_workflow_gather aggregate --when all_terminal --on-impossible triage
}

# Relax one site: the step every spawned child runs.
step_relax() {
    local site
    site=$(httk_workflow_input site)
    printf '%s\n' "$site" >site.txt
    if [ "$(httk_workflow_input diverge)" = true ]; then
        httk_workflow_fail relax.diverged "site $site did not relax"
    else
        httk_workflow_put site.txt results/site.txt >/dev/null
        httk_workflow_succeed
    fi
}

# Report the children that succeeded, and triage the campaign if any failed.
step_aggregate() {
    local label state key workdir data failed=
    : >report.tsv
    while IFS=$'\t' read -r label state key workdir data; do
        printf '%s\t%s\n' "$label" "$(cat "$workdir/site.txt")" >>report.tsv
    done < <(httk_workflow_children --succeeded)
    while IFS=$'\t' read -r label state key workdir data; do
        if [ -n "$failed" ]; then
            failed="$failed,$label"
        else
            failed=$label
        fi
    done < <(httk_workflow_children --failed)
    if [ -n "$failed" ]; then
        httk_workflow_advance triage --state failed="$failed"
    else
        httk_workflow_succeed
    fi
}

# Record which children failed and end the campaign by name.
step_triage() {
    local failed
    failed=$(httk_workflow_state_get failed || true)
    printf '%s\n' "$failed" >triage.txt
    httk_workflow_runlog_note "triaged after ${failed:-no} failing children"
    httk_workflow_fail defects.child_failed "failed: $failed"
}

# Dispatch the step the manager asked for, and publish exactly one outcome.
httk_workflow_main
