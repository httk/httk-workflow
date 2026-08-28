#!/usr/bin/env bash

# One VASP relaxation, authored in Bash: the same workflow as vasp_relax.py.
#
# The two runners implement one contract — the same workflow name, the same steps,
# the same job inputs and parameters, the same job state, the same failure codes, and the same
# files in the workdir and in the published data — because they call the same
# helpers through the Bash bridge. The job inputs and parameters are documented in
# httk.workflow.vasp.runners; a job may run either file and get the same result.
set -euo pipefail
source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_runner httk.vasp.relax-bash prepare run publish

_vasp_collect_default="INCAR KPOINTS OUTCAR CONTCAR OSZICAR vasprun.xml vasp-run-report.json POTCAR.provenance.json"

# Stage the payload inputs into the workdir and derive everything else. The
# preparation options are written beside the attempt, not into the workdir, so the
# workdir holds exactly the calculation.
step_prepare() {
    local poscar incar potcar library options density centering accuracy tags parallel_tag parallel_value
    poscar=$HTTK_WORKFLOW_JOB_DIR/$(httk_workflow_parameter poscar files/POSCAR)
    if [ ! -f "$poscar" ]; then
        printf '{"expected": "%s"}\n' "$poscar" >"$HTTK_WORKFLOW_CONTROL_DIR/vasp-input-missing.json"
        httk_workflow_fail vasp.input_missing \
            "the starting structure ${poscar##*/} is not in this payload" \
            --details "@$HTTK_WORKFLOW_CONTROL_DIR/vasp-input-missing.json"
        return
    fi
    cp "$poscar" POSCAR
    incar=$HTTK_WORKFLOW_JOB_DIR/$(httk_workflow_parameter incar files/INCAR)
    if [ -f "$incar" ]; then
        cp "$incar" INCAR
    else
        : >INCAR
    fi
    library=$(httk_workflow_parameter pseudopotential_library '')
    if [ "$library" = null ]; then
        library=
    fi
    potcar=$HTTK_WORKFLOW_JOB_DIR/$(httk_workflow_parameter potcar files/POTCAR)
    if [ -f "$potcar" ]; then
        cp "$potcar" POTCAR
        library=
    fi
    density=$(httk_workflow_parameter kpoint_density 20.0)
    centering=$(httk_workflow_parameter centering Monkhorst-Pack)
    accuracy=$(httk_workflow_parameter accuracy_per_atom 0.001)
    tags=$(httk_workflow_parameter incar_tags '{}')
    parallel_tag=$(httk_workflow_parameter parallel_tag '')
    parallel_value=$(httk_workflow_parameter parallel_value 0)
    options=$HTTK_WORKFLOW_CONTROL_DIR/vasp-options.json
    {
        printf '{"kpoint_density": %s, "centering": "%s", "accuracy_per_atom": %s, "incar_tags": %s' \
            "$density" "$centering" "$accuracy" "$tags"
        if [ -n "$library" ]; then
            printf ', "pseudopotential_library": "%s"' "$library"
        fi
        if [ -n "$parallel_tag" ] && [ "$parallel_tag" != null ]; then
            printf ', "parallel_tag": "%s", "parallel_value": %s' "$parallel_tag" "$parallel_value"
        fi
        printf '}\n'
    } >"$options"
    httk_vasp_prepare --directory . --options "$options" >/dev/null
    httk_workflow_runlog_note "prepared a $HTTK_WORKFLOW_RUNNER_WORKFLOW calculation"
    httk_workflow_advance run
}

# Run VASP once and publish what its classified result means: the next step when
# it completed, another attempt when a remedy was applied, and vasp.failed when
# the reviewed ladder has nothing left to try.
step_run() {
    local command timeout status classification energy applied maximum decision policy problem message amplitude
    command=$(httk_workflow_setting vasp.command "$(httk_workflow_parameter vasp_command '')")
    if [ -z "$command" ]; then
        httk_workflow_fail vasp.command_missing \
            "no VASP command is configured: set it with httk workspace settings set --key vasp.command --value '...' WORKSPACE, or set HTTK_VASP_COMMAND on the machine that runs this job, or give the job a vasp_command parameter"
        return
    fi
    timeout=$(httk_workflow_parameter timeout 86400)
    httk_vasp_preclean --directory . --keep WAVECAR --keep CHGCAR --keep CHG >/dev/null
    status=0
    # Deliberately unquoted: the resolved VASP command is one argv string, not one path.
    # shellcheck disable=SC2086
    httk_vasp_run --directory . --timeout "$timeout" --report vasp-run-report.json -- $command || status=$?
    case $status in
        0) classification=completed ;;
        20) classification=diagnosed_stop ;;
        21) classification=nonconverged ;;
        22) classification=process_failure ;;
        124) classification=timeout ;;
        *)
            httk_workflow_fail vasp.failed "httk_vasp_run could not run VASP at all (status $status)"
            return
            ;;
    esac
    energy=$(httk_vasp_energy OSZICAR || true)
    httk_workflow_runlog_note "VASP $classification"
    if [ "$classification" = completed ]; then
        if [ -n "$energy" ]; then
            httk_workflow_advance publish --state classification="$classification" --state energy="$energy"
        else
            httk_workflow_advance publish --state classification="$classification"
        fi
        return
    fi
    applied=$(httk_workflow_state_get remedies || echo 0)
    maximum=$(httk_workflow_parameter maximum_remedies 8)
    decision=$HTTK_WORKFLOW_CONTROL_DIR/vasp-remedy-decision.json
    policy=$(httk_workflow_parameter remedy_policy reviewed-v1)
    status=0
    problem=$(
        httk_vasp_remedy_plan vasp-run-report.json --directory . --policy "$policy" --output "$decision"
    ) || status=$?
    if [ "$status" -ne 0 ] && [ "$status" -ne 3 ]; then
        httk_workflow_fail vasp.failed "planning a VASP remedy failed with status $status"
        return
    fi
    if [ "$status" -eq 3 ] || [ "$applied" -ge "$maximum" ]; then
        if [ "$applied" -ge "$maximum" ]; then
            message="VASP $classification after $applied remedies"
        else
            message="VASP $classification with no remaining remedy"
        fi
        _vasp_merge_run_state "$classification" "$energy" ''
        httk_workflow_fail vasp.failed "$message" --details "@$decision"
        return
    fi
    httk_vasp_remedy_apply "$decision" --directory .
    amplitude=$(httk_workflow_parameter rattle_amplitude 0)
    if [ "$(httk_calc "$amplitude > 0")" = 1 ]; then
        # The entropy is the attempt itself, so the perturbation is reproducible
        # and no two attempts of this job rattle the same way.
        httk_vasp_rattle_poscar POSCAR --amplitude "$amplitude" \
            --entropy "$(httk_workflow_context job_key):$(httk_workflow_context attempt_ordinal)"
    fi
    _vasp_merge_run_state "$classification" "$energy" "$((applied + 1))"
    httk_workflow_runlog_note "applied a remedy for $problem"
    httk_workflow_retry "applied the $policy remedy for $problem"
}

# Publish the collected files, or leave them in the persistent workdir when this
# job has no transactional data.
step_publish() {
    local prefix name published=
    prefix=$(httk_workflow_parameter data_prefix vasp)
    for name in $(httk_workflow_parameter collect "$_vasp_collect_default"); do
        if [ ! -f "$name" ]; then
            continue
        fi
        if [ -n "${HTTK_WORKFLOW_DATA_DIR:-}" ]; then
            httk_workflow_put "$name" "$prefix/$name" >/dev/null
        fi
        if [ -n "$published" ]; then
            published="$published, $name"
        else
            published=$name
        fi
    done
    if [ -n "${HTTK_WORKFLOW_DATA_DIR:-}" ]; then
        httk_workflow_runlog_note "published to data/$prefix: ${published:-nothing}"
    else
        httk_workflow_runlog_note "kept in the workdir: ${published:-nothing}"
    fi
    httk_workflow_succeed
}

# Record exactly the run state the Python runner records: the classification, the
# energy when the run produced one, and the remedy count when one was applied.
_vasp_merge_run_state() {
    local assignments=(classification="$1")
    if [ -n "$2" ]; then
        assignments+=(energy="$2")
    fi
    if [ -n "$3" ]; then
        assignments+=(remedies="$3")
    fi
    httk_workflow_state_merge "${assignments[@]}"
}

httk_workflow_main
