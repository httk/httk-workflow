# Migrating an *httk* v1 workflow to *httk₂*

This guide takes an existing `ht_steps` or `ht_run` workflow from *httk* v1 to
*httk-workflow*. You can keep the workflow unchanged behind the compatibility
runner, migrate one job type at a time, or replace the legacy API completely
with the native *httk₂* Bash or Python API.

The central rule is:

> Migrate task definitions and newly instantiated task directories, not a live
> *httk* v1 task-manager queue.

The *httk₂* manager never claims or rewrites an existing *httk* v1 queue tree. Once a task
has been prepared and submitted to an *httk₂* workspace, its `job.json`, marker, and
journal are authoritative.

## 1. Choose a migration route

You do not need to migrate every workflow at once.

| Route | Workflow changes | Manager | Best use |
| --- | --- | --- | --- |
| Compatibility | None, normally | `httk-v1-taskmanager` | Establish an *httk₂* operational baseline quickly |
| Mixed | Per job type | Both managers on one workspace | Incremental migration with a direct fallback |
| Native Bash | Replace `HT_TASK_*` and `VASP_*` calls | `httk-taskmanager` | Preserve a shell-oriented workflow |
| Native Python | Replace the runner with Python calls | `httk-taskmanager` | New development and more structured logic |

Start with compatibility unless you already have tests that describe the
workflow's inputs, outputs, restart behavior, and child-task behavior. A
compatibility run gives you a useful reference result before semantics change.

## 2. Inventory the *httk* v1 workflow

Make a copy of an instantiated task directory and record:

- whether the entry point is `ht_steps` or `ht_run`;
- every sourced helper, especially
  `$HTTK_DIR/Execution/tasks/ht_tasks_api.sh` and
  `$HTTK_DIR/Execution/tasks/vasp/vasptools.sh`;
- use of `HT_TASK_ATOMIC_*`, `HT_TASK_CREATE`, `HT_TASK_SUBTASKS`,
  `HT_TASK_STORE_VAR`, controlled processes, checkers, templates, or
  compression;
- files that must survive a retry, such as `WAVECAR`, `CONTCAR`, or application
  checkpoints;
- files that are final results rather than attempt scratch;
- task-set, priority, timeout, retry-limit, and resource assumptions;
- any `ht.instantiate.py` imports from the old httk Python package;
- automatic VASP remedies on which the workflow depends;
- child tasks that may still be running independently.

Do not infer completion from a legacy task-directory suffix alone. Capture the
actual input files, logs, expected results, and exit behavior.

## 3. Run the workflow unchanged in an *httk₂* workspace

Initialize an *httk₂* workspace:

```console
httk workflow workspace init workflow-workspace
```

The standalone alias is equivalent:

```console
httk-taskmanager init workflow-workspace
```

Prepare an already instantiated *httk* v1 task without consuming the source:

```console
httk workflow v1 prepare legacy-task prepared-v1 \
  --tag silicon-relax \
  --set vasp \
  --priority 4 \
  --attempts 10
```

Submit the prepared payload:

```console
httk workflow job submit workflow-workspace prepared-v1 \
  --placement migration/reference/silicon-relax
```

Preparation and submission can also be combined:

```console
httk workflow v1 submit workflow-workspace legacy-task \
  --placement migration/reference/silicon-relax \
  --tag silicon-relax \
  --set vasp \
  --priority 4 \
  --attempts 10
```

Run only *httk* v1 compatibility jobs:

```console
httk workflow v1 run workflow-workspace \
  --set any \
  --workers 4 \
  --until-idle
```

The exact old source paths remain available below the compatibility
`HTTK_DIR`. An unchanged step may therefore continue to use:

```bash
source "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
source "$HTTK_DIR/Execution/tasks/vasp/vasptools.sh"
```

Inspect the result through the *httk₂* source of truth:

```console
httk workflow workspace status workflow-workspace --json
```

The compatibility runner preserves the persistent `ht.run.current/`
workdir, translates *httk* v1 decisions and dynamic subtasks, and completes
published *httk* v1 atomic sections after interruption. See
[*httk* v1 task compatibility](v1_compatibility.md) for the precise
compatibility boundary.

## 4. Migrate project, configuration, and computers separately

These imports do not migrate workflow code or task queues.

Import safe user configuration explicitly:

```console
httk workflow config import-v1
```

Create *httk₂* project metadata from a local `ht.project` without modifying it:

```console
httk workflow project import-v1 . --source ./ht.project
```

This imports safe metadata and public identities. It does not import private
keys or the *httk* v1 queue. Imported project metadata records
`legacy_queue_imported: false`. The project workspace is initialized for detached
transfer, not transactional data. Use persistent `data.mode: "none"` jobs
there, or initialize a separate transactional workspace before submission.

Recognized *httk* v1 computer definitions can be mapped explicitly:

```console
httk workflow computer import-v1 ~/.httk/computers/cluster-a \
  --name cluster-a
httk workflow computer configure cluster-a:default \
  --set workspace=/remote/path/to/workflow-workspace
```

Review every generated adapter before installation. Legacy shell executables
and credentials are not copied or executed by the importer.

## 5. Run compatibility and native jobs side by side

The *httk* v1 and native managers select different runner backends, so they can share
one *httk₂* workspace:

```console
# Terminal or service 1: compatibility jobs
httk-v1-taskmanager run workflow-workspace --set any --workers 2

# Terminal or service 2: native jobs
httk-taskmanager run workflow-workspace --pool vasp-native --workers 2
```

If the future native jobs will use transactional data, create this shared
workspace with `--extension transactional-data-v1` before submitting the
compatibility baseline. That extension cannot currently be added to an
existing workspace in place.

Give the first native version a new job UUID and preferably a distinct tag and
placement. Do not edit the immutable `job.json` of an already submitted job to
change its runner backend.

Migrate one representative task first. Compare it with the compatibility
reference before moving a larger batch.

## 6. Replace the *httk* v1 control flow with native Bash

A typical *httk* v1 runner looks like:

```bash
#!/usr/bin/env bash

source "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
source "$HTTK_DIR/Execution/tasks/vasp/vasptools.sh"
HT_TASK_INIT "$@"

case "$STEP" in
  prepare)
    VASP_PREPARE_CALC
    HT_TASK_NEXT run
    ;;
  run)
    VASP_PRECLEAN
    VASP_RUN_CONTROLLED 86400 vasp_std
    HT_TASK_NEXT collect
    ;;
  collect)
    HT_TASK_FINISHED
    ;;
esac
```

The native Bash equivalent sources paths supplied by the manager and publishes
structured outcomes:

```bash
#!/usr/bin/env bash
set -euo pipefail

source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_init

case "$HTTK_WORKFLOW_STEP" in
  prepare)
    for input in POSCAR INCAR; do
      if [ ! -e "$input" ]; then
        cp -- "$HTTK_WORKFLOW_JOB_DIR/files/$input" "$input"
      fi
    done
    httk_vasp_prepare \
      --options "$HTTK_WORKFLOW_JOB_DIR/files/vasp-options.json"
    httk_workflow_advance run
    ;;

  run)
    httk_vasp_preclean --keep WAVECAR
    if httk_vasp_run \
      --timeout 86400 \
      --report vasp-run-report.json \
      -- vasp_std; then
        httk_workflow_advance collect
    else
      status=$?
      if httk_vasp_remedy_plan \
        vasp-run-report.json \
        --output remedy.json; then
          httk_vasp_remedy_apply remedy.json
          httk_workflow_retry "reviewed VASP remedy applied"
      fi
      httk_workflow_fail vasp.failed \
        "VASP stopped with status $status"
    fi
    ;;

  collect)
    httk_workflow_outcome_begin >/dev/null
    httk_workflow_transaction_put_file \
      outcar "$PWD/OUTCAR" results/OUTCAR
    httk_workflow_transaction_put_file \
      oszicar "$PWD/OSZICAR" results/OSZICAR
    httk_workflow_succeed
    ;;
esac
```

The outcome functions publish exactly one decision and then exit the Bash
runner. Do not additionally return a legacy decision code or write
`ht.nextstep`.

The `collect` example uses transactional data. Its workspace must have been
created with that extension:

```console
httk-taskmanager init native-workspace \
  --extension transactional-data-v1
```

`transactional-data-v1` cannot currently be added to an existing workspace by an
in-place upgrade, so decide this when creating the native workspace. If the job
uses `data.mode: "none"`, omit the transaction and keep restartable working
files in its persistent workdir instead.

### Prepare the native payload

Put the runner and static inputs below one payload directory:

```console
mkdir -p native-job/files
cp run.sh vasp-options.json POSCAR INCAR native-job/files/
chmod +x native-job/files/run.sh
```

Create the immutable `job.json` through the Python builder:

```python
from httk.workflow import JobSpec, prepare_job_payload

prepare_job_payload(
    "native-job",
    JobSpec(
        name="silicon relaxation",
        workflow="example.vasp-relax",
        runner_path="files/run.sh",
        initial_step="prepare",
        tag="silicon-relax",
        workdir_mode="persistent",
        data_mode="transactional",
        priority=700,
        claim_pool="vasp-native",
        maximum_attempts_per_activation=5,
        maximum_total_attempts=20,
    ),
)
```

Submit and run it:

```console
httk-taskmanager submit native-workspace native-job \
  --placement migration/native/silicon-relax
httk-taskmanager run native-workspace \
  --pool vasp-native \
  --until-idle
```

Static payload files are available below `HTTK_WORKFLOW_JOB_DIR`; the selected
workdir is `HTTK_WORKFLOW_WORKDIR`. Copy or link static inputs into the
workdir in an explicit preparation step when necessary.

## 7. Translate the commonly used task helpers

The native API is intentionally not a spelling change of the *httk* v1 API.

| *httk* v1 operation | Native Bash | Native Python |
| --- | --- | --- |
| `HT_TASK_INIT` | `httk_workflow_init` | `AttemptRuntime.initialize()` |
| `HT_TASK_NEXT step` | `httk_workflow_advance step` | `runtime.advance("step")` |
| `HT_TASK_FINISHED` | `httk_workflow_succeed` | `runtime.succeed()` |
| `HT_TASK_BROKEN` | `httk_workflow_fail CODE MESSAGE` | `runtime.fail(code, message)` |
| `HT_TASK_SUBTASKS` | explicit children plus `httk_workflow_wait` | `OutcomeBuilder` plus `JoinSpec` |
| `HT_TASK_ATOMIC_*` | outcome transaction or workdir spec | `TransactionBuilder` or `ReplayableWorkdirBatch` |
| `HT_TASK_STORE_VAR` | `httk_workflow_state_set` | `runtime.state.set()` |
| `HT_TASK_RUN_CONTROLLED` | `httk_workflow_run` | `ProcessSupervisor` |
| `HT_TASK_SET_PRIORITY` | `--priority` on an outcome | `priority=` on publication |
| run-log helpers | `httk_workflow_runlog_*` | `runtime.runlog.append()` |
| `HT_FCALC` / `HT_FTEST` | `httk_calc` | `evaluate_expression()` |
| `HT_TEMPLATE` | `httk_template_render` | `render_template()` |
| compress/uncompress | explicit `httk_compress` / `httk_decompress` paths | `compress_files()` / `decompress_files()` |
| `HT_FIND_NBR_NODES` | declared attempt resources | `runtime.context.resources` |

State values are JSON, not sourced shell assignments:

```bash
httk_workflow_state_set relaxation_index 3
httk_workflow_state_set phase '"ionic"'

relaxation_index=$(httk_workflow_state_get relaxation_index)
```

Templates use `string.Template` placeholders and an explicit JSON value file:

```text
# INCAR.template
ENCUT = $ENCUT
SYSTEM = $SYSTEM
```

```json
{
  "ENCUT": 520,
  "SYSTEM": "silicon"
}
```

```bash
httk_template_render INCAR.template INCAR template-values.json
```

There is no shell `eval` in the native template or arithmetic implementation.

## 8. Replace controlled-run checkers

For simple programs, use argv-only supervision directly:

```bash
if httk_workflow_run \
  --timeout 3600 \
  --report process-report.json \
  --stdout program.out \
  --stderr program.err \
  -- simulation --input input.dat; then
    httk_workflow_advance collect
else
  status=$?
  httk_workflow_retry "simulation stopped with status $status"
fi
```

For application-specific monitoring, write a checker spec:

```json
{
  "format": "httk-workflow-checker-spec",
  "format_version": 1,
  "argv": ["./checker.py"],
  "required": true,
  "sources": [
    {
      "path": "progress.log",
      "name": "progress",
      "inactivity_timeout": 600
    }
  ]
}
```

This example assumes the executable checker and its specification were copied
from the immutable job payload into the workdir during preparation.

The executable reads `httk-workflow-checker-event` JSON lines from stdin and
emits versioned results on stdout:

```python
#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    event = json.loads(line)
    if event["event"] == "line" and "FATAL" in event.get("line", ""):
        print(
            json.dumps(
                {
                    "format": "httk-workflow-checker-result",
                    "format_version": 1,
                    "code": "application_fatal",
                    "severity": "fatal",
                    "summary": "application reported a fatal error",
                    "source": event["source"],
                    "evidence": event["line"],
                    "stop": True,
                }
            ),
            flush=True,
        )
```

Invoke it without constructing a shell command:

```bash
httk_workflow_run \
  --checker checker.json \
  --timeout 3600 \
  -- simulation --input input.dat
```

Checker diagnostics belong on stderr. Do not reproduce the *httk* v1 signal,
temporary-message-file, or process-discovery convention.

## 9. Replace VASP helpers

VASP input choices can be recorded in JSON:

```json
{
  "kpoint_density": 40.0,
  "centering": "Gamma",
  "accuracy_per_atom": 0.001,
  "pseudopotential_library": "/data/vasp/potpaw_PBE",
  "parallel_tag": "NPAR",
  "parallel_value": 4,
  "normalize_handedness": true
}
```

The native Bash operations include:

```bash
httk_vasp_prepare --options vasp-options.json
httk_vasp_get_tag EDIFF INCAR
httk_vasp_set_tag ISYM 0 INCAR
httk_vasp_prepare_kpoints 40 --centering Gamma
httk_vasp_prepare_potcar /data/vasp/potpaw_PBE
httk_vasp_nbands --divisor 4
httk_vasp_preclean --keep WAVECAR
httk_vasp_run --timeout 86400 -- vasp_std
httk_vasp_energy OSZICAR
httk_vasp_volume vasprun.xml
httk_vasp_promote_contcar
httk_vasp_clean_outcar
```

Important behavioral differences are:

- diagnostics work with VASP 5 and VASP 6 output;
- input diagnosis never changes files;
- remedies are bounded proposals under the explicit `reviewed-v1` policy;
- `httk_vasp_remedy_apply` is a separate, auditable mutation;
- remedy history records before and after input digests;
- rerun cleanup is explicit and can preserve named files;
- commands are argv arrays and never interpolated shell strings.

The old Python modules `httk.task.ht_tasks_api` and
`httk.task.vasptools` are not available in *httk₂*. Use the public functions in
`httk.workflow` instead. Their independent design and the prior *httk* v1
contributor work are described in the packaged compatibility `NOTICE`.

## 10. Migrate dynamic subtasks

Do not recreate the *httk* v1 `ht.task.<set>...waitstart` filename protocol in a
native workflow. Prepare explicit child payloads and publish their identities
with the parent outcome.

A Python parent can create a fixed child set as follows:

```python
import shutil
import tempfile
import uuid
from pathlib import Path

from httk.workflow import AttemptRuntime, JobSpec, JoinSpec, prepare_job_payload

runtime = AttemptRuntime.initialize()
outcome = runtime.outcome()

with tempfile.TemporaryDirectory(dir=runtime.workdir) as draft_root:
    for index, parameter in enumerate(("0.95", "1.00", "1.05")):
        child = Path(draft_root) / f"child-{index}"
        shutil.copytree(runtime.job / "files" / "child-template", child)
        (child / "parameter.txt").write_text(
            parameter + "\n",
            encoding="utf-8",
        )
        child_id = uuid.uuid5(
            uuid.UUID(runtime.context.job_id),
            f"volume-{index}",
        )
        prepare_job_payload(
            child,
            JobSpec(
                name=f"volume point {index}",
                workflow="example.volume-point",
                runner_path="files/run.py",
                tag=f"volume-{index}",
                job_id=str(child_id),
                initial_step="run",
                claim_pool="vasp-native",
            ),
        )
        outcome.add_child(child, f"volume-scan/{index:03d}")

join = JoinSpec(outcome.children, condition="all_terminal")
outcome.publish("wait", next_step="collect", join=join)
```

Use `all_succeeded` when any failed child should make the join impossible.
`all_terminal` most closely matches the compatibility behavior in which a
broken descendant no longer counts as active. Other native conditions are
`any_succeeded` and `at_least`.

For Bash, prepare child directories and specifications first, then compose one
outcome:

```bash
httk_workflow_outcome_begin >/dev/null
httk_workflow_child_add child-0 volume-scan/000
httk_workflow_child_add child-1 volume-scan/001
httk_workflow_wait collect --condition all_terminal
```

The complete child set is sealed with the outcome. A native parent cannot add
untracked children after publication. Child UUIDs must remain stable if the
parent recreates the same unpublished outcome after an interrupted attempt.

## 11. Migrate to a Python runner

A native Python VASP runner can express the same steps without a shell facade:

```python
#!/usr/bin/env python3
import shutil

from httk.workflow import (
    AttemptRuntime,
    VaspPreparationOptions,
    apply_vasp_remedy,
    plan_vasp_remedy,
    prepare_vasp_inputs,
    run_vasp,
)


def main() -> int:
    runtime = AttemptRuntime.initialize()

    if runtime.context.step == "prepare":
        for name in ("POSCAR", "INCAR"):
            destination = runtime.workdir / name
            if not destination.exists():
                shutil.copy2(runtime.job / "files" / name, destination)
        prepare_vasp_inputs(
            VaspPreparationOptions(
                kpoint_density=40,
                centering="Gamma",
                pseudopotential_library="/data/vasp/potpaw_PBE",
                parallel_tag="NPAR",
                parallel_value=4,
            ),
            directory=runtime.workdir,
        )
        runtime.advance("run")
        return 0

    if runtime.context.step == "run":
        report = run_vasp(
            ["vasp_std"],
            directory=runtime.workdir,
            timeout=86400,
        )
        if report.classification == "completed":
            runtime.advance("collect")
            return 0

        history = ".httk-vasp/remedies.json"
        decision = plan_vasp_remedy(
            report.diagnostics,
            history_path=history,
        )
        if not decision.give_up:
            apply_vasp_remedy(
                decision,
                directory=runtime.workdir,
                history_path=history,
            )
            runtime.retry("reviewed VASP remedy applied")
            return 0

        runtime.fail(
            "vasp_failure",
            f"VASP stopped with classification {report.classification}",
            details=report.as_mapping(),
        )
        return 0

    if runtime.context.step == "collect":
        outcome = runtime.outcome()
        transaction = outcome.transaction()
        transaction.put_file(
            "outcar",
            runtime.workdir / "OUTCAR",
            "results/OUTCAR",
        )
        transaction.put_file(
            "oszicar",
            runtime.workdir / "OSZICAR",
            "results/OSZICAR",
        )
        outcome.publish("succeed")
        return 0

    runtime.fail("unknown_step", f"unknown step {runtime.context.step!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

As in the Bash example, the transaction requires a transactional-data job and
a workspace initialized with `transactional-data-v1`.

## 12. Validate before switching production work

For each migrated job type:

1. Run one fixed input through the compatibility backend.
2. Run the same fixed input through the native backend under a new UUID.
3. Compare prepared `INCAR`, `KPOINTS`, POTCAR metadata, final energies,
   structures, and retained result files.
4. Compare failure classification and retry limits.
5. Stop the runner during preparation, execution, remedy application, and
   result publication; verify that restart does not duplicate work or lose the
   authoritative outcome.
6. Exercise a failed child as well as an all-successful child set.
7. Verify `httk-taskmanager status --json` and the journal rather than relying
   on directory names.
8. Run several jobs with the intended pool, capabilities, resources, and
   worker count.

Keep the original template and the known-good compatibility payload until the
native result has passed these checks.

## 13. Cut over and retire compatibility deliberately

Stop instantiating new compatibility jobs first. Let submitted *httk* v1 jobs reach a
terminal state or cancel them through recorded operator requests:

```console
httk-taskmanager request workflow-workspace JOB_UUID cancel \
  --operator "$USER" \
  --reason "replaced by validated native workflow"
```

Then stop `httk-v1-taskmanager` while leaving `httk-taskmanager` running.
Retain the legacy source, its attribution, and reference results for
reproducibility.

Do not delete or reinterpret the old queue as part of cutover. Archive it
read-only according to the project's provenance and retention policy.

## Migration checklist

- [ ] An instantiated *httk* v1 task runs successfully through compatibility.
- [ ] Project/configuration/computer imports were reviewed separately.
- [ ] No live *httk* v1 queue is being treated as an *httk₂* workspace.
- [ ] Persistent scratch and committed result files are distinguished.
- [ ] The workspace's extensions match the native job's data model.
- [ ] Every `HT_TASK_*` and `VASP_*` dependency has an explicit replacement.
- [ ] Automatic remedies became explicit plan-and-apply decisions.
- [ ] Child jobs use stable identities and an explicit join condition.
- [ ] Native Bash commands use quoted argv elements and no `eval`.
- [ ] Compatibility and native reference results agree.
- [ ] Restart and interruption boundaries were exercised.
- [ ] New production submissions use native payloads and new UUIDs.

For API details, continue with {doc}`native_bash_api`,
{doc}`runtime_helpers`, and {doc}`workflow_filesystem_api`.
