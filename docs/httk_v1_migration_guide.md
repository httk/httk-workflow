# Migrating an *httk* v1 workflow to *httk₂*

*For maintainers of an httk v1 workflow, moving it to httk₂ at whatever pace suits.*

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
| Compatibility | None, normally | `httk workflow v1 run` | Establish an *httk₂* operational baseline quickly |
| Mixed | Per job type | Both managers on one workspace | Incremental migration with a direct fallback |
| Native Bash | Replace `HT_TASK_*` and `VASP_*` calls | `httk workflow manager run` | Preserve a shell-oriented workflow |
| Native Python | Replace the runner with Python calls | `httk workflow manager run` | New development and more structured logic |

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
  --taskset vasp \
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
  --taskset vasp \
  --priority 4 \
  --attempts 10
```

Run only *httk* v1 compatibility jobs:

```console
httk workflow v1 run workflow-workspace \
  --taskset any \
  --workers 4 \
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

## 4. Migrate project, configuration, and remotes separately

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
`legacy_queue_imported: false`. The project workspace is a core-v2 workspace,
with detached transfer and transactional data available to native jobs.

Recognized *httk* v1 computer definitions can be mapped explicitly into
*httk₂* remotes:

```console
httk workflow remote import-v1 ~/.httk/computers/cluster-a \
  --name cluster-a
httk workflow remote configure cluster-a:default \
  --set workspace=/remote/path/to/workflow-workspace
```

Review every generated adapter before installation. Legacy shell executables
and credentials are not copied or executed by the importer.

## 5. Run compatibility and native jobs side by side

The *httk* v1 and native managers select different runner backends, so they can share
one *httk₂* workspace:

```console
# Terminal or service 1: compatibility jobs
httk workflow v1 run workflow-workspace --taskset any --workers 2

# Terminal or service 2: native jobs
httk workflow manager run workflow-workspace --pool vasp-native --workers 2
```

The shared core-v2 workspace already provides transactional data and detached
transfer for native jobs.

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

```{admonition} A relaxation may need no runner at all
:class: tip

The workflow below ships with the module, in Bash and in Python, as
`vasp_relax.sh` and `vasp_relax.py`. A campaign that wants the ordinary relaxation
submits jobs naming the installed file and writes nothing: see
{doc}`vasp_runners`. Write your own when your practice differs from the packaged
one — starting from a copy of it.
```

The native Bash equivalent sources paths supplied by the manager and publishes
structured outcomes:

```bash
#!/usr/bin/env bash
set -euo pipefail

source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_runner vasp.relax prepare run collect

step_prepare() {
  local input
  for input in POSCAR INCAR; do
    if [ ! -e "$input" ]; then
      cp -- "$HTTK_WORKFLOW_JOB_DIR/files/$input" "$input"
    fi
  done
  httk_vasp_prepare \
    --options "$HTTK_WORKFLOW_JOB_DIR/files/vasp-options.json"
  httk_workflow_advance run
}

step_run() {
  local status=0
  httk_vasp_preclean --keep WAVECAR
  if httk_vasp_run \
    --timeout 86400 \
    --report vasp-run-report.json \
    -- vasp_std; then
      httk_workflow_advance collect
      return
  fi
  status=$?
  if httk_vasp_remedy_plan \
    vasp-run-report.json \
    --output remedy.json; then
      httk_vasp_remedy_apply remedy.json
      httk_workflow_retry "reviewed VASP remedy applied"
      return
  fi
  httk_workflow_fail vasp.failed \
    "VASP stopped with status $status"
}

step_collect() {
  httk_workflow_put OUTCAR results/OUTCAR >/dev/null
  httk_workflow_put OSZICAR results/OSZICAR >/dev/null
  httk_workflow_succeed
}

httk_workflow_main
```

The outcome functions publish exactly one decision and then *return*:
`httk_workflow_main` owns the process exit status. Do not additionally return a
legacy decision code or write `ht.nextstep`.

The `collect` example uses transactional data. Its workspace is core-v2:

```console
httk workflow workspace init native-workspace
```

If the job uses `data.mode: "none"`, omit the transaction and keep restartable
working files in its persistent workdir instead.

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
httk workflow job submit native-workspace native-job \
  --placement migration/native/silicon-relax
httk workflow manager run native-workspace \
  --pool vasp-native \
```

Static payload files are available below `HTTK_WORKFLOW_JOB_DIR`; the selected
workdir is `HTTK_WORKFLOW_WORKDIR`. Copy or link static inputs into the
workdir in an explicit preparation step when necessary.

## 7. Translate the commonly used task helpers

The native API is intentionally not a spelling change of the *httk* v1 API.

| *httk* v1 operation | Native Bash | Native Python |
| --- | --- | --- |
| `HT_TASK_INIT` | `httk_workflow_runner` plus `httk_workflow_main` | `Runner.main()` dispatches the step |
| `HT_TASK_NEXT step` | `httk_workflow_advance step` | `a.advance("step")` |
| `HT_TASK_FINISHED` | `httk_workflow_succeed` | `a.succeed()` |
| `HT_TASK_BROKEN` | `httk_workflow_fail CODE MESSAGE` | `a.fail(code, message)` |
| `HT_TASK_SUBTASKS` | `httk_workflow_spawn` plus `httk_workflow_gather` | `a.spawn(ChildSpec(...), label=...)` plus `a.gather(step)` |
| `HT_TASK_ATOMIC_*` | `httk_workflow_put` / `remove` or a workdir spec | `a.put()` / `a.remove()` or `a.workdir_batch()` |
| `HT_TASK_STORE_VAR` | `httk_workflow_state_set` | `a.state["name"] = value` |
| `HT_TASK_RUN_CONTROLLED` | `httk_workflow_run` | `a.run(argv)` or `ProcessSupervisor` |
| `HT_TASK_SET_PRIORITY` | `--priority` on an outcome | `priority=` on publication |
| run-log helpers | `httk_workflow_runlog_*` | `a.log.append()` |
| `HT_FCALC` / `HT_FTEST` | `httk_calc` | `evaluate_expression()` |
| `HT_TEMPLATE` | `httk_template_render` | `render_template()` |
| compress/uncompress | explicit `httk_compress` / `httk_decompress` paths | `compress_files()` / `decompress_files()` |
| `HT_FIND_NBR_NODES` | declared attempt resources | `a.context.resources` |

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

from httk.workflow import JobSpec, Runner, prepare_job_payload

run = Runner("example.volume-scan")


@run.step
def branch(a):
    with tempfile.TemporaryDirectory(dir=a.workdir) as draft_root:
        for index, parameter in enumerate(("0.95", "1.00", "1.05")):
            child = Path(draft_root) / f"child-{index}"
            shutil.copytree(a.payload / "files" / "child-template", child)
            (child / "parameter.txt").write_text(
                parameter + "\n",
                encoding="utf-8",
            )
            child_id = uuid.uuid5(
                uuid.UUID(a.context.job_id),
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
            a.spawn(
                child,
                label=f"volume-{index}",
                placement=f"volume-scan/{index:03d}",
            )
    a.gather("collect", when="all_terminal")
```

A child whose steps live in the same runner needs no payload at all: publish the
runner once in the workspace and spawn a `ChildSpec`, which synthesizes the whole
child job from its step and inputs and inherits the parent's runner reference.

```python
a.spawn(ChildSpec(step="run", inputs={"scale": parameter}), label=f"volume-{index}")
```

Use `all_succeeded` when any failed child should make the join impossible.
`all_terminal` most closely matches the compatibility behavior in which a
broken descendant no longer counts as active. Other native conditions are
`any_succeeded` and `at_least`.

A Bash step spawns the same children by step and inputs, and gathers exactly the
ones it spawned:

```bash
for index in 000 001; do
    httk_workflow_spawn "volume-$index" \
        --step run \
        --input scale="0.$index" \
        --placement "volume-scan/$index" >/dev/null
done
httk_workflow_gather collect --when all_terminal
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
    Runner,
    VaspPreparationOptions,
    apply_vasp_remedy,
    plan_vasp_remedy,
    prepare_vasp_inputs,
    run_vasp,
)

run = Runner("example.vasp-relax")


@run.step
def prepare(a):
    for name in ("POSCAR", "INCAR"):
        destination = a.workdir / name
        if not destination.exists():
            shutil.copy2(a.payload / "files" / name, destination)
    prepare_vasp_inputs(
        VaspPreparationOptions(
            kpoint_density=40,
            centering="Gamma",
            pseudopotential_library="/data/vasp/potpaw_PBE",
            parallel_tag="NPAR",
            parallel_value=4,
        ),
        directory=a.workdir,
    )
    a.advance("run")


@run.step(name="run")
def run_step(a):
    report = run_vasp(["vasp_std"], directory=a.workdir, timeout=86400)
    if report.classification == "completed":
        a.advance("collect")
        return

    history = ".httk-vasp/remedies.json"
    decision = plan_vasp_remedy(report.diagnostics, history_path=history)
    if not decision.give_up:
        apply_vasp_remedy(decision, directory=a.workdir, history_path=history)
        a.retry("reviewed VASP remedy applied")
        return

    a.fail(
        "vasp_failure",
        f"VASP stopped with classification {report.classification}",
        details=report.as_mapping(),
    )


@run.step
def collect(a):
    a.put(a.workdir / "OUTCAR", "results/OUTCAR")
    a.put(a.workdir / "OSZICAR", "results/OSZICAR")
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
```

There is no step-dispatch chain and no `unknown_step` branch to write: `Runner.main`
dispatches the step the manager asked for, and reports an unimplemented step, a step
that published nothing, and a step that raised as the corresponding outcome.

As in the Bash example, the transaction requires a transactional-data job in a
core-v2 workspace.

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
7. Verify `httk workflow workspace status workflow-workspace --json` and the journal
   rather than relying on directory names.
8. Run several jobs with the intended pool, capabilities, resources, and
   worker count.

Keep the original template and the known-good compatibility payload until the
native result has passed these checks.

## 13. Cut over and retire compatibility deliberately

Stop instantiating new compatibility jobs first. Let submitted *httk* v1 jobs reach a
terminal state or cancel them through recorded operator requests:

```console
httk workflow job request workflow-workspace JOB_UUID cancel \
  --operator "$USER" \
  --reason "replaced by validated native workflow"
```

Then stop the `httk workflow v1 run` managers while leaving the
`httk workflow manager run` ones running.
Retain the legacy source, its attribution, and reference results for
reproducibility.

Do not delete or reinterpret the old queue as part of cutover. Archive it
read-only according to the project's provenance and retention policy.

## Migration checklist

- [ ] An instantiated *httk* v1 task runs successfully through compatibility.
- [ ] Project/configuration/remote imports were reviewed separately.
- [ ] No live *httk* v1 queue is being treated as an *httk₂* workspace.
- [ ] Persistent scratch and committed result files are distinguished.
- [ ] The core-v2 workspace matches the native job's data model.
- [ ] Every `HT_TASK_*` and `VASP_*` dependency has an explicit replacement.
- [ ] Automatic remedies became explicit plan-and-apply decisions.
- [ ] Child jobs use stable identities and an explicit join condition.
- [ ] Native Bash commands use quoted argv elements and no `eval`.
- [ ] Compatibility and native reference results agree.
- [ ] Restart and interruption boundaries were exercised.
- [ ] New production submissions use native payloads and new UUIDs.

For API details, continue with {doc}`native_bash_api`,
{doc}`runtime_helpers`, and {doc}`workflow_filesystem_api`.
