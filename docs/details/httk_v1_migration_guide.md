# Migrating an *httk* v1 workflow to *httk₂* in detail

This guide moves an existing `ht_steps` or `ht_run` workflow from *httk* v1 to
*httk-workflow*, at whatever pace suits its maintainers. You can keep the
workflow unchanged in a converted package, migrate one job type at a time, or
replace the legacy API with the native *httk₂* Bash or Python API.

Migrate task definitions and newly instantiated task directories, not a live
*httk* v1 task-manager queue. The *httk₂* manager never claims or rewrites an
existing v1 queue tree. Once a task has been prepared and submitted to an
*httk₂* workspace, its `job.json`, `state.json` and run log are authoritative.

## 1. Choose a migration route

You do not need to migrate every workflow at once.

| Route | Workflow changes | Manager | Best use |
| --- | --- | --- | --- |
| Converted package | None, normally | normal `httk workflow run --pool POOL` | Establish an *httk₂* operational baseline quickly |
| Mixed | Per job type | the normal manager on one workspace | Incremental migration with a direct fallback |
| Native Bash | Replace `HT_TASK_*` and `VASP_*` calls | `httk workflow run` | Preserve a shell-oriented workflow |
| Native Python | Replace the runner with Python calls | `httk workflow run` | New development and more structured logic |

Start with a converted package unless tests already describe the workflow's
inputs, outputs, restart behavior and child tasks. A package run gives a
reference result before any semantics change.

## 2. Inventory the *httk* v1 workflow

Copy an instantiated task directory and record:

- whether the entry point is `ht_steps` or `ht_run`;
- every sourced helper, especially
  `$HTTK_DIR/Execution/tasks/ht_tasks_api.sh` and
  `$HTTK_DIR/Execution/tasks/vasp/vasptools.sh`;
- use of `HT_TASK_ATOMIC_*`, `HT_TASK_CREATE`, `HT_TASK_SUBTASKS`,
  `HT_TASK_STORE_VAR`, controlled processes, checkers, templates or
  compression;
- files that must survive a retry, such as `WAVECAR`, `CONTCAR` or application
  checkpoints;
- files that are final results rather than attempt scratch;
- task-set, priority, timeout, retry-limit and resource assumptions;
- any `ht.instantiate.py` imports from the old httk Python package (convert
  them as described in section 12);
- automatic VASP remedies the workflow depends on;
- child tasks that may still be running independently.

Do not infer completion from a legacy task-directory suffix alone. Capture the
actual input files, logs, expected results and exit behavior.

## 3. Run the workflow unchanged in an *httk₂* workspace

Initialize an *httk₂* workspace:

```console
httk workspace init workflow-workspace
```

Wrap the task in a converted package, install it, and submit it through the
normal path:

```console
httk workflow install --workspace workflow-workspace ./legacy-package
httk job new --workspace workflow-workspace --workflow-dir ./legacy-package \
  --placement migration/reference/silicon-relax
httk workflow run --workspace workflow-workspace --pool vasp --workers 4
```

The old source paths remain available below the compatibility `HTTK_DIR`, so
an unchanged step can keep using:

```bash
source "$HTTK_DIR/Execution/tasks/ht_tasks_api.sh"
source "$HTTK_DIR/Execution/tasks/vasp/vasptools.sh"
```

Inspect the result through the *httk₂* source of truth:

```console
httk workspace status --json workflow-workspace
```

The packaged v1 runner keeps the persistent `ht.run.current/` workdir,
translates v1 decisions and dynamic subtasks, and completes published v1
atomic sections after an interruption. See
{doc}`/details/v1_compatibility` for the exact
compatibility boundary.

### Wrap an existing template as a package

An existing template directory becomes a package without being rendered
first:

```text
silicon-relax/
├── httk_workflow.toml
├── ht_steps.template
├── ht.instantiate.py
├── INCAR.template
└── collect.py
```

The manifest makes the v1 contract explicit:

```toml
[workflow]
name = "legacy.silicon-relax"

[workflow.runner]
format = "httk-v1"
taskset = "vasp"
attempts = 10

[workflow.inputs.structure]
entry_type = "structures"

[workflow.parameters.encut]
type = "number"
default = 520

[workflow.collect]
file = "collect.py"
```

Pre-rename alpha jobs that carry the `workflow_postprocess` parameter lose
package-hook collection; re-scaffold them.

Install it, then submit a one-shot job or a structure campaign:

```console
httk workflow install --workspace WS ./silicon-relax
httk job new --workspace WS --workflow-dir ./silicon-relax \
  --input-from structure structures/*.cif --parameter encut=520
httk workflow run --workspace WS --pool vasp
httk collect --workspace WS
```

### Template rendering and `ht.instantiate.py`

At preparation the installed package is snapshotted, and each job gets its own rendered
payload. `ht_steps` or `ht_run` must be executable after rendering. The v1
template engine is trusted and supports `$name`, `$(expr)`, `${code}`, escaped
`\$` and `.template` filenames. It is available as `apply_templates` in
`httk.workflow.compat.v1.templates`.

`ht.instantiate.py` receives the declared inputs and parameters as globals. A
path-valued input with `entry_type` is loaded through `httk.core.load` first,
which is how a CIF campaign feeds structure objects. The script is still v1
Python, and old imports are not all supported. A script written against v1's
own Python API (for example v1 `Structure`) works only when it receives
objects compatible with that API. Port the script, or supply compatible
objects, when it crosses this boundary.

## 4. Migrate project, configuration and remotes separately

These imports do not migrate workflow code or task queues.

### User configuration

Import safe user configuration explicitly:

```console
httk config import-v1
```

When no identity is configured, this creates a named identity from the legacy
name and email, imports the public key into the core-owned `identity.json` and
records where it came from. Otherwise, set up a fresh identity with
`httk init`.

### Project metadata

Create *httk₂* project metadata from a local `ht.project` without modifying
it:

```console
httk project import-v1 --source ./ht.project .
```

This imports safe metadata and public identities, not private keys or the v1
queue. Imported project metadata records `legacy_queue_imported: false`. The
project can record a workspace default, but the core-v3 workspace itself stays
outside the project.

### Workspaces and remotes

The workspace registry is machine-owned in *httk₂*. Local workspaces are
registered with `workspace init PATH`, and remote names are registered on
their owning machine. `workspace default NAME` replaces project workspace
bindings: it records only the name in `project.json`, and the workspace stays
outside the project.

Recognized v1 computer definitions can be mapped explicitly into *httk₂*
remotes:

```console
httk remote import-v1 --name cluster-a ~/.httk/computers/cluster-a
httk workspace init --name default cluster-a:/remote/path/to/workflow-workspace
```

`workspace_root` is retired. `workspace init REMOTE:PATH` initializes and
registers the remote workspace, and `workspace settings set REMOTE:NAME …`
then sets its scheduler and application settings. `remote import-v1` does not
create a workspace. It keeps the legacy Runs hint in the remote's
`legacy_settings` so an operator can choose the path explicitly.

Review every generated adapter before installation. The importer does not copy
or execute legacy shell executables or credentials.

## 5. Run converted and native jobs side by side

Converted v1 packages and native workflows are both ordinary jobs, so one
normal manager can serve both from the same *httk₂* workspace. Select the
converted package's `taskset` with the manager pool:

```console
httk workflow run --workspace workflow-workspace --pool vasp --workers 2
httk workflow run --workspace workflow-workspace --workers 2
```

The second manager serves the `default` pool, where native jobs land.

Give the first native version a new job UUID and preferably a distinct tag and
placement. Do not edit the immutable `job.json` of a submitted job to change
its workflow. Migrate one representative task first and compare it
with the compatibility reference before moving a larger batch.

## 6. Replace the *httk* v1 control flow with native Bash

A typical v1 runner looks like:

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

The workflow below is published, in Bash and in Python, as `vasp.relax-bash`
and `vasp.relax` in [workflows-vasp](https://github.com/httk/workflows-vasp). A
campaign that wants the ordinary relaxation submits jobs naming its git URI and
writes nothing; see {doc}`/vasp_runners`. When your practice differs, write
your own, starting from a copy of it.
```

The native Bash equivalent sources paths supplied by the manager and publishes
structured outcomes. Its `httk_vasp_*` functions come from the separate
*httk-workflow-vasp* distribution (`pip install httk-workflow-vasp`), whose
Bash API the manager exports as `HTTK_WORKFLOW_VASP_BASH_API` (see
{doc}`/code_support`):

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

Each outcome function publishes one decision and then returns;
`httk_workflow_main` owns the process exit status. Do not also return a legacy
decision code or write `ht.nextstep`.

The `collect` step commits copies of the results to the job's `data/` with
`httk_workflow_put`; the manager applies them exactly once when the outcome
commits. A job that never puts anything keeps its results in its persistent
workdir only.

### Install the native workflow and submit a job

Make the runner the `run` entry of a workflow package:

```text
native-relax/
├── httk_workflow.toml
└── run                  the Bash runner above, executable
```

```toml
[workflow]
name = "vasp.relax"

[workflow.runner]
steps = ["prepare", "run", "collect"]
initial_step = "prepare"
```

Install it, stage the static inputs into a job, and run it:

```console
httk workspace init native-workspace
httk workflow install --workspace native-workspace ./native-relax
httk job new --workspace native-workspace --workflow-dir ./native-relax \
  --file POSCAR=POSCAR --file INCAR=INCAR --file vasp-options.json=vasp-options.json \
  --placement migration/native/silicon-relax --tag silicon-relax --priority 700
httk workflow run --workspace native-workspace
```

Staged files are below `HTTK_WORKFLOW_JOB_DIR` (a bare `--file` name lands in
`files/`), and the persistent workdir is `HTTK_WORKFLOW_WORKDIR`. Copy or link static inputs into the
workdir in an explicit preparation step when needed.

## 7. Translate the common task helpers

The native API is not a respelling of the v1 API.

| *httk* v1 operation | Native Bash | Native Python |
| --- | --- | --- |
| `HT_TASK_INIT` | `httk_workflow_runner` plus `httk_workflow_main` | `Runner.main()` dispatches the step |
| `HT_TASK_NEXT step` | `httk_workflow_advance step` | `a.advance("step")` |
| `HT_TASK_FINISHED` | `httk_workflow_succeed` | `a.succeed()` |
| `HT_TASK_BROKEN` | `httk_workflow_fail CODE MESSAGE` | `a.fail(code, message)` |
| `HT_TASK_SUBTASKS` | `httk_workflow_spawn` plus `httk_workflow_gather` | `a.spawn(ChildSpec(...), label=...)` plus `a.gather(step)` |
| `HT_TASK_ATOMIC_*` | `httk_workflow_put` / `httk_workflow_transaction`, or a workdir spec | `a.put()` / `a.transaction()`, or `a.workdir_batch()` |
| `HT_TASK_STORE_VAR` | `httk_workflow_state_set` | `a.state["name"] = value` |
| `HT_TASK_RUN_CONTROLLED` | `httk_workflow_run` | `a.run(argv)` or `ProcessSupervisor` |
| `HT_TASK_SET_PRIORITY` | `--priority` on an outcome | `priority=` on publication |
| run-log helpers | `httk_workflow_runlog_*` | `a.log.append()` |
| `HT_FCALC` / `HT_FTEST` | `httk_calc` | `evaluate_expression()` |
| `HT_TEMPLATE` | `httk_template_render` | `render_template()` |
| compress/uncompress | explicit `httk_compress` / `httk_decompress` paths | `compress_files()` / `decompress_files()` |
| `HT_FIND_NBR_NODES` | declared attempt resources | `a.context.resources` |

### State values

State values are JSON, not sourced shell assignments:

```bash
httk_workflow_state_set relaxation_index 3
httk_workflow_state_set phase '"ionic"'

relaxation_index=$(httk_workflow_state_get relaxation_index)
```

### Templates

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

The native template and arithmetic implementations use no shell `eval`.

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
  "format_version": 2,
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

This example assumes the preparation step copied the executable checker and
its spec from the immutable job payload into the workdir.

The checker reads `httk-workflow-checker-event` JSON lines from stdin and
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
                    "format_version": 2,
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

Invoke it without building a shell command:

```bash
httk_workflow_run \
  --checker checker.json \
  --timeout 3600 \
  -- simulation --input input.dat
```

Checker diagnostics belong on stderr. Do not reproduce the v1 signal,
temporary-message-file or process-discovery conventions.

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

Behavioral differences from v1:

- diagnostics work with VASP 5 and VASP 6 output;
- input diagnosis never changes files;
- remedies are bounded proposals under the explicit `reviewed-v1` policy;
- `httk_vasp_remedy_apply` is a separate, auditable mutation;
- remedy history records input digests before and after;
- rerun cleanup is explicit and can keep named files;
- commands are argv arrays, never interpolated shell strings.

The old Python modules `httk.task.ht_tasks_api` and `httk.task.vasptools` do
not exist in *httk₂*; use the public functions in `httk.workflow` and, for
VASP, in `httk.codes.vasp` of *httk-workflow-vasp*. The
packaged compatibility `NOTICE` describes their independent design and the
earlier v1 contributor work.

## 10. Migrate dynamic subtasks

Do not recreate the v1 `ht.task.<set>...waitstart` filename protocol in a
native workflow. Children are published with the parent outcome, by step and
parameters. A child whose steps live in the same runner needs no payload:
spawn a `ChildSpec`, which synthesizes the whole child job and runs the
parent's installed workflow.

```python
from httk.workflow import ChildSpec, Runner

run = Runner("example.volume-scan")


@run.step
def branch(a):
    for index, scale in enumerate(("0.95", "1.00", "1.05")):
        a.spawn(
            ChildSpec(step="run", parameters={"scale": scale}),
            label=f"volume-{index}",
            placement=f"volume-scan/{index:03d}",
        )
    a.gather("collect", when="all_terminal")
```

A child of another workflow is started with `a.call(alias, label=...)`, where
the alias is declared in the parent's `[workflow.calls]` and the called
workflow is installed in the workspace; see {doc}`/details/composing_workflows`.

A Bash step spawns the same children by step and parameters, and gathers
exactly the ones it spawned:

```bash
for index in 000 001; do
    httk_workflow_spawn "volume-$index" \
        --step run \
        --parameter scale="0.$index" \
        --placement "volume-scan/$index" >/dev/null
done
httk_workflow_gather collect --when all_terminal
```

### Join conditions

Use `all_succeeded` when any failed child should make the join impossible.
`all_terminal` is closest to the compatibility behavior, in which a broken
descendant no longer counts as active. The other native conditions are
`any_succeeded` and `at_least`.

The complete child set is fixed by the outcome, and a native parent cannot add
untracked children after publication. An interrupted attempt publishes
nothing, so its retry spawns afresh.

## 11. Migrate to a Python runner

A native Python VASP runner expresses the same steps without a shell facade:

```python
#!/usr/bin/env python3
import shutil

from httk.codes.vasp import (
    VaspPreparationOptions,
    apply_vasp_remedy,
    plan_vasp_remedy,
    prepare_vasp_inputs,
    run_vasp,
)
from httk.workflow import Runner

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

There is no step-dispatch chain or `unknown_step` branch to write.
`Runner.main` dispatches the step the manager asked for, and reports an
unimplemented step, a step that published nothing and a step that raised as
the corresponding outcomes. As in the Bash example, the copies `a.put` stages
are committed with the outcome.

## 12. Converting your `ht.instantiate.py`

In v1, `create_batch_task` copied the template, changed into the new task
directory and `exec`'d `ht.instantiate.py` with the `args` dictionary as its
globals. The script wrote the task files and could set `finalname` in `args`
to name the task.

### The script only wrote the structure

This is by far the most common case; every template shipped with v1 did only
this. No code is needed. Use a packaged template, or declare the input on your
own Python runner:

```python
run = Runner("example.structure", inputs={"structure": "POSCAR"})
```

Callers pass the structure object, and the scaffold writes it to
`files/POSCAR` with the registered writer (the *httk-atomistic* distribution
provides the POSCAR writer). A path input is copied instead. In the Python
API, use `new_job(ws, workflow, inputs={"structure": obj})`, or `new_jobs(...)`
items such as `{"inputs": {"structure": obj}}` for a batch. The command-line
spellings are in {doc}`/details/workflow_cli`.

### The script produced derived creation-time files

If everything the script produced derives from its arguments, declare an
input for every object that defines workflow equivalence. Give a destination
for values that can be staged directly, and `None` for values consumed by a
creation hook. Keep implementation knobs out of the declaration as job
parameters. Move the remaining logic to `@run.instantiate`:

```python
from httk.workflow import Runner

run = Runner(
    "example.supercell",
    inputs={"structure": "POSCAR"},
)


@run.instantiate
def instantiate(ctx):
    from httk.atomistic import build_supercell
    from httk.core import save

    result = build_supercell(ctx.inputs["structure"], ctx.parameters["supercell"])
    save(result.structure, ctx.payload / "files" / "POSCAR")
    ctx.suggest_tag("supercell")
```

For a v1 script whose `args` contained `structure` and `supercell`, the
mapping is:

| v1 | v2 |
| --- | --- |
| `args` dictionary | declared `inputs=` and opaque job `parameters=` |
| script current directory | `ctx.payload` |
| writing a file | a declared destination, or a write below `ctx.payload` |
| `finalname` | `ctx.suggest_tag(...)` |
| a value the runner needs later | `ctx.inputs` |

Declarative staging happens before the hook. `ctx.inputs` is read-only;
`ctx.parameters` is mutable and becomes the job's opaque parameter mapping.
The hook runs in process on the creating machine. See {doc}`/details/runtime_helpers`
for the hook reference and {doc}`/details/workflow_cli` for `--parameter NAME=VALUE`
and `--input-from NAME SOURCE...`.

### The work belongs at run time

If the script really does run-time preparation, such as deriving INCAR values
or k-points as v1 `ht_steps` commonly did, put it in the runner's `prepare`
step. Use the instantiate hook only for creation-time work that needs the
supplied domain objects or must happen before submission.

The hook is template code imported and executed at creation on the creating
machine, with the same trust and locality implications as v1's `exec`. The
template's Python file must therefore be available there. Bash runners do not
support `@run.instantiate`.

## 13. Validate before switching production work

For each migrated job type:

1. Run one fixed input through the converted package.
2. Run the same fixed input through the native runner under a new UUID.
3. Compare prepared `INCAR`, `KPOINTS`, POTCAR metadata, final energies,
   structures and retained result files.
4. Compare failure classification and retry limits.
5. Stop the runner during preparation, execution, remedy application and
   result publication; verify that restart neither duplicates work nor loses
   the authoritative outcome.
6. Exercise a failed child as well as an all-successful child set.
7. Check `httk job show` and `httk job log` instead of relying on directory
   names.
8. Run several jobs with the intended pool, capabilities, resources and
   worker count.

Keep the original template and the known-good converted payload until the
native result passes these checks.

## 14. Cut over and retire compatibility

Stop instantiating new converted jobs first. Let submitted v1 jobs reach a
terminal state, or cancel them through recorded operator requests:

```console
httk job request cancel --workspace workflow-workspace \
  --reason "replaced by validated native workflow" JOB_UUID
```

Then stop the manager serving the converted package's pool and leave the
native pool's manager running. Keep the legacy source, its attribution and the
reference results for reproducibility.

Do not delete or reinterpret the old queue during cutover. Archive it
read-only according to the project's provenance and retention policy.

## 15. Harvest old result trees

Use `finished_tasks` to inspect a tree and `collect_finished_tree` to run its
package collector against every finished task:

```python
from httk.workflow.compat.v1 import collect_finished_tree, finished_tasks

for task in finished_tasks("/archive/ht-results"):
    print(task.task_id, task.rundir, task.code_name, task.code_version)

collected = collect_finished_tree(
    "/archive/ht-results", workflow_dir="./silicon-relax"
)
```

The CLI equivalent is:

```console
httk v1 collect --workflow-dir ./silicon-relax \
  --into results.sqlite --id-base httk.v1 /archive/ht-results
```

The harvester selects the latest dated `ht.run.*`, reads code metadata from
lines 2–3 of `ht_steps` or `ht_run`, and calls the authored hook with
`run_directory`, `code_of` and `task_file` from `httk.workflow.compat.v1`. A
per-task hook failure degrades that task and the sweep continues.
Manifest-backed UUIDv5 identity survives moving the tree; path-derived
identity does not.

## 16. What stays behind

The compatibility layer does not recreate every v1 subsystem:

| v1 surface left behind | v2 replacement |
| --- | --- |
| ssh/rsync computer templates and send/receive transport | v2 remotes and transfer protocol |
| openmaterialsdb submission and signing arc | v2 project manifests, keys, and remote transfer |
| `ht.parameters` resource fields | declared workflow inputs, opaque parameters, and manager policy |
| `--daemon` | explicit v2 managers and workers |
| runtime priority rewrites | immutable job priority and recorded operator requests |

These are migration boundaries, not hidden package options. Keep a v1
installation only where the packaged runner's trusted runtime or a template
still needs it.

## Migration checklist

- [ ] An instantiated *httk* v1 task runs successfully through its converted package.
- [ ] Project/configuration/remote imports were reviewed separately.
- [ ] No live *httk* v1 queue is being treated as an *httk₂* workspace.
- [ ] Persistent scratch and committed result files are distinguished.
- [ ] Every `HT_TASK_*` and `VASP_*` dependency has an explicit replacement.
- [ ] Automatic remedies became explicit plan-and-apply decisions.
- [ ] Child jobs use stable identities and an explicit join condition.
- [ ] Native Bash commands use quoted argv elements and no `eval`.
- [ ] Compatibility and native reference results agree.
- [ ] Restart and interruption boundaries were exercised.
- [ ] Every `ht.instantiate.py` is converted to declared parameters or `@run.instantiate`.
- [ ] New production submissions use the installed native workflow and new UUIDs.

For API details, continue with {doc}`/sdks/bash_api`,
{doc}`/details/runtime_helpers` and {doc}`/details/workflow_filesystem_api`.
