# Native Bash runner API

The normal *httk₂* manager exports absolute paths to two packaged, sourced
libraries. A runner does not need to locate Python package data itself:

```bash
#!/usr/bin/env bash
set -euo pipefail

source "$HTTK_WORKFLOW_BASH_API"
source "$HTTK_WORKFLOW_VASP_BASH_API"
httk_workflow_init

case "$HTTK_WORKFLOW_STEP" in
  prepare)
    httk_vasp_prepare --options vasp-options.json
    httk_workflow_advance run
    ;;
  run)
    if httk_vasp_run --timeout 86400 --report vasp-run-report.json -- vasp_std; then
      httk_workflow_advance collect
    else
      status=$?
      if httk_vasp_remedy_plan vasp-run-report.json --output remedy.json; then
        httk_vasp_remedy_apply remedy.json
        httk_workflow_retry "reviewed VASP remedy applied"
      fi
      httk_workflow_fail vasp.failed "VASP stopped (status $status)"
    fi
    ;;
  collect)
    httk_workflow_succeed
    ;;
esac
```

The libraries require Bash 4.2 or newer and work with `set -euo pipefail`.
They call the manager's `HTTK_WORKFLOW_PYTHON` with argv arrays; they never
evaluate a command string.

## Workflow functions

- `httk_workflow_context [FIELD]`, `state_get`, `state_set`, and
  `state_delete` expose context and atomic JSON state.
- `httk_workflow_runlog_note`, `runlog_headline`, and `runlog_append` retain
  structured evidence. Ordinary stdout/stderr are retained by the manager too.
- `httk_workflow_advance`, `wait`, `succeed`, `fail`, `retry`, and `pause`
  publish one outcome and exit the runner.
- `httk_workflow_outcome_begin` starts a composed draft.
  `transaction_mkdir`, `transaction_put_file`, `transaction_put_tree`,
  `transaction_replace_tree`, and `transaction_remove` add durable-data
  operations. `child_add` adds prepared child payloads. The final outcome
  function publishes the complete bundle.
- `httk_workflow_job_prepare DEST SPEC.json` creates a native payload.
  `httk_workflow_workspace_apply SPEC.json` applies a replayable set of
  persistent-workspace changes.
- `httk_workflow_run` supervises an argv command. `--checker SPEC.json` adds a
  required versioned executable checker.
- `httk_calc`, `httk_template_render`, `httk_compress`, and `httk_decompress`
  are safe replacements for commonly used *httk* v1 conveniences. Templates use
  `string.Template` and an explicit JSON values object, never `eval`.

Transaction and workspace specs use the protocol operation names
`make-dir`, `put-file`, `put-tree`, `replace-tree`, and `remove`. Sources are
copied into a sealed bundle before publication. Symlinks and special files are
rejected.

A checker spec has format `httk-workflow-checker-spec`, format version 1, an
`argv` string array, and optional `required` and `sources` fields. Each source
contains `path` and may contain `name` and a positive `inactivity_timeout`.
The checker receives one `httk-workflow-checker-event` JSON object per line on
stdin and emits `httk-workflow-checker-result` version 1 objects on stdout.
Diagnostics belong on stderr.

## VASP functions

The `httk_vasp_*` surface corresponds directly to functions in
`httk.workflow.vasp`:

- `prepare`, `prepare_kpoints`, `prepare_potcar`, `get_tag`, `set_tag`, and
  `nbands`;
- `run` and `diagnose`;
- `remedy_plan` and `remedy_apply`;
- `preclean`, `clean_outcar`, `normalize_poscar`, `scale_poscar`,
  `rattle_poscar`, and the energy, volume, POTIM, plane-wave, and POTCAR
  summary extractors.

`httk_vasp_run` returns 0 for clean completion, 20 for a diagnostic stop, 21
for completed nonconvergence, 22 for process failure, and 124 for timeout. The
versioned JSON report is authoritative.

Remedies are never automatic. `remedy_plan` returns 3 when the reviewed policy
has no safe remaining action. Applying a decision uses a replayable workspace
batch and records before/after input digests and policy history. Preparation
also enforces the conservative 240-byte VASP workspace-path limit.

## Mapping from *httk* v1

| *httk* v1 capability | Native *httk₂* replacement |
| --- | --- |
| `HT_TASK_INIT`, next/finished/broken | Context initialization and structured outcome functions |
| `HT_TASK_SUBTASKS`, `HT_TASK_CREATE` | Prepared child payloads plus explicit `JoinSpec` |
| `HT_TASK_ATOMIC_*` | Protocol transactions or `ReplayableWorkspaceBatch` |
| `HT_TASK_STORE_VAR` | `WorkspaceState` / Bash state functions |
| `HT_TASK_RUN_CONTROLLED`, follow-file checkers | `ProcessSupervisor` and checker JSON-lines protocol |
| priority file | `priority` on the published outcome |
| run log helpers | structured `RunLog` |
| math, template, compression | safe native utilities; no shell evaluation |
| node probing through `mpirun` | requested resources in the attempt context |
| POTCAR/KPOINTS/INCAR preparation | typed VASP preparation APIs |
| stdout/OSZICAR/OUTCAR checkers | VASP 5/6 structured diagnostics |
| `VASP_INPUTS_ADJUST/FIX_ERROR` | explicit `reviewed-v1` plan and apply calls |
| energy/volume/POTIM/plane waves/cleanup | native VASP extraction and cleanup functions |

Trivial path matching and field splitting use normal quoted Bash constructs.
Native code does not source or expose the legacy `HT_TASK_*` or `VASP_*`
function names. Unchanged *httk* v1 workflows continue to use
[*httk* v1 task compatibility](v1_compatibility.md). For a step-by-step
conversion, see
[*httk* v1 migration guide](httk_v1_migration_guide.md).
