# httk v1 task compatibility

Installing *httk-workflow* provides `httk-v1-taskmanager`, a specialized
executor for instantiated httk v1 task directories containing an executable
`ht_steps` or `ht_run`. It translates their filesystem decisions into the v2
state protocol while leaving the legacy shell program unchanged.

The compatibility boundary is intentionally narrow: it does not import or
continue an existing v1 task-manager queue. A task first becomes an ordinary
v2 payload with an immutable `job.json`; from submission onward, its v2 marker
and journal history are authoritative.

## Prepare and submit

Initialize the store once with the normal command:

```console
httk-taskmanager init STORE
```

An already instantiated task template can then be prepared separately:

```console
httk-v1-taskmanager prepare TEMPLATE PREPARED \
  --tag silicon-relax --set vasp --priority 4 --attempts 10
httk-taskmanager submit STORE PREPARED --placement project-a/00/17
```

Or it can be prepared and submitted in one operation:

```console
httk-v1-taskmanager submit STORE TEMPLATE \
  --placement project-a/00/17 \
  --tag silicon-relax --set vasp --priority 4 --attempts 10
```

Preparation copies the source and does not consume it. Priorities map from v1
levels 1 through 5 to v2 priorities 100, 300, 500, 700, and 900. `any` at
submission maps to the reserved `default` pool.

Python applications may materialize a template before its job definition is
sealed:

```python
from pathlib import Path

from httk.workflow import WorkflowStore, submit_v1_task


def materialize(payload: Path) -> None:
    (payload / "input.dat").write_text("calculation input\n")


store = WorkflowStore("STORE")
marker = submit_v1_task(
    store,
    "TEMPLATE",
    "project-a/00/17",
    materializer=materialize,
    tag="silicon-relax",
    pool="vasp",
)
```

`prepare_v1_payload` exposes the same preparation API without submission. Its
`instantiate_globals=` option can execute a trusted `ht.instantiate.py`, but it
does not emulate removed v1 Python imports such as `httk.iface`. New
applications should prefer a materializer callback.

## Run

Run only compatibility jobs:

```console
httk-v1-taskmanager run STORE --set any --workers 8
```

`--set NAME` restricts claiming to that v1 task set; `any` accepts every pool.
Useful legacy-style controls are:

- `--wrap` / `-w` to prefix each shell invocation with one executable;
- `--task-timeout` / `-t` to terminate an overlong process group;
- `--attempts` / `-a` to cap the submitted job's legacy retry setting and the
  policy inherited by dynamically created children;
- `--no-bzip2log` / `-b` or `--zstdlog` to select log retention;
- `--httk-v1-root` to use an external v1 installation as `HTTK_DIR`.

Without `--httk-v1-root`, the distribution's bundled
`Execution/tasks/ht_tasks_api.sh` and VASP shell helpers are used.

The ordinary `httk-taskmanager` executes only native `path` jobs, and the v1
manager executes only `httk-v1` jobs. They may therefore be attached to the
same store. Both can still use the common status and request interface:

```console
httk-taskmanager status STORE
httk-taskmanager request STORE JOB_UUID continue \
  --operator "$USER" --reason "manual repair complete"
```

## Shell behavior

For `ht_steps`, the persistent application workspace is
`ht.run.current/`. It is reused across clean step advances and managed retries,
so a VASP workflow may continue updating a large `WAVECAR` in place. No
transactional `data/` output is required. `ht_run` executes at the payload
root, matching v1.

The adapter exports the familiar v1 variables, including `HTTK_DIR`,
`HT_TASK_TOP_DIR`, `HT_TASK_CURRENT_DIR`, `HT_TASK_STEP`,
`HT_TASKMGR_TIMEOUT`, `HT_TASKMGR_SET`, and `HT_TASKMGR_ATTEMPTS`. It also
passes through the v2 restart evidence:

```bash
if [ "$HTTK_WORKFLOW_IS_RESTART" = 1 ]; then
    # This activation has already had an attempt.
fi

if [ "$HTTK_WORKFLOW_UNCLEAN_RESTART" = 1 ]; then
    # The previous attempt ended without a committed outcome.
fi
```

The full structured record is the JSON file named by
`HTTK_WORKFLOW_CONTEXT`. Workspace reuse alone does not imply a restart: a
normal next step receives `HTTK_WORKFLOW_IS_RESTART=0`.

The legacy decisions map as follows:

| v1 decision | v2 result |
| --- | --- |
| `HT_TASK_NEXT step` / exit 2 | Advance to a new activation for `step` |
| `HT_TASK_SUBTASKS step` / exit 3 | Publish children, wait, then advance |
| `HT_TASK_FINISHED` / exit 10 | Succeed |
| `ht_run` exit 0 | Succeed |
| `HT_TASK_BROKEN` / exit 4 | Fail after best-effort `ht_steps freeze` |
| Timeout / exit 99 | Fail after best-effort `ht_steps freeze` |
| Other exit status | Fail with a structured `process_failure` |

Before invoking the shell program, the adapter idempotently removes abandoned
`ht.tmp.atomic.*` directories and completes every published `ht.atomic.*`
rename. It then examines `ht.nextstep`. A committed next-step, finished, or
broken decision is converted directly into an outcome without rerunning the
old step. Thus interruption during legacy atomic replay is safe to repeat.

## Dynamic subtasks

Direct `ht.task.<set>.<id>...waitstart` and `waitstep` directories created
before exit 3 become deterministic v2 child jobs during outcome commit. The
parent waits on an explicit `all_terminal` join, matching v1's rule that a
broken descendant no longer counted as active. Each child performs the same
translation recursively, preserving normal nested subtree behavior.

The original child directory is replaced with a relative `ht.task.*.<status>`
symlink to the canonical child payload. Its suffix is reconciled for manual
inspection (`running`, `waitsubtasks`, `finished`, `broken`, and so on), but
that link is a derived compatibility view. It may be recreated safely and must
never be used as the source of truth.

## Deliberate limitations

- Existing v1 queue trees are not migrated or claimed.
- Legacy task-directory renames are not authoritative state transitions.
- `ht.instantiate.py` is not a promise of source-compatible access to httk v1's
  Python package.
- Subtasks are frozen into an explicit child set at outcome publication.
  Detached grandchildren must be joined by their own parent.
- Arbitrary user shell code and instantiators are trusted code, as they were in
  v1.
