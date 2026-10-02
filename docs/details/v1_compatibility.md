# *httk* v1 task compatibility

This page is for operators who bring existing `ht_steps` or `ht_run` task
directories onto the ordinary *httk-workflow* engine.

The primary path is a converted workflow package. Put the legacy task files and
an `httk_workflow.toml` manifest in one directory and submit it with the normal
`job new` command. The package prepares an ordinary job that uses the packaged
path runner `httk.workflow.compat.v1.v1_runner`. There is no special manager,
capability or executor: run it with the normal manager and select its claim
pool with `--pool`.

```console
httk workspace init WORKSPACE
httk job new --workspace WORKSPACE --workflow-dir ./legacy-package \
  --placement project-a/00/17
httk workflow run --workspace WORKSPACE --pool vasp
```

## Converted packages

A v1 package selects the `httk-v1` format and may set the task pool and retry
budget:

```toml
[workflow]
name = "legacy.silicon"

[workflow.runner]
format = "httk-v1"
taskset = "vasp"
attempts = 10

[workflow.inputs.structure]
entry_type = "structures"

[workflow.collect]
file = "collect.py"
```

The package contains an executable `ht_steps` or `ht_run`, or one of their
`.template` forms, plus regular support files. Preparation snapshots the
package, renders every `*.template` member, runs `ht.instantiate.py` when
present, and seals the job. The template and instantiator see the inputs and
parameters; path-valued structure inputs are loaded through `httk.core`.

`taskset` becomes the job's claim pool and `attempts` is the legacy retry
budget. The realization forces a persistent `ht.run.current` workdir and no
transactional data. It has no format-default collector, so a package that
needs collection declares `[workflow.collect]`.

## Runtime fidelity

### Task protocol

The packaged runner keeps the legacy task protocol and publishes native
workflow outcomes. It:

- translates the legacy task environment;
- replays `ht.atomic.*` moves idempotently;
- resumes `ht_steps` from `ht.run.resume`;
- honors the legacy exit status and `ht.nextstep` decisions;
- runs `ht_steps freeze` on broken or timed-out work when applicable;
- archives `ht.taskmgr.stdout` in the workdir at completion, compressed with
  `httk_v1.log_compression` (default `bzip2`; `none` and `zstd` also
  accepted).

The shell runtime keeps its legacy environment names, including `HTTK_DIR`,
`HT_TASK_TOP_DIR`, `HT_TASK_CURRENT_DIR`, `HT_TASK_STEP`,
`HT_TASKMGR_TIMEOUT`, `HT_TASKMGR_SET` and `HT_TASKMGR_ATTEMPTS`. The native
restart variables and the structured `HTTK_WORKFLOW_CONTEXT` are also
available.

### Decision mapping

| Legacy decision | Native result |
| --- | --- |
| `HT_TASK_NEXT step` / exit 2 | advance to `step` |
| `HT_TASK_SUBTASKS step` / exit 3 | create native children, wait, then advance |
| `HT_TASK_FINISHED` / exit 10 | succeed |
| `ht_run` exit 0 | succeed |
| `HT_TASK_BROKEN` / exit 4 | fail after best-effort `freeze` |
| timeout / exit 99 | fail after best-effort `freeze` |
| any other exit status | structured `process_failure` |

## Dynamic subtasks under the native manager

When a legacy task publishes subtasks, the discovered `waitstart` and
`waitstep` directories become native child jobs. The parent records the child
set and waits with an `all_terminal` join. Nested v1 subtasks follow the same
path recursively. State-based deduplication stops a legacy directory from being
registered twice.

The original legacy directories stay in the workdir as directories; they are
not replaced by `ht.task.*` symlinks. The native child payload and state marker
are authoritative.

## Environment knobs

The v1 realization declares these workflow environment entries:

| Name | Type | Default | Meaning |
| --- | --- | --- | --- |
| `httk_v1.timeout` | integer | `21600` | legacy process timeout in seconds |
| `httk_v1.wrapper` | string | `""` | optional executable prefix |
| `httk_v1.log_compression` | string | `"bzip2"` | `none`, `bzip2`, or `zstd` |
| `httk_v1.root` | string | packaged compatibility runtime root | `HTTK_DIR` source |

Override them per job from the CLI or the Python API:

```console
httk job new --workspace WORKSPACE --workflow-dir ./legacy-package \
  --environment httk_v1.timeout=3600
```

```python
from httk.workflow import new_job

new_job(workspace, "./legacy-package", environment={"httk_v1.wrapper": "/usr/bin/time"})
```

Resolution order is the job override, the declared setting's `HTTK_*`
variable, the workspace setting, then the declaration default; see
{doc}`workflow_packages`.

## Bare directories and `--format`

`job new` does not accept a bare v1 directory, and this CLI has no
bare-directory `--format` mode. Add a workflow manifest and submit with
`--workflow-dir`, or import the legacy machine setup with `remote import-v1`.

## Finished-tree harvest

Harvest is the only retained `v1` command. It reads already-finished
`ht.task.*.finished` directories without submitting them:

```python
from httk.workflow.compat.v1 import collect_finished_tree, finished_tasks

tasks = list(finished_tasks("old-results"))
items = collect_finished_tree("old-results", workflow_dir="./legacy-package")
```

- `finished_tasks(root)` yields a `V1FinishedTask` per finished task
  directory, using its newest dated `ht.run.*` directory.
- `code_of` reads the code name and version from lines 2 and 3 of `ht_steps`
  or `ht_run`.
- `task_file` locates plain or `.bz2` members.
- `collect_finished_tree` calls the package hook once per task, or an
  `extract=` callback instead. Exactly one of `workflow_dir` and `extract` is
  required. A hook failure degrades that task and the sweep continues.

```console
httk workflow v1 collect --workflow-dir PKG ROOT
httk workflow v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

With a manifest, identity survives moving the tree. Without one, the UUIDv5
identity derives from the task path and dated run path and does not survive
relocation. The latest dated run is used; `ht.run.current` is not a finished
result.

## Limitations

- Existing v1 queue trees are only read by the finished-tree harvester. A
  workspace manager does not migrate or claim them.
- `ht.instantiate.py` and arbitrary shell code are trusted input. The
  compatibility layer does not recreate the old Python package imports.
- Native child jobs and their state markers are the source of truth, so legacy
  pathname suffixes are not workflow state transitions.
