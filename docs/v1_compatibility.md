# *httk* v1 task compatibility

*For operators bringing existing `ht_steps` or `ht_run` task directories onto
the ordinary httk-workflow engine.*

The primary path is a converted workflow package: put the legacy task files and
an `httk_workflow.toml` manifest in one directory, then submit it with the
normal `job new` command. The package language prepares an ordinary job with a
packaged `httk.workflow.languages.httk_v1.v1_runner` path runner. It has no special
manager, capability, or executor; run it with the normal manager and select its
claim pool with `--pool`.

```console
httk workspace init WORKSPACE
httk job new --workspace WORKSPACE --workflow-dir ./legacy-package \
  --placement project-a/00/17
httk workflow run --workspace WORKSPACE --pool vasp
```

## Converted packages

A v1 package selects the language and may set the task pool and retry budget:

```toml
[workflow]
id = "legacy.silicon"

[workflow.runner]
language = "httk-v1"
taskset = "vasp"
attempts = 10

[workflow.inputs.structure]
entry_type = "structures"

[workflow.collect]
file = "collect.py"
```

The package contains executable `ht_steps` or `ht_run`, or one of their
`.template` forms, plus regular support files. Preparation snapshots the
package, renders every `*.template` member, executes `ht.instantiate.py` when
present, and seals the resulting job. Inputs and parameters are available to
the template and instantiator; path-valued structure inputs are loaded through
`httk.core`.

`taskset` becomes the job's claim pool and `attempts` is the legacy retry
budget. The realization forces a persistent `ht.run.current` workdir and no
transactional data. It has no language-default collector, so a package that
needs collection declares `[workflow.collect]`.

## Runtime fidelity

The packaged runner preserves the legacy task protocol while publishing native
workflow outcomes. It translates the legacy task environment, replays
`ht.atomic.*` moves idempotently, resumes `ht_steps` from `ht.run.resume`, and
honors the legacy exit and `ht.nextstep` decisions. `ht_steps freeze` is run on
broken or timed-out work when applicable. `ht.taskmgr.stdout` is archived in
the workdir at completion; `httk_v1.log_compression` defaults to `bzip2` and
also accepts `none` or `zstd`.

The runner preserves the legacy environment names needed by the shell runtime,
including `HTTK_DIR`, `HT_TASK_TOP_DIR`, `HT_TASK_CURRENT_DIR`,
`HT_TASK_STEP`, `HT_TASKMGR_TIMEOUT`, `HT_TASKMGR_SET`, and
`HT_TASKMGR_ATTEMPTS`. The native restart variables and structured
`HTTK_WORKFLOW_CONTEXT` remain available too.

Legacy decisions map to native outcomes as follows:

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

When a legacy task publishes subtasks, discovered `waitstart` and `waitstep`
directories become native child jobs. The parent records the child set and
waits with an `all_terminal` join. Nested v1 subtasks use the same path
recursively, and state-based deduplication prevents a previously registered
legacy directory from being registered again.

The original legacy directories stay in the workdir as directories. They are
not replaced by `ht.task.*` symlinks; the native child payload and state marker
are authoritative.

## Environment knobs

The v1 language declares these workflow environment entries:

| Name | Type | Default | Meaning |
| --- | --- | --- | --- |
| `httk_v1.timeout` | integer | `21600` | legacy process timeout in seconds |
| `httk_v1.wrapper` | string | `""` | optional executable prefix |
| `httk_v1.log_compression` | string | `"bzip2"` | `none`, `bzip2`, or `zstd` |
| `httk_v1.root` | string | packaged compatibility runtime root | `HTTK_DIR` source |

Override these per job with either the CLI or Python API:

```console
httk job new --workspace WORKSPACE --workflow-dir ./legacy-package \
  --environment httk_v1.timeout=3600
```

```python
from httk.workflow import new_job

new_job(workspace, "./legacy-package", environment={"httk_v1.wrapper": "/usr/bin/time"})
```

Environment resolution is documented in {doc}`workflow_packages`: job
override, the declared setting's `HTTK_*` variable, workspace setting, then
the declaration default.

## Explicit one-offs with `--format`

Bare documents and directories are not packages. Use the generic format switch
when the path does not carry its language:

```console
httk job new --workspace WORKSPACE --workflow ./old-task \
  --format httk-v1 --parameter encut=520
```

The same `--format LANG` mechanism selects bare `cwl`, `pwd`, and `jobflow`
documents. A bare v1 directory requires `--format httk-v1`; it is not
auto-matched. `--format` is refused for a manifest package or registered
workflow id, whose language comes from its manifest or registration.

## Finished-tree harvest

Harvest is the only `v1` command retained for already-finished legacy result
trees. It reads `ht.task.*.finished` directories without submitting them:

```python
from httk.workflow.compat.v1 import collect_finished_tree, finished_tasks

tasks = list(finished_tasks("old-results"))
items = collect_finished_tree("old-results", workflow_dir="./legacy-package")
```

`finished_tasks(root)` yields `V1FinishedTask` values for finished task
directories, using the newest dated `ht.run.*` directory. `code_of` reads the
code name and version from lines 2 and 3 of `ht_steps` or `ht_run`; `task_file`
locates plain or `.bz2` members. `collect_finished_tree` calls the package hook
once per task, or accepts an `extract=` callback instead; exactly one of
`workflow_dir` and `extract` is required. A hook failure degrades that task and
the sweep continues.

```console
httk workflow v1 collect --workflow-dir PKG ROOT
httk workflow v1 collect --workflow-dir PKG --into results.sqlite --id-base httk.v1 ROOT
```

Manifest-backed identity survives moving the tree. Without a manifest, the
UUIDv5 identity is derived from the task path and dated run path and therefore
does not survive relocation. The latest dated run is used; `ht.run.current` is
not a finished result.

## Deliberate limitations

- Existing v1 queue trees are read only by the finished-tree harvester; they are
  not migrated or claimed by a workspace manager.
- `ht.instantiate.py` and arbitrary shell code are trusted input; compatibility
  does not recreate the old Python package imports.
- Native child jobs and their state markers are the source of truth, so legacy
  pathname suffixes are not workflow state transitions.
