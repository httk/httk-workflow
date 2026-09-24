# VASP workflows

*For campaigns that want an ordinary VASP calculation without writing a runner at all.*

The ready-made VASP workflows live in the
[workflows-vasp](https://github.com/httk/workflows-vasp) repository, one
workflow package per subdirectory. *httk-workflow* does not bundle them; it
ships the VASP helper API they are built on, listed at the end of this page.

| URI | Short name | What it does |
| --- | --- | --- |
| `git+https://github.com/httk/workflows-vasp#vasp-relax` | `vasp.relax` | relax a structure with the reviewed remedy ladder |
| `git+https://github.com/httk/workflows-vasp#vasp-relax-bash` | `vasp.relax-bash` | the same relaxation, authored in Bash |
| `git+https://github.com/httk/workflows-vasp#vasp-static` | `vasp.static` | one single-point calculation of a fixed structure |
| `git+https://github.com/httk/workflows-vasp#vasp-relax-static` | `vasp.relax-static` | relax, then evaluate the relaxed structure statically |

Each takes one required input, `structure`, staged as `files/POSCAR`. Their job
parameters, failure codes, postprocess scripts (`relaxation-report`,
`relaxation-plot`) and result layout are documented in that repository.

## Install, run, uninstall

Referencing a URI fetches and installs the workflow; the job records its
canonical URI, pinned to the full commit (append `@<ref>` before `#` to choose a
branch, tag or commit). `httk workflow install` does the same without creating a
job. Once installed, the short name selects it:

```console
httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" default
httk job new --workflow vasp.relax --input structure=POSCAR --tag silicon
httk workflow run
httk workflow collect
httk workflow uninstall vasp.relax
```

`httk plugin install git+https://github.com/httk/workflows-vasp` installs all
four at once as a plugin instead. See {doc}`workflow_uris` for URI resolution,
short names and the trust model.

The VASP command is the `vasp.command` application setting, resolved most
specific first: a job's own `vasp.command` parameter, then `HTTK_VASP_COMMAND`
in the environment, then the workspace setting. The pseudopotential library
resolves the same way as `vasp.pseudo_library` (`HTTK_VASP_PSEUDO_LIBRARY`).
See {doc}`sdks/sdk_parity` for the resolution table. The workflows default to
`data.mode="none"`: the persistent `run/` workdir is the result; pass
`--data-mode transactional` to `job new` to also publish a curated copy into
`data/`.

## Writing a VASP runner in Python

The workflows-vasp Python runners are built on the step API of
{py:mod}`httk.workflow.vasp` (defined in `httk.workflow.vasp.steps`): each
runner is a few step declarations, and the attempt-level work — staging the
payload inputs, one supervised VASP run with the reviewed remedy ladder, promoting
a relaxation to a single point, and publishing a stage — is imported. A
relaxation runner is this whole file:

```python
from httk.workflow import Attempt, Runner
from httk.workflow.vasp import publish_vasp_files, run_vasp_step, stage_vasp_inputs, vasp_data_prefix

run = Runner("mygroup.vasp-relax")


@run.step
def prepare(a: Attempt) -> None:
    if stage_vasp_inputs(a) is not None:
        a.advance("run")


@run.step(name="run")
def run_step(a: Attempt) -> None:
    run_vasp_step(a, next_step="publish")


@run.step
def publish(a: Attempt) -> None:
    publish_vasp_files(a, prefix=vasp_data_prefix(a))
    a.succeed()


if __name__ == "__main__":
    raise SystemExit(run.main())
```

`stage_vasp_inputs` fails the job `vasp.input_missing` when the structure is
absent; {py:func}`~httk.workflow.vasp.run_vasp_step` advances on a completed
run, applies one remedy and retries, or fails `vasp.failed`
(`vasp.command_missing` without a command), and keeps the job state
`classification`, `energy` and `remedies`. A relax-then-static runner adds a
`promote` step calling `promote_vasp_relaxation(a, next_step="static")`
({py:func}`~httk.workflow.vasp.promote_vasp_relaxation`, which fails
`vasp.no_relaxed_structure` without a CONTCAR) and a `static` step running
`run_vasp_step` again. A single-point runner stages with
`stage_vasp_inputs(a, extra_tags=vasp_static_tags(a))`, and
`vasp_data_prefix(a)` reads the `data_prefix` parameter a stage publishes
under. The step functions read the same job parameters as the workflows-vasp
runners; the Bash counterpart is the Bash VASP API below.

## What stays in httk-workflow

- {py:mod}`httk.workflow.vasp`: the dependency-free helpers the runners import —
  input preparation (`prepare_vasp_inputs`, k-point grids, POTCAR assembly),
  diagnostics, the reviewed remedy ladder (`plan_vasp_remedy`,
  {py:func}`~httk.workflow.vasp.register_remedy_policy`), supervised execution
  (`run_vasp`), the attempt-level steps above (`httk.workflow.vasp.steps`),
  and the result collectors in `httk.workflow.vasp.collect`. See
  {doc}`runtime_helpers`.
- The Bash VASP API: a Bash runner sources `$HTTK_WORKFLOW_VASP_BASH_API` after
  `$HTTK_WORKFLOW_BASH_API`; see {doc}`sdks/native_bash_api`.

A group whose practice differs copies a workflow package from the repository
and edits it, or keeps the workflows and registers its own remedy policy.
