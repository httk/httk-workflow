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

A Python VASP runner is an ordinary {py:class}`~httk.workflow.Runner` whose
steps spell out their work on the {py:mod}`httk.workflow.codes.vasp` primitives, the
same functions the Bash VASP API wraps. The workflows-vasp runners
(`vasp-relax/run`, `vasp-static/run`, `vasp-relax-static/run`) are the worked
examples: copy one and edit it. Each step reads its job parameters directly with
`a.parameter(...)`, `a.setting(...)` and `a.state`, the way the Bash runner
reads `httk_workflow_parameter`:

- `prepare` copies the payload POSCAR (failing `vasp.input_missing` when it is
  absent), INCAR and POTCAR into the workdir, builds a
  {py:class}`~httk.workflow.codes.vasp.VaspPreparationOptions` from the job
  parameters and calls {py:func}`~httk.workflow.codes.vasp.prepare_vasp_inputs`.
- `run` resolves the `vasp.command` setting, calls
  {py:func}`~httk.workflow.codes.vasp.clean_vasp_outputs` and
  {py:func}`~httk.workflow.codes.vasp.run_vasp`, and advances on a completed run with
  the `classification` and the energy from
  {py:func}`~httk.workflow.codes.vasp.last_oszicar_energy`. Otherwise it plans a remedy
  with {py:func}`~httk.workflow.codes.vasp.plan_vasp_remedy`, fails `vasp.failed`
  when the ladder or the remedy budget is exhausted, and else applies it with
  {py:func}`~httk.workflow.codes.vasp.apply_vasp_remedy`, optionally rattles the POSCAR
  ({py:func}`~httk.workflow.codes.vasp.rattle_poscar`), counts `remedies`, and retries.
- `publish` puts the collected files into transactional data, or only notes
  them when the persistent workdir is the result, and succeeds.

A condensed form of the `run` step of `vasp-relax/run` (the file adds the
command check, the remedy-policy parameter, rattling and log notes):

```python
report = run_vasp(argv, directory=a.workdir, timeout=a.parameter("timeout", 86400.0))
oszicar = a.workdir / "OSZICAR"
energy = last_oszicar_energy(oszicar) if oszicar.is_file() else None
state: dict[str, object] = {"classification": report.classification}
if energy is not None:
    state["energy"] = energy
if report.classification == "completed":
    a.advance("publish", state=state)
    return
applied = int(a.state.get("remedies", 0))
history = job_remedy_history_path(a.payload)
decision = plan_vasp_remedy(report.diagnostics, directory=a.workdir, history_path=history)
if decision.give_up or applied >= int(a.parameter("maximum_remedies", 8)):
    a.state.merge(state)
    a.fail("vasp.failed", f"VASP {report.classification}", details=decision.as_mapping())
    return
apply_vasp_remedy(decision, directory=a.workdir, history_path=history)
a.state.merge({**state, "remedies": applied + 1})
a.retry(f"applied the {decision.policy} remedy for {decision.problem}")
```

A relax-then-static runner (`vasp-relax-static/run`) adds a `promote` step that
archives the relaxation, turns its CONTCAR into the next POSCAR with
{py:func}`~httk.workflow.codes.vasp.contcar_to_poscar`, and re-derives the inputs
with the static tags; the Bash counterpart is the Bash VASP API below.

## What stays in httk-workflow

- {py:mod}`httk.workflow.codes.vasp`: the dependency-free helpers the runners import —
  input preparation (`prepare_vasp_inputs`, k-point grids, POTCAR assembly),
  diagnostics, the reviewed remedy ladder (`plan_vasp_remedy`,
  {py:func}`~httk.workflow.codes.vasp.register_remedy_policy`), supervised execution
  (`run_vasp`), and the result collectors in `httk.workflow.codes.vasp.collect`. See
  {doc}`runtime_helpers`.
- The Bash VASP API: a Bash runner sources `$HTTK_WORKFLOW_VASP_BASH_API` after
  `$HTTK_WORKFLOW_BASH_API`; see {doc}`sdks/bash_api`.

A group whose practice differs copies a workflow package from the repository
and edits it, or keeps the workflows and registers its own remedy policy.
