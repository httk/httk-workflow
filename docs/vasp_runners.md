# Packaged VASP runners

*For campaigns that want an ordinary VASP calculation without writing a runner at all.*

*httk-workflow* ships complete VASP runners. A campaign that wants the ordinary
thing — relax a structure, run a single point, or both in sequence — writes no
runner at all: it submits jobs that name one of the installed files in
`httk.workflow.vasp.runners`.

| Runner | Workflow | Steps |
| --- | --- | --- |
| `vasp_relax.py` | `httk.vasp.relax` | prepare, run, collect |
| `vasp_relax.sh` | `httk.vasp.relax` | prepare, run, collect |
| `vasp_static.py` | `httk.vasp.static` | prepare, run, collect |
| `vasp_relax_static.py` | `httk.vasp.relax-static` | prepare, run, promote, static, collect |

`vasp_relax.sh` is the Bash authoring of exactly the same workflow as
`vasp_relax.py`: the same steps, the same job inputs, the same job state, the same
failure codes, and the same files in the workdir and in the published data. It is
both a working runner and the proof that the two authoring SDKs are one protocol.

## Submitting a job

Reference a packaged runner as an installed runner, which needs no publication —
the manager resolves the reserved `pkg:` form inside its own module allowlist, and
`httk.workflow` is in that allowlist by default:

```python
from httk.workflow import JobSpec, Workspace, prepare_job_payload
from httk.workflow.runners import runner_reference

reference = runner_reference("vasp_relax.py")
job = prepare_job_payload(
    "prepared-job",
    JobSpec(
        name="Relax silicon",
        workflow="httk.vasp.relax",
        runner_path=str(reference["path"]),      # pkg:httk.workflow.vasp.runners/vasp_relax.py
        runner_source="installed",
        runner_sha256=str(reference["sha256"]),
        initial_step="prepare",
        data_mode="transactional",
        inputs={"kpoint_density": 30.0, "incar_tags": {"ENCUT": 520}},
    ),
)
```

The payload carries the structure and the INCAR it starts from:

```console
mkdir -p prepared-job/files
cp POSCAR INCAR POTCAR prepared-job/files/
httk project init --name workflow
httk workflow job submit prepared-job --placement project/si
httk workflow manager run
```

Every `httk workflow` command names the *registered* workspace, never a path;
`workspace init` both creates the workspace and registers the name — see
{doc}`workflow_cli`.

The same file can be published to the workspace runner store instead, which pins
it by digest for a whole campaign:

```python
workspace = Workspace.initialize("workflow-workspace")
published = workspace.publish_runner("/path/to/vasp_relax.py", name="vasp/relax.py")
```

## The command to run

The VASP command is the `vasp.command` application setting, which the runners
resolve through `a.setting("vasp.command")` — most
specific first: a job's own `vasp.command` input, then `HTTK_VASP_COMMAND` in the
environment, then the workspace's `vasp.command` setting, and finally the job's
legacy `vasp_command` input. The chosen string is split the way a shell would.

Two spellings therefore both work, and mean different things. A machine that
invokes VASP its own way still exports the environment variable, and it wins over
everything a workspace configures — deployment state a job submitted elsewhere
cannot know:

Configure the command once on the workspace, so no one has to export it for
every job — and a workspace bound to a remote is even seeded with it from the
remote definition when it is created:

```console
httk workflow workspace settings set vasp.command "srun -n 32 vasp_std"
httk workflow manager run --pool vasp
```

The pseudopotential library resolves the same way, as `vasp.pseudo_library`
(environment `HTTK_VASP_PSEUDO_LIBRARY`), falling back to the job's
`pseudopotential_library` input. See {doc}`sdk_parity` for the resolution table
and {doc}`workflow_cli` for `workspace settings`.

## Job inputs

Every input is optional and every one is documented, with its default, in the API
reference of {py:mod}`httk.workflow.vasp.runners`. The ones a campaign normally sets are
`poscar`, `incar_tags`, `kpoint_density`, `pseudopotential_library`, `timeout`, and
`collect`.

Two job members are not inputs but requirements: `workdir.mode` must be
`persistent`, because the inputs a remedy rewrites have to be the inputs the next
attempt reads, and `data.mode` must be `transactional` for the collected files to
be published. With `data.mode` `none` the results simply stay in the workdir.

## What a run does

`prepare` stages the payload files into the workdir, then derives what the job did
not give: the k-point grid from `kpoint_density`, `EDIFF` and `EDIFFG` from
`accuracy_per_atom`, `MAGMOM` from the structure, `NBANDS` from the POTCAR, and a
POTCAR from `pseudopotential_library` when none was staged. Explicit `incar_tags`
are applied first and win over every derived value.

`run` executes VASP under supervision, writes `vasp-run-report.json`, and acts on
the classification:

| Classification | What the runner does |
| --- | --- |
| `completed` | records the energy and advances |
| anything else with a remedy left | applies exactly one remedy and asks for another attempt |
| anything else without one | fails with `vasp.failed`, carrying the decision that ran out |

Remedies are the bounded ladder of the policy named by `remedy_policy`, which
defaults to `reviewed-v1`; they are planned and applied explicitly and never
invented: see {doc}`runtime_helpers`. A group with its own reviewed practice
registers a policy and names it in the job inputs rather than editing a runner. The
ladder position is recorded in the job state directory, so it survives every attempt
of the job, and `maximum_remedies` bounds how many a single job may apply.

`collect` publishes the files named by `collect` into the job's transactional data,
under `data_prefix`. `vasp_relax_static.py` archives the relaxation inside the
workdir before the single point overwrites it, and publishes the two stages side by
side as `relax/` and `static/`.

## Failure codes

| Code | Meaning |
| --- | --- |
| `vasp.command_missing` | no `vasp.command` resolved — set it with `httk workflow workspace settings set vasp.command '...'`, or use `HTTK_VASP_COMMAND` as a deployment override; no `vasp_command` input was provided |
| `vasp.input_missing` | the structure named by `poscar` is not in the payload |
| `vasp.no_relaxed_structure` | the chained runner's relaxation left no CONTCAR |
| `vasp.failed` | VASP did not complete and no remedy remained |

## Formation energies

`vasp_relax_static.py` is the *httk* v1 formation-energy template without its
database half. A formation energy is assembled from the total energies of several
finished jobs and the elemental references they are compared against, which is an
analysis over jobs rather than a step inside one. What the runner guarantees is the
pair of numbers that assembly needs: one relaxed structure and one total energy of
it, each with the evidence of how it was produced.

## Copying one

A group whose practice differs from the packaged one should copy the file and edit
it. Each runner is one self-contained file that imports nothing but an installed
*httk-workflow*, so a copy is a complete runner, publishable as it stands. A group
with its own reviewed remedy practice can also keep the packaged runners and
register its own policy with
{py:func}`httk.workflow.vasp.register_remedy_policy`.
