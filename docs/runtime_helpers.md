# Native runner helpers

Native *httk₂* runners receive their attempt identity and filesystem paths through
manager-provided environment variables. `AttemptRuntime` validates that
context and publishes outcomes with the required temporary-directory plus
atomic-rename protocol:

```python
from httk.workflow import AttemptRuntime, run_command

runtime = AttemptRuntime.initialize()
result = run_command(["simulation", "--input", "input.dat"], timeout=3600)

if result.timed_out:
    runtime.retry("simulation timed out")
elif result.returncode:
    runtime.fail(
        "simulation_failure",
        f"simulation exited with status {result.returncode}",
    )
else:
    runtime.advance("collect")
```

Other convenience outcomes are `succeed()` and `pause(reason)`. The API uses
argv arrays and structured *httk₂* outcomes; it deliberately does not reproduce
the *httk* v1 exit-code and `ht.nextstep` interface.

`initialize()` also completes any sealed `ReplayableWorkdirBatch` left by an
interrupted attempt. `runtime.state` is atomic JSON application state, not an
executable shell fragment.

## Transactions, children, and joins

Complex outcomes are assembled below the current attempt-control directory and
have no effect until the final publication rename:

```python
from httk.workflow import AttemptRuntime

runtime = AttemptRuntime.initialize()
outcome = runtime.outcome()
transaction = outcome.transaction()
transaction.make_dir("results-dir", "results")
transaction.put_file("energy", "energy.json", "results/energy.json")
child = outcome.add_child("prepared-child", "branches/01")
outcome.publish("wait", next_step="aggregate")
```

The default wait condition is `all_succeeded`. Use `JoinSpec` to select
`all_terminal`, `any_succeeded`, or `at_least`. `prepare_job_payload` and
`JobSpec` create validated native payloads for submission or dynamic children.

`ProcessSupervisor` adds streamed stdout/stderr and followed-file monitoring,
process-group timeout handling, and versioned executable checkers. A checker
receives `httk-workflow-checker-event` version 1 JSON lines and emits
`httk-workflow-checker-result` version 1 JSON lines. Commands and checker
commands are always argument arrays.

## VASP files

The dependency-free VASP helpers cover preparation, supervised execution,
structured diagnosis, and explicit remedies:

```python
from httk.workflow import (
    assemble_potcar,
    automatic_kpoint_grid,
    update_incar,
    write_automatic_kpoints,
)

grid = automatic_kpoint_grid(40, poscar="POSCAR")
write_automatic_kpoints(grid, "KPOINTS")
update_incar({"ENCUT": 520, "ISPIN": 2}, "INCAR")
assemble_potcar("/data/vasp/potpaw_PBE", poscar="POSCAR", output="POTCAR")
```

The API also provides `read_poscar_header`, `suggested_magnetic_moments`,
`read_incar`, `calculate_nbands`, `prepare_vasp_inputs`, `run_vasp`,
`plan_vasp_remedy`, `apply_vasp_remedy`, output extraction/cleaning, and
`contcar_to_poscar`. `validate_vasp_workdir` checks VASP's conservative path
limit and `clean_vasp_outputs` performs explicit pre-run cleanup while
preserving requested files. Diagnosis never changes inputs. The bounded
`reviewed-v1` policy must be planned and applied explicitly, and records every
change in structured history.

These functions are independent *httk₂* interfaces. They do not import the Python *httk* v1
`httk.task.ht_tasks_api` or `httk.task.vasptools` implementations.
The native runtime, supervision, utility, and VASP modules were audited to use
only the Python standard library and other `httk.workflow` modules. No numeric
or atomistic capability package is required. Legacy Unix-compress
`POTCAR.Z` is the one external-format exception: it is read through a checked
argv invocation of `gzip` or `uncompress` when either executable is available.

Native Bash runners use the same implementation through {doc}`native_bash_api`.

For unchanged *httk* v1 `ht_steps`, continue sourcing the exact historic filenames
under `$HTTK_DIR/Execution/tasks/`. Those are thin redirects to the attributed
compatibility implementation described in [*httk* v1 task
compatibility](v1_compatibility.md).
