# Native runner helpers

Native v2 runners receive their attempt identity and filesystem paths through
manager-provided environment variables. `AttemptRuntime` validates that
context and publishes outcomes with the required temporary-directory plus
atomic-rename protocol:

```python
from httk.workflow import AttemptRuntime, run_command

runtime = AttemptRuntime.from_environment()
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
argv arrays and structured v2 outcomes; it deliberately does not reproduce
the v1 exit-code and `ht.nextstep` interface.

## VASP files

The dependency-free VASP helpers are small data transformations suitable for
native runners:

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
`read_incar`, `last_oszicar_energy`, and `contcar_to_poscar`. These functions
are independent v2 interfaces. They do not import the Python v1
`httk.task.ht_tasks_api` or `httk.task.vasptools` implementations.

For unchanged v1 `ht_steps`, continue sourcing the exact historic filenames
under `$HTTK_DIR/Execution/tasks/`. Those are thin redirects to the attributed
compatibility implementation described in [httk v1 task
compatibility](v1_compatibility.md).
