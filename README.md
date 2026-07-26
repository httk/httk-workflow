# httk-workflow

*httk-workflow* is the filesystem-native workflow engine for
[*httk₂*](https://github.com/httk/httk2).

It provides the `httk.workflow` Python API and registers `httk workflow`, the
canonical command tree for everything below. The `httk-taskmanager` and
`httk-v1-taskmanager` executables remain installed as aliases of that tree; the
second one prepares and executes legacy `ht_steps`/`ht_run` task templates
through the same *httk₂* workspace. Jobs communicate through atomically
published filesystem state, so interrupted managers and calculations can be
recovered without cleanup hooks.

From nothing to a finished VASP relaxation, without writing a runner:

```console
httk workflow workspace init workflow-workspace --extension transactional-data-v1
httk workflow job new workflow-workspace --template vasp-relax --from POSCAR --tag silicon
export HTTK_VASP_COMMAND="$PWD/examples/mock_vasp.py"   # or: srun -n 32 vasp_std
httk workflow manager run workflow-workspace --until-idle
httk workflow harvest workflow-workspace
```

[`docs/quickstart.md`](docs/quickstart.md) explains each command, and
`examples/quickstart.sh` runs the whole sequence — with the mock VASP above
standing in for VASP on a machine that has none.

The unified CLI also manages XDG configuration, signed projects, versioned
computer adapters, and crash-recoverable detached transfer:

```console
httk workflow project init . --name example
httk workflow project manifest create
httk workflow workspace status .
```

The precise on-disk protocol is documented in
[`docs/workflow_filesystem_api.md`](docs/workflow_filesystem_api.md). See
[`docs/v1_compatibility.md`](docs/v1_compatibility.md) for the compatibility
executor and its deliberate boundary.
