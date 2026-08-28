# A VASP relaxation runner in Java

`Relax.java` has the three-step `prepare`, `run`, and `publish` shape, using the
native Java SDK. `HttkWorkflow.java` is vendored beside it so this package is
self-contained; the parity test keeps the copy identical to the shipped SDK.

The manifest declares a compiled package. Build and register its classes once
on each machine before starting a manager:

```console
httk workflow build --workspace WORKSPACE ./relax_java
httk workflow job new --workspace WORKSPACE --workflow-dir ./relax_java --step prepare \
    --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow run --workspace WORKSPACE
httk workflow collect --workspace WORKSPACE
```

`make` remains useful for local SDK exploration:

```console
make
./relax --describe
```

Build registration compiles the vendored sources and stores `classes/` in the
workspace's machine-local runner-build cache. Package publication transfers
sources only, so the same package can be built independently on another
machine. When the manager runs it, `run` reads the compiled classes from
`$HTTK_WORKFLOW_RUNNER_ARTIFACTS`.

The example is intentionally minimal and mock-VASP compatible; the packaged
`vasp-relax` runner adds the production restart and diagnostic behavior.
