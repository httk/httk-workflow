# A VASP relaxation runner in Java

`Relax.java` has the same three-step `prepare`, `run`, and `publish` shape as
the Rust and Perl examples, using the native Java SDK in
`src/httk/workflow/native/java/`. It is a std-only bridge client: every
`Attempt` verb execs `$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge`.
The workflow id is `httk.vasp.relax-java`.

Build the SDK and example into `classes/`:

```console
make
./relax --describe
```

This is a workflow package directory. The manifest's required `run` entry
delegates to the `relax` launcher, and package publication hashes and transfers
the whole directory—including `relax`, `run`, and `classes/`—as one
digest-pinned tree. That is why this Java example uses the package form while
a single-binary runner can be published as one file.

From a separate workspace directory, drive one relaxation with the package
job-new sequence:

```console
cd ..
httk project init --name relax-java
httk workflow workspace init ./relax-java-workspace --name default
cd relax-java-workspace
httk workflow job new --workflow-dir ../relax_java --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

The example is intentionally minimal and mock-VASP compatible; the packaged
`vasp-relax` runner adds the production restart and diagnostic behavior.
