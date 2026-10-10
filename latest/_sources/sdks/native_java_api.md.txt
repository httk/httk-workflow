# Native Java runner API

The Java SDK is a standalone, `java.base`-only bridge client. It is one source
file, `native/java/HttkWorkflow.java`, with nested `Runner`, `Attempt`,
`BridgeError`, and `Gather` types. Every bridge-backed verb uses
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge` through
`ProcessBuilder`; only `--describe` is native. It needs no JNI, C linkage,
Gradle, Maven, or third-party dependency, and compiles with Java 17.

## A complete runner

```java
public static void main(String[] args) {
    new HttkWorkflow.Runner("my.workflow", "prepare", "run")
        .step("prepare", attempt -> {
            attempt.advance("run");
            return 0;
        })
        .step("run", attempt -> {
            attempt.succeed();
            return 0;
        })
        .main(args);
}
```

`Runner` validates the workflow and step names against
`[A-Za-z0-9._-]+` before describing. `--describe` and
`HTTK_WORKFLOW_DESCRIBE=1` print the hand-composed canonical description with
byte-sorted steps. `main` dispatches the manager-selected step and owns the
process exit status. A handler that returns nonzero or throws publishes an
`error.json` breadcrumb with exception `JavaError`; a handler that publishes
nothing becomes `no_outcome`, and an unregistered step becomes `unknown_step`.

The source is in the default Java package so it can be copied beside a runner
without a package tree. Compile it with the runner:

```console
javac --release 17 -Werror -Xlint:all -d classes HttkWorkflow.java Relax.java
```

`examples/relax_java/` includes this build, a POSIX `relax` launcher, and a
three-step mock-VASP-compatible relaxation. Its `httk_workflow.toml` package
uses the required `run` entry to delegate to `relax`, so publication transfers
the launcher and its compiled `classes/` directory together as one pinned tree.

## Values and errors

Reads return `Optional<String>`: `Optional.empty()` is the bridge's exit `1`
absent answer, while a present empty string is `Optional.of("")`. Exit `2` or
higher throws `BridgeError`, which also carries the bridge status through
`status()`. Bridge spawn failures and an unset `HTTK_WORKFLOW_PYTHON` are
`BridgeError`s. Command methods return the bridge exit status, including the
classified statuses from `run`. Captured stdout has all trailing newlines
removed; stderr and stdin remain inherited.

`Attempt.log(level, message)` is local and writes the UTC timestamped
`LEVEL MESSAGE` line to stderr, matching the other interpreted SDKs.

## Method table

| Java | Bridge verb |
| --- | --- |
| `Runner(String, steps)`, `step`, `main` | runner registration and dispatch |
| `invoke` | caller-supplied verb and arguments |
| `context`, `parameter`, `setting`, `environment` | same-named reads |
| `stateGet`, `stateSet`, `stateDelete`, `stateMerge` | `state-get`, `state-set`, `state-delete`, `state-merge` |
| `declaration`, `declare` | `declaration`, `declare` |
| `runlogNote`, `runlogHeadline`, `runlogAppend` | `runlog` |
| `log` | local stderr helper |
| `put`, `remove` | `put`, `remove` |
| `children`, `child`, `spawn` | `children`, `child`, `spawn` |
| `advance`, `gather` | `advance`, `gather` |
| `succeed`, `fail`, `retry`, `pause` | same-named commands |
| `batch`, `jobPrepare`, `workdirApply` | same-named commands |
| `run`, `calc`, `templateRender` | `run`, `calc`, `template` |
| `compress`, `decompress` | same-named commands |

`Gather` accepts optional `when`, `count`, `onImpossible`, and `priority`
values through chainable setters. Array arguments are passed as literal
`ProcessBuilder` arguments and never through a shell.
