# Writing runners

A runner is one program implementing the steps of one workflow. The manager
launches it once per attempt, names the step, and reads exactly one published
outcome back. There is no graph language; a step decides at run time what to
spawn and what runs next.

```python
#!/usr/bin/env python3
from httk.workflow import Runner

run = Runner("demo.relax")


@run.step
def prepare(a):
    a.put("POSCAR", "POSCAR")            # stage into the job's data
    a.advance("relax")


@run.step
def relax(a):
    result = a.run(["vasp-or-mock"], timeout=3600)
    if result.returncode:
        a.fail("relax_failed", "relaxation failed", retryable=True)
    else:
        a.succeed()


raise SystemExit(run.main())
```

`httk job new --from-runner ./relax.py --step prepare` installs the file in
the workspace as an `adhoc:` workflow and creates a job of it. Inside a
step, the `Attempt` object `a` gives the job's parameters, settings and
declared environment, a private `state` that survives retries, the workdir and
payload paths, data commits (`put`, `transaction`), and the outcomes: `advance`, `retry`,
`succeed`, `fail`. `a.spawn` fans a job out into children that run this
runner's steps and `a.gather` waits for them; `a.call` runs another workflow
as a child ({doc}`workflow_packages`).

The same surface exists in Bash, C, Fortran, Rust, Perl, Ada, C++ and Java
({doc}`sdks/index`), with {doc}`sdks/sdk_parity` as the normative operation
table.

The full guide, {doc}`details/runtime_helpers`, covers the complete `Attempt`
surface, child specifications and join conditions, outcome and retry
semantics, logging, and a complete defect-campaign example;
{doc}`details/composing_workflows` covers calling other workflows and sharing
files with children.
