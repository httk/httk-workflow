# Native runner helpers

*For authors writing a workflow runner in Python.* A runner is one program
implementing the steps of one workflow: the manager launches it once per
attempt, names the step, and reads exactly one published outcome back. There
is no graph language — a step decides at run time what to spawn and what runs
next:

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

`job new --workflow ./relax.py` publishes and digest-pins it; the same surface
exists in Bash, C, Fortran, Rust, Perl, Ada, C++, and Java ({doc}`sdks/index`),
and the normative operation table is {doc}`sdks/sdk_parity`.

The full guide, {doc}`details/runtime_helpers`, covers the complete `Attempt`
surface — parameters, settings, declared environment, state, transactional
data, spawning and gathering children (`ChildSpec`, join conditions),
outcomes and retry semantics, logging, and a full defect-campaign example.
