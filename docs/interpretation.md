# Workflow interpretation

The deep-import module `httk.workflow.interpretation` dispatches a harvested
job to the interpreter registered for its workflow. `InterpretedRun` contains
the `httk.core.Run`, the domain entries that `SqlStore.save()` and entry
providers consume, and any `ProductLink` values; run edges refer to those
entries by their served ids.

Importing `httk.workflow.vasp` registers interpreters for the three built-in
VASP workflows. They load published structures and energies from the runner's
transactional data layout, rebuild the provenance run with matching artifact
and output edges, preserve declared input edges, and link an input structure to
a relaxed structure when its served id is declared. Static jobs produce only a
total-energy entry and never a product link.

Relax and static read `data/vasp/{CONTCAR,OUTCAR}` by default; relax-static
reads `data/relax/CONTCAR` and `data/static/OUTCAR`. A non-empty `data_prefix`
is prepended to these paths.

```python
from httk.workflow import harvest
from httk.workflow.interpretation import interpret

record = next(harvest(workspace))
interpreted = interpret(record)
for entry in interpreted.entries:
    store.save(entry)  # storing entries and products is the httk-data side
```
