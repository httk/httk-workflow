# Run provenance

*For workflow authors and data-layer authors connecting one `JobRecord` to
one stored `httk.core.Run`.*

The `provenance` declaration describes the entries one workflow execution
consumed, created, and returned. All members are optional:

```json
{
  "workflow_declaration_uri": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax",
  "inputs":    {"initial_structure": {"type": "structures", "id": "<served id>"}},
  "artifacts": {"relaxed_structure": {"type": "structures", "id": "..."}},
  "outputs":   {"total_energy":      {"type": "_httk_records", "id": "..."}}
}
```

The object keys are labels, unique per side. Targets are loose served-entry
references. The declaration is carried verbatim by workflow; this page
documents the collection used by `run_record`.

File-valued output roles yield run edges with `type = "files"` and the
corresponding `FileRecord` id, so stored provenance names the file entry
directly.

## Declared and observed

Inputs known while scaffolding can be declared in `JobSpec`:

```python
declared = {
    "workflow_declaration_uri": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax",
    "inputs": {"initial_structure": {"type": "structures", "id": "structures/si"}},
}
prepare_job_payload(payload, JobSpec(..., declarations={"provenance": declared}))
```

At collect time, a runner writes the complete observed document once produced
entry ids exist:

```python
a.declare("provenance", {
    "workflow_declaration_uri": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax",
    "inputs": {"initial_structure": {"type": "structures", "id": "structures/si"}},
    "artifacts": {"relaxed_structure": {"type": "structures", "id": "structures/si-relaxed"}},
    "outputs": {"total_energy": {"type": "_httk_records", "id": "records/energy-1"}},
})
```

Observed replaces declared wholesale; it is a full replacement document, not
a merge. If no provenance document exists, `run_record` still uses the `$id`
from the `workflow` declaration when available.

The end-to-end handoff is:

```python
from httk.workflow import job_records
from httk.workflow.provenance import run_record

record = next(job_records(workspace))
run = run_record(record)
store.save(run)  # the httk-data side
```

`run_record` does not fold children into the parent run. Each child collects to
its own `Run`; a parent names child products explicitly in its observed
declaration. Runner identity, the attempt timeline, and failure remain on the
`JobRecord` for callers that need them.

For directory workflows, the runner tree digest and generated or external
workflow declaration travel with the job and anchor this provenance chain to
the exact published package. See {doc}`workflow_packages`.

VASP runners will adopt this declaration in future work.

Built-in VASP result collection is documented in {doc}`collecting`.
