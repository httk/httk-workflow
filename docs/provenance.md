# Provenance, declarations, and stable ids

Three things connect a stored result back to the work that produced it: the
**declarations** a job carries about what it was meant to do, the **run**
record a collect derives from what actually happened, and the **id ledger**
that keeps a stored entry's id the same when the database is rebuilt. A
**seal** then proves the result has not changed since; see {doc}`running`.

## Declarations

A *workflow declaration* says what a workflow is: the inputs it consumes, the
method it applies, the outputs it produces, without describing a graph.
OPTIMADE is standardizing this document, so *httk-workflow* carries it
verbatim and never looks inside it. A declaration attaches in two places that
are reported side by side and never merged:

| | Where | When |
| --- | --- | --- |
| **declared** | the `declarations` member of `job.json`, covered by the job digest | at submission |
| **observed** | `.httk-job/declarations/<name>.json` in the payload | written by the runner at run time |

A runner refines the observed document with `a.declare(name, document)` and
reads it back with `a.declaration(name)`; the Bash API has
`httk_workflow_declare` and `httk_workflow_declaration`. Workflow packages
generate the declaration from their manifest or carry an authored one. See
{doc}`details/declarations`.

## The run

The `provenance` declaration lists the entries one execution consumed,
created and returned, as labelled references:

```json
{
  "workflow_declaration_uri": "https://schemas.httk.org/defs/v0.1/workflows/vasp-relax",
  "inputs":    {"initial_structure": {"type": "structures", "id": "<served id>"}},
  "artifacts": {"relaxed_structure": {"type": "structures", "id": "..."}},
  "outputs":   {"total_energy":      {"type": "records", "id": "..."}}
}
```

At collect time `run_record` turns one `JobRecord` into one `httk.core.Run`
whose `source_id` is `<workspace_id>:<job_id>`, so repeated collection of a job
deduplicates while distinct jobs stay distinct. A run names its workflow
twice: `workflow_declaration_uri` is the `$id` of the declaration document, and
`workflow_definition_uri` is the commit-pinned git URI of the code that ran,
when there is one. Each child job collects to its own run, and
`collect --into` links a parent's run to its children's. See
{doc}`details/provenance`.

## Stable ids

A store mints entry ids in save order, so rebuilding it from scratch renumbers
everything. With `collect --into`, an **id ledger** beside the store
(`<into>.ids.sqlite`) maps a stable source key, built from the workspace and
job identity, to a permanent id and returns the same id forever; a changed
result becomes a new revision under the same id. The ledger is a signed,
append-only SQLite file meant to be committed next to the store. `--id-ledger
PATH` relocates it and `--no-id-ledger` opts out, with a warning that the ids
are then not stable. See {doc}`details/stable_ids`.
