# Run provenance

This page describes how one `JobRecord` becomes one stored `httk.core.Run`.
The `provenance` declaration describes the entries one workflow execution
consumed, created, and returned. All members are optional:

```json
{
  "workflow_declaration_uri": "https://raw.githubusercontent.com/httk/workflows-vasp/refs/tags/wf-decl-v0.1.0/vasp-relax/declaration.json",
  "inputs":    {"initial_structure": {"type": "structures", "id": "<served id>"}},
  "artifacts": {"relaxed_structure": {"type": "structures", "id": "..."}},
  "outputs":   {"total_energy":      {"type": "records", "id": "..."}}
}
```

The object keys are labels, unique per side, and the targets are loose
served-entry references. The workflow layer carries the declaration verbatim;
this page documents how `run_record` collects it.

The `type` strings are *httk*'s internal entry-type names (`records`, `runs`,
`structures`, `files`). OPTIMADE wire prefixing (`_httk_records`,
`_httk_runs`, …) is applied only at the OPTIMADE serving edge. File-valued
output roles yield run edges with `type = "files"` and the corresponding
`FileRecord` id, so stored provenance names the file entry directly.

## Declared and observed

Inputs known while scaffolding can be declared in
`httk.workflow.protocol.JobSpec`:

```python
declared = {
    "workflow_declaration_uri": "https://raw.githubusercontent.com/httk/workflows-vasp/refs/tags/wf-decl-v0.1.0/vasp-relax/declaration.json",
    "inputs": {"initial_structure": {"type": "structures", "id": "structures/si"}},
}
prepare_job_payload(payload, JobSpec(..., declarations={"provenance": declared}))
```

Once produced entry ids exist, a runner writes the complete observed document
for collection:

```python
a.declare("provenance", {
    "workflow_declaration_uri": "https://raw.githubusercontent.com/httk/workflows-vasp/refs/tags/wf-decl-v0.1.0/vasp-relax/declaration.json",
    "inputs": {"initial_structure": {"type": "structures", "id": "structures/si"}},
    "artifacts": {"relaxed_structure": {"type": "structures", "id": "structures/si-relaxed"}},
    "outputs": {"total_energy": {"type": "records", "id": "records/energy-1"}},
})
```

The observed document replaces the declared one wholesale; the two are not
merged. Without any provenance document, `run_record` still uses the `$id` of
the `workflow` declaration when available.

When the job declares workflow environment entries, the runtime also records
an observed `environment` declaration. This
`httk-workflow-environment-resolution` version 2 document carries each value
and the layer that supplied it, so provenance identifies the settings that
drove the run.

## Declaration and definition

A `Run` names the workflow twice, for two different things:

- `workflow_declaration_uri` is the `$id` of the workflow *declaration*, the
  document describing what the workflow consumes and produces (see
  {doc}`/details/declarations`).
- `workflow_definition_uri` identifies the workflow *definition*, the code
  that ran. `run_record` sets it to the job's `workflow` when that is a
  commit-pinned git URI (see {doc}`/details/workflow_uris`), and to `None` otherwise.

A git workflow without a `declaration_uri` therefore has a definition URI but
no declaration URI. The v1 reader records its package's declaration URI, if
any, and no definition URI. `httk collect` reports both in each run summary.

## From job record to stored run

The end-to-end handoff is:

```python
from httk.workflow import job_records
from httk.workflow.provenance import run_record

record = next(job_records(workspace))
run = run_record(record)
store.save(run)  # the httk-store side
```

`Run.source_id` is the executing system's identity for the job, formatted by
*httk-workflow* as `"<workspace_id>:<job_id>"`. It participates in content
identity, so collecting one job repeatedly deduplicates while distinct jobs
stay distinct, and `collect --into` stores a job's changed run as a new
revision of the same entry rather than a second entry. `Run.immutable_id` is
left `None` for *httk-store* to mint as its per-revision identifier.

A run also carries a readable `name` and its timing, all metadata outside
content identity, so re-collecting a job still deduplicates. `Run.name` is the
job's name, or its job key when the job has none; a *jobflow* sub-job is named
after its *jobflow* job, e.g. `tight relax 1`. `Run.started_at` is the start of
the job's first attempt and `Run.finished_at` (also `last_modified`) the end of
its last, both timezone-aware. Over OPTIMADE they are served as `_httk_name`,
`_httk_started_at` and `_httk_finished_at`, filterable and sortable. A store
created before these fields existed refuses the new run layout on reopen;
`httk collect --into STORE --upgrade` adds the empty columns once without
rewriting stored rows. Runs stored before the upgrade keep no name or timing,
and collecting such a job again reports a metadata conflict for it, since
stored metadata is never silently rewritten; rebuild the store from the
workspace to name and time every run.

`run_record` does not fold children into the parent run. Each child collects
to its own `Run`, including a child that only called or spawned further jobs,
and `collect --into` links them: the parent's stored run gains a
`has_artifact` edge of type `runs` to each child's run (see
{doc}`/details/collecting`). A parent still names child products explicitly in its
observed declaration. The installed-workflow pin (`runner_provenance`), the
rest of the attempt timeline, and failure stay on the `JobRecord` for callers
that need them.

For package workflows, the installed tree digest in `runner_provenance` and
the generated or external workflow declaration in `job.json` anchor this
provenance chain to the exact installed package; see
{doc}`/details/workflow_packages`.

## Postprocess runs

A curated postprocess script's output is stored as a separate run per job and
script: it takes the job run and all of its outputs as inputs and the
script's `files` as outputs, under `source_id`
`<workspace_id>:<job_id>:postprocess:<script>`. See
{doc}`collecting`.

## Provenance and sealing

Provenance records where a result came from; a **seal** proves it has not
changed since. A manager seals each job as it succeeds, signing its payload's
file hashes, and workspaces and projects can be sealed on top to pin whole
trees under one signature that travels with a transfer. When integrity, not
only origin, matters, see {doc}`/details/sealing`.
