# Workflow declarations

*For workflow authors and data-layer authors: what a workflow says about itself, carried verbatim beside the result.*

A *workflow declaration* is a property-like document that says what a workflow
**is** — the inputs it consumes, the method it applies, the outputs it produces —
without describing a graph. It is what a data layer stores next to a result so
the result can later be explained: not the trace of what ran, which is the
{doc}`collecting` provenance, but the statement of what was meant to run.

OPTIMADE is standardizing exactly this document, graph-free and versioned like
its other property definitions. That work is in progress, so *httk-workflow*
deliberately takes no position on its contents.

## Carried verbatim

A declaration is opaque to this engine. `job.json` carries the document
unchanged, the protocol validates only that it *is* a JSON object under a
well-formed name, and nothing here ever looks inside it — no member is added,
removed, renamed, or rewritten.

There is no *httk* envelope around it, and there will not be one. Versioning and
self-description belong **inside** the document, in the `$id`-style members the
OPTIMADE property-definition conventions already define: a document says which
vocabulary and which version it follows, and a consumer that understands that
vocabulary gives it meaning. An engine that wrapped or changed the document
could only ever come to disagree with the standard it is carrying.

collection therefore belongs to *httk-data* and OPTIMADE tooling. Carriage,
digest coverage, and honest reporting belong here.

Packaged workflows may carry declarations into every scaffolded `job.json`; the
built-in VASP workflows declare their `workflow` `$id` using the published
`schemas.httk.org` IRIs. See {doc}`provenance` for the rule that uses this `$id`
as the workflow URI fallback.

Directory packages can generate this declaration from their manifest or carry
an externally authored, validated declaration file; see {doc}`workflow_packages`.

## Declared and observed

A declaration attaches in two places, and the two are never merged.

| | Where | When | Mutable |
| --- | --- | --- | --- |
| **declared** | the `declarations` member of `job.json` | at submission | no — the immutable job digest covers it |
| **observed** | `.httk-job/declarations/<name>.json` in the payload | at run time | yes — the last write wins |

The declared document is the static statement of intent, pinned by the immutable
job digest like every other member of `job.json`.

The observed document exists because *httk₂* campaigns are dynamic: nothing
declares the shape of a workflow up front, so a step may only discover at run
time which children it spawned and which outputs it actually produced. A step
writes the refined document as it learns it. That write is runner-private — the
whole `.httk-job/` directory is excluded from every payload digest — so declaring
can never disturb the immutability check of a payload, a child registration, or a
detached transfer.

A collect reports both, side by side, per name. Reconciling them requires
understanding the vocabulary, which is the consumer's job, not the engine's.

## A job that declares

The document below is one plausible shape, shown to make the mechanics concrete;
the normative shape is the one OPTIMADE settles on.

```python
from httk.workflow import JobSpec, prepare_job_payload

RELAXATION = {
    "$id": "https://example.org/workflows/vasp-relax/v1.0.0",
    "$schema": "https://schemas.optimade.org/defs/v1.2/workflow_declaration.json",
    "title": "VASP structure relaxation",
    "x-optimade-definition": {
        "kind": "workflow",
        "version": "1.0.0",
        "name": "vasp_relax",
    },
    "inputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
    "method": {"code": "vasp", "task": "relaxation", "convergence": {"ediffg": -0.01}},
    "outputs": {"structure": {"$ref": "https://schemas.optimade.org/defs/v1.2/types/structure"}},
}

prepare_job_payload(
    payload,
    JobSpec(
        name="Silicon relaxation",
        workflow="example.vasp-relax",
        runner_path="files/runner",
        parameters={"kpoint_density": 30.0},
        declarations={"workflow": RELAXATION},
    ),
)
```

A spawned child declares for itself. Nothing is inherited: a declaration
describes the job it belongs to, and a child that runs a different step is a
different thing from its parent.

```python
a.spawn(
    ChildSpec(step="relax", parameters={"site": site}, declarations={"workflow": SITE_RELAXATION}),
    label=f"site-{site}",
)
```

## Declaring what a job observed

A step records the refined document with one call, and reads one back with
another. Reading answers with the observed document when the job wrote one, the
declared one from `job.json` otherwise, and `None` when the job knows neither.

```python
@run.step
def aggregate(a):
    declared = a.declaration("workflow")
    a.declare("workflow", {**declared, "outputs": {"structures": len(a.children.succeeded)}})
    a.succeed()
```

The Bash API is the same two calls, and publishes the same bytes. The document
is passed as a file, because a whole JSON object is what a command line cannot
quote; reading prints it compactly and returns 1 when there is no declaration of
that name at all.

```bash
step_aggregate() {
    httk_workflow_declaration workflow >observed.json || printf '{}\n' >observed.json
    jq '.outputs = {"structures": 3}' observed.json >refined.json
    httk_workflow_declare workflow refined.json
    httk_workflow_succeed
}
```

Declaration names are keys and file basenames at once, so they are single safe
path components: letters, digits, `_`, `.`, and `-`, starting with a letter,
digit, or underscore, at most 64 characters. The whole `declarations` object of
one `job.json` is limited to 262144 serialized bytes, the same allowance
`inputs` has and for the same reason — bulk content belongs in the payload or in
transactional `data/`.

## What a collect reports

`JobRecord.declarations` maps every name either source knows to both sides:

```json
{
  "workflow": {
    "declared": {"$id": "https://example.org/workflows/vasp-relax/v1.0.0", "...": "..."},
    "observed": {"$id": "https://example.org/workflows/vasp-relax/v1.0.0", "...": "..."}
  },
  "provenance_hint": {
    "declared": null,
    "observed": {"$id": "https://example.org/hints/v1", "...": "..."}
  }
}
```

A name that only `job.json` declared has `"observed": null`; a name only the
runner wrote has `"declared": null`. An observed document that cannot be read is
reported as `null` and sets `provenance.gaps` on the record, exactly like every
other damaged evidence a collect still reports rather than hides.

See {doc}`collecting` for the record as a whole, {doc}`runtime_helpers` and
{doc}`native_bash_api` for the two authoring APIs, and
{doc}`workflow_filesystem_api` for the normative statement of the `declarations`
member and the payload area it is stored in. See {doc}`provenance` for the
collection of the `provenance` declaration.
