# Workflow declarations

A *workflow declaration* is a property-like document that states what a
workflow is: the inputs it consumes, the method it applies, and the outputs it
produces, without describing a graph. A data layer stores it next to a result
so the result can later be explained. It records what was meant to run; the
trace of what actually ran is the {doc}`collecting` provenance.

OPTIMADE is standardizing this document, graph-free and versioned like its
other property definitions. That work is in progress, so *httk-workflow* takes
no position on its contents.

## Carried verbatim

A declaration is opaque to the engine. `job.json` carries the document
unchanged; the protocol checks only that it is a JSON object under a
well-formed name, and never adds, removes, or rewrites a member. There is no
*httk* envelope around it. Versioning and self-description belong inside the
document, in the `$id`-style members of the OPTIMADE property-definition
conventions, so that a consumer who understands the vocabulary gives the
document its meaning. Interpreting declarations is the business of
*httk-store* and OPTIMADE tooling; this module carries them, covers them by
digest, and reports them faithfully.

### Where declarations come from

- Packaged workflows may carry declarations into every scaffolded `job.json`.
  The built-in VASP workflows declare their `workflow` `$id` with the
  published `schemas.httk.org` URIs. {doc}`provenance` describes the rule that
  uses this `$id` as the workflow URI fallback.
- Directory packages can generate the declaration from their manifest or carry
  an externally authored, validated declaration file; see
  {doc}`workflow_packages`.

A declaration `$id` names the declaration document, never the code. A
workflow referenced by git URI keeps that URI as its *definition* URI, and its
generated declaration carries a `$id` only when the manifest names a
`declaration_uri`. See {doc}`workflow_uris`.

## Declared and observed

A declaration attaches in two places, and the two are never merged.

| | Where | When | Mutable |
| --- | --- | --- | --- |
| **declared** | the `declarations` member of `job.json` | at submission | no — the immutable job digest covers it |
| **observed** | `.httk-job/declarations/<name>.json` in the payload | at run time | yes — the last write wins |

The declared document is the static statement of intent, pinned by the
immutable job digest like every other `job.json` member.

The observed document exists because campaigns are dynamic: a step may only
learn at run time which children it spawned and which outputs it produced, and
it writes the refined document as it goes. The whole `.httk-job/` directory is
excluded from every payload digest, so declaring never disturbs the
immutability check of a payload, a child registration, or a transfer.

A collect reports both side by side, per name. Reconciling them requires
understanding the vocabulary, which is the consumer's job, not the engine's.

## A job that declares

The document below is one plausible shape, shown to make the mechanics
concrete; the normative shape is the one OPTIMADE settles on.

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

A spawned child declares for itself and inherits nothing: a declaration
describes the job it belongs to, and a child that runs a different step is a
different thing from its parent.

```python
a.spawn(
    ChildSpec(step="relax", parameters={"site": site}, declarations={"workflow": SITE_RELAXATION}),
    label=f"site-{site}",
)
```

## Declaring what a job observed

A step records the refined document with one call and reads one back with
another. Reading returns the observed document when the job wrote one,
otherwise the declared one from `job.json`, and `None` when the job has
neither.

```python
@run.step
def aggregate(a):
    declared = a.declaration("workflow")
    a.declare("workflow", {**declared, "outputs": {"structures": len(a.children.succeeded)}})
    a.succeed()
```

The Bash API has the same two calls and publishes the same bytes. The
document is passed as a file, because a command line cannot quote a whole
JSON object. Reading prints the document compactly and returns 1 when there is
no declaration of that name.

```bash
step_aggregate() {
    httk_workflow_declaration workflow >observed.json || printf '{}\n' >observed.json
    jq '.outputs = {"structures": 3}' observed.json >refined.json
    httk_workflow_declare workflow refined.json
    httk_workflow_succeed
}
```

## Names and size limits

Declaration names are both keys and file basenames, so they are single safe
path components: letters, digits, `_`, `.`, and `-`, starting with a letter,
digit, or underscore, at most 64 characters. The whole `declarations` object
of one `job.json` is limited to 262144 serialized bytes, the same allowance
`inputs` has and for the same reason: bulk content belongs in the payload or
in transactional `data/`.

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

A name only `job.json` declared has `"observed": null`, and a name only the
runner wrote has `"declared": null`. An observed document that cannot be read
is reported as `null` and sets `provenance.gaps` on the record, like all
damaged evidence a collect reports rather than hides.

See {doc}`collecting` for the record as a whole, {doc}`runtime_helpers` and
{doc}`../sdks/bash_api` for the two authoring APIs, and
{doc}`workflow_filesystem_api` for the normative statement of the
`declarations` member and the payload area it is stored in. See
{doc}`provenance` for the collection of the `provenance` declaration.
