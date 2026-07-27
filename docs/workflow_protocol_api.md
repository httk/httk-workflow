# The filesystem protocol API

{py:mod}`httk.workflow.protocol` is the one deliberate public home of the
language-neutral filesystem protocol: the document shapes, validators, and
primitives an implementation reads and writes to interoperate through a
workspace. It is the Python expression of the contract; the normative words are
in {doc}`workflow_filesystem_api`.

## The contract

An independent inspection or verification tool — one that reads a workspace it
did not write, checks it, or exports from it — should be able to work from
{py:mod}`httk.workflow.protocol` together with {doc}`workflow_filesystem_api`
**alone**, without importing anything else from the package. Everything the
protocol defines is re-exported there:

- **Workspace format and profile** — the `core-v2` profile, its readable
  ancestors, the supported and withdrawn extensions, and
  {py:class}`~httk.workflow.protocol.WorkspacePolicy`.
- **Immutable job definitions** — {py:class}`~httk.workflow.protocol.JobDefinition`,
  {py:func}`~httk.workflow.protocol.job_digest`,
  {py:class}`~httk.workflow.protocol.Failure`, and the `validate_*` helpers that
  check every member of a job.
- **Markers and transitions** — {py:class}`~httk.workflow.protocol.Marker`,
  {py:class}`~httk.workflow.protocol.MarkerFault`,
  {py:class}`~httk.workflow.protocol.StateFrame`, and
  {py:func}`~httk.workflow.protocol.marker_basename`.
- **Journal records and references** — encode and parse an `hwref-v1` reference,
  and read or verify the frame it names.
- **Attempt context and outcome documents** —
  {py:class}`~httk.workflow.protocol.AttemptContext` and
  {py:class}`~httk.workflow.protocol.OutcomeDraft`, the shape a runner publishes.
- **Replayable data transactions** —
  {py:class}`~httk.workflow.protocol.TransactionBuilder` and
  {py:func}`~httk.workflow.protocol.replay_transaction`.
- **The protocol error family** — {py:class}`~httk.workflow.protocol.WorkflowError`
  and its subclasses.

Nothing in this namespace is manager bookkeeping, a subprocess wrapper, a CLI
handler, or a scheduling pass; those are implementation and live elsewhere. The
modules that implement these shapes are internal detail from the protocol's
point of view — import the names from {py:mod}`httk.workflow.protocol`.

The complete list of names is the module's `__all__`, documented in the
{doc}`API reference <reference/index>`.
