# Bundled httk v1 shell runtime

The historic source paths are retained as thin redirect files:

- `Execution/tasks/ht_tasks_api.sh`;
- `Execution/tasks/vasp/vasptools.sh`.

They load the attributed compatibility implementations below `compat/`, so
existing `ht_steps` files can source the exact old names through the
`HTTK_DIR` environment variable supplied by the packaged httk-v1 runner.

The compatibility functions retain their httk v1 behavior. Native v2 runners
instead use `httk.workflow.Runner` and `httk.workflow.Attempt` together with the
data-oriented helpers in `httk.workflow.vasp`; these are independent interfaces,
not renamed v1 API methods.

See `NOTICE` for the v1 contributor history, including Henrik Levämäki's
authorship of the separate v1 Python APIs. The compatibility files are
distributed under AGPL-3.0-or-later. An external v1 root may still be selected
for workflows that depend on locally modified or additional helper files.
