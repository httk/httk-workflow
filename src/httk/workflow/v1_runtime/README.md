# Bundled httk v1 shell runtime

These files are compatibility copies of:

- `old/httk/Execution/tasks/ht_tasks_api.sh`
- `old/httk/Execution/tasks/vasp/vasptools.sh`

They retain their httk v1 behavior so existing `ht_steps` files can source
them through the `HTTK_DIR` environment variable supplied by
`httk-v1-taskmanager`. The files are distributed under the project’s
AGPL-3.0-or-later license. An external v1 root may be selected for workflows
that depend on locally modified or additional helper files.
