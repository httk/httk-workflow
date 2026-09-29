# Supporting a simulation code

*For developers adding helpers for a simulation code — input preparation,
supervised runs, diagnostics, Bash functions — that workflow authors can reuse.*

*httk-workflow* itself bundles no code support. Each simulation code is
supported by its own distribution, `httk-workflow-<code>`, that plugs into the
runtime through the *httk* registry. The VASP helpers, for example, live in the
separate *httk-workflow-vasp* distribution (`pip install httk-workflow-vasp`);
this page uses it as the worked example throughout.

## What a code-support distribution provides

| Part | VASP example | Role |
| --- | --- | --- |
| Python library | `httk.codes.vasp` | the helpers a Python runner imports: `prepare_vasp_inputs`, `run_vasp`, `plan_vasp_remedy`, … |
| Bash API file | `httk-vasp.sh` in `httk.codes.vasp` | the functions a Bash runner sources: `httk_vasp_prepare`, `httk_vasp_run`, … |
| Bridge module | `httk.codes.vasp._bridge` | the `vasp-*` subcommands of the shell bridge that each Bash function calls |
| Registry package | `httk.registry.codes.vasp` | calls `register_code` so the code is discovered at `import httk.core` |

`httk.codes` is a PEP 420 namespace like `httk` itself: a distribution adds
`src/httk/codes/<code>/` and never an `httk/codes/__init__.py`. The registry
package is the whole registration, and it only records strings, so importing it
imports nothing of the code. The bridge modules of all installed codes are
imported whenever the shell bridge builds its parser, that is on every bridge
call; a bridge module that fails to import only disables that code's
subcommands, with one line on stderr naming the code:

```python
# src/httk/registry/codes/vasp/__init__.py
from httk.core.register import register_code

register_code("vasp", bridge="httk.codes.vasp._bridge", bash_api="httk.codes.vasp:httk-vasp.sh")
```

The code name (`vasp`) is also the prefix of its bridge subcommands (`vasp-`)
and the stem of its environment variable (`HTTK_WORKFLOW_VASP_BASH_API`).

## The bridge module

Every function of a Bash API is one invocation of one subcommand of the private
shell bridge, `python -m httk.workflow._shell_bridge` (see
{py:mod}`httk.workflow.shell_bridge`). The bridge walks the registered codes and
mounts each code's subcommands beside its own; the bridge module defines two
functions:

- `add_commands(subparsers)` adds the code's subcommands, each named
  `<code>-<verb>` (`vasp-prepare`, `vasp-run`, …), to the bridge's argparse
  subparsers;
- `run_command(namespace) -> int` runs one of them. It returns `0` on success
  and {py:data}`~httk.workflow.codes.BRIDGE_ABSENT` (`1`) for a legitimately
  absent answer, such as an unset INCAR tag; an exception it raises is reported
  as a refusal, exit code `2`. A subcommand that runs a program may define its
  own outcome codes, as `vasp-run` does.

Batched bridge calls (`httk_workflow_batch`) reach code subcommands too, with
the same exit codes.

## The Bash API file

The manager exports the registered Bash API file of every installed code to
each attempt, and to describe mode, as `HTTK_WORKFLOW_<CODE>_BASH_API`. The
variable exists only when the code's distribution is installed, so a Bash runner
guards it before sourcing it after the generic library:

```bash
source "$HTTK_WORKFLOW_BASH_API"
: "${HTTK_WORKFLOW_VASP_BASH_API:?install httk-workflow-vasp}"
source "$HTTK_WORKFLOW_VASP_BASH_API"
```

A registered Bash API resource that cannot be found fails the attempt with the
failure code `code_support_unavailable` rather than starting the runner.

Each function it defines forwards to its subcommand through the generic
library's `_httk_workflow_bridge`, passing the arguments through untouched:

```bash
httk_vasp_get_tag() {
    _httk_workflow_bridge vasp-get-tag "$@"
}
```

Runners in the other languages reach the same subcommands through their
`invoke` operation (for example `httk_workflow_invoke` in C) with the
`<code>-<verb>` name.

## The toolkit

{py:mod}`httk.workflow.codes` is the public toolkit a code-support package
builds on. It promotes the runtime pieces a code library needs to one stable
home, so a package never imports private *httk-workflow* modules:

- {py:class}`~httk.workflow.codes.ProcessSupervisor`,
  {py:class}`~httk.workflow.codes.ProcessReport`,
  {py:class}`~httk.workflow.codes.CheckerSpec`,
  {py:class}`~httk.workflow.codes.FollowSource`,
  {py:class}`~httk.workflow.codes.SourceEvent`, and
  {py:class}`~httk.workflow.codes.Diagnostic` — run the code under supervision
  and turn its output into structured diagnostics (`run_vasp`,
  `diagnose_vasp_files`);
- {py:class}`~httk.workflow.codes.ReplayableWorkdirBatch` — apply input
  changes as one replayable batch, so an interrupted remedy is completed rather
  than half-applied (`apply_vasp_remedy`);
- {py:data}`~httk.workflow.codes.JOB_STATE_DIRECTORY` — the job state
  directory a job-scoped record belongs in (the VASP remedy history);
- {py:func}`~httk.workflow.codes.read_json`,
  {py:func}`~httk.workflow.codes.write_json_atomic`, and
  {py:func}`~httk.workflow.codes.utc_now` — the runtime's JSON and timestamp
  conventions for reports and histories;
- {py:data}`~httk.workflow.codes.BRIDGE_ABSENT` — the bridge's absent exit code;
- {py:func}`~httk.workflow.codes.installed_codes` and
  {py:func}`~httk.workflow.codes.code_environment` — the installed codes as
  `CodeSupport` records, and the `HTTK_WORKFLOW_<CODE>_BASH_API` variables the
  manager exports for them.

List what is installed with:

```python
from httk.workflow.codes import installed_codes

print([code.name for code in installed_codes()])
```
