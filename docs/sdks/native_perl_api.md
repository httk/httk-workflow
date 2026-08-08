# Native Perl runner API

*For authors writing a workflow runner in Perl.* The Perl SDK is the same
authoring surface as the {doc}`Python <../runtime_helpers>`, {doc}`Bash
<native_bash_api>`, {doc}`C <native_c_api>`, {doc}`Fortran
<native_fortran_api>`, and {doc}`Rust <native_rust_api>` ones. It is a pure-core
Perl **bridge client**: every bridge-backed `Attempt` method invokes
`$HTTK_WORKFLOW_PYTHON -m httk.workflow._shell_bridge <verb> …` without a
shell, so a Perl runner publishes the same protocol bytes. Only `--describe` is
answered natively. The normative cross-language semantics are in
{doc}`sdk_parity`.

## A complete runner

```perl
#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib $ENV{HTTK_WORKFLOW_PERL_API} // "$FindBin::Bin/../../src/httk/workflow/native/perl";
use HttkWorkflow;

my $runner = HttkWorkflow::Runner->new(
    workflow => 'my.workflow',
    steps => [qw(prepare run)],
);
$runner->step(prepare => sub {
    my ($attempt) = @_;
    $attempt->advance('run', []);
    return 0;
})->step(run => sub {
    my ($attempt) = @_;
    $attempt->succeed();
    return 0;
})->main();
```

`Runner->step` is chainable. `Runner->main` owns the process exit status and
never returns. It validates the workflow and step names against
`[A-Za-z0-9._-]+` before `--describe`, dispatches the manager-selected step,
and turns a handler that returns without publishing into `no_outcome`.
Unknown steps become `unknown_step`. A nonzero handler return or a Perl
exception aborts the unpublished draft with the `PerlError` breadcrumb label.

The native description is hand-composed, with byte-sorted steps and one
trailing newline:

```console
./runner.pl --describe
{"format": "httk-workflow-runner-description", "format_version": 1, "steps": ["prepare", "run"], "workflow": "my.workflow"}
```

`HTTK_WORKFLOW_DESCRIBE=1` has the same effect. Managers and the describe
helper export `HTTK_WORKFLOW_PERL_API` as the installed `native/perl` directory;
use that environment variable in `use lib` so published and transferred
single-file runners find the SDK. An in-source-tree runner may fall back to a
script-relative path. The runner is interpreted; the module is one
`HttkWorkflow.pm` file and has no CPAN dependencies.

## Values and errors

Read methods return the captured stdout with all trailing newlines removed, or
`undef` when the bridge exits `1` (a legitimately absent answer). A bridge
refusal (`2` or higher), an unset `HTTK_WORKFLOW_PYTHON`, or a spawn failure
dies with a structured `HttkWorkflow::BridgeError` object. Command methods
return the bridge exit status, including classified `run` statuses.

The unset-interpreter diagnostic is the same as the other SDKs:
`httk-workflow: HTTK_WORKFLOW_PYTHON is not set by the workflow manager`.
Bridge execution uses Perl's list-form `system` and `open '-|'`; stdin and
stderr remain inherited, while capture methods capture stdout only.

`log` is the one local helper, matching Bash and Rust: the bridge has no `log`
subcommand, so it writes a timestamped level/message line to stderr.

## The Perl method table

Each method below is one bridge subcommand. Array arguments are Perl array
references; `gather` accepts a hash reference or key/value options. An omitted
read default means the bridge's absent convention applies.

| Perl | Python | Bash |
| --- | --- | --- |
| `Runner->new(workflow => $id, steps => \@names)` | `Runner` | `httk_workflow_runner` |
| `Runner->step($name => \&handler)` | `Runner.step` | `step_<name>` |
| `Runner->main` | `Runner.main` | `httk_workflow_main` |
| `Attempt->invoke(\@argv)` | — | — |
| `context`, `parameter`, `setting`, `environment` | same-named methods | same-named bridge functions |
| `state_get`, `state_set`, `state_delete`, `state_merge` | same-named methods | same-named bridge functions |
| `declaration`, `declare` | same-named methods | same-named bridge functions |
| `children`, `child`, `spawn` | same-named methods | same-named bridge functions |
| `runlog_note`, `runlog_headline`, `runlog_append` | `log.append` | same-named bridge functions |
| `log` | logging | `httk_workflow_log` |
| `put`, `remove` | same-named methods | same-named bridge functions |
| `advance`, `gather` | same-named methods | same-named bridge functions |
| `succeed`, `fail`, `retry`, `pause` | same-named methods | same-named bridge functions |
| `batch`, `job_prepare`, `workdir_apply` | same-named methods | same-named bridge functions |
| `run`, `calc`, `template_render` | same-named methods | same-named bridge functions |
| `compress`, `decompress` | same-named methods | same-named bridge functions |

`fail($code, $message, $retryable)` and `remove($path, $missing_ok)` take Perl
booleans. `gather($step, { when => ..., count => ..., on_impossible => ...,
priority => ... })` forwards only defined options. The `invoke` method is the
escape hatch for bridge subcommands without a dedicated wrapper.

## The `examples/relax_perl` walkthrough

`examples/relax_perl/relax.pl` has the same `prepare`, `run`, and `publish`
shape as the C and Rust examples. It stages `POSCAR`, resolves `vasp.command`,
runs the configured command through `Attempt->run`, records a completed
classification, and publishes the finished files into transactional data.

```console
cd examples/relax_perl
perl relax.pl --describe
httk project init --name relax-perl
httk workflow workspace init . --name default
httk workflow job new --workflow ./relax.pl --step prepare --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow workspace settings set vasp.command "$PWD/../mock_vasp.py"
httk workflow run
httk workflow collect
```

The example resolves the SDK through `HTTK_WORKFLOW_PERL_API` under a manager
and falls back to its script-relative `use lib` path for a bare in-source-tree
invocation. It needs no compile step.
