# The command line

Everything is one nested command tree under `httk`, and every group and
command answers `--help`:

```text
Setup
httk setup | identity      operator identities (core-owned)
httk config                show | set | unset | import-v1

Organize
httk project               init | show | check | repair | manifest | verify | seal | unseal | import-v1 (core-owned)
httk workspace             init | register | list | default | show | set | unset | status | owners | attest-dead | check | repair | gc | seal | unseal | verify | ...
httk remote                list | add | set | unset | check | show | remove | import-v1 | take-back
httk launcher              list | add | set | unset | check | show | remove
httk daemon                init | set | unset | show | check | run | reload
httk campaign              init | show | set | unset | remove

Run
httk workflow              list | show | install | uninstall | build | fetch | forget
httk job                   new | submit | cancel | pause | resume | restart | set | delete | detach | seal | unseal | unlock | list | show | log | why | precheck | debug | verify | move | eject | adopt
httk run | monitor          run managers; watch workspaces interactively
httk push | pull            move ready jobs to a workspace, succeeded jobs back
httk manager               status | log | cancel

Results
httk collect               workspaces, campaign partitions and calculation trees
httk postprocess           run a declared postprocess script on collected jobs

Legacy
httk v1                    collect
```

`httk job new SOURCE` takes the workflow as one positional, detected as
`httk workflow install SOURCE` detects it (a runner file, document, package
directory, git URI or name), or `--command TEMPLATE` instead. `job new`,
`collect` and `run` act on a campaign's partitions with `--campaign`
({doc}`campaigns`). `httk job unlock` puts a job in debug mode, and
`httk collect --force` collects such a job; see {doc}`restarting_jobs`.

{doc}`quickstart` walks the everyday sequence, {doc}`workspaces` and
{doc}`running` explain the concepts behind it, and the full reference,
{doc}`details/workflow_cli`, documents every command and option: workspace
selection, job creation, inspection and debugging, projects and signed
manifests, configuration, remotes and transfers, and the frozen protocol
spellings.
