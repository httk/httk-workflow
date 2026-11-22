# Campaigns

A **campaign** runs work at a scale one workspace should not hold. It is not
a new engine or scheduler, only a *partition map* over ordinary registered
workspaces: each partition is a named bucket pointing at one workspace, and
every command that drives a workspace drives a partition unchanged. Partition
sizes are chosen from local measurements such as those in
{doc}`details/benchmarks`. The map lives in the project's `project.json`, so
it travels with the project.

Two rules keep this simple:

- **Root jobs are assigned by policy.** When you submit a root job, the
  campaign picks its partition by hash, by position, or by your explicit choice.
- **Spawned children inherit their parent's workspace.** The engine already
  scaffolds a child into the workspace its parent runs in, so the whole tree
  below a root stays in the partition the root was assigned.

## The partition map

```console
$ httk campaign init \
      --partition north=screening-a \
      --partition south=screening-b \
      --assignment hash
$ httk campaign show
assignment	hash
north	screening-a
south	screening-b
```

Each `--partition NAME=WORKSPACE` names a bucket and the registered workspace
it points at, and `--assignment` sets how a root job's partition is chosen.
The workspaces must exist first ({doc}`workspaces`); a partition names a
registered workspace, never a bare path, because the campaign records it.

Change individual entries without restating the whole map:

```console
httk campaign set partitions.east=screening-c
httk campaign unset partitions.east
httk campaign set assignment=explicit
httk campaign unset assignment
httk campaign remove --yes
```

Unsetting `assignment` restores `hash`. Removing the campaign clears its
configuration and leaves its workspaces and jobs on disk. Campaign changes
are refused while the project is sealed.

### Assignment policies

| Policy | How a root is placed |
| --- | --- |
| `hash` | The job's tag is hashed to a partition deterministically, so the same tag always lands in the same partition. This is the default: it spreads a stream of distinct keys evenly and reproducibly without any shared counter. |
| `round-robin` | The job's position in its `job new` batch, counted from `--campaign-index` (default 0), selects the partition, so a batch fans out evenly across the partitions in order. |
| `explicit` | `--partition NAME` names the partition: the caller places each root outright. |

The partitions are always visited in a stable (sorted) order, so both a hash and
an index map to a partition reproducibly, run after run.

## Submitting into a campaign

```console
$ httk job new vasp.relax --campaign \
      --input structure=structures/Si.vasp --tag silicon
silicon--0c4f…	/…/screening-a/workspace/jobs/silicon--0c4f…
$ httk job new vasp.relax --campaign --input-from structure structures/
```

`job new --campaign` assigns each new job to a partition and submits it into
that partition's workspace: under `hash` by its tag (`--tag`, or the tag an
input supplies; a job without one is refused), under `round-robin` by its
position in the batch, and under `explicit` to `--partition NAME`, which
also pins the partition under the other policies. The workflow must be
installed in that workspace; `--install` installs it there first. Every other
`job new` option applies, and everything the root later spawns follows it
there. `hash` sees only the tag known before the job is scaffolded: a tag an
instantiate hook derives does not count, so a workflow that derives its tag
needs `--tag` or `--partition` under `hash`. `--campaign-index` applies only to
`round-robin` without `--partition`.

Before submitting anything, `job new --campaign` checks every partition the
batch reaches: without `--install` or `--embed-workflow` the workflow must be
installed there, and with the default `--duplicate-tag refuse` no job tag may
be taken in that placement or given to two jobs of one partition. Such a
refusal submits nothing anywhere. A failure found only while a partition's
jobs are being created can still leave the partitions before it submitted:
an input that cannot be staged, an instantiate hook that fails or derives a
taken tag, a failed `--install`, a step or setting the partition's
installation rejects, or a failed sync. Rerun the same command with
`--duplicate-tag skip`; it creates only the jobs that do not exist yet. In
Python:

```python
from httk.workflow.campaigns import assign_partition, campaign_submit

# Where would this key go?
partition = assign_partition("silicon", project="my-project")

# Submit the root there; its children inherit the same workspace.
job = campaign_submit(
    "vasp.relax",
    key="silicon",
    project="my-project",
    files={"POSCAR": "structures/Si.vasp"},
    tag="silicon",
    install=True,
)
```

A job is created where the client runs, so a partition that points at a
remote workspace is submitted to locally, moved with `httk push` and
fetched home with `httk pull` ({doc}`running`); the pushed jobs stay in
the local partition as `away` copies (`httk job list --state away`) until the
pull replaces them.

A job with a `--tag` is created only once per placement, whatever state the
existing one is in (an away copy counts): re-running the creation script with
`--duplicate-tag skip` creates just the jobs that do not exist yet.

## Running and collecting across partitions

```console
$ httk run --campaign                     # every partition
$ httk run --campaign --partition north
$ httk collect --campaign --state succeeded
```

`run --campaign` starts managers at each selected partition through that
workspace's own launcher, exactly as `httk run` does for one workspace; for a
remote partition the manager command runs on the owning machine. It passes
`--workers`, `--worker-resource`, `--count`, `--launcher`, `--idle-timeout`
and `--adapter-timeout` on and refuses the other `run` options.
`collect --campaign` collects the partitions one after another in stable
order as one sweep and takes the workspace options of `httk collect`
(`--into`, `--batch-size`, `--fail-fast`, `--state`, …) except `--job` and
`--postprocess`. Both take `--partition` (repeatable) to act on a subset.
With `--into`, `collect --campaign` keeps the default id ledger beside the
store (`<into>.ids.sqlite`, signed with the project or operator identity key)
as every `collect --into` does; a store built by the former ledger-less
`campaign collect --into X` gets a fresh `X.ids.sqlite` on its next sweep,
unless `--no-id-ledger` is passed or the store is rebuilt.

## Bounded fan-out: placement recipes

Partitioning bounds how many jobs one workspace holds; within a workspace,
`--placement` bounds how wide any one directory gets, since a shallow tree is
cheaper to list than one flat directory of jobs. The engine
imposes no scheme; common ones are a short hash prefix of the job key
(`project/<hash-prefix>/<batch>`), one subtree per submission batch
(`project/<date>/<run>`), or one per structure family. A manager started with
`--placement` serves exactly one such subtree.

Spawned children go, by default, to `<parent placement>/<parent job key>.children`,
so a tree is visible as a tree within each state directory and a
`--placement` manager serves a root's whole tree. The workspace setting
`children.placement=flat` puts children at their parent's placement instead,
for tools that need short absolute paths to other jobs' files; an explicit
`placement=` always wins.

## Where to go next

- {doc}`details/workflow_cli` for the `campaign` group in full.
- {doc}`collecting` for the records `collect --campaign` yields.
- {doc}`running` for managers, transfers and sealing.
