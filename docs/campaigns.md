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
$ httk workflow campaign init \
      --partition north=screening-a \
      --partition south=screening-b \
      --assignment hash
$ httk workflow campaign show
assignment	hash
north	screening-a
south	screening-b
```

Each `--partition NAME=WORKSPACE` names a bucket and the registered workspace
it points at, and `--assignment` sets how a root job's partition is chosen.
The workspaces must exist first ({doc}`workspaces`); a partition names a
workspace the way every other command does, never a bare path.

### Assignment policies

| Policy | How a root is placed |
| --- | --- |
| `hash` | The submission key is hashed to a partition deterministically, so the same key always lands in the same partition. This is the default: it spreads a stream of distinct keys evenly and reproducibly without any shared counter. |
| `round-robin` | The batch position (`--index`) selects the partition, so a batch fans out evenly across the partitions in order. |
| `explicit` | The key *is* the partition name: the caller places each root outright. |

The partitions are always visited in a stable (sorted) order, so both a hash and
an index map to a partition reproducibly, run after run.

## Submitting into a campaign

```console
$ httk workflow campaign submit --workflow vasp.relax --key silicon \
      --input structure=structures/Si.vasp --tag silicon
silicon--0c4f…	/…/screening-a/jobs/silicon--0c4f…
```

`campaign submit` assigns `--key` to a partition and submits one root job into
that partition's workspace. Everything the root later spawns follows it there.
In Python:

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
)
```

A job is created where the client runs, so a partition that points at a
remote workspace is submitted to locally and moved with `httk job transfer`
({doc}`running`).

## Running and collecting across partitions

```console
$ httk workflow campaign start-managers            # every partition
$ httk workflow campaign start-managers --partition north
$ httk workflow campaign collect --state succeeded
```

`campaign start-managers` starts managers at each selected partition through
that workspace's own launcher, exactly as `httk workflow run` does for one
workspace; for a remote partition the manager command runs on the owning
machine. `campaign collect` collects the partitions one after another in
stable order and accepts the same `--into`, `--batch-size` and `--fail-fast`
options as `httk collect`. Both take `--partition` to act on a subset.

## Bounded fan-out: placement recipes

Partitioning bounds how many jobs one workspace holds; within a workspace,
`--placement` bounds how wide any one directory gets, since a shallow tree is
cheaper to scan and resume than one flat directory of markers. The engine
imposes no scheme; common ones are a short hash prefix of the job key
(`project/<hash-prefix>/<batch>`), one subtree per submission batch
(`project/<date>/<run>`), or one per structure family. A manager started with
`--placement-prefix` serves exactly one such subtree.

## Where to go next

- {doc}`details/workflow_cli` for the `campaign` group in full.
- {doc}`collecting` for the records `campaign collect` yields.
- {doc}`running` for managers, transfers and sealing.
