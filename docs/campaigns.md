# Campaigns

*Spreading a very large body of work across many workspaces — a convention and a
few helpers, not a new scheduler.*

A **campaign** is how one project runs work at a scale a single workspace should
not hold: not a new graph, not a new engine, not a sharding policy inside the
scheduler, but a *partition map* over the ordinary workspaces you already
{doc}`register <workflow_cli>`. Each partition is a named bucket that points at
one workspace name in the machine registry; every command that drives a
workspace drives a partition's workspace unchanged. Multi-workspace partitioning is the intended
route to a campaign larger than one measured workspace: its partition sizes are
chosen from the local measurements in {doc}`benchmarks`, with each partition an
ordinary workspace a manager serves and a collect reads.

The map lives in the project, in a `campaign` member of `project.json`, so it
travels with the project and every helper reads it from there.

## The two rules

Two rules keep this safe at scale without any new machinery:

- **Root jobs are assigned per policy.** When you submit a *root* job, the
  campaign picks which partition it lands in — by content, by position, or by
  your explicit choice.
- **Dynamically spawned children always inherit their parent's workspace.** This
  is already true of the execution engine: a child is scaffolded into the same
  workspace its parent runs in. A campaign therefore never re-routes a subtree,
  and this convention changes nothing about spawning — the whole tree below a
  root stays in the partition the root was assigned.

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

`campaign init` writes the map: each `--partition NAME=WORKSPACE` names a bucket
and the workspace name it points at, and `--assignment` sets how a root
job's partition is chosen. The workspaces must be registered first with
{doc}`workspace init <workflow_cli>` — a partition names a workspace the same way
every other command does, never a bare path.

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
$ httk workflow campaign submit --workflow vasp-relax --key silicon \
      --parameter structure=structures/Si.vasp --tag silicon
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
    "vasp-relax",
    key="silicon",
    project="my-project",
    files={"POSCAR": "structures/Si.vasp"},
    tag="silicon",
)
```

Only the partition's *local* workspaces can be submitted into directly; a
partition that points at a remote workspace is submitted to locally and moved
with {doc}`transfer <workflow_cli>`, because a job is created where the client
runs.

## Running and collecting across partitions

```console
$ httk workflow campaign start-managers            # every partition
$ httk workflow campaign start-managers --partition north
$ httk workflow campaign collect --state succeeded
```

`campaign start-managers` starts one manager per selected partition: in this
process for a local partition, and submitted through the remote's scheduler for a
remote one — exactly as {doc}`manager run <workflow_cli>` does for a single
workspace. Each partition's target workspace supplies its own scheduler profile
through workspace settings. `campaign collect` chains `campaign_collect()` across the
partitions
lazily, one workspace after another in stable order, so a campaign spread over
many workspaces streams as one collect without ever materializing more than the
record in hand.

Both take `--partition` to act on a subset, so a campaign can be managed and
collected a few partitions at a time.

## Bounded fan-out: placement recipes

Partitioning across workspaces bounds *how many jobs one workspace holds*; within
a workspace, `--placement` bounds *how wide any one directory gets*. A workspace
scans its state tree by placement, so a flat directory with many markers is one
wide `scandir`, while a shallow tree is cheaper to walk and resume. These are
recipes, not policy — the engine imposes none of them:

- **Hash-prefix fan-out** — place a job under a short prefix of its key's hash:
  `project/<hash-prefix>/<batch>`. Multiple prefixes turn one wide directory
  into a shallow tree with a bounded fan-out at every level.
- **Batch buckets** — place a submission run under its own `project/<date>/<run>`,
  so each batch is a subtree a collect or a placement-scoped manager can take on
  its own.
- **Per-source subtrees** — place each structure family under
  `project/<family>/…`, so a partition's work is browsable by what it is.

A manager restricted with `--placement-prefix` serves exactly one such subtree,
so a campaign's partitions *and* their internal subtrees can each be given their
own managers.

## Where to go next

- {doc}`workflow_cli` — `workspace init`, `transfer`, `manager run`, and the
  `campaign` group in full.
- {doc}`collecting` — the `JobRecord` stream `campaign_collect()` provides, and the data-layer
  boundary it marks.
- {doc}`taskmanager` — running managers for real.
