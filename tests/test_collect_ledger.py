"""The stable-id ledger hook of ``collect --into``.

The ledger keys a permanent entry id to each producing job coordinate, so a
store rebuilt from the same jobs keeps its ids no matter what order the sweep
visits them in. These tests drive the workflow helper (:func:`ledger_key`) and
the collect hook (:func:`_store_collected`) that allocates through the ledger,
each asserting something that fails without the machinery it exercises.
"""

import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from httk.core import DataRecord, Run, RunEdge
from httk.core.crypto import ed25519_generate_seed
from httk.core.storage import content_id

from httk.workflow import CollectedJob, JobRecord, UnstableIdentityError, job_records, ledger_key
from test_collect_fallback import _finished, _synthetic_item


def _ledger_records(path: Path) -> list[dict[str, str]]:
    """Read the sqlite id-ledger's records table as JSON-record-shaped dicts.

    The ledger is a stdlib-``sqlite3`` database now, not a JSON seal document;
    each row becomes a ``{key, family, id, alias_of, supersedes}`` mapping with
    absent (NULL) fields dropped, matching the old seal-record shape the
    assertions below were written against.

    :param path: The ledger database path.
    :return: The ordered ledger records.
    """

    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT key, family, id, alias_of, supersedes FROM records ORDER BY seq"
        ).fetchall()
    fields = ("key", "family", "id", "alias_of", "supersedes")
    return [{name: value for name, value in zip(fields, row) if value is not None} for row in rows]


def _keys() -> list[tuple[str, bytes]]:
    return [("test", ed25519_generate_seed())]


# --------------------------------------------------------------------------- #
# The key helper (native grammar + the identity_stable guard).
# --------------------------------------------------------------------------- #


def test_ledger_key_shapes(tmp_path: Path) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    item = _synthetic_item(record, "job-1", {"x": DataRecord.from_value("https://x", "a", 1)}, Run(source_id="ws:x"))
    ws, job = record.workspace_id, "job-1"

    assert ledger_key(item) == f"{ws}:{job}"
    assert ledger_key(item, role="total_energy") == f"{ws}:{job}:total_energy"
    assert ledger_key(item, path="OUTCAR") == f"{ws}:{job}:file:OUTCAR"
    assert ledger_key(item, role="total_energy", path="sub/OUTCAR") == f"{ws}:{job}:total_energy:file:sub/OUTCAR"
    # A bare JobRecord (live coordinate) yields the same base key.
    assert ledger_key(replace(record, job_id="job-1")) == f"{ws}:{job}"


def test_ledger_key_refuses_unstable_identity(tmp_path: Path) -> None:
    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    stable = _synthetic_item(record, "j", {"x": DataRecord.from_value("https://x", "a", 1)}, Run(source_id="ws:x"))
    unstable = replace(stable, identity_stable=False)

    with pytest.raises(UnstableIdentityError, match="not stable"):
        ledger_key(unstable, role="x")
    # The escape hatch builds the key anyway.
    assert ledger_key(unstable, role="x", force=True) == f"{record.workspace_id}:j:x"
    # A manifest-backed (True) or live (None) job is never refused.
    assert ledger_key(replace(stable, identity_stable=True), role="x").endswith(":x")


# --------------------------------------------------------------------------- #
# The collect hook.
# --------------------------------------------------------------------------- #


def _record_item(record: JobRecord, job_id: str, uri: str, name: str, value: int) -> tuple[CollectedJob, DataRecord]:
    entry = DataRecord.from_value(uri, name, value)
    run = Run(outputs=(RunEdge("out", "records", content_id(entry)),), source_id=f"ws:{job_id}")
    return _synthetic_item(record, job_id, {"out": entry}, run), entry


def test_ledger_stabilizes_ids_across_fresh_stores_regardless_of_sweep_order(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.core import DataRecordEntry

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    a, ea = _record_item(record, "A", "https://x", "a", 1)
    b, eb = _record_item(record, "B", "https://x", "b", 2)
    c, ec = _record_item(record, "C", "https://x", "c", 3)

    ledger = tmp_path / "shared.ids.sqlite"
    first_reports = _store_collected(
        [a, b, c],
        str(tmp_path / "one.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    # Second sweep: FRESH store, jobs visited in a different order, reusing the
    # ledger.  Without the ledger the store would renumber by visit order.
    second_reports = _store_collected(
        [c, a, b],
        str(tmp_path / "two.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )

    def _ids(store_path: Path) -> dict[str, str]:
        from httk.store import Backend, SqlStore

        with Backend.sqlite(store_path) as database:
            store = SqlStore(database)
            return {
                content_id(e): store.fetch_entry(DataRecordEntry, content_id(e), eager=True).id for e in (ea, eb, ec)
            }

    first = _ids(tmp_path / "one.sqlite")
    second = _ids(tmp_path / "two.sqlite")
    assert first == second
    # And the ids really are the ledger's, in first-sweep visit order.
    assert first[content_id(ea)] == "httk.probe.records-1-1"
    assert first[content_id(eb)] == "httk.probe.records-1-2"
    assert first[content_id(ec)] == "httk.probe.records-1-3"

    # Runs are ledgered too: the same job's run keeps its id across both stores,
    # even though the two sweeps visited the jobs in a different order.
    def _run_ids(items: list, reports: list) -> dict[str, str]:
        return {item.record.job_id: cast(Any, report["stored"])["run"] for item, report in zip(items, reports)}

    first_runs = _run_ids([a, b, c], first_reports)
    second_runs = _run_ids([c, a, b], second_reports)
    assert first_runs == second_runs
    # Distinct runs got distinct ledger ids, in first-sweep visit order.
    assert sorted(first_runs.values()) == ["httk.probe.runs-1-1", "httk.probe.runs-1-2", "httk.probe.runs-1-3"]

    # Remap is orthogonal to the run's own ledger id: the run's output edge still
    # resolves to the output entry's id, in both stores.
    def _run_out_edge(store_path: Path, run_id: str) -> str:
        from httk.core import Run
        from httk.store import Backend, SqlStore

        with Backend.sqlite(store_path) as database:
            store = SqlStore(database)
            searcher = store.searcher()
            variable = searcher.variable(Run)
            searcher.add(variable.id == run_id)
            searcher.output(variable, "run")
            return next(iter(searcher)).values[0].outputs[0].entry_id

    assert _run_out_edge(tmp_path / "one.sqlite", first_runs[a.record.job_id]) == first[content_id(ea)]
    assert _run_out_edge(tmp_path / "two.sqlite", second_runs[a.record.job_id]) == second[content_id(ea)]


def test_ledger_aliases_content_identical_outputs(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.core import DataRecordEntry
    from httk.store import Backend, SqlStore

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    # Two jobs, byte-identical outputs.
    a, entry = _record_item(record, "A", "https://x", "same", 7)
    b, entry_b = _record_item(record, "B", "https://x", "same", 7)
    assert content_id(entry) == content_id(entry_b)

    ledger = tmp_path / "dedup.ids.sqlite"
    reports = _store_collected(
        [a, b],
        str(tmp_path / "s.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    assert all("storage_error" not in r for r in reports)

    with Backend.sqlite(tmp_path / "s.sqlite") as database:
        store = SqlStore(database)
        stored_id = store.fetch_entry(DataRecordEntry, content_id(entry), eager=True).id
    # One row, one id: both reports name it.
    assert cast(Any, reports[0]["stored"])["entries"] == [stored_id]
    assert cast(Any, reports[1]["stored"])["entries"] == [stored_id]

    ws = record.workspace_id
    records = _ledger_records(ledger)
    assigned = {r["key"]: r for r in records if "id" in r}
    aliases = {r["key"]: r["alias_of"] for r in records if "alias_of" in r}
    # The output is assigned to one job and aliased from the other.
    assert assigned[f"{ws}:A:out"]["id"] == stored_id
    assert assigned[f"{ws}:A:out"]["family"] == "records"
    assert aliases == {f"{ws}:B:out": stored_id}
    # Each job's run is ledgered too, under the bare job coordinate.
    assert assigned[f"{ws}:A"]["family"] == "runs"
    assert assigned[f"{ws}:B"]["family"] == "runs"
    assert assigned[f"{ws}:A"]["id"] != assigned[f"{ws}:B"]["id"]


def test_ledger_skips_outputs_already_carrying_an_id(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    preassigned = replace(DataRecord.from_value("https://x", "a", 1), id="external.db-9-42")
    run = Run(outputs=(RunEdge("out", "records", "external.db-9-42"),), source_id="ws:pre")
    item = _synthetic_item(record, "pre", {"out": preassigned}, run)

    ledger = tmp_path / "skip.ids.sqlite"
    reports = _store_collected(
        [item],
        str(tmp_path / "s.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    assert cast(Any, reports[0]["stored"])["entries"] == ["external.db-9-42"]
    ws = record.workspace_id
    keys_in_ledger = {r["key"] for r in _ledger_records(ledger)}
    # The pre-assigned output never entered the ledger; only the run did.
    assert f"{ws}:pre:out" not in keys_in_ledger
    assert keys_in_ledger == {f"{ws}:pre"}


def test_unstable_identity_degrades_without_failing_the_collect(tmp_path: Path, caplog) -> None:
    pytest.importorskip("httk.store")

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    item, _entry = _record_item(record, "v1", "https://x", "a", 1)
    item = replace(item, identity_stable=False)

    ledger = tmp_path / "v1.ids.sqlite"
    with caplog.at_level("WARNING"):
        reports = _store_collected(
            [item],
            str(tmp_path / "s.sqlite"),
            id_base="httk.probe",
            id_series="1",
            ledger_path=str(ledger),
            ledger_keys=keys,
        )
    # Collect succeeds: the output is store-minted, and the ledger holds nothing.
    assert "storage_error" not in reports[0]
    assert cast(Any, reports[0]["stored"])["entries"] == ["httk.probe-1-1"]
    assert list(_ledger_records(ledger)) == []
    assert any("unstable identity" in message for message in caplog.messages)


def test_collect_into_default_ledger_is_created_and_opt_out_warns(tmp_path: Path, capsys, caplog) -> None:
    pytest.importorskip("httk.store")
    import base64

    from httk.core.cli import CLIContext

    from conftest import register_ws
    from httk.workflow.workflow_cli import command

    workspace, _ = _finished(tmp_path)
    # Point the workspace at a signing seed so the default ledger can be sealed.
    seed = tmp_path / "seal.seed"
    seed.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    workspace.set_setting("seal.keys", str(seed))

    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "collect-ledger")

    store = tmp_path / "into.sqlite"
    with caplog.at_level("WARNING"):
        assert (
            command(
                [
                    "collect",
                    "--workspace",
                    name,
                    "--allow-job-collector",
                    "--into",
                    str(store),
                    "--id-base",
                    "httk.probe",
                ],
                context,
            )
            == 0
        )
    # On by default: a keep-worthy ledger appears beside the store, announced loudly.
    assert (tmp_path / "into.sqlite.ids.sqlite").exists()
    assert any("creating id ledger" in message for message in caplog.messages)

    caplog.clear()
    optout = tmp_path / "plain.sqlite"
    with caplog.at_level("WARNING"):
        assert (
            command(
                [
                    "collect",
                    "--workspace",
                    name,
                    "--allow-job-collector",
                    "--into",
                    str(optout),
                    "--id-base",
                    "httk.probe",
                    "--no-id-ledger",
                ],
                context,
            )
            == 0
        )
    assert not (tmp_path / "plain.sqlite.ids.sqlite").exists()
    assert any("not be stable across rebuilds" in message.lower() for message in caplog.messages)


def test_collect_refuses_default_ledger_when_legacy_json_sibling_exists(tmp_path: Path) -> None:
    # Pre-release format change: the default ledger is <into>.ids.sqlite now. A
    # store with a stale <into>.ids.json sibling must NOT silently get a fresh
    # sqlite ledger (which would re-mint every id from 1); collect refuses.
    pytest.importorskip("httk.store")
    import base64

    from httk.core.cli import CLIContext

    from conftest import register_ws
    from httk.workflow.workflow_cli import command

    workspace, _ = _finished(tmp_path)
    seed = tmp_path / "seal.seed"
    seed.write_text(base64.b64encode(ed25519_generate_seed()).decode("ascii"), encoding="utf-8")
    workspace.set_setting("seal.keys", str(seed))
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, workspace.root, "collect-legacy-ledger")

    store = tmp_path / "into.sqlite"
    (tmp_path / "into.sqlite.ids.json").write_text("{}", encoding="utf-8")
    assert (
        command(
            ["collect", "--workspace", name, "--allow-job-collector", "--into", str(store), "--id-base", "httk.probe"],
            context,
        )
        == 2
    )
    # No fresh sqlite ledger was minted behind the user's back.
    assert not (tmp_path / "into.sqlite.ids.sqlite").exists()


def test_reopen_after_multi_family_sweep_does_not_brick(tmp_path: Path) -> None:
    # Records and runs are two families in one ledger.  With one shared base they
    # would both mint <base>-<series>-1 and the ledger's global id-uniqueness
    # would reject the reopen; family-distinct bases keep them apart.
    pytest.importorskip("httk.store")
    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    item, _entry = _record_item(record, "A", "https://x", "a", 1)
    ledger = tmp_path / "multi.ids.sqlite"

    first = _store_collected(
        [item],
        str(tmp_path / "one.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    # The bricking scenario: a second sweep must reopen the ledger cleanly.
    second = _store_collected(
        [item],
        str(tmp_path / "two.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    assert all("storage_error" not in r for r in (*first, *second))
    assert cast(Any, first[0]["stored"])["entries"] == cast(Any, second[0]["stored"])["entries"]
    assert first[0]["stored"] == second[0]["stored"]


def test_cross_sweep_content_dedup_aliases_without_bogus_assignment(tmp_path: Path) -> None:
    # Sweep 1 ledgers A.  Sweep 2 re-collects A and adds B, whose output is
    # byte-identical to A's, into a FRESH store reusing the same ledger.  B must
    # alias onto A's id, not mint a fresh (bogus) one.
    pytest.importorskip("httk.store")
    from httk.core import DataRecordEntry
    from httk.store import Backend, SqlStore

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    a, entry = _record_item(record, "A", "https://x", "same", 5)
    b, entry_b = _record_item(record, "B", "https://x", "same", 5)
    assert content_id(entry) == content_id(entry_b)
    ledger = tmp_path / "cross.ids.sqlite"

    _store_collected(
        [a],
        str(tmp_path / "one.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    reports = _store_collected(
        [a, b],
        str(tmp_path / "two.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    with Backend.sqlite(tmp_path / "two.sqlite") as database:
        store = SqlStore(database)
        stored_id = store.fetch_entry(DataRecordEntry, content_id(entry), eager=True).id
    assert cast(Any, reports[0]["stored"])["entries"] == [stored_id]
    assert cast(Any, reports[1]["stored"])["entries"] == [stored_id]

    ws = record.workspace_id
    records = _ledger_records(ledger)
    assigned = {r["key"]: r for r in records if "id" in r}
    aliases = {r["key"]: r["alias_of"] for r in records if "alias_of" in r}
    # A's output owns the id; B's output is an alias — never its own assignment.
    assert assigned[f"{ws}:A:out"]["id"] == stored_id
    assert f"{ws}:B:out" not in assigned
    assert aliases[f"{ws}:B:out"] == stored_id


def test_ledger_leaves_a_non_conforming_user_id_untouched(tmp_path: Path) -> None:
    # A user id the store accepts but that is not the base-series-number shape
    # (e.g. "mydb:foo") must NOT be mistaken for "no id yet" and overwritten.
    pytest.importorskip("httk.store")

    from httk.workflow.workflow_cli._collect import _store_collected

    workspace, _ = _finished(tmp_path)
    record = next(job_records(workspace))
    keys = _keys()
    preassigned = replace(DataRecord.from_value("https://x", "a", 1), id="mydb:foo")
    run = Run(outputs=(RunEdge("out", "records", "mydb:foo"),), source_id="ws:nc")
    item = _synthetic_item(record, "nc", {"out": preassigned}, run)

    ledger = tmp_path / "nc.ids.sqlite"
    reports = _store_collected(
        [item],
        str(tmp_path / "s.sqlite"),
        id_base="httk.probe",
        id_series="1",
        ledger_path=str(ledger),
        ledger_keys=keys,
    )
    # Saved with its own id, and the output never entered the ledger.
    assert cast(Any, reports[0]["stored"])["entries"] == ["mydb:foo"]
    ws = record.workspace_id
    assert f"{ws}:nc:out" not in {r["key"] for r in _ledger_records(ledger)}
