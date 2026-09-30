"""The recognized-calculation walker: collect_tree, claims and content_digest."""

import bz2
import gzip
import logging
import os
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pytest
from httk.core.register import codes
from httk.core.register.codes import register_collector
from httk.core.storage import content_id

from conftest import TestProfile as _TestProfile
from httk.workflow import (
    AmbiguousClaimError,
    DirectoryClaim,
    IdentityCollisionError,
    Workspace,
    claims,
    collect_tree,
    store_collected,
)
from httk.workflow.calculations import content_digest

_MANIFEST = """
[workflow]
name = "{name}"
description = "a finished test calculation"

[workflow.recognize]
file = "recognize.py"
priority = {priority}
requires = {requires}

[workflow.inputs.structure]
role = "initial_structure"
entry_type = "structures"

[workflow.outputs.total_energy]
role = "total_energy"
entry_type = "records"
ref = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"
product_of = "initial_structure"

[workflow.collect]
file = "collect.py"
"""

_RECOGNIZE = """
from pathlib import Path

from httk.workflow.calculations import content_digest
from httk.workflow.hookapi import Claim, Unclaimed


def recognize(directory):
    with Path(__file__).with_name("calls.log").open("a", encoding="utf-8") as log:
        log.write(directory.name + "\\n")
    if (directory / "DECLINE").exists():
        return Unclaimed("declined on request")
    if (directory / "BOOM").exists():
        raise RuntimeError("boom")
    if (directory / "ODD").exists():
        return "not a claim"
    return Claim(content_digest(directory, ["INPUT"]))
"""

_COLLECT = """
from httk.core import DataRecord

TOTAL_ENERGY = "https://schemas.httk.org/defs/v0.1/properties/core/total_energy"


def collect(record):
    workdir = record.workdir
    if (workdir / "FAIL").exists():
        raise RuntimeError("cannot read the energy")
    energy = float(record.result_file("ENERGY").read_text(encoding="utf-8"))
    result = {"total_energy": DataRecord.from_value(TOTAL_ENERGY, "_httk_total_energy", energy)}
    if (workdir / "POSCAR").exists():
        import httk.core

        result["initial_structure"] = httk.core.load(workdir / "POSCAR", precision=1e-3)
    if (workdir / "CONTCAR").exists():
        import httk.core

        result["relaxed_structure"] = httk.core.load(workdir / "CONTCAR", precision=1e-3)
    return result
"""

_POSCAR = "Si\n1.0\n5 0 0\n0 5 0\n0 0 5\nSi\n1\nDirect\n0 0 0\n"


def _collector(
    root: Path,
    name: str = "tests.calc",
    *,
    priority: int = 1,
    requires: tuple[str, ...] = ("INPUT", "ENERGY"),
    recognize: str = _RECOGNIZE,
    extra: str = "",
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    markers = "[" + ", ".join(f'"{marker}"' for marker in requires) + "]"
    manifest = _MANIFEST.format(name=name, priority=priority, requires=markers) + extra
    (root / "httk_workflow.toml").write_text(manifest, encoding="utf-8")
    (root / "recognize.py").write_text(recognize, encoding="utf-8")
    (root / "collect.py").write_text(_COLLECT, encoding="utf-8")
    return root


def _calls(package: Path) -> list[str]:
    log = package / "calls.log"
    return log.read_text(encoding="utf-8").split() if log.exists() else []


def _calculation(directory: Path, input_text: str = "input", energy: str = "-1.5", **extra: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "INPUT").write_text(input_text, encoding="utf-8")
    (directory / "ENERGY").write_text(energy, encoding="utf-8")
    for name, text in extra.items():
        (directory / name).write_text(text, encoding="utf-8")
    return directory


@pytest.fixture(autouse=True)
def _no_registered_collectors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(codes, "_collectors", {})


def test_a_calculation_directory_is_collected_with_its_input(tmp_path: Path) -> None:
    pytest.importorskip("httk.atomistic")
    package = _collector(tmp_path / "pkg")
    calculation = _calculation(tmp_path / "tree" / "si", POSCAR=_POSCAR)

    items = list(collect_tree(tmp_path / "tree", collectors=(package,)))

    assert len(items) == 1
    item = items[0]
    identity = content_digest(calculation, ["INPUT"])
    assert item.missing_collector is None
    assert item.record.workspace_id == "tests.calc" and item.record.job_id == identity
    assert item.record.workdir == calculation
    assert item.run.source_id == f"tests.calc:{identity}"
    assert item.run.workflow_definition_uri is None
    assert item.identity_stable is True
    structure = item.inputs["initial_structure"]
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in item.run.inputs] == [
        ("initial_structure", "structures", content_id(structure))
    ]
    energy = item.outputs["total_energy"]
    assert energy.value == -1.5  # type: ignore[attr-defined]
    assert [(edge.label, edge.entry_id) for edge in energy.product_of] == [  # type: ignore[attr-defined]
        ("initial_structure", content_id(structure))
    ]
    assert [(link.source_id, link.target_id) for link in item.products] == [(content_id(structure), content_id(energy))]


def test_a_nested_tree_is_collected_in_sorted_walk_order(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "b1", "one")
    _calculation(tree / "b1" / "sub", "two")
    _calculation(tree / "a" / "mid" / "c", "three")
    (tree / "a" / "plain.txt").write_text("not a calculation", encoding="utf-8")

    items = list(collect_tree(tree, collectors=(package,)))

    assert [item.record.workdir_path.as_posix() for item in items if item.record.workdir_path] == [
        "a/mid/c",
        "b1",
        "b1/sub",
    ]
    assert [item.record.placement.as_posix() for item in items] == ["a/mid", ".", "b1"]
    assert all(item.missing_collector is None and "initial_structure" not in item.inputs for item in items)
    assert all(item.run.inputs == () for item in items)


def test_the_root_itself_is_a_candidate(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = _calculation(tmp_path / "tree")

    (item,) = collect_tree(tree, collectors=(package,))

    assert item.record.workdir_path is not None and item.record.workdir_path.as_posix() == "."
    assert item.record.workdir == tree.resolve()


def test_markers_gate_the_hook_case_insensitively_through_compression(tmp_path: Path) -> None:
    exact = _collector(tmp_path / "exact")
    glob = _collector(tmp_path / "glob", "tests.glob", requires=("*.out",))
    tree = tmp_path / "tree"
    (tree / "missing").mkdir(parents=True)
    (tree / "missing" / "INPUT").write_text("only the input", encoding="utf-8")
    lower = tree / "lower"
    lower.mkdir()
    (lower / "input").write_text("lower", encoding="utf-8")
    (lower / "energy.gz").write_bytes(gzip.compress(b"-2"))
    compressed = tree / "compressed"
    compressed.mkdir()
    (compressed / "INPUT").write_text("compressed", encoding="utf-8")
    (compressed / "x.out.bz2").write_bytes(bz2.compress(b"output"))
    upper = tree / "upper"
    upper.mkdir()
    (upper / "INPUT").write_text("upper", encoding="utf-8")
    (upper / "RUN.OUT").write_text("output", encoding="utf-8")

    outcomes = list(claims(tree, collectors=(exact, glob)))

    assert [(item.directory, item.collector) for item in outcomes] == [
        ("compressed", "tests.glob"),
        ("lower", "tests.calc"),
        ("upper", "tests.glob"),
    ]
    assert "missing" not in _calls(exact) + _calls(glob)
    assert _calls(exact) == ["lower"]


def test_the_highest_priority_wins_and_ties_need_prefer(tmp_path: Path) -> None:
    low = _collector(tmp_path / "low", "tests.low", priority=1)
    high = _collector(tmp_path / "high", "tests.high", priority=5)
    tie = _collector(tmp_path / "tie", "tests.tie", priority=5)
    tree = _calculation(tmp_path / "tree" / "calc").parent

    (outcome,) = claims(tree, collectors=(low, high))
    assert outcome == DirectoryClaim(
        "calc",
        "claimed",
        "tests.high",
        5,
        content_digest(tree / "calc", ["INPUT"]),
        also_matched=("tests.low",),
    )

    with pytest.raises(AmbiguousClaimError, match=r"calc: collectors tests\.high, tests\.tie .*priority 5.*prefer"):
        list(claims(tree, collectors=(low, high, tie)))
    (outcome,) = claims(tree, collectors=(low, high, tie), prefer=("tests.nothing", "tests.tie"))
    assert outcome.collector == "tests.tie" and outcome.also_matched == ("tests.high", "tests.low")
    (item,) = collect_tree(tree, collectors=(low, high, tie), prefer=("tests.tie",))
    assert item.record.workspace_id == "tests.tie"


def test_declined_and_failing_hooks_are_reported_unclaimed(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "boom", BOOM="")
    _calculation(tree / "declined", DECLINE="")
    _calculation(tree / "odd", ODD="")

    outcomes = list(claims(tree, collectors=(package,)))

    assert [(item.directory, item.kind, item.collector) for item in outcomes] == [
        ("boom", "unclaimed", "tests.calc"),
        ("declined", "unclaimed", "tests.calc"),
        ("odd", "unclaimed", "tests.calc"),
    ]
    assert outcomes[0].reason == "recognize failed: boom"
    assert outcomes[1].reason == "declined on request"
    assert outcomes[2].reason == "recognize returned str"
    assert list(collect_tree(tree, collectors=(package,))) == []


def test_copies_deduplicate_and_different_content_collides(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "orig")
    _calculation(tree / "copy1")

    items = list(collect_tree(tree, collectors=(package,)))
    assert [item.record.workdir_path.as_posix() for item in items if item.record.workdir_path] == ["copy1"]
    outcomes = list(claims(tree, collectors=(package,)))
    assert [(item.directory, item.duplicate_of) for item in outcomes] == [("copy1", None), ("orig", "copy1")]

    (tree / "orig" / "ENERGY").write_text("-7.0", encoding="utf-8")
    with pytest.raises(IdentityCollisionError, match=r"copy1 and orig both claim tests\.calc:.*exclude"):
        list(collect_tree(tree, collectors=(package,)))
    (item,) = collect_tree(tree, collectors=(package,), exclude=("copy*",))
    assert item.record.workdir_path is not None and item.record.workdir_path.as_posix() == "orig"


def test_a_nested_workspace_is_reported_and_not_descended(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    workspace = Workspace.initialize(tree / "ws")
    _calculation(workspace.root / "calc")
    _calculation(tree / "zcalc")

    outcomes = list(claims(tree, collectors=(package,)))

    assert [(item.directory, item.kind) for item in outcomes] == [("ws", "workspace"), ("zcalc", "claimed")]
    assert outcomes[0].collector is None
    assert _calls(package) == ["zcalc"]
    items = list(collect_tree(tree, collectors=(package,)))
    assert [item.record.workspace_id for item in items] == ["tests.calc"]


def test_symlinked_and_dot_directories_are_not_visited(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / ".hidden")
    outside = _calculation(tmp_path / "outside")
    tree.joinpath("link").symlink_to(outside, target_is_directory=True)

    assert list(claims(tree, collectors=(package,))) == []
    assert _calls(package) == []


def test_a_collect_hook_failure_degrades_or_fails_fast(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "bad", FAIL="")

    (item,) = collect_tree(tree, collectors=(package,))
    assert item.missing_collector == "bad: cannot read the energy"
    assert item.unfulfilled == ("total_energy",) and item.identity_stable is True

    with pytest.raises(ValueError, match="bad: cannot read the energy"):
        list(collect_tree(tree, collectors=(package,), fail_fast=True))


def test_a_local_collector_overrides_a_registered_one(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fixtures = tmp_path / "importable"
    registered = _collector(fixtures / "calcfixtures" / "collectors" / "calc")
    monkeypatch.syspath_prepend(str(fixtures))
    register_collector("tests.calc", package="calcfixtures:collectors/calc")
    tree = tmp_path / "tree"
    _calculation(tree / "calc")

    (outcome,) = claims(tree)
    assert outcome.kind == "claimed" and outcome.collector == "tests.calc"
    assert _calls(registered) == ["calc"]

    declining = (
        'from httk.workflow.hookapi import Unclaimed\n\ndef recognize(directory):\n    return Unclaimed("local")\n'
    )
    local = _collector(tmp_path / "local", recognize=declining)
    (outcome,) = claims(tree, collectors=(local,))
    assert (outcome.kind, outcome.reason) == ("unclaimed", "local")
    assert _calls(registered) == ["calc"]


def test_a_broken_registered_collector_is_logged_and_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    register_collector("tests.broken", package="calcfixtures_absent:collectors/broken")
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "calc")

    with caplog.at_level(logging.WARNING, logger="httk.workflow.calculations"):
        outcomes = list(claims(tree, collectors=(package,)))

    assert [item.collector for item in outcomes] == ["tests.calc"]
    assert "skipping registered collector tests.broken" in caplog.text


def test_sweep_arguments_are_validated(tmp_path: Path) -> None:
    package = _collector(tmp_path / "pkg")
    with pytest.raises(ValueError, match="not a directory"):
        list(claims(tmp_path / "absent", collectors=(package,)))
    with pytest.raises(ValueError, match="two collector packages are named"):
        list(claims(tmp_path, collectors=(package, package)))
    runner = tmp_path / "runner"
    runner.mkdir()
    (runner / "httk_workflow.toml").write_text('[workflow]\nname = "x"\n\n[workflow.runner]\nsteps = ["start"]\n')
    (runner / "run").write_text("")
    with pytest.raises(ValueError, match="no \\[workflow.recognize\\]"):
        list(claims(tmp_path, collectors=(runner,)))


def test_content_digest_frames_presence_compression_and_order(tmp_path: Path) -> None:
    plain = tmp_path / "plain"
    plain.mkdir()
    (plain / "x").write_bytes(b"content")
    (plain / "y").write_bytes(b"")
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / "x.gz").write_bytes(gzip.compress(b"content"))
    (packed / "y").write_bytes(b"")
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "x").write_bytes(b"content")

    assert content_digest(plain, ["x", "y"]) == content_digest(packed, ["y", "x", "x"])
    assert content_digest(plain, ["x", "y"]) != content_digest(empty, ["x", "y"])
    assert content_digest(plain, ["x"]) == content_digest(empty, ["x"])


def test_unclaimed_directories_are_reported_while_collecting(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "declined", DECLINE="")
    _calculation(tree / "kept")
    seen: list[DirectoryClaim] = []

    with caplog.at_level(logging.WARNING, logger="httk.workflow.calculations"):
        items = list(collect_tree(tree, collectors=(package,), on_unclaimed=seen.append))

    assert [item.record.workdir_path for item in items] == [PurePosixPath("kept")]
    assert [(claim.directory, claim.reason) for claim in seen] == [("declined", "declined on request")]
    assert "declined: not collected by tests.calc: declined on request" in caplog.text


def test_store_collected_stores_the_input_the_run_names(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    pytest.importorskip("httk.atomistic")
    from httk.atomistic.entries.structures import StructureEntry  # pyright: ignore[reportMissingImports]
    from httk.core import Run
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    package = _collector(tmp_path / "pkg")
    _calculation(tmp_path / "tree" / "si", POSCAR=_POSCAR)
    (item,) = collect_tree(tmp_path / "tree", collectors=(package,))

    (report,) = store_collected([item], str(tmp_path / "store.sqlite"), id_base="httk.probe")

    assert "storage_error" not in report and report["revised"] is False
    stored = report["stored"]
    assert isinstance(stored, dict)
    with Backend.sqlite(tmp_path / "store.sqlite") as database:
        store = SqlStore(database)
        structure = store.fetch_entry(StructureEntry, content_id(item.inputs["initial_structure"]), eager=True)
        searcher = store.searcher()
        variable = searcher.variable(Run)
        searcher.add(variable.id == stored["run"])
        run = next(iter(searcher.results(run=variable))).run
    assert structure is not None and structure.id in stored["entries"]
    assert [(edge.label, edge.entry_type, edge.entry_id) for edge in run.inputs] == [
        ("initial_structure", "structures", structure.id)
    ]


def test_a_recollected_changed_calculation_is_a_revision(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.core import DataRecord
    from httk.core.crypto import ed25519_generate_seed
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    _calculation(tree / "calc")
    path = tmp_path / "store.sqlite"
    options: dict[str, Any] = {
        "id_base": "httk.probe",
        "ledger_path": str(tmp_path / "store.sqlite.ids.sqlite"),
        "ledger_keys": [("test", ed25519_generate_seed())],
    }

    def sweep() -> dict[str, object]:
        (report,) = store_collected(list(collect_tree(tree, collectors=(package,))), str(path), **options)
        assert "storage_error" not in report
        return report

    first = sweep()
    (tree / "calc" / "ENERGY").write_text("-3.0", encoding="utf-8")
    second = sweep()
    third = sweep()

    assert (first["revised"], second["revised"], third["revised"]) == (False, True, False)
    assert first["stored"] == second["stored"] == third["stored"]
    stored = first["stored"]
    assert isinstance(stored, dict)
    (record_id,) = stored["entries"]
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        searcher = store.searcher(only_latest=True)
        variable = searcher.variable(DataRecord)
        searcher.add(variable.id == record_id)
        latest = next(iter(searcher.results(entry=variable))).entry
        assert latest.value == -3.0
        assert [revision.value for revision in store.history(latest)] == [-1.5, -3.0]

    # Reverting to an earlier revision's content is an idempotent no-op replace in
    # the store: no row is written, so the sweep does not report a revision (and the
    # lineage keeps its latest row).
    (tree / "calc" / "ENERGY").write_text("-1.5", encoding="utf-8")
    assert sweep()["revised"] is False
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        searcher = store.searcher(only_latest=True)
        variable = searcher.variable(DataRecord)
        searcher.add(variable.id == record_id)
        latest = next(iter(searcher.results(entry=variable))).entry
        assert [revision.value for revision in store.history(latest)] == [-1.5, -3.0]


def test_a_changed_job_never_revises_an_entry_it_shares_through_an_alias(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from httk.core import DataRecord
    from httk.core.crypto import ed25519_generate_seed
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    # Different inputs (two calculations), identical energy records: b is aliased to a's id.
    _calculation(tree / "a", "input a")
    _calculation(tree / "b", "input b")
    path = tmp_path / "store.sqlite"
    options: dict[str, Any] = {
        "id_base": "httk.probe",
        "ledger_path": str(tmp_path / "store.sqlite.ids.sqlite"),
        "ledger_keys": [("test", ed25519_generate_seed())],
    }
    first = store_collected(list(collect_tree(tree, collectors=(package,))), str(path), **options)
    ids = [cast(dict[str, list[str]], report["stored"])["entries"] for report in first]
    assert ids[0] == ids[1]
    (record_id,) = ids[0]

    (tree / "b" / "ENERGY").write_text("-3.0", encoding="utf-8")
    a, b = store_collected(list(collect_tree(tree, collectors=(package,))), str(path), **options)

    # b's change is refused loudly, exactly as before revisions existed, rather than
    # rewriting the entry a still names.
    assert a["revised"] is False and "storage_error" not in a
    assert b["revised"] is False
    assert f"entry id {record_id!r}" in str(b["storage_error"]) and "belongs to" in str(b["storage_error"])
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        searcher = store.searcher(only_latest=True)
        variable = searcher.variable(DataRecord)
        searcher.add(variable.id == record_id)
        latest = next(iter(searcher.results(entry=variable))).entry
        assert latest.value == -1.5
        assert len(store.history(latest)) == 1


def test_a_recognize_hook_import_error_propagates(tmp_path: Path) -> None:
    hook = "def recognize(directory):\n    import httk_no_such_module_for_this_test  # noqa: F401\n"
    package = _collector(tmp_path / "pkg", recognize=hook)
    _calculation(tmp_path / "tree" / "calc")

    with pytest.raises(ImportError, match="httk_no_such_module_for_this_test"):
        list(collect_tree(tmp_path / "tree", collectors=(package,)))


def test_an_unstorable_input_type_is_a_storage_error(tmp_path: Path) -> None:
    pytest.importorskip("httk.store")
    from dataclasses import replace

    package = _collector(tmp_path / "pkg")
    _calculation(tmp_path / "tree" / "calc")
    (item,) = collect_tree(tmp_path / "tree", collectors=(package,))
    odd = type("Odd", (), {"type": "httk_no_such_entry_type", "id": "x"})()
    item = replace(item, inputs={"initial_structure": odd})

    (report,) = store_collected([item], str(tmp_path / "store.sqlite"), id_base="httk.probe")

    assert "cannot store entry type 'httk_no_such_entry_type'" in str(report["storage_error"])


_RELAXED = """
[workflow.outputs.relaxed_structure]
role = "relaxed_structure"
entry_type = "structures"
product_of = "initial_structure"
"""


def test_an_unchanged_relaxation_is_not_its_own_product(tmp_path: Path) -> None:
    pytest.importorskip("httk.atomistic")
    package = _collector(tmp_path / "pkg", extra=_RELAXED)
    _calculation(tmp_path / "tree" / "si", POSCAR=_POSCAR, CONTCAR=_POSCAR)

    (item,) = collect_tree(tmp_path / "tree", collectors=(package,))

    assert item.missing_collector is None and item.products_unlinked == ()
    structure = content_id(item.inputs["initial_structure"])
    assert content_id(item.outputs["relaxed_structure"]) == structure
    assert [(edge.label, edge.entry_id) for edge in item.run.inputs] == [("initial_structure", structure)]
    assert ("relaxed_structure", structure) in [(edge.label, edge.entry_id) for edge in item.run.outputs]
    # The other curation still links: the energy is a product of the structure.
    assert [(link.label, link.source_id) for link in item.products] == [("total_energy", structure)]

    pytest.importorskip("httk.store")
    from httk.core import Run
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    (report,) = store_collected([item], str(tmp_path / "store.sqlite"), id_base="httk.probe")
    assert "storage_error" not in report
    stored = report["stored"]
    assert isinstance(stored, dict)
    with Backend.sqlite(tmp_path / "store.sqlite") as database:
        searcher = SqlStore(database).searcher()
        variable = searcher.variable(Run)
        searcher.add(variable.id == stored["run"])
        run = next(iter(searcher.results(run=variable))).run
    (source,) = run.inputs
    assert {edge.label: edge.entry_id for edge in run.outputs}["relaxed_structure"] == source.entry_id


def test_claims_scale_to_wide_trees(tmp_path: Path, test_profile: _TestProfile) -> None:
    empty, calculations = test_profile.scale(normal=(200, 5), extended=(5000, 50))
    package = _collector(tmp_path / "pkg")
    tree = tmp_path / "tree"
    for index in range(empty):
        os.makedirs(tree / f"e{index:05d}" / "sub")
    for index in range(calculations):
        _calculation(tree / f"c{index:05d}", f"input {index}")

    outcomes = [item for item in claims(tree, collectors=(package,)) if item.kind == "claimed"]

    assert len(outcomes) == calculations
