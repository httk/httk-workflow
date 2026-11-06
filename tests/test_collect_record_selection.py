"""Record selection during collection: ``store_collected(entry_records=...)`` and ``--records``."""

import datetime
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from fractions import Fraction
from pathlib import Path
from typing import Annotated, Any, ClassVar, cast

import pytest
from httk.core import DataRecord, FileRecord, Run, RunEdge
from httk.core.cli import CLIContext
from httk.core.storage import (
    IdentitySkip,
    IdentitySkipWhenNone,
    Indexed,
    Shape,
    StorageInfo,
    StoredPropertyProjection,
    Unique,
    content_id,
    float_matrix,
)

from conftest import register_ws
from httk.workflow import job_records, store_collected
from httk.workflow.collecting import JobRecord, collect
from httk.workflow.storing import _record_selection, _storage_layout
from httk.workflow.workflow_cli import command
from test_collect_fallback import _finished as _finished_job
from test_collect_fallback import _synthetic_item
from test_collect_inputs import (
    _DIAMOND,
    _input_edge,
    _stored,
    _structure_ids,
    _unregister,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)
from test_collect_inputs import _finished as _finished_inputs

pytest.importorskip("httk.store")
pytest.importorskip("httk.atomistic")
from httk.atomistic import UnitcellStructure

_NUMERIC = {"structures": ["atomistic-numeric-unitcell-structure"]}
_SITES = "test-collect-selection-sites-structure"
_FILELESS = "test-collect-selection-fileless-structure"


@dataclass(frozen=True)
class SiteRow:
    """One site of :class:`SitesStructureRecord`: its fractional position and element."""

    __httk_storage__: ClassVar[StorageInfo] = StorageInfo(storage_name="test_collect_selection_site", dedup="none")

    x: float
    y: float
    z: float
    symbol: str


@dataclass(frozen=True)
class SitesStructureRecord:
    """A user-defined structure layout: a float basis and a table of sites."""

    __httk_storage__: ClassVar[StorageInfo] = StorageInfo(storage_name="test_collect_selection_sites_structure")
    __httk_canonical_source__: ClassVar = UnitcellStructure
    __httk_stored_properties__: ClassVar[Mapping[str, StoredPropertyProjection]] = {
        "structure_features": StoredPropertyProjection(response=lambda record: [])
    }

    basis: Annotated[tuple[tuple[float, ...], ...], Shape(3, 3)]
    sites: tuple[SiteRow, ...]
    source_file: Annotated[FileRecord | None, IdentitySkipWhenNone()] = None
    id: Annotated[str | None, IdentitySkip(), Indexed()] = field(default=None, compare=False)
    immutable_id: Annotated[str | None, IdentitySkip(), Unique()] = field(default=None, compare=False)
    last_modified: Annotated[datetime.datetime | None, IdentitySkip()] = field(default=None, compare=False)

    @property
    def type(self) -> str:
        """Return the entry type name."""
        return "structures"

    @classmethod
    def __httk_project__(cls, structure: Any) -> Mapping[str, object]:
        if any(len(species.chemical_symbols) != 1 for species in structure.species):
            raise KeyError("a site row carries exactly one element")
        symbol_of = {s.name: s.chemical_symbols[0] for s in structure.species}
        positions = zip(structure.fractional_site_positions, structure.species_at_sites, strict=True)
        return {
            "basis": float_matrix(structure.lattice_vectors, 3, rows=3),
            "sites": tuple(SiteRow(float(x), float(y), float(z), symbol_of[n]) for (x, y, z), n in positions),
            "source_file": None,
            "id": None,
            "immutable_id": None,
            "last_modified": structure.last_modified,
        }


@dataclass(frozen=True)
class FilelessSitesStructureRecord:
    """:class:`SitesStructureRecord` without a ``source_file`` field."""

    __httk_storage__: ClassVar[StorageInfo] = StorageInfo(storage_name="test_collect_selection_fileless_structure")
    __httk_canonical_source__: ClassVar = UnitcellStructure
    __httk_stored_properties__: ClassVar = SitesStructureRecord.__httk_stored_properties__

    basis: Annotated[tuple[tuple[float, ...], ...], Shape(3, 3)]
    sites: tuple[SiteRow, ...]
    id: Annotated[str | None, IdentitySkip(), Indexed()] = field(default=None, compare=False)
    immutable_id: Annotated[str | None, IdentitySkip(), Unique()] = field(default=None, compare=False)
    last_modified: Annotated[datetime.datetime | None, IdentitySkip()] = field(default=None, compare=False)

    @property
    def type(self) -> str:
        """Return the entry type name."""
        return "structures"

    @classmethod
    def __httk_project__(cls, structure: Any) -> Mapping[str, object]:
        return {k: v for k, v in SitesStructureRecord.__httk_project__(structure).items() if k != "source_file"}


@pytest.fixture
def sites_record(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register :class:`SitesStructureRecord` for one test, leaving the shared registry untouched."""

    from httk.core.register import entries, register_entry_record

    monkeypatch.setattr(entries, "_entry_records", dict(entries._entry_records))
    register_entry_record(name=_SITES, family="structures", record=f"{__name__}:SitesStructureRecord")
    register_entry_record(name=_FILELESS, family="structures", record=f"{__name__}:FilelessSitesStructureRecord")


@pytest.fixture(scope="module")
def record(tmp_path_factory: pytest.TempPathFactory) -> JobRecord:
    """One finished job's record, to build synthetic collected jobs from."""

    workspace, _ = _finished_job(tmp_path_factory.mktemp("job"))
    return next(job_records(workspace))


def _view(**options: Any) -> Any:
    from httk.atomistic import Cell, Species, UnitcellStructure, UnitcellStructureView

    return UnitcellStructureView(
        UnitcellStructure(
            Cell([[1, 0, 0], [0, 1, 0], [0, 0, 1]]), [[0, 0, 0]], [Species("Si", ("Si",), (1,))], ["Si"], **options
        )
    )


def _item(record: JobRecord, job_id: str, structure: Any, *, product: bool = False) -> Any:
    """A job whose ``structure`` output, and optionally a data record made from it, are collected."""

    key = content_id(structure)
    outputs: dict[str, object] = {"structure": structure}
    edges = [RunEdge("structure", "structures", key)]
    if product:
        energy = DataRecord.from_value(
            "https://example.test/energy", "total", 1.5, product_of=(RunEdge("structure", "structures", key),)
        )
        outputs["energy"] = energy
        edges.append(RunEdge("energy", "records", content_id(energy)))
    return _synthetic_item(record, job_id, outputs, Run(outputs=tuple(edges), source_id=f"ws:{job_id}"))


def _structure_records(path: Path) -> tuple[type, ...]:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        return next(
            records for family, records in store.entry_records.items() if getattr(family, "type", None) == "structures"
        )


def _output_edges(path: Path, report: dict[str, Any]) -> dict[str, str]:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    with Backend.sqlite(path) as database:
        run = _stored(SqlStore(database), Run, report["stored"]["run"])
    return {edge.label: edge.entry_id for edge in run.outputs}


def test_a_selection_declares_only_its_records_after_the_persisted_ones(record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructureRecord, UnitcellStructureRecord

    items = [_item(record, "a", _view())]
    selected = _record_selection(_NUMERIC)
    layout, failures = _storage_layout(items, selected=selected)
    assert failures == {}
    (structures,) = [records for family, records in layout.items() if getattr(family, "type", None) == "structures"]
    assert structures == (NumericUnitcellStructureRecord,)
    # Unselected families keep every registered record.
    (records,) = [records for family, records in layout.items() if getattr(family, "type", None) == "records"]
    assert len(records) > 1

    persisted = {"structures": ("atomistic-unitcell-structure",)}
    layout, _ = _storage_layout(items, persisted, selected=selected)
    (structures,) = [records for family, records in layout.items() if getattr(family, "type", None) == "structures"]
    assert structures == (UnitcellStructureRecord, NumericUnitcellStructureRecord)


def test_a_selection_is_validated() -> None:
    from httk.atomistic import NumericUnitcellStructureRecord

    assert _record_selection(
        {"structures": ["atomistic-numeric-unitcell-structure", "atomistic-unitcell-structure"]}
    ) == {"structures": ("atomistic-numeric-unitcell-structure", "atomistic-unitcell-structure")}
    for selection, message in (
        ({"nonsense": ["core-data-record"]}, "unknown entry type 'nonsense'"),
        ({"structures": ["no-such-record"]}, "unknown record 'no-such-record'"),
        ({"structures": ["core-data-record"]}, "'core-data-record' is not of entry type 'structures'"),
        # Records are selected by registered name, never by class.
        ({"structures": [NumericUnitcellStructureRecord]}, "unknown record <class"),
        ({"structures": []}, "non-empty sequence"),
        ({"structures": "atomistic-unitcell-structure"}, "non-empty sequence"),
        ({"structures": ["atomistic-unitcell-structure"] * 2}, "selected twice"),
    ):
        with pytest.raises(ValueError, match=message):
            _record_selection(cast(Any, selection))


def test_a_numeric_selection_stores_a_structure_view_as_a_numeric_record(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructureRecord
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    path = tmp_path / "numeric.sqlite"
    (report,) = store_collected(
        [_item(record, "a", _view(), product=True)], str(path), id_base="httk.t", entry_records=_NUMERIC
    )
    assert "storage_error" not in report, report["storage_error"]
    assert _structure_records(path) == (NumericUnitcellStructureRecord,)
    edges = _output_edges(path, report)
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        stored = _stored(store, NumericUnitcellStructureRecord, edges["structure"])
        energy = _stored(store, DataRecord, edges["energy"])
    assert isinstance(stored, NumericUnitcellStructureRecord)
    assert stored.species_at_sites == ("Si",)
    # The deferred product_of edge names the converted structure's stored id.
    assert [(edge.label, edge.entry_id) for edge in energy.product_of] == [("structure", edges["structure"])]


def test_a_structure_the_numeric_record_cannot_carry_fails_only_its_job(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import Assembly

    path = tmp_path / "numeric.sqlite"
    assembled = _view(assemblies=(Assembly(((0,),), (Fraction(1),)),))
    reports = store_collected(
        [_item(record, "bad", assembled), _item(record, "good", _view())],
        str(path),
        id_base="httk.t",
        entry_records=_NUMERIC,
    )
    error = str(reports[0]["storage_error"])
    assert "assemblies" in error and "atomistic-numeric-unitcell-structure" in error
    assert reports[0].get("stored") is None
    assert "storage_error" not in reports[1] and reports[1]["stored"]


def test_an_exact_selection_converts_a_numeric_value(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructure, NumericUnitcellStructureRecord, UnitcellStructureRecord
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    path = tmp_path / "exact.sqlite"
    # A collector's numeric structure output is a numeric record (the value class names no entry type).
    numeric = NumericUnitcellStructureRecord.from_structure(NumericUnitcellStructure(_view()))
    (report,) = store_collected(
        [_item(record, "a", numeric)],
        str(path),
        id_base="httk.t",
        entry_records={"structures": ["atomistic-unitcell-structure"]},
    )
    assert "storage_error" not in report, report["storage_error"]
    assert _structure_records(path) == (UnitcellStructureRecord,)
    with Backend.sqlite(path) as database:
        stored = _stored(SqlStore(database), UnitcellStructureRecord, _output_edges(path, report)["structure"])
    assert isinstance(stored, UnitcellStructureRecord)


def test_a_selection_without_a_conversion_fails_the_job(tmp_path: Path, record: JobRecord) -> None:
    (report,) = store_collected(
        [_item(record, "a", _view())],
        str(tmp_path / "asu.sqlite"),
        id_base="httk.t",
        entry_records={"structures": ["atomistic-asu-structure"]},
    )
    assert "no conversion to atomistic-asu-structure is defined" in str(report["storage_error"])


def test_selecting_a_record_an_existing_store_lacks_needs_an_upgrade(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructureRecord, UnitcellStructureRecord

    path = tmp_path / "store.sqlite"
    exact = {"structures": ["atomistic-unitcell-structure"]}
    (first,) = store_collected([_item(record, "a", _view())], str(path), id_base="httk.t", entry_records=exact)
    assert "storage_error" not in first
    with pytest.raises(ValueError, match="needs an additive layout upgrade"):
        store_collected([_item(record, "b", _view())], str(path), id_base="httk.t", entry_records=_NUMERIC)
    (second,) = store_collected(
        [_item(record, "b", _view())], str(path), id_base="httk.t", entry_records=_NUMERIC, upgrade=True
    )
    assert "storage_error" not in second, second["storage_error"]
    assert _structure_records(path) == (UnitcellStructureRecord, NumericUnitcellStructureRecord)


def test_collect_records_stores_a_staged_structure_input_as_numeric(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.atomistic import NumericUnitcellStructureRecord
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    ws = _finished_inputs(tmp_path, members={"files/POSCAR": _DIAMOND})
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "stored")
    path = tmp_path / "results.sqlite"
    argv = ["collect", "--workspace", name, "--allow-job-collector", "--into", str(path), "--id-base", "httk.t"]
    assert command([*argv, "--records", "structures=atomistic-numeric-unitcell-structure"], context) == 0
    report = json.loads(capsys.readouterr().out.splitlines()[0])
    assert "storage_error" not in report, report["storage_error"]
    main_id, alternative_id = _structure_ids(report)
    assert _structure_records(path) == (NumericUnitcellStructureRecord,)
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        searcher = store.searcher(only_main_alt=False)
        variable = searcher.variable(NumericUnitcellStructureRecord)
        searcher.add(variable.id == main_id)
        rows = {row.entry.immutable_id.rsplit("~", 1)[0]: row.entry for row in searcher.results(entry=variable)}
        assert set(rows) == {main_id, alternative_id}
        assert rows[main_id].source_file is None
        assert rows[alternative_id].source_file.sha256 == hashlib.sha256(_DIAMOND.encode()).hexdigest()
        assert _input_edge(store, report) == alternative_id
        run = _stored(store, Run, report["stored"]["run"])
        energy_id = next(edge.entry_id for edge in run.outputs if edge.label == "relaxed_structure")
        product = _stored(store, DataRecord, energy_id)
    # The deferred product names the stored numeric alternative.
    assert [edge.entry_id for edge in product.product_of] == [alternative_id]


def test_collect_records_usage_errors(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    ws = _finished_inputs(tmp_path)
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "stored")
    into = ["--into", str(tmp_path / "s.sqlite"), "--id-base", "httk.t"]
    for extra, message in (
        (["--records", "structures=atomistic-numeric-unitcell-structure"], "--records only applies with --into"),
        ([*into, "--records", "structures"], "must be spelled TYPE=NAME"),
        ([*into, "--records", "structures=a,,b"], "must be spelled TYPE=NAME"),
        ([*into, "--records", "structures=no-such-record"], "unknown record 'no-such-record'"),
        (
            [*into, "--records", "structures=atomistic-unitcell-structure", "--records", "structures=x"],
            "names entry type 'structures' twice",
        ),
        ([*into, "--postprocess", "post", "--records", "structures=x"], "cannot be combined with --records"),
    ):
        assert command(["collect", "--workspace", name, *extra], context) == 2, extra
        assert message in capsys.readouterr().err, extra
    assert not (tmp_path / "s.sqlite").exists()


def test_campaign_and_v1_collect_take_records(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    context = CLIContext("httk", tmp_path)
    records = ["--records", "structures=atomistic-numeric-unitcell-structure"]
    for argv in (["campaign", "collect"], ["v1", "collect", str(tmp_path), "--workflow-dir", str(tmp_path)]):
        assert command([*argv, *records], context) == 2, argv
        assert "--records only applies with --into" in capsys.readouterr().err, argv


def test_a_selected_type_is_declared_even_when_the_sweep_stores_none(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructureRecord

    path = tmp_path / "empty.sqlite"
    energy = DataRecord.from_value("https://example.test/energy", "total", 1.5)
    item = _synthetic_item(record, "a", {"energy": energy}, Run(source_id="ws:a"))
    (report,) = store_collected([item], str(path), id_base="httk.t", entry_records=_NUMERIC)
    assert "storage_error" not in report, report["storage_error"]
    assert _structure_records(path) == (NumericUnitcellStructureRecord,)


class _Unstorable:
    """A structures-typed output that names no storage record."""

    type = "structures"


def test_a_selected_value_without_a_storage_record_fails_only_its_job(tmp_path: Path, record: JobRecord) -> None:
    reports = store_collected(
        [
            _synthetic_item(record, "bad", {"structure": _Unstorable()}, Run(source_id="ws:bad")),
            _item(record, "good", _view()),
        ],
        str(tmp_path / "s.sqlite"),
        id_base="httk.t",
        entry_records=_NUMERIC,
    )
    assert "cannot store role 'structure'" in str(reports[0]["storage_error"])
    assert "storage_error" not in reports[1] and reports[1]["stored"]


@pytest.mark.usefixtures("sites_record")
def test_a_user_record_is_built_through_its_canonical_projection(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import Cell, Species, UnitcellStructureView
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    path = tmp_path / "sites.sqlite"
    structure = UnitcellStructureView(
        UnitcellStructure(
            Cell([[2, 0, 0], [0, 2, 0], [0, 0, 2]]),
            [[0, 0, 0], [Fraction(1, 2), Fraction(1, 2), Fraction(1, 2)]],
            [Species("Na", ("Na",), (1,)), Species("Cl", ("Cl",), (1,))],
            ["Na", "Cl"],
        )
    )
    (report,) = store_collected(
        [_item(record, "a", structure)], str(path), id_base="httk.t", entry_records={"structures": [_SITES]}
    )
    assert "storage_error" not in report, report["storage_error"]
    assert _structure_records(path) == (SitesStructureRecord,)
    with Backend.sqlite(path) as database:
        store = SqlStore(database)
        searcher = store.searcher()
        variable = searcher.variable(SitesStructureRecord)
        rows = [row.entry for row in searcher.results(entry=variable)]
    assert len(rows) == 1
    assert [site.symbol for site in rows[0].sites] == ["Na", "Cl"]
    assert rows[0].basis == ((2.0, 0.0, 0.0), (0.0, 2.0, 0.0), (0.0, 0.0, 2.0))


@pytest.mark.usefixtures("sites_record")
def test_a_user_record_does_not_convert_a_value_outside_its_canonical_source(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import NumericUnitcellStructure, NumericUnitcellStructureRecord

    numeric = NumericUnitcellStructureRecord.from_structure(NumericUnitcellStructure(_view()))
    (report,) = store_collected(
        [_item(record, "a", numeric)],
        str(tmp_path / "sites.sqlite"),
        id_base="httk.t",
        entry_records={"structures": [_SITES]},
    )
    assert f"no conversion to {_SITES} is defined" in str(report["storage_error"])


@pytest.mark.usefixtures("sites_record")
def test_a_refused_user_projection_fails_only_its_job(tmp_path: Path, record: JobRecord) -> None:
    from httk.atomistic import Cell, Species, UnitcellStructureView

    alloy = UnitcellStructureView(
        UnitcellStructure(
            Cell([[1, 0, 0], [0, 1, 0], [0, 0, 1]]),
            [[0, 0, 0]],
            [Species("SiGe", ("Si", "Ge"), (Fraction(1, 2), Fraction(1, 2)))],
            ["SiGe"],
        )
    )
    reports = store_collected(
        [_item(record, "bad", alloy), _item(record, "good", _view())],
        str(tmp_path / "sites.sqlite"),
        id_base="httk.t",
        entry_records={"structures": [_SITES]},
    )
    # A KeyError, not only TypeError/ValueError, from user projection code fails just this job.
    assert f"cannot convert to {_SITES}: 'a site row carries exactly one element'" in str(reports[0]["storage_error"])
    assert reports[0].get("stored") is None
    assert "storage_error" not in reports[1] and reports[1]["stored"]


@pytest.mark.usefixtures("sites_record")
def test_collect_records_stores_a_staged_structure_input_as_a_user_record(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from httk.store import Backend, SqlStore  # pyright: ignore[reportMissingImports]

    ws = _finished_inputs(tmp_path, members={"files/POSCAR": _DIAMOND})
    context = CLIContext("httk", tmp_path)
    name = register_ws(context, ws.root, "stored")
    path = tmp_path / "results.sqlite"
    argv = ["collect", "--workspace", name, "--allow-job-collector", "--into", str(path), "--id-base", "httk.t"]
    assert command([*argv, "--records", f"structures={_SITES}"], context) == 0
    report = json.loads(capsys.readouterr().out.splitlines()[0])
    assert "storage_error" not in report, report["storage_error"]
    main_id, alternative_id = _structure_ids(report)
    with Backend.sqlite(path) as database:
        searcher = SqlStore(database).searcher(only_main_alt=False)
        variable = searcher.variable(SitesStructureRecord)
        searcher.add(variable.id == main_id)
        rows = {row.entry.immutable_id.rsplit("~", 1)[0]: row.entry for row in searcher.results(entry=variable)}
    assert set(rows) == {main_id, alternative_id}
    assert rows[main_id].source_file is None
    assert rows[alternative_id].source_file.sha256 == hashlib.sha256(_DIAMOND.encode()).hexdigest()
    assert rows[alternative_id].sites == rows[main_id].sites


@pytest.mark.usefixtures("sites_record")
def test_a_staged_structure_input_needs_a_source_file_field(tmp_path: Path, record: JobRecord) -> None:
    ws = _finished_inputs(tmp_path, members={"files/POSCAR": _DIAMOND})
    staged = list(collect(ws, allow_job_collector=True))
    reports = store_collected(
        [*staged, _item(record, "good", _view())],
        str(tmp_path / "s.sqlite"),
        id_base="httk.t",
        entry_records={"structures": [_FILELESS]},
    )
    assert "FilelessSitesStructureRecord declares no source_file field to record a staged structure input" in str(
        reports[0]["storage_error"]
    )
    assert "storage_error" not in reports[-1] and reports[-1]["stored"]
