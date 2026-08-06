"""Registry semantics for harvested workflow interpreters."""

from pathlib import Path, PurePosixPath

import httk.core
import pytest

from httk.workflow.harvesting import HarvestRecord
from httk.workflow.interpretation import InterpretedRun, interpret, register_interpreter, registered_interpreters


def _record(workflow: str) -> HarvestRecord:
    return HarvestRecord(
        workspace_root=Path("."),
        workspace_id="ws",
        job_id="12345678-1234-4234-8234-123456789abc",
        job_key="job--12345678-1234-4234-8234-123456789abc",
        job={"workflow": workflow},
        runner_provenance=None,
        state="succeeded",
        failure=None,
        placement=PurePosixPath("jobs"),
        payload_path=PurePosixPath("jobs/job"),
        workdir_path=None,
        data_path=None,
        data_generation=None,
        provenance={},
        runner_steps=None,
        children={},
        declarations={},
    )


def test_registry_dispatches_a_toy_interpreter() -> None:
    expected = InterpretedRun(run=httk.core.Run())

    def toy(record: HarvestRecord) -> InterpretedRun:
        assert record.job["workflow"] == "tests.interpretation.toy"
        return expected

    register_interpreter(workflow="tests.interpretation.toy", interpreter=toy)
    assert "tests.interpretation.toy" in registered_interpreters()
    assert interpret(_record("tests.interpretation.toy")) is expected


def test_duplicate_and_unknown_interpreters_are_refused() -> None:
    register_interpreter(
        workflow="tests.interpretation.duplicate", interpreter=lambda record: InterpretedRun(httk.core.Run())
    )
    with pytest.raises(ValueError, match="already registered"):
        register_interpreter(
            workflow="tests.interpretation.duplicate",
            interpreter=lambda record: InterpretedRun(httk.core.Run()),
        )
    with pytest.raises(ValueError, match="owning workflow module first"):
        interpret(_record("tests.interpretation.unknown"))
