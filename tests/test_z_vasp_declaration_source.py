"""The VASP providers carry the published workflow declaration documents."""

import json
from pathlib import Path

import pytest

from httk.workflow.vasp.workflows import PROVIDERS

_SCHEMA_ROOT = Path(__file__).parents[2] / "schemas-scource" / "output" / "defs" / "v0.1" / "workflows"


@pytest.mark.skipif(not _SCHEMA_ROOT.is_dir(), reason="workspace-only: sibling schemas-scource checkout required")
def test_vasp_provider_declarations_equal_published_schema_source() -> None:
    for provider in PROVIDERS:
        name = provider.workflow_id.replace("httk.vasp.", "vasp-")
        source_name = "vasp-relax" if name == "vasp-relax-bash" else name
        expected = json.loads((_SCHEMA_ROOT / f"{source_name}.json").read_text(encoding="utf-8"))
        assert provider.declarations["workflow"] == expected, provider.workflow_id
