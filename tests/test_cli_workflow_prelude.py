"""Per-workflow preludes as ``prelude.<workflow-id>`` keys of ``httk workspace show/set/unset``.

Layer 2 of the two-layer prelude: a per-workflow shell prelude, keyed by
workflow id, stored on the workspace and echoed back through the same CLI the
operator sets it with. The storage itself is Packet 1's; this exercises the
operator-facing verbs round-tripping through it.
"""

import json
from pathlib import Path

from httk.core.cli import CLIContext

from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import command


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def test_set_show_unset_round_trip(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = _context(tmp_path)

    assert command(["workspace", "set", "-w", "ws", "prelude.relax-vasp=module load VASP/6.2.1"], context) == 0
    assert json.loads(capsys.readouterr().out) == {"prelude.relax-vasp": "module load VASP/6.2.1"}

    assert command(["workspace", "show", "-w", "ws", "prelude.relax-vasp"], context) == 0
    assert capsys.readouterr().out.strip() == "module load VASP/6.2.1"

    assert command(["workspace", "show", "-w", "ws", "--json"], context) == 0
    assert "prelude.relax-vasp" in json.loads(capsys.readouterr().out)

    assert command(["workspace", "unset", "-w", "ws", "prelude.relax-vasp"], context) == 0
    assert json.loads(capsys.readouterr().out) == ["prelude.relax-vasp"]

    assert command(["workspace", "show", "-w", "ws", "--json"], context) == 0
    assert "prelude.relax-vasp" not in json.loads(capsys.readouterr().out)


def test_missing_workflow_is_reported(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert command(["workspace", "show", "-w", "ws", "prelude.relax-vasp"], _context(tmp_path)) == 2
    assert "prelude.relax-vasp" in capsys.readouterr().err


def test_at_file_reads_the_shell_text_from_disk(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    script = tmp_path / "prelude.sh"
    body = "module purge\nmodule load VASP/6.2.1\nexport OMP_NUM_THREADS=1\n"
    script.write_text(body, encoding="utf-8")

    assert command(["workspace", "set", "-w", "ws", f"prelude.relax-vasp=@{script}"], _context(tmp_path)) == 0
    capsys.readouterr()
    assert command(["workspace", "show", "-w", "ws", "prelude.relax-vasp"], _context(tmp_path)) == 0
    # The stored value is the file's text verbatim, not a JSON-parsed rendering;
    # ``print`` adds one trailing newline to the file's own trailing newline.
    assert capsys.readouterr().out == body + "\n"


def test_a_literal_string_is_stored_verbatim(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = _context(tmp_path)
    # A value that looks like JSON must NOT be parsed: it is shell text.
    assert command(["workspace", "set", "-w", "ws", "prelude.wf=[not json]"], context) == 0
    capsys.readouterr()
    assert command(["workspace", "show", "-w", "ws", "prelude.wf"], context) == 0
    assert capsys.readouterr().out.strip() == "[not json]"


def test_a_whitespace_bearing_workflow_id_is_rejected(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert command(["workspace", "set", "-w", "ws", "prelude.bad id=module load x"], _context(tmp_path)) == 2
    assert capsys.readouterr().err != ""
