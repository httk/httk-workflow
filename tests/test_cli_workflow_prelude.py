"""The ``workspace workflow-prelude`` command group.

Layer 2 of the two-layer prelude: a per-workflow shell prelude, keyed by
workflow id, stored on the workspace and echoed back through the same CLI the
operator sets it with. The storage itself is Packet 1's; this exercises the
operator-facing verbs round-tripping through it.
"""

from pathlib import Path

from httk.core.cli import CLIContext

from httk.workflow.registry import create_workspace
from httk.workflow.workflow_cli import command


def _context(root: Path) -> CLIContext:
    return CLIContext("httk", root)


def test_set_show_unset_round_trip(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = _context(tmp_path)

    assert (
        command(
            [
                "workspace",
                "workflow-prelude",
                "set",
                "--workflow",
                "relax-vasp",
                "--value",
                "module load VASP/6.2.1",
                "ws",
            ],
            context,
        )
        == 0
    )
    assert capsys.readouterr().out.strip() == "module load VASP/6.2.1"

    assert command(["workspace", "workflow-prelude", "show", "--workflow", "relax-vasp", "ws"], context) == 0
    assert capsys.readouterr().out.strip() == "module load VASP/6.2.1"

    assert command(["workspace", "workflow-prelude", "show", "--json", "ws"], context) == 0
    assert "relax-vasp" in capsys.readouterr().out

    assert command(["workspace", "workflow-prelude", "unset", "--workflow", "relax-vasp", "ws"], context) == 0
    assert capsys.readouterr().out == ""

    assert command(["workspace", "workflow-prelude", "show", "--json", "ws"], context) == 0
    assert "relax-vasp" not in capsys.readouterr().out


def test_missing_workflow_is_reported(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert command(["workspace", "workflow-prelude", "show", "--workflow", "relax-vasp", "ws"], _context(tmp_path)) == 1
    assert "relax-vasp" in capsys.readouterr().err


def test_at_file_reads_the_shell_text_from_disk(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    script = tmp_path / "prelude.sh"
    body = "module purge\nmodule load VASP/6.2.1\nexport OMP_NUM_THREADS=1\n"
    script.write_text(body, encoding="utf-8")

    assert (
        command(
            ["workspace", "workflow-prelude", "set", "--workflow", "relax-vasp", "--value", f"@{script}", "ws"],
            _context(tmp_path),
        )
        == 0
    )
    capsys.readouterr()
    assert command(["workspace", "workflow-prelude", "show", "--workflow", "relax-vasp", "ws"], _context(tmp_path)) == 0
    # The stored value is the file's text verbatim, not a JSON-parsed rendering;
    # ``print`` adds one trailing newline to the file's own trailing newline.
    assert capsys.readouterr().out == body + "\n"


def test_a_literal_string_is_stored_verbatim(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    context = _context(tmp_path)
    # A value that looks like JSON must NOT be parsed: it is shell text.
    assert (
        command(["workspace", "workflow-prelude", "set", "--workflow", "wf", "--value", "[not json]", "ws"], context)
        == 0
    )
    capsys.readouterr()
    assert command(["workspace", "workflow-prelude", "show", "--workflow", "wf", "ws"], context) == 0
    assert capsys.readouterr().out.strip() == "[not json]"


def test_a_whitespace_bearing_workflow_id_is_rejected(tmp_path: Path, capsys) -> None:
    create_workspace("ws", tmp_path / "ws")
    assert (
        command(
            ["workspace", "workflow-prelude", "set", "--workflow", "bad id", "--value", "module load x", "ws"],
            _context(tmp_path),
        )
        == 1
    )
    assert capsys.readouterr().err != ""
