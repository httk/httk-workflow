from pathlib import Path

from httk.workflow.compat.v1.templates import apply_template, apply_templates, apply_templates_in_place


def test_template_passes_and_escape(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.write_text(r"$name $(value + 1) ${print(value * 2)} \$missing", encoding="utf-8")
    apply_template(source, output, envglobals={"name": "global", "value": 2}, envlocals={"name": "local"})
    assert output.read_text(encoding="utf-8") == "local 3 4 $missing"


def test_nested_eval_uses_legacy_scanner(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    source.write_text("$(dict(value=(1, 2))['value'][0])", encoding="utf-8")
    apply_template(source, output)
    assert output.read_text(encoding="utf-8") == "1"


def test_apply_templates_renders_strips_and_preserves_mode(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    template = source / "run.template"
    template.write_text("#!/bin/sh\necho $value\n", encoding="utf-8")
    template.chmod(0o751)
    (source / "plain.txt").write_text("plain", encoding="utf-8")
    apply_templates(source, destination, envglobals={"value": "ok"})
    assert (destination / "run").read_text(encoding="utf-8").endswith("echo ok\n")
    assert (destination / "run").stat().st_mode & 0o777 == 0o751
    assert (destination / "plain.txt").read_text(encoding="utf-8") == "plain"


def test_apply_templates_in_place(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    template = root / "value.template"
    template.write_text("$value", encoding="utf-8")
    template.chmod(0o740)
    (root / "keep.txt").write_text("keep", encoding="utf-8")
    apply_templates_in_place(root, {"value": "rendered"})
    assert (root / "value").read_text(encoding="utf-8") == "rendered"
    assert (root / "value").stat().st_mode & 0o777 == 0o740
    assert not template.exists()
    assert (root / "keep.txt").read_text(encoding="utf-8") == "keep"
