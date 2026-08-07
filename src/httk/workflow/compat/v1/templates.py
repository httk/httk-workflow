"""Render trusted httk v1 templates.

Template code is executed by design, exactly like the original v1 engine.
"""

from __future__ import annotations

import os
import shlex
import shutil
import sys
from collections.abc import Mapping
from io import StringIO
from pathlib import Path
from string import Template

__all__ = ["apply_template", "apply_templates", "apply_templates_in_place"]


def apply_template(
    template: str | os.PathLike[str],
    output: str | os.PathLike[str],
    envglobals: Mapping[str, object] | None = None,
    envlocals: Mapping[str, object] | None = None,
) -> None:
    """Render one v1 template into *output*.

    :param template: Locate the source template.
    :param output: Locate the rendered file.
    :param envglobals: Provide names for Python evaluation and execution.
    :param envlocals: Provide names taking precedence for ``$name`` values.
    """

    locals_ = {} if envlocals is None else dict(envlocals)
    globals_ = {} if envglobals is None else dict(envglobals)
    source = Path(template).read_text(encoding="utf-8")
    result_step1 = Template(Template(source).safe_substitute(locals_)).safe_substitute(globals_)

    shebang, separator, body = result_step1.partition("\n")
    if shebang.startswith("#!") and separator:
        result_step1 = body
        result_step2 = shebang + separator
    else:
        result_step2 = ""
    lexer = shlex.shlex(result_step1)
    lexer.whitespace = ""
    eval_nesting = 0
    exec_nesting = 0
    command = ""
    for token in lexer:
        if eval_nesting == 0 and exec_nesting == 0:
            if token == "\\":
                token += lexer.get_token() or ""
            if token == "$":
                token += lexer.get_token() or ""
            if token == "$(":
                eval_nesting = 1
                command = ""
                continue
            if token == "${":
                exec_nesting = 1
                command = ""
                continue
            if token == "\\$":
                token = "$"
            result_step2 += token
        elif exec_nesting != 0:
            if token == "{":
                exec_nesting += 1
            if token == "}":
                exec_nesting -= 1
            if exec_nesting == 0:
                sys.stdout = StringIO()
                try:
                    exec(command, globals_, locals_)  # noqa: S102 - trusted v1 template code
                    result_step2 += sys.stdout.getvalue()
                except Exception:
                    print("Failed to execute:" + command)
                    raise
                finally:
                    sys.stdout = sys.__stdout__
                result_step2 = result_step2.removesuffix("\n")
                continue
            command += token
        elif eval_nesting != 0:
            if token == "(":
                eval_nesting += 1
            if token == ")":
                eval_nesting -= 1
            if eval_nesting == 0:
                try:
                    result_step2 += str(eval(command, globals_, locals_))
                except Exception:
                    print("Failed to eval:" + command)
                    raise
                continue
            command += token

    destination = Path(output)
    if destination.exists() or destination.is_symlink():
        destination.unlink()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(result_step2, encoding="utf-8")


def apply_templates(
    inputpath: str | os.PathLike[str],
    outpath: str | os.PathLike[str],
    template_suffixes: str | list[str] = "template",
    envglobals: Mapping[str, object] | None = None,
    envlocals: Mapping[str, object] | None = None,
    mkdir: bool = True,
) -> None:
    """Copy a tree while rendering files with a selected suffix.

    :param inputpath: Locate the source tree.
    :param outpath: Locate the destination tree.
    :param template_suffixes: Select template suffixes without their dot.
    :param envglobals: Provide names for Python evaluation and execution.
    :param envlocals: Provide names taking precedence for ``$name`` values.
    :param mkdir: Create the destination root when true.
    """

    source_root = Path(inputpath)
    destination_root = Path(outpath)
    if not source_root.exists():
        raise ValueError("apply_templates: template does not exist")
    if mkdir:
        destination_root.mkdir()
    suffixes = [template_suffixes] if isinstance(template_suffixes, str) else template_suffixes
    for root, dirs, files in os.walk(source_root):
        relative_root = Path(root).relative_to(source_root)
        target_root = destination_root / relative_root
        target_root.mkdir(parents=True, exist_ok=True)
        for directory in dirs:
            source_directory = Path(root) / directory
            target_directory = target_root / directory
            target_directory.mkdir(exist_ok=True)
            shutil.copymode(source_directory, target_directory)
        for filename in files:
            source = Path(root) / filename
            target_name = next(
                (filename[: -len(suffix) - 1] for suffix in suffixes if filename.endswith(f".{suffix}")),
                None,
            )
            target = target_root / (target_name if target_name is not None else filename)
            if target_name is not None:
                apply_template(source, target, envglobals=envglobals, envlocals=envlocals)
                shutil.copymode(source, target)
            else:
                if target.exists() or target.is_symlink():
                    target.unlink()
                shutil.copy(source, target)


def apply_templates_in_place(root: str | os.PathLike[str], envglobals: Mapping[str, object] | None = None) -> None:
    """Render and remove every ``*.template`` member below *root*.

    :param root: Locate the tree to modify.
    :param envglobals: Provide names for Python evaluation and execution.
    """

    directory = Path(root)
    for source in sorted(directory.rglob("*.template")):
        if not source.is_file() and not source.is_symlink():
            continue
        target = source.with_name(source.name[: -len(".template")])
        apply_template(source, target, envglobals=envglobals)
        shutil.copymode(source, target)
        source.unlink()
