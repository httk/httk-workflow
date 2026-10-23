"""Which installed component reads an application setting, for ``settings show``, ``launcher show`` and warnings.

A setting is *recognized* when httk-workflow reads it, or a registered code-support package or a workflow
installed in the workspace declares reading it; *unrecognized* when it lies in a namespace httk-workflow owns
(or, on a launcher, anywhere outside what launchers read) and nothing reads it; and *undeclared* otherwise:
workflows are installable, so a workflow may still read it.
"""

import difflib
import os
import shlex
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal

from httk.core.register import code_support, known_codes

from ._confine import _KNOWN_KEYS, CONFINE_OVERRIDE_KEYS
from ._daemon_policy import SBATCH_OPTIONS_SETTING, SLURM_EXPORT_SETTING, sbatch_option_lines
from .errors import WorkflowError

if TYPE_CHECKING:
    from ._kernel import KernelWorkspace

type SettingLayer = Literal["workspace", "launcher"]

_ENGINE = "httk-workflow"
_JOBFLOW = "httk-workflow jobflow runner"
#: Namespaces whose keys nothing outside httk-workflow reads.
_ENGINE_NAMESPACES = frozenset(
    {"manager", "slurm", "confine", "environment", "seal", "postprocess", "children", "httk_v1"}
)
_ENVIRONMENT_PREFIX = "confine.environment."
_NO_WORKFLOWS: Mapping[str, tuple[str, ...]] = MappingProxyType({})
_MAY_STILL_READ = "a workflow may still read it without declaring it, or a future workflow may"
_SGR = {"recognized": ("32", ""), "unrecognized": ("1;33", "1;33"), "undeclared": ("", "2")}


@cache
def _engine_keys(layer: SettingLayer) -> frozenset[str]:
    # Imported here: launch_runtime is the launcher dispatcher, noticeably slower to import than this module.
    from .launch_runtime import BATCH_DIRECTIVES

    slurm = {key for key, _directive in BATCH_DIRECTIVES} | {"slurm.time_limit_max", SBATCH_OPTIONS_SETTING}
    launcher = {
        "manager.workers",
        "manager.allocation",
        "manager.command",
        "environment.prelude",
        *CONFINE_OVERRIDE_KEYS,
        *_KNOWN_KEYS,
        *slurm,
    }
    if layer == "launcher":
        # The sbatch --export mode is read only from a daemon launcher's own settings.
        return frozenset({*launcher, SLURM_EXPORT_SETTING})
    return frozenset(
        {
            *launcher,
            "manager.launch",
            "manager.count",
            "seal.succeeded",
            "seal.keys",
            "postprocess.directory",
            "children.placement",
            "httk_v1.timeout",
            "httk_v1.wrapper",
            "httk_v1.log_compression",
            "httk_v1.root",
        }
    )


@dataclass(frozen=True)
class SettingStatus:
    """Whether a setting key is read by an installed component.

    :param state: ``recognized`` (read by ``readers``), ``unrecognized`` (definitely ignored) or
        ``undeclared`` (nothing installed declares it; a workflow may still read it).
    :param readers: The components that read the key: httk-workflow first, then code-support packages, then
        installed workflows.
    :param note: Why an unrecognized or undeclared key is classified so; empty when recognized.
    """

    state: Literal["recognized", "unrecognized", "undeclared"]
    readers: tuple[str, ...] = ()
    note: str = ""

    @property
    def annotation(self) -> str:
        """The plain-text marker shown beside the setting."""

        if self.state != "recognized":
            return f"{self.state}: {self.note}"
        workflows = [reader for reader in self.readers if reader.startswith("workflow ")]
        shown = list(self.readers)
        if len(workflows) > 2:
            # "workflow a, workflow b and 3 more": the workflows come last, in name order.
            shown[len(shown) - len(workflows) + 1 :] = [f"{workflows[1]} and {len(workflows) - 2} more"]
        return f"recognized by {', '.join(shown)}"


def _readers(key: str, layer: SettingLayer, workflows: Mapping[str, tuple[str, ...]]) -> tuple[str, ...]:
    readers: list[str] = []
    if key in _engine_keys(layer) or key.startswith(_ENVIRONMENT_PREFIX):
        readers.append(_ENGINE)
    if layer == "launcher":
        return tuple(readers)
    # Imported here: the runner is a workflow module, not something every CLI command needs.
    from .compat.jobflow.jobflow_runner import _FORWARDED_PARAMETERS, _LIBRARY_PREFIXES

    if key in _FORWARDED_PARAMETERS or key.startswith(tuple(_LIBRARY_PREFIXES)):
        readers.append(_JOBFLOW)
    readers += [f"{name} code support" for name in known_codes() if key in code_support(name).settings]
    if key.partition(".")[0] not in _ENGINE_NAMESPACES:
        # As the manifest refuses: a workflow declaring an engine key cannot make an ignored key read.
        readers += [f"workflow {name}" for name in sorted(workflows) if key in workflows[name]]
    return tuple(readers)


def _sbatch_hint(key: str, value: object) -> str:
    option = f"--{key.removeprefix('slurm.').replace('_', '-')}={shlex.quote(str(value))}"
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        try:
            sbatch_option_lines(option)
        except ValueError as exc:
            return f"; {SBATCH_OPTIONS_SETTING} cannot carry it either: {exc}"
        else:
            return f"; pass it with {SBATCH_OPTIONS_SETTING}={shlex.quote(option)}"
    return f"; pass sbatch options with {SBATCH_OPTIONS_SETTING}"


def classify_setting(
    key: str,
    value: object,
    *,
    layer: SettingLayer = "workspace",
    kind: str | None = "slurm",
    workflows: Mapping[str, tuple[str, ...]] = _NO_WORKFLOWS,
) -> SettingStatus:
    """Classify one application setting by whether an installed component reads it.

    :param key: The dotted setting key.
    :param value: The setting value, used only to suggest an equivalent ``slurm.sbatch_options`` entry.
    :param layer: ``workspace`` for a workspace setting, ``launcher`` for a launcher bundle setting.
    :param kind: The launcher bundle's kind; only the maintained ``slurm`` kind has a known set of keys, any
        other kind's own executable may read any key.
    :param workflows: The settings each installed workflow declares, by short name (see
        :func:`installed_workflow_settings`); counted only at the workspace layer.
    :return: The setting's status.
    """

    if layer == "launcher" and kind != "slurm":
        return SettingStatus("undeclared", note="this launcher's own executable may read it")
    readers = _readers(key, layer, workflows)
    if readers:
        return SettingStatus("recognized", readers)
    namespace, _dot, rest = key.partition(".")
    if layer == "launcher" and _readers(key, "workspace", _NO_WORKFLOWS):
        return SettingStatus("unrecognized", note="launchers do not read it; set it as a workspace setting")
    if layer == "launcher" or namespace in _ENGINE_NAMESPACES:
        if layer == "workspace" and key in _engine_keys("launcher"):
            return SettingStatus("unrecognized", note="only launchers read it; set it on the launcher")
        if layer == "launcher":
            note = "launchers read only the documented manager, slurm, confine and environment.prelude settings"
        else:
            note = f"httk-workflow reads no setting {key}"
        # Compare within the namespace: the shared prefix would make every slurm.* key look close.
        siblings = {name.partition(".")[2]: name for name in _engine_keys(layer) if name.startswith(namespace + ".")}
        match = difflib.get_close_matches(rest, siblings, n=1)
        if match:
            note += f"; did you mean {siblings[match[0]]}?"
        elif namespace == "slurm":
            note += _sbatch_hint(key, value)
        if namespace == "confine":
            note += "; managers refuse to start while it is set"
        return SettingStatus("unrecognized", note=note)
    if namespace in known_codes():
        return SettingStatus(
            "undeclared",
            note=f"neither {namespace} code support nor an installed workflow declares reading it; {_MAY_STILL_READ}",
        )
    return SettingStatus("undeclared", note=f"nothing installed declares reading it; {_MAY_STILL_READ}")


def installed_workflow_settings(workspace: "KernelWorkspace") -> dict[str, tuple[str, ...]]:
    """Return the settings each workflow installed in *workspace* declares reading, by short name.

    An installation whose manifest no longer parses is skipped, and an unreadable store counts as empty;
    installations sharing a short name pool their settings.

    :param workspace: The workspace whose installed workflows to read.
    :return: The declared settings, keyed by workflow short name.
    """

    # Imported here: the store pulls in package parsing, which classifying needs only for this.
    from . import _store

    try:
        found = _store.list_installed(workspace)
    except OSError:
        return {}
    declared: dict[str, set[str]] = {}
    for installed in found:
        try:
            settings = installed.provider().settings
        except (WorkflowError, OSError, ValueError, RuntimeError):
            continue
        declared.setdefault(installed.name, set()).update(settings)
    return {name: tuple(sorted(keys)) for name, keys in declared.items()}


def use_color() -> bool:
    """Return whether standard output takes ANSI colors: a terminal, ``NO_COLOR`` unset or empty, not ``TERM=dumb``.

    :return: Whether to color.
    """

    return sys.stdout.isatty() and os.environ.get("NO_COLOR", "") == "" and os.environ.get("TERM") != "dumb"


def _paint(text: str, sgr: str) -> str:
    return f"\x1b[{sgr}m{text}\x1b[0m" if sgr else text


def annotate_setting(
    key: str,
    value: object,
    *,
    layer: SettingLayer = "workspace",
    kind: str | None = "slurm",
    workflows: Mapping[str, tuple[str, ...]] = _NO_WORKFLOWS,
    color: bool = False,
) -> tuple[str, str]:
    """Return a setting's key and its annotation, colored when *color* is true.

    :param key: The dotted setting key.
    :param value: The setting value.
    :param layer: ``workspace`` or ``launcher``.
    :param kind: The launcher bundle's kind, as for :func:`classify_setting`.
    :param workflows: The settings installed workflows declare, as for :func:`classify_setting`.
    :param color: Whether to add ANSI colors; the text markers are present either way.
    :return: The key text and the annotation text.
    """

    status = classify_setting(key, value, layer=layer, kind=kind, workflows=workflows)
    key_sgr, note_sgr = _SGR[status.state] if color else ("", "")
    return _paint(key, key_sgr), _paint(status.annotation, note_sgr)


def report_written_settings(
    settings: Mapping[str, object],
    *,
    layer: SettingLayer = "workspace",
    kind: str | None = "slurm",
    workflows: Mapping[str, tuple[str, ...]] = _NO_WORKFLOWS,
) -> None:
    """Print a ``warning:`` or ``notice:`` on standard error for each written setting nothing reads or declares.

    An unrecognized setting, definitely ignored, gets a warning; at the workspace layer an undeclared one gets a
    notice, since a workflow may still read it.

    :param settings: The settings just written.
    :param layer: ``workspace`` or ``launcher``.
    :param kind: The launcher bundle's kind, as for :func:`classify_setting`.
    :param workflows: The settings installed workflows declare, as for :func:`classify_setting`.
    """

    for key, value in settings.items():
        status = classify_setting(key, value, layer=layer, kind=kind, workflows=workflows)
        if status.state == "unrecognized":
            print(f"warning: setting {key} is unrecognized: {status.note}", file=sys.stderr)
        elif status.state == "undeclared" and layer == "workspace":
            print(f"notice: setting {key} is undeclared: {status.note}", file=sys.stderr)
