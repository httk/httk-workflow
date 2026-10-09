"""Pure builders of an attempt's context document and runner environment.

Both take plain values, never manager, marker or frame objects, so any
manager can feed them from whatever state it keeps.
"""

import logging
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ._util import interpreter_first_path, json_bytes
from .errors import FormatError

#: Records keep the manager's logger name: operators and tests filter on it.
_LOGGER = logging.getLogger("httk.workflow.manager")
_LANGUAGES = Path(__file__).with_name("languages")


def _setting_variable_name(key: str) -> str:
    """Return the environment variable synthesized for one setting name."""

    return "HTTK_" + key.upper().replace(".", "_")


def attempt_context(
    *,
    workspace_id: str,
    job_id: str,
    job_key: str,
    placement: str,
    payload: str,
    step: str | None,
    activation_id: str | None,
    activation_ordinal: int | None,
    attempt_id: str,
    attempt_ordinal: int | None,
    total_attempts: int | None,
    is_unclean_restart: bool,
    attempt_reason: str | None,
    previous_attempt_id: str | None,
    activation_reason: str | None,
    durable: bool,
    settings: Mapping[str, Any],
    resources: Mapping[str, int],
    deadline: int | None,
    binding: Mapping[str, Any] | None,
    join: object,
    children: list[dict[str, object]],
) -> dict[str, object]:
    """Return the attempt context document a runner reads from ``HTTK_WORKFLOW_CONTEXT``.

    :param workspace_id: The workspace identifier.
    :param job_id: The job identifier.
    :param job_key: The job key.
    :param placement: The job placement as text.
    :param payload: The resolved payload path.
    :param step: The activation step.
    :param activation_id: The activation identifier.
    :param activation_ordinal: The activation ordinal.
    :param attempt_id: The attempt identifier.
    :param attempt_ordinal: The attempt ordinal; above one makes the attempt a restart.
    :param total_attempts: The total attempt count.
    :param is_unclean_restart: Whether the previous attempt ended uncleanly.
    :param attempt_reason: Why this attempt started; ``None`` reads as ``claim``.
    :param previous_attempt_id: The previous attempt's identifier.
    :param activation_reason: Why the activation started.
    :param durable: The workspace durability mode.
    :param settings: The effective workspace settings.
    :param resources: The attempt's resource reservation.
    :param deadline: The epoch second the attempt must finish by, or ``None``.
    :param binding: The context binding, or ``None`` for an attempt without a placement.
    :param join: The activation's join summary.
    :param children: The labeled observations of the activation's join.
    :return: The context mapping.
    """

    return {
        "format": "httk-workflow-attempt-context",
        "format_version": 2,
        "workspace_id": workspace_id,
        "job_id": job_id,
        "job_key": job_key,
        "placement": placement,
        "payload": payload,
        "step": step,
        "activation_id": activation_id,
        "activation_ordinal": activation_ordinal,
        "attempt_id": attempt_id,
        "attempt_ordinal": attempt_ordinal,
        "total_attempts": total_attempts,
        "is_restart": (attempt_ordinal or 0) > 1,
        "is_unclean_restart": is_unclean_restart,
        "attempt_reason": attempt_reason or "claim",
        "previous_attempt_id": previous_attempt_id,
        "activation_reason": activation_reason,
        # The workspace durability mode, so every artifact the runner
        # publishes is synchronized to the same standard as the marker and
        # journal that will reference it.
        "durable": durable,
        # The workspace application settings, snapshotted at claim time, so a
        # runner resolves a.setting("code.command") without the operator
        # re-exporting it for every job. This is the workspace layer of the
        # parameters → environment → workspace → default resolution.
        "settings": settings,
        "resources": dict(resources),
        "deadline": deadline,
        **({} if binding is None else {"binding": binding}),
        "join": join,
        # The enriched, labeled observations of this activation's join, or an
        # empty array when the activation follows no join. ``join`` keeps the
        # summary exactly as earlier profiles published it.
        "children": children,
    }


def runner_environment(
    *,
    base: Mapping[str, str],
    context: Mapping[str, Any],
    control: Path,
    workspace_root: Path,
    payload: Path,
    workdir: Path,
    durable: bool,
    deadline: int | None,
    binding_environment: Mapping[str, str],
    code_variables: Mapping[str, str],
    data_dir: Path | None,
    declared_environment: object,
    settings: Mapping[str, Any],
) -> dict[str, str]:
    """Return the runner environment of one attempt, built over *base*.

    The runner-store variables ``HTTK_WORKFLOW_RUNNER_ROOT`` and
    ``HTTK_WORKFLOW_RUNNER_ARTIFACTS`` are removed from *base* and left for the
    caller to set once the runner is verified.

    :param base: The manager's own environment.
    :param context: The attempt context from :func:`attempt_context`.
    :param control: The attempt-control directory.
    :param workspace_root: The workspace root.
    :param payload: The job's payload directory.
    :param workdir: The attempt's working directory.
    :param durable: The workspace durability mode.
    :param deadline: The epoch second the attempt must finish by, or ``None``.
    :param binding_environment: The binding, GPU and launch-client variables.
    :param code_variables: The installed codes' Bash API variables.
    :param data_dir: The job's data directory, or ``None`` to leave ``HTTK_WORKFLOW_DATA_DIR`` unset.
    :param declared_environment: The job's declared environment; settings it consumes are not exported.
    :param settings: The effective workspace settings, exported as ``HTTK_<KEY>`` variables.
    :return: The environment mapping.
    :raises httk.workflow.errors.FormatError: If the serialized context reaches the 100000-byte limit.
    """

    context_value = json_bytes(context)
    if len(context_value) >= 100_000:
        binding = context.get("binding")
        raise FormatError(
            "attempt context exceeds the 100000-byte environment limit"
            + ("" if binding is None else f"; its binding of {len(binding['nodes'])} nodes is too large")
        )
    context_json = context_value.decode("utf-8")
    environment = dict(base)
    environment.pop("HTTK_WORKFLOW_RUNNER_ARTIFACTS", None)
    environment.pop("HTTK_WORKFLOW_RUNNER_ROOT", None)
    environment.pop("HTTK_WORKFLOW_DEADLINE", None)
    if deadline is not None:
        environment["HTTK_WORKFLOW_DEADLINE"] = str(deadline)
    for variable in (
        "HTTK_WORKFLOW_NODELIST",
        "HTTK_WORKFLOW_NODEFILE",
        "HTTK_WORKFLOW_LAUNCH",
    ):
        environment.pop(variable, None)
    environment.update(binding_environment)
    environment.update(
        {
            # A runner's ``#!/usr/bin/env python3`` finds this interpreter, the
            # one the job's ``requires`` were checked in at claim time.
            "PATH": interpreter_first_path(base.get("PATH")),
            "HTTK_WORKFLOW_CONTEXT": context_json,
            "HTTK_WORKFLOW_CONTROL_DIR": str(control),
            "HTTK_WORKFLOW_WORKSPACE_DIR": str(workspace_root),
            "HTTK_WORKFLOW_JOB_DIR": str(payload),
            "HTTK_WORKFLOW_WORKDIR": str(workdir),
            "HTTK_WORKFLOW_IS_RESTART": "1" if context["is_restart"] else "0",
            "HTTK_WORKFLOW_UNCLEAN_RESTART": "1" if context["is_unclean_restart"] else "0",
            "HTTK_WORKFLOW_DURABLE": "1" if durable else "0",
            "HTTK_WORKFLOW_ATTEMPT_REASON": str(context["attempt_reason"]),
            "HTTK_WORKFLOW_STEP": str(context["step"]),
            "HTTK_WORKFLOW_PYTHON": sys.executable,
            "HTTK_WORKFLOW_BASH_API": str(_LANGUAGES / "bash" / "httk-workflow.sh"),
            "HTTK_WORKFLOW_LANGUAGES_DIR": str(_LANGUAGES),
            "HTTK_WORKFLOW_PERL_API": str(_LANGUAGES / "perl"),
        }
    )
    environment.update(code_variables)
    if data_dir is not None:
        environment["HTTK_WORKFLOW_DATA_DIR"] = str(data_dir)
    consumed_variables: set[str] = set()
    if isinstance(declared_environment, Mapping):
        consumed_variables = {
            _setting_variable_name(setting)
            for name, metadata in declared_environment.items()
            if isinstance(metadata, Mapping)
            for setting in (metadata.get("setting", name),)
            if isinstance(setting, str)
        }
    for key in sorted(settings):
        value = settings[key]
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            continue
        variable = _setting_variable_name(key)
        if variable in consumed_variables:
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", variable) is None:
            _LOGGER.warning("setting %s has an invalid environment variable name; not exported", key)
            continue
        if variable.startswith("HTTK_WORKFLOW_"):
            _LOGGER.warning(
                "setting %s shadows the reserved HTTK_WORKFLOW_ namespace; not exported",
                key,
            )
            continue
        environment.setdefault(variable, str(value))
    return environment
