"""The reserved time resource labels and their Slurm ``--time`` spelling.

``maxtime`` and ``mintime`` are job requirements in integer seconds in the
protocol. Every authoring surface spells them as Slurm ``--time`` strings, so
a bare number is never ambiguous between seconds and minutes.
"""

import re
from collections.abc import Mapping

TIME_RESOURCES: frozenset[str] = frozenset({"maxtime", "mintime"})

_DURATION = re.compile(r"(?:([0-9]+)-)?([0-9]+)(?::([0-9]+))?(?::([0-9]+))?")
_FORMS = "M, M:S, H:M:S, D-H, D-H:M or D-H:M:S, with seconds below 60, minutes below 60 after H:, and hours below 24 after D-"


def parse_slurm_duration(text: str) -> int:
    """Return the seconds of one Slurm ``--time`` duration.

    Without a ``D-`` prefix one field is minutes, two are minutes and seconds,
    and three are hours, minutes, and seconds; with it one field is hours, two
    are hours and minutes, and three are hours, minutes, and seconds.

    :param text: The duration text.
    :return: The duration in seconds.
    :raises ValueError: If the text is not one of the accepted forms.
    """

    match = _DURATION.fullmatch(text)
    if match is not None:
        days = match.group(1)
        values = [int(field) for field in match.groups()[1:] if field is not None]
        leading_minutes = days is None and len(values) < 3
        if leading_minutes:
            values.insert(0, 0)
        hours, minutes, seconds = (values + [0, 0])[:3]
        if seconds < 60 and (minutes < 60 or leading_minutes) and (days is None or hours < 24):
            return ((int(days or 0) * 24 + hours) * 60 + minutes) * 60 + seconds
    raise ValueError(f"{text!r} is not a Slurm duration: use {_FORMS}")


def format_duration(seconds: int) -> str:
    """Return *seconds* as ``HH:MM:SS``, or ``D-HH:MM:SS`` from one day.

    :param seconds: The non-negative duration in seconds.
    :return: The Slurm duration text.
    """

    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, rest = divmod(rest, 60)
    clock = f"{hours:02d}:{minutes:02d}:{rest:02d}"
    return f"{days}-{clock}" if days else clock


def cap_maxtime(
    resources: Mapping[str, int],
    step_resources: Mapping[str, Mapping[str, int]],
    cap: int | None,
) -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Clamp a child job's ``maxtime`` requirements to its parent's *cap*.

    The job-level requirement always ends with ``maxtime`` at most *cap*; a
    step-level requirement is clamped only when it declares ``maxtime``. A
    ``mintime`` above a clamped ``maxtime`` in the same mapping is lowered to it.

    :param resources: The child's job-level requirement in protocol seconds.
    :param step_resources: The child's per-step requirements in protocol seconds.
    :param cap: The parent attempt's effective ``maxtime``, or ``None`` for no cap.
    :return: The capped job-level and per-step requirements.
    """

    def clamp(mapping: Mapping[str, int], always: bool) -> dict[str, int]:
        result = dict(mapping)
        if cap is not None and (always or "maxtime" in result):
            result["maxtime"] = min(result.get("maxtime", cap), cap)
            if result.get("mintime", 0) > result["maxtime"]:
                result["mintime"] = result["maxtime"]
        return result

    return clamp(resources, True), {step: clamp(mapping, False) for step, mapping in step_resources.items()}
