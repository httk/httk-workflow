"""What a manager learns about the batch allocation it runs inside."""

import logging
from collections.abc import Mapping

_LOGGER = logging.getLogger(__name__)


def slurm_end_time(environ: Mapping[str, str]) -> float | None:
    """Return the epoch second the enclosing Slurm job ends, if it is known.

    :param environ: The environment to read ``SLURM_JOB_ID`` and ``SLURM_JOB_END_TIME`` from.
    :return: The end time, or ``None`` outside a Slurm job or when it is not a positive integer.
    """

    raw = environ.get("SLURM_JOB_END_TIME")
    if "SLURM_JOB_ID" not in environ or raw is None:
        return None
    text = raw.strip()
    if not text.isdigit() or int(text) <= 0:
        _LOGGER.warning("ignoring SLURM_JOB_END_TIME=%r: not a positive epoch second", raw)
        return None
    return float(text)
