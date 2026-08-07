"""Supervised VASP execution and its classified run report.

This is the execution side of the dependency-free VASP interface: a live monitor
that turns VASP-5/6 output lines into diagnostics, :func:`run_vasp` which drives
one supervised process and combines the live and file-level diagnostics into a
classified :class:`VaspRunReport`. Historical authorship is documented in
``v1_runtime/NOTICE``.
"""

import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .._util import write_json_atomic
from ..supervision import (
    Diagnostic,
    FollowSource,
    ProcessReport,
    ProcessSupervisor,
    SourceEvent,
)
from .diagnostics import _VASP_PATTERNS, diagnose_vasp_files


class _VaspMonitor:
    def __call__(self, event: SourceEvent) -> Sequence[Diagnostic]:
        if event.event != "line" or event.line is None:
            return ()
        result: list[Diagnostic] = []
        allocation = re.search(r"total allocation\s*:\s*([0-9]+)\s*KBytes", event.line, re.IGNORECASE)
        if allocation is not None and int(allocation.group(1)) > 500_000:
            result.append(
                Diagnostic(
                    "realspace_allocation_too_large",
                    "fatal",
                    "VASP requested more than 500000 KBytes for real-space projection",
                    event.source,
                    event.line,
                    True,
                )
            )
        for pattern, code, severity, stop in _VASP_PATTERNS:
            if pattern.search(event.line):
                result.append(
                    Diagnostic(
                        code,
                        severity,  # type: ignore[arg-type]
                        event.line.strip(),
                        event.source,
                        event.line,
                        stop,
                    )
                )
        return result


@dataclass(frozen=True)
class VaspRunReport:
    """Classified result of one supervised VASP execution.

    :param process: Preserve the supervised process result.
    :param classification: Record the final VASP run classification.
    :param diagnostics: Preserve live and file-level diagnostics.
    """

    process: ProcessReport
    classification: str
    diagnostics: tuple[Diagnostic, ...]

    def as_mapping(self) -> dict[str, object]:
        """Serialize the report for JSON storage.

        :return: The JSON-compatible report mapping.
        """
        return {
            "format": "httk-vasp-run-report",
            "format_version": 1,
            "process": self.process.as_mapping(),
            "classification": self.classification,
            "diagnostics": [item.as_mapping() for item in self.diagnostics],
        }

    def write(self, path: str | os.PathLike[str]) -> Path:
        """Write the report as JSON.

        :param path: Write the report to this path.
        :return: The report path.
        """
        destination = Path(path)
        write_json_atomic(destination, self.as_mapping())
        return destination


def run_vasp(
    argv: Sequence[str],
    *,
    directory: str | os.PathLike[str] = ".",
    timeout: float | None = None,
    termination_grace: float = 10.0,
    report_path: str | os.PathLike[str] = "vasp-run-report.json",
) -> VaspRunReport:
    """Run VASP with live VASP-5/6 diagnostics and a structured report.

    :param argv: Execute this VASP command argument vector.
    :param directory: Run VASP in this directory.
    :param timeout: Stop the process after this duration when set.
    :param termination_grace: Allow this duration for graceful termination.
    :param report_path: Write the structured report at this workdir-relative path.
    :return: The classified VASP run report.
    """

    root = Path(directory).resolve()
    supervisor = ProcessSupervisor(
        monitors=(_VaspMonitor(),),
        follow=(
            FollowSource(root / "OSZICAR", "OSZICAR"),
            FollowSource(root / "OUTCAR", "OUTCAR"),
        ),
    )
    process = supervisor.run(
        argv,
        timeout=timeout,
        cwd=root,
        termination_grace=termination_grace,
        stdout_path=root / "vasp.out",
        stderr_path=root / "vasp.err",
    )
    diagnostics = _deduplicate((*process.diagnostics, *diagnose_vasp_files(root)))
    if process.timed_out:
        classification = "timeout"
    elif any(item.stop for item in diagnostics):
        classification = "diagnosed_stop"
    elif process.returncode:
        classification = "process_failure"
    elif any(
        item.code in {"electronic_nonconvergence", "ionic_nonconvergence", "positive_final_energy"}
        for item in diagnostics
    ):
        classification = "nonconverged"
    elif any(item.code == "incomplete_outcar" for item in diagnostics):
        classification = "process_failure"
    else:
        classification = "completed"
    report = VaspRunReport(process, classification, diagnostics)
    report.write(root / report_path)
    return report


def _deduplicate(values: Sequence[Diagnostic]) -> tuple[Diagnostic, ...]:
    result: list[Diagnostic] = []
    seen: set[tuple[str, str, str | None]] = set()
    for item in values:
        key = item.code, item.source, item.evidence
        if key not in seen:
            seen.add(key)
            result.append(item)
    return tuple(result)
