"""The ``vasp-*`` subcommands of the private native Bash command bridge.

``httk.workflow._shell_bridge`` registers these beside its own subcommands, so
each function of ``codes/vasp/httk-vasp.sh`` is one invocation of one command
here. A legitimately absent answer returns the bridge's uniform exit code ``1``;
a refused call raises, which the bridge reports as ``2``.
"""

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path

from ..._util import read_json, write_json_atomic
from ...supervision import Diagnostic
from . import (
    DEFAULT_KPOINT_CENTERING,
    DEFAULT_REMEDY_HISTORY,
    VaspPreparationOptions,
    VaspRemedyDecision,
    apply_vasp_remedy,
    assemble_potcar,
    automatic_kpoint_grid,
    calculate_nbands,
    clean_outcar,
    clean_vasp_outputs,
    contcar_to_poscar,
    diagnose_vasp_files,
    job_remedy_history_path,
    last_oszicar_energy,
    last_vasprun_volume,
    normalize_poscar_handedness,
    outcar_plane_wave_count,
    outcar_potim,
    plan_vasp_remedy,
    potcar_summary,
    prepare_vasp_inputs,
    rattle_poscar,
    read_incar,
    run_vasp,
    scale_poscar_lattice,
    update_incar,
    write_automatic_kpoints,
)

# The bridge's uniform "legitimately absent" exit code.
_ABSENT = 1


def _remedy_history_default() -> str:
    """Return where a Bash runner records its remedy ladder by default.

    Inside an attempt the ladder belongs to the job, not to the directory one
    attempt ran in, so it defaults to the job-scoped file of
    :func:`httk.workflow.codes.vasp.job_remedy_history_path`. Outside an attempt — a
    bridge call made by hand — the historic workdir-relative name is kept.
    """

    payload = os.environ.get("HTTK_WORKFLOW_JOB_DIR")
    return str(job_remedy_history_path(payload)) if payload else DEFAULT_REMEDY_HISTORY


def add_commands(commands: "argparse._SubParsersAction[argparse.ArgumentParser]") -> None:
    """Register the ``vasp-*`` subcommands on the bridge's subparsers.

    :param commands: the bridge's subcommand collection.
    """

    vasp_prepare = commands.add_parser("vasp-prepare")
    vasp_prepare.add_argument("--directory", default=".")
    vasp_prepare.add_argument("--options")
    vasp_get = commands.add_parser("vasp-get-tag")
    vasp_get.add_argument("tag")
    vasp_get.add_argument("path", nargs="?", default="INCAR")
    vasp_set = commands.add_parser("vasp-set-tag")
    vasp_set.add_argument("tag")
    vasp_set.add_argument("value")
    vasp_set.add_argument("path", nargs="?", default="INCAR")
    grid = commands.add_parser("vasp-kpoints")
    grid.add_argument("density", type=float)
    grid.add_argument("--poscar", default="POSCAR")
    grid.add_argument("--output", default="KPOINTS")
    grid.add_argument("--centering", default=DEFAULT_KPOINT_CENTERING)
    grid.add_argument("--equal", action="store_true")
    grid.add_argument("--bump", type=int, default=0)
    potcar = commands.add_parser("vasp-potcar")
    potcar.add_argument("library")
    potcar.add_argument("--poscar", default="POSCAR")
    potcar.add_argument("--output", default="POTCAR")
    nbands = commands.add_parser("vasp-nbands")
    nbands.add_argument("--poscar", default="POSCAR")
    nbands.add_argument("--potcar", default="POTCAR")
    nbands.add_argument("--incar", default="INCAR")
    nbands.add_argument("--divisor", type=int)
    for name, default in (
        ("vasp-energy", "OSZICAR"),
        ("vasp-volume", "vasprun.xml"),
        ("vasp-potim", "OUTCAR"),
        ("vasp-plane-waves", "OUTCAR"),
    ):
        item = commands.add_parser(name)
        item.add_argument("path", nargs="?", default=default)
    promote = commands.add_parser("vasp-promote-contcar")
    promote.add_argument("--contcar", default="CONTCAR")
    promote.add_argument("--reference", default="POSCAR")
    promote.add_argument("--output", default="POSCAR")
    summary = commands.add_parser("vasp-potcar-summary")
    summary.add_argument("--path", default="POTCAR")
    summary.add_argument("--output", default="POTCAR.summary")
    clean = commands.add_parser("vasp-clean-outcar")
    clean.add_argument("--path", default="OUTCAR")
    clean.add_argument("--output", default="OUTCAR.cleaned")
    preclean = commands.add_parser("vasp-preclean")
    preclean.add_argument("--directory", default=".")
    preclean.add_argument("--keep", action="append", default=[])
    # CONTCAR and vasp-run-report.json survive a preclean unless named here.
    preclean.add_argument("--also-remove", action="append", default=[], dest="also_remove")
    normalize = commands.add_parser("vasp-normalize-poscar")
    normalize.add_argument("path", nargs="?", default="POSCAR")
    scale = commands.add_parser("vasp-scale-poscar")
    scale.add_argument("factor", type=float)
    scale.add_argument("path", nargs="?", default="POSCAR")
    rattle = commands.add_parser("vasp-rattle-poscar")
    rattle.add_argument("path", nargs="?", default="POSCAR")
    rattle.add_argument("--amplitude", type=float, default=0.01)
    # A rattle needs caller-supplied entropy: two retries that rattle identically
    # are two identical calculations, so neither default invents a stream.
    rattle.add_argument("--seed", type=int)
    rattle.add_argument("--entropy")
    vasp_run = commands.add_parser("vasp-run")
    vasp_run.add_argument("--directory", default=".")
    vasp_run.add_argument("--timeout", type=float)
    vasp_run.add_argument("--grace", type=float, default=10.0)
    vasp_run.add_argument("--report", default="vasp-run-report.json")
    vasp_run.add_argument("argv", nargs=argparse.REMAINDER)
    diagnose = commands.add_parser("vasp-diagnose")
    diagnose.add_argument("--directory", default=".")
    diagnose.add_argument("--output", default="vasp-diagnostics.json")
    remedy_plan = commands.add_parser("vasp-remedy-plan")
    remedy_plan.add_argument("report")
    remedy_plan.add_argument("--directory", default=".")
    remedy_plan.add_argument("--history", default=_remedy_history_default())
    remedy_plan.add_argument("--output", default="vasp-remedy-decision.json")
    remedy_plan.add_argument("--policy", default="reviewed-v1")
    remedy_apply = commands.add_parser("vasp-remedy-apply")
    remedy_apply.add_argument("decision")
    remedy_apply.add_argument("--directory", default=".")
    remedy_apply.add_argument("--history", default=_remedy_history_default())


def _diagnostics_from_report(path: Path):
    value = read_json(path)
    raw_items = value.get("diagnostics", [])
    result = []
    for item in raw_items:
        if isinstance(item, Mapping):
            result.append(
                Diagnostic(
                    str(item.get("code", "")),
                    str(item.get("severity", "error")),  # type: ignore[arg-type]
                    str(item.get("summary", "")),
                    str(item.get("source", "")),
                    None if item.get("evidence") is None else str(item["evidence"]),
                    bool(item.get("stop", False)),
                )
            )
    return tuple(result)


def _decision(path: Path) -> VaspRemedyDecision:
    value = read_json(path)
    raw_changes = value.get("changes", [])
    changes = tuple(
        (str(item["operation"]), item.get("value"))
        for item in raw_changes
        if isinstance(item, Mapping) and "operation" in item
    )
    return VaspRemedyDecision(
        str(value.get("policy", "")),
        str(value.get("problem", "")),
        int(value.get("step", 0)),
        changes,
        bool(value.get("give_up", False)),
        str(value.get("reason", "")),
    )


def run_command(arguments: argparse.Namespace) -> int:
    """Run one parsed ``vasp-*`` subcommand.

    :param arguments: the parsed bridge command line.
    :return: the subcommand's exit code.
    """

    command = arguments.command
    if command == "vasp-prepare":
        options = VaspPreparationOptions()
        if arguments.options:
            options = VaspPreparationOptions(**read_json(Path(arguments.options)))
        print(
            json.dumps(
                prepare_vasp_inputs(options, directory=arguments.directory), sort_keys=True, separators=(",", ":")
            )
        )
    elif command == "vasp-get-tag":
        value = read_incar(arguments.path).get(arguments.tag.upper())
        if value is None:
            return _ABSENT
        print(value)
    elif command == "vasp-set-tag":
        update_incar({arguments.tag: arguments.value}, arguments.path)
    elif command == "vasp-kpoints":
        grid = automatic_kpoint_grid(
            arguments.density, poscar=arguments.poscar, equal=arguments.equal, bump=arguments.bump
        )
        write_automatic_kpoints(grid, arguments.output, centering=arguments.centering)
        print(" ".join(str(value) for value in grid))
    elif command == "vasp-potcar":
        assemble_potcar(arguments.library, poscar=arguments.poscar, output=arguments.output)
    elif command == "vasp-nbands":
        print(
            calculate_nbands(
                poscar=arguments.poscar, potcar=arguments.potcar, incar=arguments.incar, divisor=arguments.divisor
            )
        )
    elif command in {"vasp-energy", "vasp-volume", "vasp-potim"}:
        reader = {
            "vasp-energy": last_oszicar_energy,
            "vasp-volume": last_vasprun_volume,
            "vasp-potim": outcar_potim,
        }[command]
        number = reader(arguments.path)
        if number is None:
            return _ABSENT
        print(f"{number:.16g}")
    elif command == "vasp-plane-waves":
        count = outcar_plane_wave_count(arguments.path)
        if count is None:
            return _ABSENT
        print(count)
    elif command == "vasp-promote-contcar":
        contcar_to_poscar(arguments.contcar, reference=arguments.reference, output=arguments.output)
    elif command == "vasp-potcar-summary":
        potcar_summary(arguments.path, arguments.output)
    elif command == "vasp-clean-outcar":
        clean_outcar(arguments.path, arguments.output)
    elif command == "vasp-preclean":
        for removed in clean_vasp_outputs(arguments.directory, keep=arguments.keep, also_remove=arguments.also_remove):
            print(removed)
    elif command == "vasp-normalize-poscar":
        normalize_poscar_handedness(arguments.path)
    elif command == "vasp-scale-poscar":
        scale_poscar_lattice(arguments.factor, arguments.path)
    elif command == "vasp-rattle-poscar":
        if arguments.seed is None and arguments.entropy is None:
            raise ValueError("vasp-rattle-poscar needs --seed or --entropy: an unseeded retry rattles identically")
        rattle_poscar(
            arguments.path,
            amplitude=arguments.amplitude,
            seed=arguments.seed,
            entropy=arguments.entropy,
        )
    elif command == "vasp-run":
        argv = arguments.argv[1:] if arguments.argv[:1] == ["--"] else arguments.argv
        report = run_vasp(
            argv,
            directory=arguments.directory,
            timeout=arguments.timeout,
            termination_grace=arguments.grace,
            report_path=arguments.report,
        )
        return {
            "completed": 0,
            "diagnosed_stop": 20,
            "nonconverged": 21,
            "process_failure": 22,
            "timeout": 124,
        }[report.classification]
    elif command == "vasp-diagnose":
        diagnostics = diagnose_vasp_files(arguments.directory)
        write_json_atomic(
            Path(arguments.output),
            {
                "format": "httk-vasp-diagnostics",
                "format_version": 2,
                "diagnostics": [item.as_mapping() for item in diagnostics],
            },
        )
        return 0 if not diagnostics else 20
    elif command == "vasp-remedy-plan":
        decision = plan_vasp_remedy(
            _diagnostics_from_report(Path(arguments.report)),
            directory=arguments.directory,
            history_path=arguments.history,
            policy=arguments.policy,
        )
        write_json_atomic(Path(arguments.output), decision.as_mapping())
        # The diagnosed problem is printed so a Bash runner can name it in the
        # outcome it publishes without parsing the decision it just wrote.
        print(decision.problem)
        return 3 if decision.give_up else 0
    elif command == "vasp-remedy-apply":
        apply_vasp_remedy(
            _decision(Path(arguments.decision)),
            directory=arguments.directory,
            history_path=arguments.history,
            # The language-neutral durability contract, honoured without binding
            # an attempt so the utility still runs by hand outside one.
            durable=os.environ.get("HTTK_WORKFLOW_DURABLE") == "1",
        )
    else:
        raise AssertionError(command)
    return 0
