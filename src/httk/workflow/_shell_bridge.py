"""Private command bridge used by the packaged native Bash libraries."""

import argparse
import json
import shutil
import sys
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath

from ._util import read_json, sha256_file, tree_digest, write_json_atomic
from .models import Failure, validate_label
from .runtime import AttemptRuntime
from .runtime_builders import (
    JobSpec,
    JoinSpec,
    OutcomeBuilder,
    ReplayableWorkdirBatch,
    prepare_job_payload,
)
from .runtime_utils import (
    compress_files,
    decompress_files,
    evaluate_expression,
    render_template,
)
from .supervision import CheckerSpec, ProcessSupervisor
from .vasp import (
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="httk-workflow-shell-bridge")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    context = commands.add_parser("context")
    context.add_argument("field", nargs="?")
    state_get = commands.add_parser("state-get")
    state_get.add_argument("name")
    state_set = commands.add_parser("state-set")
    state_set.add_argument("name")
    state_set.add_argument("json_value")
    state_delete = commands.add_parser("state-delete")
    state_delete.add_argument("name")
    runlog = commands.add_parser("runlog")
    runlog.add_argument("kind")
    runlog.add_argument("message")
    runlog.add_argument("files", nargs="*")
    commands.add_parser("outcome-begin")
    job_prepare = commands.add_parser("job-prepare")
    job_prepare.add_argument("destination")
    job_prepare.add_argument("spec")
    workdir_apply = commands.add_parser("workdir-apply")
    workdir_apply.add_argument("spec")

    for name in ("tx-mkdir", "tx-remove"):
        item = commands.add_parser(name)
        item.add_argument("draft")
        item.add_argument("operation_id")
        item.add_argument("path")
        if name == "tx-remove":
            item.add_argument("--missing-ok", action="store_true")
    for name in ("tx-put-file", "tx-put-tree", "tx-replace-tree"):
        item = commands.add_parser(name)
        item.add_argument("draft")
        item.add_argument("operation_id")
        item.add_argument("source")
        item.add_argument("path")
    child = commands.add_parser("child-add")
    child.add_argument("draft")
    child.add_argument("payload")
    child.add_argument("placement")

    for name in ("advance", "wait"):
        item = commands.add_parser(name)
        item.add_argument("next_step")
        item.add_argument("--draft")
        item.add_argument("--priority", type=int)
        if name == "wait":
            item.add_argument(
                "--condition",
                choices=("all_succeeded", "all_terminal", "any_succeeded", "at_least"),
                default="all_succeeded",
            )
            item.add_argument("--count", type=int)
            item.add_argument("--on-impossible")
    succeed = commands.add_parser("succeed")
    succeed.add_argument("--draft")
    fail = commands.add_parser("fail")
    fail.add_argument("code")
    fail.add_argument("message")
    fail.add_argument("--details")
    fail.add_argument("--draft")
    retry = commands.add_parser("retry")
    retry.add_argument("reason")
    retry.add_argument("--draft")
    pause = commands.add_parser("pause")
    pause.add_argument("reason")
    pause.add_argument("--draft")

    run = commands.add_parser("run")
    run.add_argument("--timeout", type=float)
    run.add_argument("--grace", type=float, default=10.0)
    run.add_argument("--report", default="process-report.json")
    run.add_argument("--stdout")
    run.add_argument("--stderr")
    run.add_argument("--checker", action="append", default=[])
    run.add_argument("argv", nargs=argparse.REMAINDER)

    calc = commands.add_parser("calc")
    calc.add_argument("expression")
    template = commands.add_parser("template")
    template.add_argument("template")
    template.add_argument("output")
    template.add_argument("values")
    for name in ("compress", "decompress"):
        item = commands.add_parser(name)
        if name == "compress":
            item.add_argument("--method", choices=("bz2", "gz", "xz"), default="bz2")
        item.add_argument("--remove-source", action="store_true")
        item.add_argument("paths", nargs="+")

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
    grid.add_argument("--centering", default="Gamma")
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
    normalize = commands.add_parser("vasp-normalize-poscar")
    normalize.add_argument("path", nargs="?", default="POSCAR")
    scale = commands.add_parser("vasp-scale-poscar")
    scale.add_argument("factor", type=float)
    scale.add_argument("path", nargs="?", default="POSCAR")
    rattle = commands.add_parser("vasp-rattle-poscar")
    rattle.add_argument("path", nargs="?", default="POSCAR")
    rattle.add_argument("--amplitude", type=float, default=0.01)
    rattle.add_argument("--seed", type=int, default=0)
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
    remedy_plan.add_argument("--history", default=".httk-vasp/remedies.json")
    remedy_plan.add_argument("--output", default="vasp-remedy-decision.json")
    remedy_apply = commands.add_parser("vasp-remedy-apply")
    remedy_apply.add_argument("decision")
    remedy_apply.add_argument("--directory", default=".")
    remedy_apply.add_argument("--history", default=".httk-vasp/remedies.json")
    return parser


def _runtime() -> AttemptRuntime:
    return AttemptRuntime.from_environment()


def _builder(draft: str | None) -> OutcomeBuilder:
    runtime = _runtime()
    return runtime.outcome() if draft is None else OutcomeBuilder.resume(runtime, draft)


def _safe_target(value: str) -> PurePosixPath:
    if "\0" in value:
        raise ValueError("transaction target contains a NUL byte")
    target = PurePosixPath(value)
    if (
        target.is_absolute()
        or not target.parts
        or any(part in {"", ".", "..", ".httk-workflow", ".httk-runner"} for part in target.parts)
    ):
        raise ValueError("transaction target must be a normalized relative path")
    return target


def _transaction_add(arguments: argparse.Namespace) -> None:
    runtime = _runtime()
    draft = Path(arguments.draft).resolve()
    if draft.parent != runtime.control or not draft.name.startswith("outcome.tmp."):
        raise ValueError("outcome draft is not owned by this attempt")
    root = draft / "transaction"
    payload = root / "payload"
    payload.mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
    else:
        if runtime.context.data_generation is None:
            raise ValueError("this job does not use transactional data")
        manifest = {
            "format": "httk-workflow-transaction",
            "format_version": 1,
            "id": str(uuid.uuid4()),
            "expected_data_generation": runtime.context.data_generation,
            "operations": [],
        }
    operations = manifest["operations"]
    if not isinstance(operations, list):
        raise ValueError("transaction manifest operations are invalid")
    identifier = validate_label(arguments.operation_id, "transaction operation id")
    if any(isinstance(item, Mapping) and item.get("id") == identifier for item in operations):
        raise ValueError(f"duplicate transaction operation id: {identifier}")
    operation_name = arguments.command.removeprefix("tx-")
    target = _safe_target(arguments.path)
    if operation_name != "mkdir":
        for raw in operations:
            if not isinstance(raw, Mapping) or raw.get("op") == "make-dir":
                continue
            existing = _safe_target(str(raw["path"]))
            if target == existing or target in existing.parents or existing in target.parents:
                raise ValueError(f"transaction targets overlap: {existing} and {target}")
    operation = {
        "id": identifier,
        "op": {
            "mkdir": "make-dir",
            "replace-tree": "replace-tree",
            "put-tree": "put-tree",
            "put-file": "put-file",
            "remove": "remove",
        }[operation_name],
        "path": target.as_posix(),
    }
    if operation_name == "remove":
        operation["missing_ok"] = arguments.missing_ok
    elif operation_name in {"put-file", "put-tree", "replace-tree"}:
        source = Path(arguments.source)
        staged = payload / identifier
        if operation_name == "put-file":
            if not source.is_file() or source.is_symlink():
                raise ValueError("put-file source must be a regular file")
            shutil.copy2(source, staged)
            digest = sha256_file(staged)
        else:
            shutil.copytree(source, staged)
            digest = tree_digest(staged)
        operation.update({"source": f"payload/{identifier}", "sha256": digest})
    operations.append(operation)
    write_json_atomic(manifest_path, manifest)


def _diagnostics_from_report(path: Path):
    from .supervision import Diagnostic

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


def _command(arguments: argparse.Namespace) -> int:
    command = arguments.command
    if command == "init":
        runtime = AttemptRuntime.initialize()
        print(runtime.context.step)
    elif command == "context":
        raw = _runtime().context.raw
        if arguments.field is None:
            print(json.dumps(raw, sort_keys=True, separators=(",", ":")))
        elif arguments.field not in raw:
            return 1
        elif isinstance(raw[arguments.field], (dict, list)):
            print(json.dumps(raw[arguments.field], sort_keys=True, separators=(",", ":")))
        elif raw[arguments.field] is not None:
            print(raw[arguments.field])
    elif command == "state-get":
        value = _runtime().state.get(arguments.name)
        if value is None:
            return 1
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
    elif command == "state-set":
        _runtime().state.set(arguments.name, json.loads(arguments.json_value))
    elif command == "state-delete":
        return 0 if _runtime().state.delete(arguments.name) else 1
    elif command == "runlog":
        _runtime().runlog.append(arguments.kind, arguments.message, files=arguments.files)
    elif command == "outcome-begin":
        print(_runtime().outcome().root)
    elif command == "job-prepare":
        raw = read_json(Path(arguments.spec))
        prepare_job_payload(arguments.destination, JobSpec(**raw))
    elif command == "workdir-apply":
        raw = read_json(Path(arguments.spec))
        operations = raw.get("operations")
        if not isinstance(operations, Sequence) or isinstance(operations, (str, bytes)):
            raise ValueError("workdir operation spec requires an operations array")
        batch = ReplayableWorkdirBatch.create(_runtime().workdir)
        for item in operations:
            if not isinstance(item, Mapping):
                raise ValueError("workdir operation must be an object")
            operation = str(item.get("op", ""))
            identifier = str(item.get("id", ""))
            path = str(item.get("path", ""))
            if operation == "make-dir":
                batch.transaction.make_dir(identifier, path)
            elif operation == "put-file":
                batch.transaction.put_file(identifier, str(item.get("source", "")), path)
            elif operation in {"put-tree", "replace-tree"}:
                batch.transaction.put_tree(
                    identifier,
                    str(item.get("source", "")),
                    path,
                    replace=operation == "replace-tree",
                )
            elif operation == "remove":
                batch.transaction.remove(identifier, path, missing_ok=bool(item.get("missing_ok", False)))
            else:
                raise ValueError(f"unknown workdir operation: {operation}")
        print(batch.commit())
    elif command.startswith("tx-"):
        _transaction_add(arguments)
    elif command == "child-add":
        reference = OutcomeBuilder.resume(_runtime(), arguments.draft).add_child(arguments.payload, arguments.placement)
        print(json.dumps(reference.as_mapping(), sort_keys=True, separators=(",", ":")))
    elif command == "advance":
        _builder(arguments.draft).publish("advance", next_step=arguments.next_step, priority=arguments.priority)
    elif command == "wait":
        builder = _builder(arguments.draft)
        join = JoinSpec(
            builder.children,
            arguments.condition,
            arguments.count,
            arguments.on_impossible,
        )
        builder.publish("wait", next_step=arguments.next_step, priority=arguments.priority, join=join)
    elif command == "succeed":
        _builder(arguments.draft).publish("succeed")
    elif command == "fail":
        failure = Failure(
            arguments.code,
            arguments.message,
            details=None if not arguments.details else read_json(Path(arguments.details)),
        )
        _builder(arguments.draft).publish("fail", failure=failure.as_mapping())
    elif command == "retry":
        _builder(arguments.draft).publish("retry", retry={"reason": arguments.reason})
    elif command == "pause":
        _builder(arguments.draft).publish("pause", pause={"reason": arguments.reason})
    elif command == "run":
        argv = arguments.argv[1:] if arguments.argv[:1] == ["--"] else arguments.argv
        checkers = tuple(CheckerSpec.from_mapping(read_json(Path(path))) for path in arguments.checker)
        process_report = ProcessSupervisor(
            checkers=checkers,
            follow=tuple(source for checker in checkers for source in checker.sources),
        ).run(
            argv,
            timeout=arguments.timeout,
            termination_grace=arguments.grace,
            stdout_path=arguments.stdout,
            stderr_path=arguments.stderr,
        )
        process_report.write(arguments.report)
        return (
            124
            if process_report.timed_out
            else (
                125
                if process_report.termination.startswith("checker")
                or any(item.stop for item in process_report.diagnostics)
                else (0 if process_report.returncode == 0 else 22)
            )
        )
    elif command == "calc":
        value = evaluate_expression(arguments.expression)
        print(int(value) if isinstance(value, bool) else f"{value:.14g}" if isinstance(value, float) else value)
    elif command == "template":
        render_template(arguments.template, arguments.output, read_json(Path(arguments.values)))
    elif command == "compress":
        for output_path in compress_files(
            arguments.paths, method=arguments.method, remove_source=arguments.remove_source
        ):
            print(output_path)
    elif command == "decompress":
        for output_path in decompress_files(arguments.paths, remove_source=arguments.remove_source):
            print(output_path)
    elif command == "vasp-prepare":
        options = VaspPreparationOptions()
        if arguments.options:
            raw = read_json(Path(arguments.options))
            options = VaspPreparationOptions(**raw)
        print(
            json.dumps(
                prepare_vasp_inputs(options, directory=arguments.directory), sort_keys=True, separators=(",", ":")
            )
        )
    elif command == "vasp-get-tag":
        value = read_incar(arguments.path).get(arguments.tag.upper())
        if value is None:
            return 1
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
    elif command == "vasp-energy":
        value = last_oszicar_energy(arguments.path)
        if value is None:
            return 1
        print(f"{value:.16g}")
    elif command == "vasp-volume":
        value = last_vasprun_volume(arguments.path)
        if value is None:
            return 1
        print(f"{value:.16g}")
    elif command == "vasp-potim":
        value = outcar_potim(arguments.path)
        if value is None:
            return 1
        print(f"{value:.16g}")
    elif command == "vasp-plane-waves":
        value = outcar_plane_wave_count(arguments.path)
        if value is None:
            return 1
        print(value)
    elif command == "vasp-promote-contcar":
        contcar_to_poscar(arguments.contcar, reference=arguments.reference, output=arguments.output)
    elif command == "vasp-potcar-summary":
        potcar_summary(arguments.path, arguments.output)
    elif command == "vasp-clean-outcar":
        clean_outcar(arguments.path, arguments.output)
    elif command == "vasp-preclean":
        for removed in clean_vasp_outputs(arguments.directory, keep=arguments.keep):
            print(removed)
    elif command == "vasp-normalize-poscar":
        normalize_poscar_handedness(arguments.path)
    elif command == "vasp-scale-poscar":
        scale_poscar_lattice(arguments.factor, arguments.path)
    elif command == "vasp-rattle-poscar":
        rattle_poscar(arguments.path, amplitude=arguments.amplitude, seed=arguments.seed)
    elif command == "vasp-run":
        argv = arguments.argv[1:] if arguments.argv[:1] == ["--"] else arguments.argv
        vasp_report = run_vasp(
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
        }[vasp_report.classification]
    elif command == "vasp-diagnose":
        diagnostics = diagnose_vasp_files(arguments.directory)
        write_json_atomic(
            Path(arguments.output),
            {
                "format": "httk-vasp-diagnostics",
                "format_version": 1,
                "diagnostics": [item.as_mapping() for item in diagnostics],
            },
        )
        return 0 if not diagnostics else 20
    elif command == "vasp-remedy-plan":
        decision = plan_vasp_remedy(_diagnostics_from_report(Path(arguments.report)), history_path=arguments.history)
        write_json_atomic(Path(arguments.output), decision.as_mapping())
        return 3 if decision.give_up else 0
    elif command == "vasp-remedy-apply":
        apply_vasp_remedy(
            _decision(Path(arguments.decision)),
            directory=arguments.directory,
            history_path=arguments.history,
        )
    else:
        raise AssertionError(command)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return _command(_parser().parse_args(argv))
    except Exception as exc:
        print(f"httk-workflow: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
