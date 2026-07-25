"""Packaged implementations used by the maintained adapter wrappers."""

import json
import shutil
import subprocess
import sys
from pathlib import Path


def _result(operation: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "format": "httk-computer-result",
                "format_version": 1,
                "operation": operation,
                "ok": True,
                **values,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _copy(source: Path, destination: Path) -> None:
    if source.is_dir():
        if destination.exists():
            source_manifest = source / ".httk-transfer" / "manifest.json"
            destination_manifest = destination / ".httk-transfer" / "manifest.json"
            if (
                not destination.is_dir()
                or not source_manifest.is_file()
                or not destination_manifest.is_file()
                or source_manifest.read_bytes() != destination_manifest.read_bytes()
            ):
                raise FileExistsError(destination)
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, symlinks=True)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)


def _local_httk_fallback(arguments: list[str]) -> list[str]:
    if arguments[0] == "httk" and shutil.which("httk") is None:
        return [sys.executable, "-m", "httk.core.cli", *arguments[1:]]
    return arguments


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 2:
        print("adapter operation expects OPERATION REQUEST.json", file=sys.stderr)
        return 2
    operation, request_name = arguments
    try:
        request = json.loads(Path(request_name).read_text(encoding="utf-8"))
        if not isinstance(request, dict) or request.get("operation") != operation:
            raise ValueError("request operation mismatch")
        if operation in {"configure", "install"}:
            _result(operation, configured=True)
        elif operation in {"push", "pull"}:
            source = Path(str(request["source"])).expanduser().resolve()
            destination = Path(str(request["destination"])).expanduser().resolve()
            _copy(source, destination)
            _result(operation, path=str(destination))
        elif operation == "invoke":
            raw = request.get("argv")
            if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
                raise ValueError("invoke argv must be a nonempty string array")
            cwd = request.get("cwd")
            raw = _local_httk_fallback(raw)
            completed = subprocess.run(
                raw,
                cwd=None if cwd is None else str(cwd),
                text=True,
                capture_output=True,
                check=False,
            )
            _result(
                operation,
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        elif operation == "start-manager":
            raw = request.get("argv")
            if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
                raise ValueError("start-manager argv must be a nonempty string array")
            raw = _local_httk_fallback(raw)
            process = subprocess.Popen(
                raw,
                cwd=None if request.get("cwd") is None else str(request["cwd"]),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            _result(operation, pid=process.pid)
        elif operation == "status":
            raw = request.get("argv")
            if not isinstance(raw, list) or not raw or not all(isinstance(item, str) for item in raw):
                raise ValueError("status argv must be a nonempty string array")
            completed = subprocess.run(
                _local_httk_fallback(raw),
                text=True,
                capture_output=True,
                check=False,
            )
            _result(
                operation,
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        else:
            raise ValueError(f"unsupported maintained adapter operation: {operation}")
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        print(f"adapter {operation}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
