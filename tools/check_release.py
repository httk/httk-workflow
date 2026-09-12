"""Verify a committed httk module in disposable release and wheel environments.

Run with Python 3.12+, Git, make, and (for JavaScript projects) Node/npm.
This standalone repository tool does not require an installed httk package.
"""

import argparse
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path


def _environment(work: Path) -> dict[str, str]:
    """Remove workspace Python, package-index, and test-selection overrides."""
    excluded = {
        "VIRTUAL_ENV",
        "MAKEFLAGS",
        "MFLAGS",
        "MAKELEVEL",
        "MAKEFILES",
        "GNUMAKEFLAGS",
        "MYPYPATH",
        "NODE_OPTIONS",
        "PYTHON",
        "NODE",
        "NPM",
        "DIST_DIR",
        "DOCS_BASE_URL",
        "MEMGUARD",
        "TEST_TIMEOUT_SECONDS",
        "EXTENDED_TEST_TIMEOUT_SECONDS",
    }
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in excluded and not key.startswith(("PYTHON", "PYTEST", "PIP_", "UV_", "HTTK_", "CONDA_"))
    }
    paths = []
    for entry in os.environ.get("PATH", os.defpath).split(os.pathsep):
        path = Path(entry)
        if path.is_absolute() and not (path.resolve().parent / "pyvenv.cfg").is_file():
            paths.append(entry)
    env.update(
        PATH=os.pathsep.join([str(work / ".venv/bin"), *paths]),
        PYTHONNOUSERSITE="1",
        PIP_CONFIG_FILE=os.devnull,
        PIP_INDEX_URL="https://pypi.org/simple",
        PIP_CACHE_DIR=str(work / "cache/pip"),
        UV_CONFIG_FILE=os.devnull,
        UV_DEFAULT_INDEX="https://pypi.org/simple",
        UV_CACHE_DIR=str(work / "cache/uv"),
        npm_config_cache=str(work / "cache/npm"),
        TMPDIR=str(work / "tmp"),
    )
    return env


def _candidate_files(repo: Path) -> list[str]:
    """List present tracked and nonignored untracked candidate files."""
    entries = subprocess.check_output(
        ["git", "ls-files", "--stage"],
        cwd=repo,
        text=True,
    )
    if any(line.startswith("160000 ") for line in entries.splitlines()):
        raise ValueError("Check individual module repositories; gitlinks/submodules are unsupported")
    paths = (
        subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=repo,
        )
        .decode()
        .split("\0")
    )
    return sorted({name for name in paths if name and ((repo / name).is_file() or (repo / name).is_symlink())})


def _manifest(root: Path, paths: list[str]) -> dict[str, object]:
    """Fingerprint file contents, executable modes, and symlink targets."""
    result: dict[str, object] = {}
    for name in paths:
        path = root / name
        if path.is_symlink():
            result[name] = {"symlink": os.readlink(path)}
        else:
            result[name] = {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "executable": bool(path.stat().st_mode & 0o111),
            }
    return result


def _release_inputs(source: Path) -> list[str]:
    """Identify the generated inputs that preparation may copy back."""
    return [
        "docs/requirements.lock",
        *sorted(str(path.relative_to(source)) for path in (source / "docs/_inventories").glob("*.inv")),
    ]


def _assert_source_unchanged(repo: Path, source: Path, candidate: dict[str, object]) -> None:
    """Reject changed candidate files and new nonignored source files from gates."""
    if _manifest(source, list(candidate)) != candidate:
        raise ValueError("A gate changed candidate source files; review the changes and re-run preparation.")
    added = {
        str(path.relative_to(source))
        for path in source.rglob("*")
        if (path.is_file() or path.is_symlink()) and str(path.relative_to(source)) not in candidate
    }
    if not added:
        return
    ignored = subprocess.run(
        ["git", "check-ignore", "--no-index", "-z", "--stdin"],
        cwd=repo,
        input="\0".join(added) + "\0",
        text=True,
        capture_output=True,
        check=False,
    )
    if ignored.returncode not in (0, 1):
        raise ValueError(f"Could not check generated files against repository ignore rules: {ignored.stderr}")
    added.difference_update(ignored.stdout.split("\0"))
    if added:
        raise ValueError(f"A gate added source files; review and re-run preparation: {', '.join(sorted(added))}")


def _copy_inputs(repo: Path, source: Path, original: dict[str, object], candidate: dict[str, object]) -> None:
    """Copy verified generated inputs only if the original candidate is unchanged."""
    if _manifest(repo, _candidate_files(repo)) != original:
        raise ValueError("Working files changed during preparation; no inputs copied back. Re-run preparation.")
    for name in _release_inputs(source):
        destination = repo / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source / name, destination)
    if _manifest(repo, _candidate_files(repo)) != candidate:
        raise ValueError("Working files do not match the verified candidate; re-run preparation before committing.")


def _next_steps(tag: str, prepared: bool = False) -> None:
    """Print the manual release handoff without changing Git or remote state."""
    if not prepared:
        print(f"For complete isolated preparation, run: make release-prepare VERSION={tag}")
    print(
        "After successful preparation:\n"
        "  1. Review and commit exactly the verified changes, including docs locks and inventories; sign as needed.\n"
        f"  2. Create the signed release tag on that final commit: git tag -s {shlex.quote(tag)}\n"
        f"  3. Push the final commit and tag: git push && git push origin {shlex.quote(tag)}\n"
        f"  4. Create and publish the GitHub release for {tag}.\n"
        "Further source edits require another release-prepare run."
    )


def _snapshot(repo: Path, work: Path, working_tree: bool = False) -> tuple[Path, str]:
    """Export exactly one clean commit, preserving executable bits and symlinks."""
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"],
        cwd=repo,
        text=True,
    )
    if dirty and not working_tree:
        raise ValueError(f"Commit or remove uncommitted changes before checking:\n{dirty}")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    tree = subprocess.check_output(["git", "ls-tree", "-r", commit], cwd=repo, text=True)
    if any(line.startswith("160000 ") for line in tree.splitlines()):
        raise ValueError("Check individual module repositories; gitlink/submodule archives are unsupported")
    source = work / "source"
    source.mkdir()
    archive = work / "source.tar"
    if working_tree:
        with tarfile.open(archive, "w") as output:
            for name in _candidate_files(repo):
                output.add(repo / name, arcname=name, recursive=False)
    else:
        with archive.open("wb") as output:
            subprocess.run(["git", "archive", commit], cwd=repo, stdout=output, check=True)
    with tarfile.open(archive) as exported:
        exported.extractall(source, filter="data")
    archive.unlink()
    return source, commit


def _run(command: list[str], cwd: Path, env: dict[str, str], log: Path) -> None:
    """Run a gate, retaining its complete output and propagating failure."""
    print(f"Running {shlex.join(command)}\n  log: {log}", flush=True)
    with log.open("w") as output:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        print("\n".join(log.read_text(errors="replace").splitlines()[-30:]), file=sys.stderr)
        raise subprocess.CalledProcessError(result.returncode, command)


def main() -> int:
    """Run the release gates and write a commit-specific verification report.

    :return: Zero when every gate passes, otherwise one.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository", nargs="?", type=Path, default=Path.cwd())
    parser.add_argument("--tag", help="Expected release tag; defaults to v<project.version>.")
    parser.add_argument("--output-dir", type=Path, help="New directory for retained logs, environments, and artifacts.")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--prepare", action="store_true", help="Refresh and verify the uncommitted release candidate.")
    modes.add_argument("--next-steps", action="store_true", help="Print the release handoff (works outside Git).")
    args = parser.parse_args()
    project = tomllib.loads((args.repository / "pyproject.toml").read_text())["project"]
    expected = f"v{project['version']}"
    if args.next_steps:
        _next_steps(expected)
        return 0
    tag = args.tag if args.tag is not None else os.environ.get("HTTK_RELEASE_VERSION") if args.prepare else expected
    if not tag:
        parser.error(f"VERSION is required. Run: make release-prepare VERSION={expected}")
    if tag != expected:
        parser.error(f"VERSION {tag!r} does not match pyproject.toml; expected {expected!r}.")
    repo = Path(
        subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=args.repository,
            text=True,
        ).strip()
    )
    if args.output_dir:
        work = args.output_dir.resolve()
        if work.is_relative_to(repo):
            parser.error("--output-dir must be outside the repository")
        work.mkdir(parents=True, exist_ok=False)
    else:
        work = Path(tempfile.mkdtemp(prefix="httk-release-check-")).resolve()
    if work.is_relative_to(repo):
        parser.error("Temporary output must be outside the repository; set TMPDIR or --output-dir accordingly.")
    print(f"Verification output: {work}", flush=True)
    report: dict[str, object] = {
        "repository": str(repo),
        "status": "failed",
        "checker_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "scope": "Declared CI/release targets, locked docs, and configured bare-wheel imports; "
        "does not add browser or live-database gates, or reproduce the GitHub OS image.",
    }
    passed: list[str] = []
    report["passed"] = passed
    try:
        original = _manifest(repo, _candidate_files(repo)) if args.prepare else {}
        source, commit = _snapshot(repo, work, working_tree=args.prepare)
        if args.prepare and _manifest(source, list(original)) != original:
            raise ValueError("Working files changed while snapshotting; re-run preparation.")
        project = tomllib.loads((source / "pyproject.toml").read_text())["project"]
        if tag != f"v{project['version']}":
            raise ValueError(f"Tag {tag!r} does not match project.version {project['version']!r}")
        report.update(commit=commit, tag=tag, python=sys.version, mode="prepare" if args.prepare else "committed")
        print(
            f"Checking {project['name']} {tag}, {'candidate based on' if args.prepare else 'commit'} {commit}",
            flush=True,
        )
        (work / "tmp").mkdir()
        env = _environment(work)
        env["HTTK_DOCS_VERSION"] = tag
        python = str(work / ".venv/bin/python")

        def gate(name: str, command: list[str], cwd: Path = source) -> None:
            _run(command, cwd, env, work / f"{name}.log")
            passed.append(name)

        gate("create-environment", [sys.executable, "-I", "-m", "venv", str(work / ".venv")])
        gate("install-dev", [python, "-I", "-m", "pip", "install", "-e", ".[dev]"])
        # The Makefiles invoke this Python tool directly, rather than via -m.
        # Require the fresh environment's executable, not a user PATH fallback.
        gate("dev-cli", [str(work / ".venv/bin/pydoclint"), "--version"])
        gate("dev-dependencies", [python, "-I", "-m", "pip", "check"])
        gate("dev-environment", [python, "-I", "-m", "pip", "freeze"])
        candidate: dict[str, object] = {}
        if args.prepare:
            gate("docs-lock", ["make", "docs-lock"])
            gate("docs-inventories", ["make", "docs-inventories"])
            candidate = _manifest(source, sorted(set(original) | set(_release_inputs(source))))
            (work / "candidate.json").write_text(json.dumps(candidate, indent=2) + "\n")
            report["candidate_sha256"] = hashlib.sha256((work / "candidate.json").read_bytes()).hexdigest()
        if (source / "package-lock.json").exists():
            gate("npm", ["npm", "ci"])
        gate("ci", ["make", "ci"])
        gate("install-release", [python, "-I", "-m", "pip", "install", "-e", ".[dev,docs,release]"])
        gate("dependencies", [python, "-I", "-m", "pip", "check"])
        gate("environment", [python, "-I", "-m", "pip", "freeze"])
        gate("preflight", [python, "-I", "-m", "httk.core.docs", "check-release", "--tag", tag])
        gate("release-check", ["make", "release-check"])
        gate("docs-lock-check", ["make", "docs-lock-check"])
        wheels = list((source / "dist").glob("*.whl"))
        if len(wheels) != 1:
            raise ValueError(f"Expected one built wheel, found {len(wheels)}")
        report["artifacts_sha256"] = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((source / "dist").iterdir())
            if path.is_file()
        }
        wheel_env = work / "wheel-env"
        gate("create-wheel-environment", [python, "-I", "-m", "venv", str(wheel_env)])
        wheel_python = str(wheel_env / "bin/python")
        gate("install-wheel", [wheel_python, "-I", "-m", "pip", "install", str(wheels[0])], work)
        gate("wheel-dependencies", [wheel_python, "-I", "-m", "pip", "check"], work)
        config = tomllib.loads((source / "docs/versioning.toml").read_text())
        roots = config["site"]["import-roots"]
        if not roots:
            raise ValueError("docs/versioning.toml must declare public import-roots")
        gate(
            "wheel-imports",
            [
                wheel_python,
                "-I",
                "-c",
                (
                    "import importlib, importlib.metadata, sys; "
                    "assert importlib.metadata.version(sys.argv[1]) == sys.argv[2]; "
                    "[importlib.import_module(name) for name in sys.argv[3:]]"
                ),
                project["name"],
                project["version"],
                project["name"].replace("-", "."),
                *(root.replace("/", ".") for root in roots),
            ],
            work,
        )
        if args.prepare:
            _assert_source_unchanged(repo, source, candidate)
            _copy_inputs(repo, source, original, candidate)
        report["status"] = "passed"
        print(f"PASS: {tag}\nArtifacts: {source / 'dist'}\nReport: {work / 'report.json'}", flush=True)
        _next_steps(tag, prepared=args.prepare)
        return 0
    except (OSError, ValueError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        report["error"] = str(error)
        print(f"FAILED: {error}\nSee logs in {work}", file=sys.stderr)
        return 1
    finally:
        (work / "report.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
