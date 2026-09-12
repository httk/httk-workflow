# Prepare a release

From the module repository, run:

```console
make release-prepare VERSION=v2.1.0
```

Use `v` followed by the version already declared in `pyproject.toml`. The
command requires `VERSION` and rejects a mismatch before installing dependencies,
refreshing files, or running checks. It never changes the project version.
The module template itself currently declares `0.1.0`, so its matching example
is `make release-prepare VERSION=v0.1.0`.

Requirements: Python 3.12+, Git, make, curl, uv, network access to PyPI and
published dependency docs, and Node/npm when the repository has a package lock.
No sibling checkout or new core publication is needed: the standalone helper
is included in `tools/check_release.py`. The template supplies the same helper
to new modules; maintain identical copies in the existing runtime modules.

Preparation accepts intended uncommitted changes. It snapshots tracked files
and nonignored untracked additions, including deletions, executable modes and
symlinks. Ignored builds, virtual environments, and sibling sources stay out.
Gitlinks/submodules are rejected: this target is for individual distributions.

The following gates run in the isolated snapshot and stop on failure:

1. Install only `.[dev]` into a fresh venv from PyPI, validate dependencies, and
   require the fresh environment's Python CLI tools.
2. Run `make docs-lock` and `make docs-inventories`, refreshing the published
   dependency inputs, then install JavaScript dependencies with `npm ci` if
   applicable and run `make ci`.
3. Add `.[dev,docs,release]`, validate dependencies and the release preflight,
   then run `HTTK_DOCS_VERSION=<VERSION> make release-check`. CI runs again
   because docs dependencies can change what is installed.
4. Run `make docs-lock-check`, including its separate locked-docs environment.
5. Install the built wheel without extras into another fresh venv, check its
   dependencies and version, and import its primary package and the public
   roots from `docs/versioning.toml`, outside the source tree.

Only after all gates pass are the refreshed documentation lock and inventory
files copied back. Preparation refuses copyback if working files changed during
verification or if a gate changed the candidate's source. Tests remain strict;
workspace packages cannot supply missing Python requirements.

The final printed handoff is:

1. Review and commit exactly the verified files, including regenerated inputs.
2. Sign the final commit and create its signed `v<version>` tag.
3. Push the final commit and tag.
4. Create and publish the matching GitHub release.

Further source changes require another preparation run. The target never
stages, commits, signs, tags, pushes, or publishes for you. `release-check`
also prints the handoff, with a reminder that complete pre-commit validation
uses `release-prepare`.

The printed temporary directory retains complete logs, dependency versions,
environments, `source/dist/` artifacts and `report.json`. Preparation also
writes `candidate.json`, the verified file-content/mode/symlink manifest; its
hash is recorded alongside artifact hashes and the starting commit in the
report. Remove the directory yourself when no longer needed.

For a chosen new output directory, invoke the helper directly:

```console
python tools/check_release.py . --prepare --tag v2.1.0 --output-dir /tmp/my-release-check
```

The older clean-commit check remains available with
`python tools/check_release.py . --tag v2.1.0`. It requires a clean HEAD and
verifies existing inputs without refreshing them or copying anything back.

This verifies dependency/source isolation and the configured CI/release gates;
it is not a reproduction of GitHub's OS image. Browser and live-database
acceptance remain separate when required, and optional skips stay visible in
the logs. A pass applies to the recorded candidate and dependency versions.
