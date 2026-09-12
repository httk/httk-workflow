# Releasing `httk-workflow`

Releases are built and published by GitHub Actions. PyPI authentication uses
Trusted Publishing, so the repository does not need a stored PyPI API token.

## One-time setup

1. Create accounts on [PyPI](https://pypi.org) and
   [TestPyPI](https://test.pypi.org), and enable two-factor authentication.
2. In the GitHub repository settings, create environments named `pypi` and
   `testpypi`. Configure a required reviewer for `pypi` (and optionally for
   `testpypi`); restricting the `pypi` environment to tags matching `v*` is
   also recommended.
3. On PyPI, add a pending GitHub Trusted Publisher with these values:

   - PyPI project name: `httk-workflow`
   - Owner: `httk`
   - Repository: `httk-workflow`
   - Workflow: `release.yml`
   - Environment: `pypi`

4. Add the corresponding pending publisher on TestPyPI, using the environment
   `testpypi` instead.

A pending publisher creates the project during its first upload. It does not
reserve the project name before then.

## Prepare and check a release

Check that `project.version` in `pyproject.toml` is the intended release version,
then run from this checkout with Python 3.12+, Git, make, curl, uv, and network
access:

```console
make release-prepare VERSION=v2.1.0
```

`VERSION` includes the leading `v` and must identify the same version as
`project.version` before preparation starts. Internal
`httk-*` dependencies must already be published on PyPI at versions satisfying
this project's dependency requirements, with their matching release docs
published as well. The checker is included in `tools/`; it needs no sibling
checkout or newly published core tooling.

The command snapshots your current working tree, including uncommitted release
edits, and verifies it in fresh environments. It installs the declared
development dependencies from PyPI, regenerates the documentation lock and
published inventories, and runs CI, the release checks, a clean locked-docs
build, and a separate bare-wheel installation. Formatting, static analysis,
tests, strict documentation, distribution builds, and package metadata checks
must all pass.

On success, the refreshed lock and inventories are copied back only if your
working tree has remained unchanged during verification. Review those files
and commit the prepared release. If you make further release edits, run
preparation again. Logs, environments, built distributions, and the report
remain in the printed verification directory. See [tools/README.md](tools/README.md)
for prerequisites and the checks' scope.

To verify an already committed, clean checkout again, use the local checker:

```console
python tools/check_release.py . --tag v2.1.0
```

`make release-check` remains available for checks in the current environment
and prints the next release steps after success. A shared workspace environment
can hide undeclared dependencies; use `release-prepare` for release preparation.

Versions on package indexes are immutable. Use a new development or release
candidate version when repeating an upload, for example `2.1.0rc1` followed by
`2.1.0`.

## TestPyPI

Run the **Publish package** workflow manually in GitHub Actions. A manual run
publishes to TestPyPI only. To retry a TestPyPI upload without committing a version bump, pass the
optional `version_suffix` workflow input (e.g. `.post1` or `rc2`); it is
appended to `project.version` for that build only.
When the workflow run has completed (approving the
`testpypi` environment first, if it has a required reviewer), test the artifact
in a fresh environment:

```console
python -m venv /tmp/httk-workflow-test
/tmp/httk-workflow-test/bin/python -m pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ httk-workflow==2.1.0
/tmp/httk-workflow-test/bin/python -c "import httk.workflow"
```

Replace `2.1.0` with the version being tested. Unlike `httk-core`, `httk-workflow`
has a runtime dependency (`httk-core`), so `--no-deps` is not appropriate here:
`import httk.workflow` pulls in `httk.core` at import time. The
`--extra-index-url` lets pip resolve that dependency (once it is published to the
real PyPI) while the package under test comes from TestPyPI.

## PyPI

1. Complete `make release-prepare VERSION=v2.1.0`, review the refreshed inputs,
   and commit the prepared release.
2. Push the commit and create a GitHub release whose tag is `v` followed by the
   package version, for example `v2.1.0`.
3. Publish the GitHub release and approve the protected `pypi` environment.
4. Verify the release from a fresh environment with `pip install httk-workflow`.

The workflow rejects a Git tag that does not match `project.version`, rebuilds
the distributions from the tagged source, checks them, and publishes them via
PyPI Trusted Publishing.
