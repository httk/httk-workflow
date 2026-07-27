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

Update `project.version` in `pyproject.toml`. From a Python 3.12 environment,
install the development tools and run the complete local check:

```console
python -m pip install -e ".[dev,docs,release,cwl]"
make release-check
```

This runs formatting, static analysis, tests, strict documentation, an isolated
sdist/wheel build, and strict package-metadata checks. The resulting files are
written to `dist/`.

The `cwl` extra is optional for a release check: the CWL importer tests skip
themselves without a CWL parser, and every other test runs regardless. Install
it to exercise them.

Versions on package indexes are immutable. Use a new development or release
candidate version when repeating an upload, for example `2.0.0rc1` followed by
`2.0.0`.

## Data that must be in the wheel

This package is not Python modules alone. Several things it ships are *data*,
declared in `[tool.setuptools.package-data]`, and a wheel that silently lost one
of them installs and imports cleanly while failing at run time:

- `shell/*.sh` — the native Bash authoring libraries, whose absolute paths the
  manager exports to every Bash runner;
- `adapter_templates/*/…` — the `remote.json` of each maintained remote
  adapter **and its seven executable operations**, which must arrive executable
  or `httk workflow remote add` refuses the bundle it just copied;
- `v1_runtime/…` — the *httk* v1 task templates and compatibility shell;
- `vasp/runners/*.sh` — the packaged Bash VASP runner beside its Python
  siblings. The Python runners and everything under `integrations/` are modules
  of their packages and need no entry.

After a build, confirm they are there rather than assuming:

```console
python -m zipfile -l dist/httk_workflow-*.whl | grep -E 'shell/|adapter_templates/|vasp/runners/.*\.sh|v1_runtime/'
```

A fresh install is the stronger check, because it also proves the executable bit
survived:

```console
/tmp/httk-workflow-test/bin/httk workflow remote add probe --template local --global
```

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
  --index-url https://test.pypi.org/simple/ --no-deps httk-workflow==0.1.0
/tmp/httk-workflow-test/bin/python -c "import httk.workflow"
```

Replace `0.1.0` with the version being tested. `--no-deps` keeps the isolation
test focused on the wheel itself; installing without it would also pull
`httk-core` (this module's runtime dependency) from the index.

## PyPI

1. Confirm that `make release-check` succeeds on the exact commit to release.
2. Push the commit and create a GitHub release whose tag is `v` followed by the
   package version, for example `v0.1.0`.
3. Publish the GitHub release and approve the protected `pypi` environment.
4. Verify the release from a fresh environment with `pip install httk-workflow`.

The workflow rejects a Git tag that does not match `project.version`, rebuilds
the distributions from the tagged source, checks them, and publishes them via
PyPI Trusted Publishing.
