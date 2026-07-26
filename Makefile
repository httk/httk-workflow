PYTHON ?= python3
DIST_DIR ?= dist

# black's default process pool can hang in constrained sandboxes; keep it
# single-worker everywhere (equivalent to passing --workers 1).
export BLACK_NUM_WORKERS = 1

# Base URL of the published httk documentation site, used for cross-linking docs
# between httk repositories (read by docs/conf.py via HTTK_DOCS_BASE_URL).
DOCS_BASE_URL ?= https://docs.httk.org

.PHONY: docs docs-live docs-clean docs-inventories clean dist-clean dist dist-check release-check format format-check typecheck typecheck_pyright lint test test_fastfail audit

docs: docs-clean
	HTTK_DOCS_BASE_URL=$(DOCS_BASE_URL) $(PYTHON) -m sphinx -E -a -b html -W --keep-going docs docs/_build/html

docs-live:
	HTTK_DOCS_BASE_URL=$(DOCS_BASE_URL) sphinx-autobuild docs docs/_build/html

docs-clean:
	rm -rf docs/_build docs/reference/autoapi

# Refresh the committed intersphinx inventories (the one docs task that uses the
# network); docs builds themselves resolve against these vendored files offline.
docs-inventories:
	curl -fsSL https://docs.python.org/3/objects.inv -o docs/_inventories/python.inv
	curl -fsSL $(DOCS_BASE_URL)/httk-core/objects.inv -o docs/_inventories/httk-core.inv

dist-clean:
	rm -rf build $(DIST_DIR) src/httk_workflow.egg-info

clean: docs-clean dist-clean
	find . -name "*.pyc" -print0 | xargs -0 rm -f
	find . -name "*~" -print0 | xargs -0 rm -f
	find . -name "__pycache__" -print0 | xargs -0 rm -rf

# black and isort walk the same three roots recursively, so a new subpackage
# (runners/, integrations/, ...) is covered the day it is created rather than
# the day someone remembers to extend a glob.
format:
	$(PYTHON) -m ruff check src examples tests --select F401 --fix
	$(PYTHON) -m isort src examples tests
	$(PYTHON) -m black --workers 1 src examples tests

format-check: lint
	$(PYTHON) -m isort --check-only src examples tests
	$(PYTHON) -m black --workers 1 --check src examples tests

lint:
	$(PYTHON) -m ruff check src examples tests

typecheck_pyright:
	$(PYTHON) -m pyright

typecheck:
	$(PYTHON) -m mypy

test:
	PYTHONPATH=src $(PYTHON) -m pytest

test_fastfail:
	PYTHONPATH=src $(PYTHON) -m pytest -q -x

check: format-check typecheck typecheck_pyright test

ci: format-check typecheck typecheck_pyright test_fastfail

dist: dist-clean
	$(PYTHON) -m build --outdir $(DIST_DIR)

dist-check: dist
	$(PYTHON) -m twine check --strict $(DIST_DIR)/*

release-check: ci docs dist-check
