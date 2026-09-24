"""A test-only runner package standing in for a domain's packaged runners.

``relax.py`` beside this file is the packaged runner of the ``tests.relax``
workflow that the ``relax_workflow`` fixture in ``conftest.py`` registers. It is
importable because ``tests/`` is on ``sys.path`` during the suite, which is also
what lets a ``pkg:workflow_fixtures/relax.py`` reference resolve.
"""
