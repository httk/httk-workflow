"""Consumers that turn work authored elsewhere into ordinary *httk₂* jobs.

Each subpackage is one *consumer* of the common execution API — the ``Attempt``
layer of :mod:`httk.workflow.sdk` and the canonical
:mod:`httk.workflow.protocol` — and nothing more. A consumer may use that API;
the API never learns which consumer uses it. The generic manager, workspace,
and CLI see only ordinary jobs, steps, children, and outcomes: no subpackage
here teaches them a compatibility format.

``v1``
    httk *v1* task templates, run unchanged on the v2 engine
    (:mod:`httk.workflow.compat.v1`).
``cwl``
    the Common Workflow Language, imported as jobs and executed natively —
    never by cwltool (:mod:`httk.workflow.compat.cwl`).
``pwd``
    the Python Workflow Definition exchange format, imported as jobs
    (:mod:`httk.workflow.compat.pwd`).

None of these subpackages imports another, none imports the manager or the
generic CLI internals, and none is imported by the common execution layer.
"""
