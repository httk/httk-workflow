"""Packaged VASP workflow runners, ready to run without being written first.

Every module beside this one is a complete native runner: one file that
implements one workflow through :class:`httk.workflow.Runner`, and depends on
nothing but an installed *httk-workflow*. A runner is therefore usable in three
ways without being copied into a payload:

* as an installed runner, with ``runner.source`` ``installed`` and
  ``runner.path`` ``pkg:httk.workflow.vasp.runners/vasp_relax.py`` — the manager
  resolves the reserved package form inside its own module allowlist, which
  contains ``httk.workflow`` by default;
* as a published workspace runner, through
  :meth:`httk.workflow.Workspace.publish_runner`, which pins the bytes by
  digest so a whole campaign shares one file;
* as a starting point to copy and edit, which is what a group whose practice
  differs from the packaged one should do.

:func:`httk.workflow.runners.runner_reference` builds the ``runner`` member of
one ``job.json`` for the first two, and resolves these files through
:data:`~httk.workflow.runners.RUNNERS` rather than by knowing where they live.

Available runners
-----------------

``vasp_relax.py``
    Prepare inputs, run VASP with the reviewed remedy ladder, collect the result.
``vasp_relax.sh``
    The same workflow authored in Bash, publishing the same outcomes.
``vasp_static.py``
    One single-point calculation of a structure, ionic relaxation switched off.
``vasp_relax_static.py``
    The chain: relax, promote CONTCAR, then a static calculation of the relaxed
    structure, in one job.

Job inputs
----------

Every packaged VASP runner reads the same ``inputs`` object, and every member is
optional. Paths are relative to the job payload; lists are space-separated
strings, so a Bash runner and a Python runner read one contract.

* ``poscar`` (default ``files/POSCAR``) — the starting structure; any VASP-5
  POSCAR or CONTCAR file will do.
* ``incar`` (default ``files/INCAR``) — the starting INCAR. When the file is
  absent the runner starts from an empty INCAR and derives everything.
* ``potcar`` (default ``files/POTCAR``) — a pre-assembled POTCAR. When the file
  is absent and ``pseudopotential_library`` is set, the POTCAR is assembled per
  species and a provenance record is written next to it.
* ``pseudopotential_library`` (default none) — root of a VASP pseudopotential
  library, one directory per variant.
* ``kpoint_density`` (default ``20.0``), ``centering`` (default
  ``Monkhorst-Pack``), ``accuracy_per_atom`` (default ``0.001``) — passed to
  :class:`httk.workflow.VaspPreparationOptions`.
* ``parallel_tag`` and ``parallel_value`` (default none) — one of ``NPAR``,
  ``NCORE``, or ``KPAR``, and its value.
* ``incar_tags`` (default empty) — explicit INCAR tags. They are applied before
  anything is derived and win over every derived value.
* ``static_incar_tags`` (default ``IBRION = -1`` and ``NSW = 0``) — only in the
  static and the relax-then-static runners: the tags that turn the calculation
  into a single point.
* ``timeout`` (default ``86400``) — seconds one VASP execution may take before its
  process group is terminated.
* ``maximum_remedies`` (default ``8``) — how many remedies this job may apply in
  total before it fails. The ladder is bounded per problem as well.
* ``remedy_policy`` (default ``reviewed-v1``) — the registered remedy policy the
  runner plans with, so a group with its own reviewed practice registers a policy
  with :func:`httk.workflow.vasp.register_remedy_policy` and names it here instead of
  editing a runner.
* ``rattle_amplitude`` (default ``0.0``) — when positive, the POSCAR is rattled by
  this amplitude after every applied remedy, with a seed derived from the attempt,
  so two retries never repeat one structure.
* ``collect`` (default ``INCAR KPOINTS OUTCAR CONTCAR OSZICAR vasprun.xml
  vasp-run-report.json POTCAR.provenance.json``) — space-separated file names
  published to the job's transactional data. Names that were never produced are
  skipped.
* ``data_prefix`` (default ``vasp``) — directory below the job's data the
  collected files are published under.
* ``vasp_command`` (default empty) — the VASP command as one argv string, split
  the way a shell splits it. The environment variable ``HTTK_VASP_COMMAND``
  overrides it, which is how a deployment — or a test — chooses the executable
  without touching any job.

A job running one of these runners needs ``workdir.mode`` ``persistent``: the
inputs a remedy rewrites have to be the inputs the next attempt reads. Publishing
collected files needs ``data.mode`` ``transactional``; with ``data.mode`` ``none``
the results simply stay in the workdir.
"""

#: The module the reserved ``pkg:`` runner form names for these runners.
PACKAGE = "httk.workflow.vasp.runners"

#: The runner files this subpackage ships, in the order they are documented.
RUNNERS = (
    "vasp_relax.py",
    "vasp_relax.sh",
    "vasp_relax_static.py",
    "vasp_static.py",
)
