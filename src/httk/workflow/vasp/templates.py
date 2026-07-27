"""Registration of the packaged VASP templates with the generic scaffold.

Importing :mod:`httk.workflow.vasp` registers these four templates as an import
side effect, so ``httk workflow job new --template vasp-relax`` resolves a runner
the generic scaffold never names. The scaffold owns the registry and the
resolution; a domain owns only the description of its own starting points, which
is exactly what one :class:`~httk.workflow.scaffold.TemplateProvider` carries.

Declaring the steps here rather than running each packaged runner to ask keeps
scaffolding cheap; the VASP runner tests hold each declaration to what the runner
really describes.
"""

from httk.workflow.scaffold import TemplateProvider, register_template

#: The subpackage the four packaged VASP runners are modules of, which is also
#: the module their reserved ``pkg:`` form names them in.
RUNNER_PACKAGE = "httk.workflow.vasp.runners"

#: Every VASP template this distribution ships, in the order they are documented.
PROVIDERS = (
    TemplateProvider(
        name="vasp-relax",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax.py",
        workflow="httk.vasp.relax",
        initial_step="prepare",
        steps=("collect", "prepare", "run"),
        data_mode="transactional",
        summary="relax one structure with the reviewed remedy ladder",
    ),
    TemplateProvider(
        name="vasp-relax-bash",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax.sh",
        workflow="httk.vasp.relax",
        initial_step="prepare",
        steps=("collect", "prepare", "run"),
        data_mode="transactional",
        summary="the same relaxation, authored in Bash",
    ),
    TemplateProvider(
        name="vasp-static",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_static.py",
        workflow="httk.vasp.static",
        initial_step="prepare",
        steps=("collect", "prepare", "run"),
        data_mode="transactional",
        summary="one single-point calculation of one structure",
    ),
    TemplateProvider(
        name="vasp-relax-static",
        runner_package=RUNNER_PACKAGE,
        runner_file="vasp_relax_static.py",
        workflow="httk.vasp.relax-static",
        initial_step="prepare",
        steps=("collect", "prepare", "promote", "run", "static"),
        data_mode="transactional",
        summary="relax, promote the relaxed structure, then run it statically",
    ),
)


def register() -> None:
    """Register every packaged VASP template with the generic scaffold."""

    for provider in PROVIDERS:
        register_template(provider)


register()
