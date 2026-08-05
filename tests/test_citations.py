import subprocess
import sys


def test_vasp_credit_is_registered_when_vasp_is_imported() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
from httk.core import credits
import httk.workflow

headings = (
    "VASP workflow automation and results handling build on httk v1 contributions by Henrik Levämäki",
    "VASP relaxation workflow templates and task scheduling build on httk v1 contributions by Christopher Tholander",
)
assert all(heading not in credits.entries() for heading in headings)
import httk.workflow.vasp
entries = credits.entries()
assert all(heading in entries for heading in headings)
assert all(len(entries[heading]) == 1 for heading in headings)
""",
        ],
        check=True,
    )
