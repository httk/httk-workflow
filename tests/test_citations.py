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

heading = "VASP workflow support builds on httk v1 contributions by Henrik Levämäki and Christopher Tholander"
assert heading not in credits.entries()
import httk.workflow.vasp
entries = credits.entries()
assert heading in entries
assert len(entries[heading]) == 2
""",
        ],
        check=True,
    )
