#!/usr/bin/env python3
"""A mock VASP, so the examples run on a machine that has no VASP.

**This is not VASP and computes nothing.** It reads the POSCAR in its working
directory and writes the files a finished VASP run leaves behind — OUTCAR,
OSZICAR, CONTCAR, and ``vasprun.xml`` — with numbers that are plausible enough for
the parsers of the packaged runners and meaningless as physics. The "relaxed"
structure is the input with its last atom nudged.

Use it exactly the way a deployment names the real thing:

.. code-block:: console

    export HTTK_VASP_COMMAND="$PWD/examples/mock_vasp.py"
    httk workflow run workflow-workspace

``HTTK_MOCK_VASP_FAIL_ONCE=1`` makes the first run in a directory fail with a
diagnosable ``ZPOTRF`` error instead, which is what makes the runners' remedy
ladder visible: the job applies one reviewed remedy and the rerun succeeds.
"""

import os
from pathlib import Path

_OUTCAR = (
    " fake vasp 6.4.1\n"
    "   NELM   =     60;   NELMIN=  2; NELMDL= -5\n"
    "   NSW    =     99    number of steps for IOM\n"
    "   maximum number of plane-waves:    1234\n"
    " General timing and accounting information for this job:\n"
)
_OSZICAR = (
    "       N       E                     dE             d eps       ncg     rms\n"
    "DAV:   1    -0.100000000000E+02   -0.10000E+02   -0.30000E+01   128   0.500E+01\n"
    "DAV:   2    -0.105000000000E+02   -0.50000E+00   -0.10000E+00   128   0.100E+00\n"
    "   1 F= -.10500000E+02 E0= -.10500000E+02  d E =-.105000E+02\n"
)
_VASPRUN = (
    '<modeling><structure name="finalpos"><crystal>'
    '<i name="volume">      8.00000000 </i></crystal></structure></modeling>\n'
)


def main() -> int:
    """Write one mock VASP result, or fail once when asked to."""

    attempts = Path("mock-vasp-attempts")
    previous = int(attempts.read_text(encoding="utf-8")) if attempts.is_file() else 0
    attempts.write_text(str(previous + 1), encoding="utf-8")
    structure = Path("POSCAR").read_text(encoding="utf-8").splitlines()
    if os.environ.get("HTTK_MOCK_VASP_FAIL_ONCE") == "1" and previous == 0:
        # A recognized failure: the runners diagnose this line, apply one reviewed
        # remedy to the inputs, and ask for another attempt.
        print("LAPACK: Routine ZPOTRF failed!")
        Path("OUTCAR").write_text(" fake vasp 6.4.1\n   NELM   =     60\n   NSW    =     99\n", encoding="utf-8")
        Path("OSZICAR").write_text("DAV:   1    -0.100000000000E+02\n", encoding="utf-8")
        return 1
    Path("OUTCAR").write_text(_OUTCAR, encoding="utf-8")
    Path("OSZICAR").write_text(_OSZICAR, encoding="utf-8")
    relaxed = list(structure)
    relaxed[0] = "relaxed by the mock vasp"
    relaxed[-1] = "0.5100000000 0.5100000000 0.5100000000"
    Path("CONTCAR").write_text("\n".join(relaxed) + "\n", encoding="utf-8")
    Path("vasprun.xml").write_text(_VASPRUN, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
