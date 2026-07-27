"""Run the canonical workflow command when the package is executed directly."""

import sys
from pathlib import Path

from httk.core import CLIContext

from . import command

if __name__ == "__main__":
    raise SystemExit(command(sys.argv[1:], CLIContext("httk", Path.cwd())))
