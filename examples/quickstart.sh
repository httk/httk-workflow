#!/usr/bin/env bash
# The commands of docs/quickstart.md, runnable (the one-time identity setup is
# deliberately left out; see below).
#
# Run it in an empty directory:
#
#     examples/quickstart.sh
#
# It creates POSCAR and httk_project/ in the working directory and drives one
# packaged VASP relaxation to completion. Without VASP installed the mock one
# beside this file stands in for it; set vasp.command to use the real thing.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# These helpers use the installed command when available and the identical
# module form otherwise.
httk_workflow() {
    if command -v httk >/dev/null 2>&1; then
        httk workflow "$@"
    else
        python3 -m httk.core.cli workflow "$@"
    fi
}

httk_cmd() {
    if command -v httk >/dev/null 2>&1; then
        httk "$@"
    else
        python3 -m httk.core.cli "$@"
    fi
}

httk_project() {
    if command -v httk >/dev/null 2>&1; then
        httk project "$@"
    else
        python3 -m httk.core.cli project "$@"
    fi
}

# The structure to relax: any VASP-5 POSCAR will do.
cat >POSCAR <<'END'
silicon
1.0
2.0 0.0 0.0
0.0 2.0 0.0
0.0 0.0 2.0
Si
2
Direct
0.0000000000 0.0000000000 0.0000000000
0.5000000000 0.5000000000 0.5000000000
END

# One-time per-user setup is `httk init --name "Your Name" --email you@example.org`;
# deliberately not run here so this example leaves your per-user configuration untouched.

# 1. The project anchor and an explicit workspace at its root.
httk_project init --name quickstart .
httk_cmd workspace init --name default .

# 2. One job of the packaged relaxation runner, starting from that structure. The
#    command prints one tab-separated line with its key and payload.
httk_cmd job new \
    --workflow vasp-relax \
    --input structure=POSCAR \
    --tag silicon

# 3. Workspace state follows the job. Without VASP, the mock beside this file
#    writes plausible outputs.
httk_cmd workspace settings set --key vasp.command --value "$here/mock_vasp.py" default

# 4. Run every ready job until nothing is left to do.
httk_workflow run

# 5. What happened, and store entries, runs, and products when httk-store is installed.
if python3 -c 'import httk.store.backend.sql' >/dev/null 2>&1; then
    httk_workflow collect --into results.sqlite --id-base httk.quickstart
else
    echo 'httk-store is not installed; skipping results.sqlite storage' >&2
    httk_workflow collect
fi

# 6. Make a plot from the published OUTCAR.
httk_workflow postprocess --script relaxation-plot

printf '\nthe published result is in jobs/*/data/vasp/\n'
