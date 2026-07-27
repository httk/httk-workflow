#!/usr/bin/env bash
# The five commands of docs/quickstart.md, runnable.
#
# Run it in an empty directory:
#
#     examples/quickstart.sh
#
# It creates POSCAR and quickstart-workspace/ in the working directory and drives
# one packaged VASP relaxation to completion. Without VASP installed the mock one
# beside this file stands in for it; set HTTK_VASP_COMMAND yourself to use the real
# thing.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# "httk workflow ..." is the canonical command for everything below. When its
# console script is not on PATH, this is the identical module form.
httk_workflow() {
    if command -v httk >/dev/null 2>&1; then
        httk workflow "$@"
    else
        python -m httk.core.cli workflow "$@"
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

# 1. A workspace whose jobs may publish their results as transactional data,
#    created here on this machine and registered under a name every later command
#    addresses it by. Being local is never implied, so --remote local is explicit.
httk_workflow workspace init quickstart-workspace \
    --remote local --path quickstart-workspace \
    --extension transactional-data-v1

# 2. One job of the packaged relaxation runner, starting from that structure. The
#    command prints one tab-separated line per job: its key and its payload.
job=$(httk_workflow job new quickstart-workspace \
    --template vasp-relax \
    --from POSCAR \
    --tag silicon | cut -f1)
printf 'submitted %s\n' "$job"

# 3. How VASP is invoked on this machine, which is deployment state and not part of
#    any job. Without VASP, the mock beside this file writes plausible outputs.
: "${HTTK_VASP_COMMAND:=$here/mock_vasp.py}"
export HTTK_VASP_COMMAND

# 4. Run every ready job in the foreground until nothing is left to do.
httk_workflow manager run quickstart-workspace --until-idle

# 5. What happened, and what it produced.
httk_workflow job list quickstart-workspace
httk_workflow job show quickstart-workspace "$job"
httk_workflow harvest quickstart-workspace

printf '\nthe published result is in quickstart-workspace/jobs/%s/data/vasp/\n' "$job"
