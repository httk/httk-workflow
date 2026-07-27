#!/bin/bash

# Thin source-compatible redirect for legacy ht_steps.
# The implementation and its attribution live outside this historic pathname.
_HTTK_V1_VASP_SOURCE="${BASH_SOURCE[0]:-$0}"
_HTTK_V1_RUNTIME_ROOT=$(cd "$(dirname "$_HTTK_V1_VASP_SOURCE")/../../.." && pwd -P)
. "$_HTTK_V1_RUNTIME_ROOT/compat/vasptools_v1.sh"
unset _HTTK_V1_VASP_SOURCE _HTTK_V1_RUNTIME_ROOT
