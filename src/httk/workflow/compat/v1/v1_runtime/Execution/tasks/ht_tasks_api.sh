#!/bin/bash

# Thin source-compatible redirect for legacy ht_steps.
# The implementation and its attribution live outside this historic pathname.
_HTTK_V1_TASKS_SOURCE="${BASH_SOURCE[0]:-$0}"
_HTTK_V1_RUNTIME_ROOT=$(cd "$(dirname "$_HTTK_V1_TASKS_SOURCE")/../.." && pwd -P)
. "$_HTTK_V1_RUNTIME_ROOT/compat/ht_tasks_api_v1.sh"
unset _HTTK_V1_TASKS_SOURCE _HTTK_V1_RUNTIME_ROOT
