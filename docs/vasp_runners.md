# VASP workflows

The ready-made VASP workflows live in the
[workflows-vasp](https://github.com/httk/workflows-vasp) repository, one
workflow package per subdirectory. *httk-workflow* does not bundle them, nor the
VASP helper API they are built on: that lives in the separate
*httk-workflow-vasp* distribution (`pip install httk-workflow-vasp`), which
provides `httk.codes.vasp` and the Bash VASP API. See {doc}`code_support` for
how a code-support distribution plugs into *httk-workflow*.

| URI | Short name | What it does |
| --- | --- | --- |
| `git+https://github.com/httk/workflows-vasp#vasp-relax` | `vasp.relax` | relax a structure with the reviewed remedy ladder |
| `git+https://github.com/httk/workflows-vasp#vasp-relax-bash` | `vasp.relax-bash` | the same relaxation, authored in Bash |
| `git+https://github.com/httk/workflows-vasp#vasp-static` | `vasp.static` | one single-point calculation of a fixed structure |
| `git+https://github.com/httk/workflows-vasp#vasp-relax-static` | `vasp.relax-static` | relax, then evaluate the relaxed structure statically |

Each takes one required input, `structure`, staged as `files/POSCAR`. Their job
parameters, failure codes, postprocess scripts (`relaxation-report`,
`relaxation-plot`) and result layout are documented in that repository.

## Install, run, uninstall

Referencing a URI fetches and installs the workflow; the job records its
canonical URI, pinned to the full commit (append `@<ref>` before `#` to choose a
branch, tag or commit). `httk workflow install` does the same without creating a
job. Once installed, the short name selects it:

```console
httk workflow install 'git+https://github.com/httk/workflows-vasp#vasp-relax'
httk workspace settings set --key vasp.command --value "srun -n 32 vasp_std" default
httk job new --workflow vasp.relax --input structure=POSCAR --tag silicon
httk workflow run
httk collect
httk workflow uninstall vasp.relax
```

`httk plugin install git+https://github.com/httk/workflows-vasp` installs all
four at once as a plugin instead. See {doc}`details/workflow_uris` for URI resolution,
short names and the trust model.

The VASP command is the `vasp.command` application setting, resolved most
specific first: a job's own `vasp.command` parameter, then `HTTK_VASP_COMMAND`
in the environment, then the workspace setting. The pseudopotential library
resolves the same way as `vasp.pseudo_library` (`HTTK_VASP_PSEUDO_LIBRARY`).
See {doc}`sdks/sdk_parity` for the resolution table. The workflows default to
`data.mode="none"`: the persistent `run/` workdir is the result; pass
`--data-mode transactional` to `job new` to also publish a curated copy into
`data/`.

## Writing a VASP runner of your own

The workflows-vasp runners (`vasp-relax/run`, `vasp-static/run`,
`vasp-relax-static/run`) are ordinary runners ({doc}`runtime_helpers`) whose
steps call the `httk.codes.vasp` helpers of *httk-workflow-vasp*: `prepare`
builds the inputs with `prepare_vasp_inputs`, `run` supervises VASP with
`run_vasp` and, on a known failure, applies a remedy from `plan_vasp_remedy`
and retries, and `publish` records the results. A Bash runner uses the same
functions through the VASP Bash API, sourced as `$HTTK_WORKFLOW_VASP_BASH_API`
after the generic `$HTTK_WORKFLOW_BASH_API`. A group whose practice differs
copies a workflow package from the repository and edits it, or keeps the
workflows and registers its own remedy policy. See {doc}`code_support` for the
helper library and the Bash API.
