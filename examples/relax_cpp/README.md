# A VASP relaxation runner in C++17

`relax.cpp` has the three-step `prepare`, `run`, and `publish` shape and uses
the native C++17 SDK. The package vendors the C++ header and C pair under
`cpp/` and `c/`, so its build has no reach-out to the source checkout.

Build and register the compiled runner once per platform class, then publish a
job and start a manager:

```console
httk workflow build --workspace WORKSPACE ./relax_cpp
httk workflow job new --workspace WORKSPACE --workflow-dir ./relax_cpp --step prepare \
    --file POSCAR=POSCAR --data-mode transactional --tag silicon
httk workflow run --workspace WORKSPACE
httk workflow collect --workspace WORKSPACE
```

The manifest's `platform = "uname -sm"` records which platform class produced
the binary. On a shared filesystem, one registration for that tag serves every
matching node; on a heterogeneous cluster, build once for each tag.

`make` and `./relax --describe` remain available for direct SDK exploration:

```console
make
./relax --describe
```

Build registration stores `relax` and its object files in the workspace's
machine-local cache. Publication transfers sources only, so each platform
build is reproducible from the self-contained package.
