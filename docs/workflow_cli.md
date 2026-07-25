# Project and workflow command line

Installing *httk-workflow* registers the lazy `workflow` command with
*httk-core*:

```console
httk workflow --help
```

The complete tree is:

```text
httk workflow store init|status|upgrade
httk workflow job submit|request
httk workflow manager run
httk workflow v1 prepare|submit|run
httk workflow config init|show|set|import-v1
httk workflow project init|import-v1|manifest create|manifest verify
httk workflow computer list|add|configure|install|import-v1
httk workflow tasks send|receive|start-manager|status
```

`httk-taskmanager` and `httk-v1-taskmanager` remain supported and dispatch the
same native and v1 command functions.

## Configuration and projects

User configuration follows the XDG base-directory convention:

- `$XDG_CONFIG_HOME/httk/config.json`;
- keys and global computers below `$XDG_DATA_HOME/httk/`.

`HTTK_CONFIG_HOME` and `HTTK_DATA_HOME` can provide explicit deployment or test
overrides. Legacy `~/.httk` data is read only through `config import-v1`; its
64-byte private material is not converted.

```console
httk workflow config init --name "A User" --email user@example.org
httk workflow project init . --name example --default-queue default
```

A project has `.httk-project/project.json`, a standard 32-byte Ed25519 seed
stored with mode `0600`, and a workflow store with
`detached-transfer-v1` enabled. Commands discover the nearest project in the
working directory's parent chain.

## Signed manifests

```console
httk workflow project manifest create
httk workflow project manifest verify
```

The v2 manifest is deterministic canonical JSON-lines compressed with bzip2.
It records sorted POSIX paths, regular-file sizes and SHA-256 hashes, empty
directories, and symlink targets. Special files are rejected. A
domain-separated body digest is signed with Ed25519. Creation fences manager
launches and refuses active work. Verification also recognizes the legacy
`ht.project/manifest.bz2` format without changing it.

## Computer adapters

Computer definitions are versioned directories containing `computer.json` and
executable `configure`, `install`, `invoke`, `push`, `pull`, `start-manager`,
and `status` operations. Each receives one versioned JSON request filename and
prints one JSON result; diagnostics belong on stderr. Commands and remote
commands are always argument arrays.

Maintained `local`, `local-slurm`, and `ssh-slurm` templates are packaged with
the module. Project definitions shadow global definitions. `NAME:QUEUE`
selects an explicit queue; otherwise the project default and then `default`
are tried. `computer import-v1` maps recognized legacy bundles by reading
assignment-only configuration; legacy shell executables are never copied or
run.

## Detached transfers

Stores can enable the implemented migration explicitly:

```console
httk workflow store upgrade STORE --extension detached-transfer-v1
```

A transfer fences an explicit quiescent marker, seals it in the payload,
validates the payload digest at import, publishes the preserved UUID and prior
state only at the destination, and retires the source only after an
idempotent acknowledgement. Transfer UUID and digest checks suppress retries;
sealed and retired bundles are retained for recovery. Repeating `tasks send`
resumes the matching sealed transfer, including the copy-before-import and
lost-acknowledgement boundaries.
