"""PMI-1 relay between Slurm's step socket and a confined rank that refuses ``MPI_Comm_spawn`` locally.

With ``srun --mpi=pmi2`` Slurm's ``mpi/pmi2`` plugin hands each task a Unix stream socket as ``PMI_FD``, on
which Intel MPI speaks the PMI-1 simple protocol: newline-terminated ``cmd=<name> key=value ...`` lines,
starting with exactly :data:`INIT_LINE`, each answered by one reply line except ``abort``. Slurm reads at
most 1024 bytes per request and treats them as one command, so the relay forwards one complete line at a
time and only when no reply is outstanding. A spawn arrives as ``mcmd=`` blocks ending in ``endcmd`` (or as
a single ``cmd=mcmd`` line) and would make ``slurmstepd`` start processes outside the sandbox: blocks are
answered locally with :data:`SPAWN_REFUSAL`. Any other init line (a PMI-2 client included), a command
outside :data:`ALLOWED_COMMANDS` and any forwarded line containing ``mcmd`` end the relay.
"""

import os
import re
import selectors
import stat
from collections.abc import Callable

#: Slurm reads one PMI-1 request per read of this many bytes; a longer line would be split into two commands.
MAX_LINE_BYTES = 1024
#: The only accepted first line: Slurm reads it in one 64-byte read and accepts only PMI 1.1.
INIT_LINE = b"cmd=init pmi_version=1 pmi_subversion=1\n"
#: The commands after :data:`INIT_LINE`: Slurm's post-init PMI-1 table without ``mcmd`` (spawn) and without
#: ``create_kvs``, ``destroy_kvs`` and ``getbyidx``, which Slurm never answers.
ALLOWED_COMMANDS: frozenset[str] = frozenset(
    {
        "get_maxes",
        "get_universe_size",
        "get_appnum",
        "barrier_in",
        "finalize",
        "abort",
        "get_my_kvsname",
        "put",
        "get",
        "publish_name",
        "unpublish_name",
        "lookup_name",
    }
)
#: The reply that refuses one spawn request.
SPAWN_REFUSAL = b"cmd=spawn_result rc=-1\n"
#: The largest spawn block in bytes; a longer block ends the relay.
MAX_SPAWN_BLOCK_BYTES = 65536

_SPAWN_COUNTER = re.compile(rb"(totspawns|spawnssofar)=([0-9]+)")
_READ_BYTES = 65536


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(descriptor, view) :]


def relay(server: int, rank: int, *, log: Callable[[str], None]) -> None:
    """Relay PMI-1 between Slurm's step socket *server* and the rank's socket *rank* until either side closes.

    Closes both descriptors before returning. End of file or an I/O error on either side ends the relay
    quietly; a refused request ends it after a line passed to *log*. Spawn requests are answered with
    :data:`SPAWN_REFUSAL` without reaching *server*, and each refusal is passed to *log*.

    :param server: The descriptor of the socket Slurm passed as ``PMI_FD``.
    :param rank: The descriptor of the socket the sandboxed rank uses as its ``PMI_FD``.
    :param log: Receives one line per refusal.
    :raises ValueError: If either descriptor is not an open socket; nothing is closed then.
    """

    for descriptor in (server, rank):
        try:
            mode = os.fstat(descriptor).st_mode
        except OSError:
            mode = 0
        if not stat.S_ISSOCK(mode):
            raise ValueError(f"PMI relay descriptor {descriptor} is not an open socket")
    try:
        _relay(server, rank, log)
    except OSError:  # BrokenPipeError and ConnectionResetError included: the peer is gone.
        pass
    finally:
        os.close(server)
        os.close(rank)


def _relay(server: int, rank: int, log: Callable[[str], None]) -> None:
    os.set_blocking(server, True)
    os.set_blocking(rank, True)
    pending = bytearray()  # rank bytes not yet consumed
    initialized = outstanding = draining = False
    spawn: int | None = None  # the size of the swallowed spawn block, while one is open
    counters: dict[bytes, int] = {}
    with selectors.DefaultSelector() as selector:
        selector.register(server, selectors.EVENT_READ)
        selector.register(rank, selectors.EVENT_READ)
        reading_rank = True
        while True:
            while not outstanding and not draining and (end := pending.find(b"\n")) >= 0:
                if end + 1 > MAX_LINE_BYTES:
                    log(f"refused: PMI line exceeds {MAX_LINE_BYTES} bytes")
                    return
                line = bytes(pending[: end + 1])
                del pending[: end + 1]
                body = line[:-1]
                if not initialized:
                    if line != INIT_LINE:
                        log(f"refused: PMI init {line[:80]!r}")
                        return
                    initialized = True
                elif spawn is not None or body.startswith(b"mcmd="):
                    spawn = (spawn or 0) + len(line)
                    if spawn > MAX_SPAWN_BLOCK_BYTES:
                        log(f"refused: PMI spawn block exceeds {MAX_SPAWN_BLOCK_BYTES} bytes")
                        return
                    if counter := _SPAWN_COUNTER.fullmatch(body):
                        counters[counter[1]] = int(counter[2])
                    if body != b"endcmd":
                        continue
                    total, so_far = counters.get(b"totspawns"), counters.get(b"spawnssofar")
                    spawn = None
                    counters.clear()
                    if total is None or so_far is None or so_far >= total:
                        log("refused MPI_Comm_spawn")
                        _write_all(rank, SPAWN_REFUSAL)
                    continue
                else:
                    name = body[4:].split(b" ", 1)[0] if body.startswith(b"cmd=") else body[:80]
                    if not body.startswith(b"cmd=") or name.decode("ascii", "replace") not in ALLOWED_COMMANDS:
                        log(f"refused: PMI command {name!r}")
                        return
                    if b"mcmd" in line:  # never rely on Slurm reading a forwarded write in one piece
                        log("refused: PMI command containing mcmd")
                        return
                    draining = name == b"abort"
                _write_all(server, line)
                outstanding = not draining
            if not outstanding and not draining:
                if len(pending) >= MAX_LINE_BYTES:
                    log(f"refused: PMI line exceeds {MAX_LINE_BYTES} bytes")
                    return
                if not initialized and not INIT_LINE.startswith(pending):
                    log(f"refused: PMI init {bytes(pending[:80])!r}")  # e.g. a PMI-2 length prefix
                    return
            if reading_rank != (not outstanding and not draining):
                reading_rank = not reading_rank
                if reading_rank:
                    selector.register(rank, selectors.EVENT_READ)
                else:
                    selector.unregister(rank)
            for key, _ in selector.select():
                data = os.read(key.fd, _READ_BYTES)
                if not data:
                    return
                if key.fd == rank:
                    pending += data
                    continue
                _write_all(rank, data)
                if b"\n" in data:
                    outstanding = False
