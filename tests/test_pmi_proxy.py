"""The PMI-1 spawn-refusing relay of confined ranks, against a fake ``slurmstepd`` on socket pairs."""

import os
import select
import socket
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from httk.workflow._pmi_proxy import MAX_SPAWN_BLOCK_BYTES, SPAWN_REFUSAL, relay

INIT = b"cmd=init pmi_version=1 pmi_subversion=1\n"
REPLIES = {
    b"init": b"cmd=response_to_init rc=0 pmi_version=1 pmi_subversion=1\n",
    b"barrier_in": b"cmd=barrier_out rc=0\n",
    b"put": b"cmd=put_result rc=0\n",
    b"get": b"cmd=get_result rc=0 msg=success value=v\n",
    b"finalize": b"cmd=finalize_ack rc=0\n",
}
SPAWN = b"mcmd=spawn\nnprocs=1\nexecname=/bin/true\ntotspawns=%d\nspawnssofar=%d\nendcmd\n"


@dataclass
class Session:
    """A relay thread between a fake Slurm socket and a fake rank socket."""

    slurm: socket.socket
    task: socket.socket
    relay_thread: threading.Thread
    server_thread: threading.Thread | None
    received: list[bytes] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)

    def ask(self, line: bytes) -> bytes:
        self.task.sendall(line)
        return self.reply()

    def reply(self) -> bytes:
        data = b""
        while not data.endswith(b"\n"):
            chunk = self.task.recv(1)  # one byte at a time: replies may arrive coalesced
            if not chunk:
                return data
            data += chunk
        return data

    def finish(self) -> None:
        self.task.close()
        self.relay_thread.join(5)
        assert not self.relay_thread.is_alive()
        if self.server_thread is not None:
            self.server_thread.join(5)
        self.slurm.close()


def serve_slurm(
    slurm: socket.socket, received: list[bytes], answer: Callable[[socket.socket, bytes], None] | None = None
) -> None:
    """Answer PMI-1 lines on *slurm* like ``slurmstepd`` until end of file, recording each line in *received*."""

    with slurm.makefile("rb") as lines:
        for line in lines:
            received.append(line)
            if answer is not None:
                answer(slurm, line)
            elif (name := line[4:].split(b" ", 1)[0].strip()) in REPLIES:
                slurm.sendall(REPLIES[name])


def _start(serve: bool = True, answer: Callable[[socket.socket, bytes], None] | None = None) -> Session:
    slurm, server = socket.socketpair()
    task, rank = socket.socketpair()
    session = Session(slurm, task, threading.Thread(target=lambda: None), None)
    session.relay_thread = threading.Thread(
        target=relay, args=(server.detach(), rank.detach()), kwargs={"log": session.logs.append}, daemon=True
    )
    session.relay_thread.start()
    if serve:
        session.server_thread = threading.Thread(
            target=serve_slurm, args=(session.slurm, session.received, answer), daemon=True
        )
        session.server_thread.start()
    return session


def _initialized() -> Session:
    session = _start()
    assert session.ask(INIT) == REPLIES[b"init"]
    return session


def _assert_refused(session: Session) -> None:
    assert session.task.recv(4096) == b""
    session.relay_thread.join(5)
    assert not session.relay_thread.is_alive()
    if session.server_thread is not None:
        session.server_thread.join(5)


def test_happy_path_round_trips() -> None:
    session = _initialized()
    commands = [b"cmd=put kvsname=k key=a value=1\n", b"cmd=get kvsname=k key=a\n", b"cmd=barrier_in\n"]
    for command in commands:
        assert session.ask(command) == REPLIES[command[4:].split(b" ")[0].strip()]
    assert session.ask(b"cmd=finalize\n") == REPLIES[b"finalize"]
    session.finish()
    assert session.received == [INIT, *commands, b"cmd=finalize\n"]
    assert session.logs == []


def test_single_spawn_is_refused_locally() -> None:
    session = _initialized()
    assert session.ask(SPAWN % (1, 1)) == SPAWN_REFUSAL
    assert session.ask(b"cmd=barrier_in\n") == REPLIES[b"barrier_in"]
    session.finish()
    assert session.received == [INIT, b"cmd=barrier_in\n"]
    assert session.logs == ["refused MPI_Comm_spawn"]


def test_spawn_multiple_gets_one_refusal_after_the_last_block() -> None:
    session = _initialized()
    session.task.sendall(SPAWN % (2, 1) + SPAWN % (2, 2) + b"cmd=barrier_in\n")
    assert session.reply() == SPAWN_REFUSAL
    assert session.reply() == REPLIES[b"barrier_in"]
    session.finish()
    assert session.received == [INIT, b"cmd=barrier_in\n"]
    assert session.logs == ["refused MPI_Comm_spawn"]


def test_spawn_blocks_without_counters_are_each_refused() -> None:
    session = _initialized()
    block = b"mcmd=spawn\nnprocs=1\nexecname=/bin/true\nendcmd\n"
    session.task.sendall(block + block + b"cmd=barrier_in\n")
    assert session.reply() == SPAWN_REFUSAL
    assert session.reply() == SPAWN_REFUSAL
    assert session.reply() == REPLIES[b"barrier_in"]
    session.finish()
    assert session.received == [INIT, b"cmd=barrier_in\n"]


def test_other_mcmd_blocks_are_swallowed() -> None:
    session = _initialized()
    session.task.sendall(b"mcmd=anything_else\nkey=value\nendcmd\n")
    assert session.reply() == SPAWN_REFUSAL  # no spawn counters: refused like any spawn block
    assert session.ask(b"cmd=barrier_in\n") == REPLIES[b"barrier_in"]
    session.finish()
    assert session.received == [INIT, b"cmd=barrier_in\n"]


def test_single_line_mcmd_fails_closed() -> None:
    session = _initialized()
    session.task.sendall(b"cmd=mcmd nprocs=1 execname=/bin/true totspawns=1 spawnssofar=1\n")
    _assert_refused(session)
    assert session.received == [INIT]
    assert any("mcmd" in line for line in session.logs)


def test_oversized_spawn_block_fails_closed() -> None:
    session = _initialized()
    filler = b"x=" + b"y" * 1000 + b"\n"
    session.task.sendall(b"mcmd=spawn\n" + filler * (MAX_SPAWN_BLOCK_BYTES // len(filler) + 1))
    _assert_refused(session)
    assert session.received == [INIT]


@pytest.mark.parametrize(
    "first",
    [
        b"cmd=barrier_in\n",
        b"000026cmd=init;pmi_version=2;",
        b"cmd=init pmi_version=2 pmi_subversion=0\n",
        b"cmd=init pmi_version=1 pmi_subversion=0\n",
        b"cmd=init pmi_version=1 pmi_subversion=11\n",
        b"cmd=init pmi_version=1 pmi_subversion=1" + b"0" * 30 + b"\n",
        b"cmd=init pmi_version=1 pmi_subversion=1 \n",
    ],
)
def test_first_line_must_be_pmi1_init(first: bytes) -> None:
    session = _start()
    session.task.sendall(first)
    _assert_refused(session)
    assert session.received == []
    assert session.logs == [f"refused: PMI init {first[:80]!r}"]


@pytest.mark.parametrize("line", [b"cmd=put " + b"v" * 1021 + b"\n", b"cmd=put " + b"v" * 1030])
def test_long_lines_fail_closed(line: bytes) -> None:
    session = _initialized()
    session.task.sendall(line)
    _assert_refused(session)
    assert session.received == [INIT]
    assert session.logs == ["refused: PMI line exceeds 1024 bytes"]


@pytest.mark.parametrize(
    "line",
    [
        b"cmd=spawn\n",
        b"cmd=\n",
        b"put key=a\n",
        b"cmd=create_kvs\n",
        b"cmd=destroy_kvs kvsname=k\n",
        b"cmd=getbyidx kvsname=k idx=0\n",
        INIT,
    ],
)
def test_unknown_commands_fail_closed(line: bytes) -> None:
    session = _initialized()
    session.task.sendall(line)
    _assert_refused(session)
    assert session.received == [INIT]
    assert session.logs[0].startswith("refused: PMI command")


def test_forwarded_lines_containing_mcmd_fail_closed() -> None:
    session = _initialized()
    session.task.sendall(b"cmd=put kvsname=k key=a value=xmcmd=spawn\n")
    _assert_refused(session)
    assert session.received == [INIT]
    assert session.logs == ["refused: PMI command containing mcmd"]


def test_commands_are_relayed_in_lock_step() -> None:
    put = b"cmd=put kvsname=k key=a value=1\n"
    early: list[bool] = []

    def answer(slurm: socket.socket, line: bytes) -> None:
        if line == put:
            # The barrier was sent together with the put; it must not arrive before the put's reply.
            early.append(select.select([slurm], [], [], 0.2)[0] != [])
        slurm.sendall(REPLIES[line[4:].split(b" ", 1)[0].strip()])

    session = _start(answer=answer)
    assert session.ask(INIT) == REPLIES[b"init"]
    session.task.sendall(put + b"cmd=barrier_in\n")
    assert session.reply() == REPLIES[b"put"]
    assert session.reply() == REPLIES[b"barrier_in"]
    session.finish()
    assert early == [False]
    assert session.received == [INIT, put, b"cmd=barrier_in\n"]


def test_server_close_ends_the_relay() -> None:
    session = _start(serve=False)
    session.task.sendall(INIT)
    assert session.slurm.recv(4096) == INIT
    session.slurm.close()
    _assert_refused(session)
    assert session.logs == []


def test_rank_close_ends_the_relay() -> None:
    session = _initialized()
    session.task.close()
    session.relay_thread.join(5)
    assert not session.relay_thread.is_alive()
    assert session.server_thread is not None
    session.server_thread.join(5)
    assert not session.server_thread.is_alive()  # the fake server saw end of file
    session.slurm.close()


def test_abort_drains_server_output_until_close() -> None:
    session = _start(serve=False)
    session.task.sendall(INIT)
    assert session.slurm.recv(4096) == INIT
    session.slurm.sendall(REPLIES[b"init"])
    assert session.reply() == REPLIES[b"init"]
    session.task.sendall(b"cmd=abort exitcode=1\ncmd=barrier_in\n")
    assert session.slurm.recv(4096) == b"cmd=abort exitcode=1\n"
    assert select.select([session.slurm], [], [], 0.2)[0] == []  # the rank is no longer read
    session.slurm.sendall(b"late output\n")
    assert session.reply() == b"late output\n"
    session.slurm.close()
    assert session.task.recv(4096) == b""
    session.relay_thread.join(5)
    assert not session.relay_thread.is_alive()


def test_non_socket_descriptors_are_rejected() -> None:
    read, write = os.pipe()
    sock, other = socket.socketpair()
    try:
        with pytest.raises(ValueError, match="not an open socket"):
            relay(sock.fileno(), read, log=lambda line: None)
        with pytest.raises(ValueError, match="not an open socket"):
            relay(read, sock.fileno(), log=lambda line: None)
        os.fstat(read)
        os.fstat(sock.fileno())  # nothing was closed
    finally:
        for descriptor in (read, write):
            os.close(descriptor)
        sock.close()
        other.close()
