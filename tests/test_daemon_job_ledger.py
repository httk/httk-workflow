"""Once-write target and execution records for remote job commands."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from httk.workflow._daemon_protocol import Request
from httk.workflow._daemon_state import Ledger

WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"
ENROLLMENT_ID = "0123456789abcdef0123456789abcdef"


def test_target_decision_and_result_have_one_stable_winner(tmp_path: Path) -> None:
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    with Ledger(state, WORKSPACE_ID, ENROLLMENT_ID, initialize=True):
        pass
    first = Ledger(state, WORKSPACE_ID, ENROLLMENT_ID)
    second = Ledger(state, WORKSPACE_ID, ENROLLMENT_ID)
    try:
        request = Request(
            "00000000000000000000000000000077",
            WORKSPACE_ID,
            "job_request",
            enrollment_id=ENROLLMENT_ID,
            configuration_digest="0" * 64,
            selector="12345678-1234-1234-1234-123456789abc",
            action="pause",
            reason="operator request",
        )
        entry = first.admit(request)
        targets = (
            b'{"job_id":"12345678-1234-1234-1234-123456789abc"}',
            b'{"job_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}',
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            target_winners = list(
                pool.map(
                    lambda args: args[0].store_target(entry, args[1]),
                    ((first, targets[0]), (second, targets[1])),
                )
            )
        assert target_winners[0] == target_winners[1] == first.read_target(entry)

        with ThreadPoolExecutor(max_workers=2) as pool:
            decisions = list(
                pool.map(
                    lambda ledger: ledger.decide_execution(entry, ledger.nonce, ledger.nonce),
                    (first, second),
                )
            )
        assert sum(won for won, _decision in decisions) == 1
        assert decisions[0][1] == decisions[1][1] == first.execution_decision(entry)

        results = (b'{"result":"applied"}', b'{"result":"uncertain"}')
        with ThreadPoolExecutor(max_workers=2) as pool:
            stored = list(
                pool.map(
                    lambda args: args[0].store_execution_result(entry, args[1]),
                    ((first, results[0]), (second, results[1])),
                )
            )
        assert stored[0] == stored[1] == first.execution_result(entry)
        assert first.verify() == 1
    finally:
        first.close()
        second.close()
