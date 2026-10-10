# Pending pre-redesign tests

These are pre-redesign tests kept only as the specification that later phases port from.
They are not collected by pytest (`collect_ignore` in `tests/conftest.py`) and are excluded from ruff, mypy and pyright.

- `test_adopt_hardening.py`: port D2 (adoption and transfer recovery never trusting a job directory; check against `tests/test_moving.py`, then delete)
- `test_bundle.py`: port D1 (bundle format and verification; check against `tests/test_bundles.py`, then delete)
- `test_docs_parity.py`: port E1 (documentation examples use the removed APIs)

Several of these import helpers from files deleted in phase D (`test_eject_adopt.py`, `test_sealing.py`); read them in git history (commit 4c51024).

Phase D3 ported or deleted the transfer suites (`job transfer` on the holds/adopt layer: `tests/test_job_transfer.py`, `tests/test_remote_job_requests.py`, restored `tests/test_remote_adapters.py` and `tests/test_mount_adapter.py`).
Phase D4 ported the exchange and daemon suites (`tests/test_exchange.py`, `tests/test_daemon_exchange_flow.py`) and deleted `test_exchange_enable.py`, `test_exchange_pass.py`, `test_adoption.py` and `test_sealing.py`.
