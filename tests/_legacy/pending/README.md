# Pending pre-redesign tests

These are the pre-redesign tests of moving jobs between workspaces, kept only as the specification that phases D and E port from.
They are not collected by pytest (`collect_ignore` in `tests/conftest.py`) and are excluded from ruff, mypy and pyright.

- `test_adopt_hardening.py`: port D2 (adoption and transfer recovery never trusting a job directory; `_moving.py` reconcilers)
- `test_adoption.py`: delete D4 (legacy adoption chain; replaced by `_moving.py` adopt tests)
- `test_bundle.py`: port D1 (bundle format and its one verification function; `_bundles.py` validator tests)
- `test_cli_job_transfer.py`: port D3
- `test_cli_transfer_tree.py`: port D3
- `test_daemon_exchange_flow.py`: port D4
- `test_docs_parity.py`: port E1 (documentation examples use the removed APIs)
- `test_eject_adopt.py`: port D3
- `test_exchange_enable.py`: port D4
- `test_exchange_pass.py`: port D4
- `test_fetch.py`: port D3
- `test_job_tree.py`: port D3
- `test_management_transfer.py`: port D3 (transfer round trip and resumed send, split out of `test_management.py`)
- `test_monitor_transfer.py`: port D3 (known-job detach and quiet remote relay, split out of `test_monitor.py`)
- `test_mount_adapter.py`: port D3
- `test_placement_rule_transfer.py`: port D2/D3 (the placement rule at import, adopt and detach, split out of `test_placement_rule.py`)
- `test_precheck_transfer.py`: port D3 (transfer-time destination advisories through the transfer layer and the transfer CLI)
- `test_remote_adapters.py`: port D3
- `test_remote_roundtrip.py`: port D3
- `test_sealing.py`: delete D4 (legacy `_sealing` copy-out driven through the removed `job eject --resume`; replaced by `_moving.py` tests)
- `test_transfer_hardening.py`: port D3
- `test_transfer_phase0_hardening.py`: port D3
- `test_transfer_residuals.py`: port D3 (gc no longer cleans legacy transfer residue; transfer residue reconcilers move to `_moving.py`)
- `test_transfer_review.py`: port D3 (sequence reuse and amortized transfer sweep work)
- `test_transfer_status.py`: port D3 (the `transfer status` operator report)
- `test_trust_and_hygiene_transfer.py`: port D2 (signed transfer acknowledgements, split out of `test_trust_and_hygiene.py`)
