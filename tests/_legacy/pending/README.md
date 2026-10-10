# Pending pre-redesign tests

These are the pre-redesign tests of moving jobs between workspaces, kept only as the specification that phases D and E port from.
They are not collected by pytest (`collect_ignore` in `tests/conftest.py`) and are excluded from ruff, mypy and pyright.

- `test_adopt_hardening.py`: port D2 (adoption and transfer recovery never trusting a job directory; `_moving.py` reconcilers)
- `test_adoption.py`: delete D4 (legacy adoption chain; replaced by `_moving.py` adopt tests)
- `test_bundle.py`: port D1 (bundle format and its one verification function; `_bundles.py` validator tests)
- `test_daemon_exchange_flow.py`: port D4
- `test_docs_parity.py`: port E1 (documentation examples use the removed APIs)
- `test_exchange_enable.py`: port D4
- `test_exchange_pass.py`: port D4
- `test_sealing.py`: delete D4 (legacy `_sealing` copy-out driven through the removed `job eject --resume`; replaced by `_moving.py` tests)

Several files here import `_payload` from `test_eject_adopt.py`, deleted in D3; read it in git history (commit 4c51024).

Ported or deleted in D3 (`job transfer` on the holds/adopt layer):

- `test_remote_adapters.py`, `test_mount_adapter.py`: restored under `tests/`; the end-to-end job test now transfers with `job transfer` and runs the job with an installed workflow; the manager-forwarding test drops the retired `--lease-seconds`/`--runner-search-path`
- `test_fetch.py`: its remote `job request` half became `tests/test_remote_job_requests.py` (the manager-applied `operator_key` check is gone: v3 `state.json` records no operator key); offer/fetch/retire were deleted
- `test_cli_job_transfer.py`, `test_cli_transfer_tree.py`, `test_management_transfer.py`, `test_transfer_status.py`, `test_remote_roundtrip.py`: ported into `tests/test_job_transfer.py` (endpoint resolution, trees, detached children, resume after a crash, `transfer status`, ssh round trip, banner on the remote stdout)
- `test_placement_rule_transfer.py`: the nesting-placement refusal at adopt is in `tests/test_job_transfer.py`; destination-placement overrides are gone
- `test_eject_adopt.py`, `test_job_tree.py`, `test_transfer_phase0_hardening.py`: deleted; what still applies is covered by `tests/test_moving.py`, `tests/test_cli_job_eject_adopt.py` and `tests/test_bundles.py` (journals, lineages, receipts, runners, markers and placement overrides are gone; a bound child may now be ejected alone, per `_moving`)
- `test_transfer_hardening.py`, `test_transfer_residuals.py`, `test_transfer_review.py`, `test_trust_and_hygiene_transfer.py`, `test_precheck_transfer.py`, `test_monitor_transfer.py`: deleted (payload digests, receipts, sequence reuse, signed acknowledgements, transfer-time environment advisories with `--strict-environment`, and the monitor's transfer action are gone; the canonical-id rule for remote sources is in `tests/test_job_transfer.py`)
