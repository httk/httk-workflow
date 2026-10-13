"""Authenticated daemon job-command execution and crash recovery."""

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import cast

from . import _kernel, _requests, removal, seals
from ._daemon_protocol import Request, Response, decode_response, encode_response, request_digest
from ._daemon_state import Entry, Ledger
from ._state import StateDoc, read_state_unowned
from .errors import FormatError, WorkflowError
from .models import parse_placement_text, placement_text
from .workspace import Workspace

_TARGET_FORMAT = "httk-daemon-job-target"
_TARGET_VERSION = 1
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


def _ascii_detail(value: object) -> str:
    """Return a nonempty, printable ASCII protocol detail of at most 1000 characters."""

    text = str(value).encode("ascii", "replace").decode("ascii")
    text = " ".join("".join(character if " " <= character <= "~" else " " for character in text).split())
    return (text or "no further detail")[:1000]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


@dataclass(frozen=True, slots=True)
class _Target:
    job_id: str
    placement: PurePosixPath
    internal_request_id: str
    document: dict[str, object]
    digest: str
    operator_key: str
    exchange_name: str | None
    adoption_nonce: str | None

    def encode(self) -> bytes:
        value: dict[str, object] = {
            "format": _TARGET_FORMAT,
            "format_version": _TARGET_VERSION,
            "job_id": self.job_id,
            "placement": placement_text(self.placement),
            "internal_request_id": self.internal_request_id,
            "request": self.document,
            "digest": self.digest,
            "operator_key": self.operator_key,
        }
        if self.exchange_name is not None:
            value["exchange_name"] = self.exchange_name
            value["adoption_nonce"] = self.adoption_nonce
        return _canonical(value)

    @classmethod
    def decode(cls, data: bytes) -> "_Target":
        try:
            value = json.loads(data)
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise ValueError("stored job target is not valid JSON") from exc
        required = {
            "format",
            "format_version",
            "job_id",
            "placement",
            "internal_request_id",
            "request",
            "digest",
            "operator_key",
        }
        optional = {"exchange_name", "adoption_nonce"}
        if not isinstance(value, dict) or not required <= set(value) <= required | optional:
            raise ValueError("stored job target fields are invalid")
        if value["format"] != _TARGET_FORMAT or value["format_version"] != _TARGET_VERSION:
            raise ValueError("stored job target format is unsupported")
        if ("exchange_name" in value) != ("adoption_nonce" in value):
            raise ValueError("stored exchange target is incomplete")
        document = value["request"]
        if not isinstance(document, dict):
            raise ValueError("stored internal request is not an object")
        _requests.validate_envelope(document)
        if set(document) & {"operator_key", "signature", "force"}:
            raise ValueError("stored internal request has invalid daemon attribution")
        job_id = str(uuid.UUID(str(value["job_id"])))
        internal_request_id = str(uuid.UUID(str(value["internal_request_id"])))
        if value["job_id"] != job_id or value["internal_request_id"] != internal_request_id:
            raise ValueError("stored job target identifiers are not canonical")
        if document["job_id"] != job_id or document["request_id"] != internal_request_id:
            raise ValueError("stored internal request identity differs from its target")
        placement = parse_placement_text(value["placement"], "stored target placement")
        if document["placement"] != placement_text(placement):
            raise ValueError("stored internal request placement differs from its target")
        digest = value["digest"]
        if (
            type(digest) is not str
            or _HEX64.fullmatch(digest) is None
            or hashlib.sha256(_canonical(document)).hexdigest() != digest
        ):
            raise ValueError("stored internal request digest is invalid")
        exchange_name = value.get("exchange_name")
        adoption_nonce = value.get("adoption_nonce")
        if exchange_name is not None:
            exchange_name = str(uuid.UUID(str(exchange_name)))
            if value["exchange_name"] != exchange_name:
                raise ValueError("stored exchange name is not canonical")
            if type(adoption_nonce) is not str or not adoption_nonce:
                raise ValueError("stored adoption nonce is invalid")
        expected_operator = "daemon" if exchange_name is None else f"daemon:{exchange_name}:{cast(str, adoption_nonce)}"
        if document.get("operator") != expected_operator:
            raise ValueError("stored internal request has invalid daemon attribution")
        operator_key = value["operator_key"]
        if type(operator_key) is not str or not operator_key:
            raise ValueError("stored authenticated operator key is invalid")
        if _canonical(value) != data:
            raise ValueError("stored job target is not canonical")
        return cls(
            job_id,
            placement,
            internal_request_id,
            cast(dict[str, object], document),
            digest,
            operator_key,
            cast(str | None, exchange_name),
            cast(str | None, adoption_nonce),
        )


class JobExecutor:
    """Resolve, arbitrate, execute and observe authenticated daemon job commands."""

    def __init__(
        self,
        workspace: Workspace,
        ledger: Ledger,
        owner: _kernel.Owner,
        *,
        configuration_digest: str,
    ) -> None:
        self.workspace = workspace
        self.ledger = ledger
        self.owner = owner
        self.configuration_digest = configuration_digest

    def _public_detail(self, request: Request, detail: object, *, eject_destination: str | None = None) -> str:
        """Remove server paths and identify an eject destination in client-visible terms."""

        value = str(detail)
        roots = {str(self.workspace.root), str(self.workspace.root.resolve())}
        for root in sorted(roots, key=len, reverse=True):
            if root:
                value = value.replace(root.rstrip("/"), "workspace")
        if eject_destination is None and request.operation == "job_request" and request.action == "eject":
            eject_destination = request.destination
        if eject_destination is not None:
            destination = f"outbox/{eject_destination}"
            if destination not in value:
                value = f"{destination}: {value}"
        return _ascii_detail(value)

    def _response(
        self,
        request: Request,
        outcome: str,
        reason: str,
        detail: object,
        *,
        eject_destination: str | None = None,
        **fields: str | None,
    ) -> Response:
        return Response(
            request.request_id,
            request.workspace_id,
            request.enrollment_id,
            request_digest(request),
            outcome,
            reason=reason,
            detail=self._public_detail(request, detail, eject_destination=eject_destination),
            command_id=request.request_id if request.operation == "job_request" else request.command_id,
            **fields,
        )

    def _job_refs(self) -> list[tuple[_kernel.JobRef, PurePosixPath, StateDoc | None]]:
        found: list[tuple[_kernel.JobRef, PurePosixPath, StateDoc | None]] = []
        for ref, placement in seals.workspace_jobs(self.workspace):
            doc, _damaged = read_state_unowned(ref.path / "state.json")
            found.append((ref, placement, doc))
        return found

    def _resolve(self, request: Request) -> _Target:
        assert request.selector is not None and request.action is not None and request.reason is not None
        matches: dict[str, tuple[_kernel.JobRef, PurePosixPath, StateDoc | None, str | None, str | None]] = {}
        for ref, placement, doc in self._job_refs():
            if not ref.job_id.startswith(request.selector):
                continue
            exchange_name: str | None = None
            adoption_nonce: str | None = None
            if doc is not None and doc.origin == "exchange" and doc.exchange_name is not None:
                indexed = _kernel.exchange_index(self.workspace, doc.exchange_name)
                if indexed is not None and indexed[0] == ref.job_id:
                    exchange_name, adoption_nonce = doc.exchange_name, indexed[2]
            matches[ref.job_id] = (ref, placement, doc, exchange_name, adoption_nonce)
        if not matches:
            raise LookupError("no internal job UUID has that prefix")
        if len(matches) != 1:
            raise RuntimeError("the UUID prefix matches more than one job")
        ref, placement, doc, exchange_name, adoption_nonce = next(iter(matches.values()))

        seals.require_cli_modifiable(self.workspace)

        internal_request_id = str(uuid.UUID(hex=request.request_id))
        created_at = (
            datetime.fromtimestamp(request.created_at, UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        document: dict[str, object] = {
            "format": _requests.REQUEST_FORMAT,
            "format_version": _requests.REQUEST_FORMAT_VERSION,
            "request_id": internal_request_id,
            "job_id": ref.job_id,
            "placement": placement_text(placement),
            "action": request.action,
            "operator": ("daemon" if exchange_name is None else f"daemon:{exchange_name}:{cast(str, adoption_nonce)}"),
            "reason": request.reason,
            "created_at": created_at,
        }
        if request.priority is not None:
            document["priority"] = request.priority
        if request.step is not None:
            document["step"] = request.step
        if request.destination is not None:
            document["destination"] = str(self.workspace.root / "exchange" / "outbox" / request.destination)
            document["tree"] = True
        _requests.validate_envelope(document)
        encoded = _canonical(document)
        return _Target(
            ref.job_id,
            placement,
            internal_request_id,
            document,
            hashlib.sha256(encoded).hexdigest(),
            cast(str, request.operator_key),
            exchange_name,
            adoption_nonce,
        )

    def _target(self, entry: Entry) -> _Target:
        stored = self.ledger.read_target(entry)
        if stored is not None:
            target = _Target.decode(stored)
            if target.operator_key != entry.request.operator_key:
                raise ValueError("stored target attribution differs from the signed command")
            return target
        request = entry.request
        assert request.operator_key is not None
        target = self._resolve(request)
        return _Target.decode(self.ledger.store_target(entry, target.encode()))

    def _current(self, target: _Target) -> _kernel.JobRef | None:
        return _kernel.locate(
            self.workspace,
            target.job_id,
            placement_hint=target.placement,
            include_owned=True,
            exhaustive=True,
        )

    def _adoption_changed(self, target: _Target) -> bool:
        if target.exchange_name is None:
            return False
        return _kernel.exchange_index(self.workspace, target.exchange_name) != (
            target.job_id,
            target.placement,
            target.adoption_nonce,
        )

    def _pre_execution(self, target: _Target) -> None:
        """Recheck mutable policy and recorded runner steps immediately before a new decision."""

        seals.require_cli_modifiable(self.workspace)
        if target.document["action"] != "override_step":
            return
        current = self._current(target)
        if current is None:
            return
        doc, _damaged = read_state_unowned(current.path / "state.json")
        known = () if doc is None or doc.runner_steps is None else tuple(str(item) for item in doc.runner_steps)
        if known and target.document.get("step") not in known:
            raise ValueError(f"step {target.document.get('step')!r} is not one of the job's recorded runner steps")

    def _stored_result(
        self, request: Request, entry: Entry, *, target: _Target | None = None, fresh_state: bool = False
    ) -> Response | None:
        data = self.ledger.execution_result(entry)
        if data is None:
            return None
        stored = decode_response(data)
        current = self._current(target) if fresh_state and target is not None else None
        return self._response(
            request,
            stored.outcome,
            stored.reason or f"request_{stored.outcome}",
            stored.detail or f"the job request was {stored.outcome}",
            job_id=stored.job_id,
            internal_request_id=stored.internal_request_id,
            current_state=(None if current is None else current.state) if fresh_state else stored.current_state,
            eject_destination=entry.request.destination if entry.request.action == "eject" else None,
        )

    def _history(self, target: _Target) -> tuple[str, str, str | None] | None:
        current = self._current(target)
        if current is None:
            return None
        doc, damaged = read_state_unowned(current.path / "state.json")
        if damaged or doc is None:
            return None
        events = [event for event in doc.history_tail if event.get("request_id") == target.internal_request_id]
        dropped = next((event for event in reversed(events) if event.get("event") == "request_dropped"), None)
        if dropped is not None:
            return "refused", _ascii_detail(dropped.get("note", "the request was refused")), current.state
        applied = next((event for event in reversed(events) if event.get("event") == "request_applied"), None)
        if applied is not None and target.document["action"] not in {"eject", "delete"}:
            return "applied", _ascii_detail(f"{target.document['action']} applied"), current.state
        return None

    def _observe(self, request: Request, entry: Entry, target: _Target) -> Response:
        if (stored := self._stored_result(request, entry)) is not None:
            return stored
        path = self.workspace.control / "requests" / f"{target.job_id}.{target.internal_request_id}.json"
        try:
            parsed = _requests.parse(path)
        except (FileNotFoundError, FormatError, OSError):
            parsed = None
        if parsed is not None and hashlib.sha256(_canonical(dict(parsed.document))).hexdigest() == target.digest:
            current = self._current(target)
            return self._response(
                request,
                "queued",
                "request_queued",
                "the exact internal request is pending",
                job_id=target.job_id,
                internal_request_id=target.internal_request_id,
                current_state=None if current is None else current.state,
                eject_destination=entry.request.destination if entry.request.action == "eject" else None,
            )
        history = self._history(target)
        if history is not None:
            outcome, detail, state = history
            return self._response(
                request,
                outcome,
                f"request_{outcome}",
                detail,
                job_id=target.job_id,
                internal_request_id=target.internal_request_id,
                current_state=state,
                eject_destination=entry.request.destination if entry.request.action == "eject" else None,
            )
        current = self._current(target)
        return self._response(
            request,
            "uncertain",
            "execution_uncertain",
            "the execution decision exists but no definitive effect evidence is available",
            job_id=target.job_id,
            internal_request_id=target.internal_request_id,
            current_state=None if current is None else current.state,
            eject_destination=entry.request.destination if entry.request.action == "eject" else None,
        )

    def execute(self, entry: Entry) -> Response:
        """Execute a newly admitted job request, or recover it without reposting."""

        request = entry.request
        decision = self.ledger.execution_decision(entry)
        if decision is None and request.configuration_digest != self.configuration_digest:
            return self._response(
                request,
                "refused",
                "stale_configuration",
                "the signed daemon configuration is no longer active",
            )
        if decision is None:
            self.owner.check_alive()
        try:
            target = self._target(entry)
        except LookupError as exc:
            return self._response(request, "refused", "unknown_job", exc)
        except (RuntimeError, ValueError, WorkflowError) as exc:
            return self._response(request, "refused", "request_refused", exc)

        if decision is not None:
            return self._observe(request, entry, target)
        try:
            self._pre_execution(target)
        except (ValueError, WorkflowError, OSError) as exc:
            return self._response(
                request,
                "refused",
                "request_refused",
                exc,
                job_id=target.job_id,
                internal_request_id=target.internal_request_id,
            )
        if self._adoption_changed(target):
            return self._response(
                request,
                "refused",
                "adoption_changed",
                "the exchange adoption changed after target resolution",
                job_id=target.job_id,
                internal_request_id=target.internal_request_id,
            )
        self.owner.check_alive()
        won, _decision = self.ledger.decide_execution(entry, self.ledger.nonce, self.owner.owner_id)
        if not won:
            return self._observe(request, entry, target)

        try:
            if self._adoption_changed(target):
                response = self._response(
                    request,
                    "refused",
                    "adoption_changed",
                    "the exchange adoption changed immediately before publication",
                    job_id=target.job_id,
                    internal_request_id=target.internal_request_id,
                )
            else:
                self.owner.check_alive()
                path = _kernel.post_request(self.workspace, target.document)
                current = self._current(target)
                if current is None:
                    response = self._response(
                        request,
                        "uncertain",
                        "execution_uncertain",
                        "the target disappeared after its internal request was published",
                        job_id=target.job_id,
                        internal_request_id=target.internal_request_id,
                    )
                else:
                    self.owner.check_alive()
                    (outcome,) = removal.serve_requests(self.workspace, [(current, path)], owner=self.owner)
                    detail = outcome.explanation
                    if request.action == "eject" and outcome.status == "applied":
                        detail = f"job ejected to outbox/{request.destination}"
                    response = self._response(
                        request,
                        outcome.status,
                        f"request_{outcome.status}",
                        detail,
                        job_id=target.job_id,
                        internal_request_id=target.internal_request_id,
                        current_state=outcome.state,
                    )
        except _kernel.OwnerLost:
            raise
        except Exception:
            if any(self.owner.holds(ref) for ref in self.owner.owned()):
                raise
            response = self._observe(request, entry, target)
        if response.outcome in {"applied", "refused"}:
            stored = self.ledger.store_execution_result(entry, encode_response(response))
            response = decode_response(stored)
        return response

    def status(self, request: Request) -> Response:
        """Observe an original job command without admitting or executing this status request."""

        assert request.command_id is not None
        entry = self.ledger.lookup_request(request.command_id)
        if entry is None or entry.request.operation != "job_request":
            return self._response(request, "refused", "unknown_command", "the original job command is unknown")
        stored_target = self.ledger.read_target(entry)
        if stored_target is None:
            if entry.response is not None:
                original = entry.response
                return self._response(
                    request,
                    original.outcome,
                    original.reason or f"request_{original.outcome}",
                    original.detail or f"the job request was {original.outcome}",
                    eject_destination=entry.request.destination if entry.request.action == "eject" else None,
                )
            return self._response(
                request,
                "uncertain",
                "execution_uncertain",
                "the command has no frozen target",
                eject_destination=entry.request.destination if entry.request.action == "eject" else None,
            )
        target = _Target.decode(stored_target)
        if (result := self._stored_result(request, entry, target=target, fresh_state=True)) is not None:
            return result
        if entry.response is not None and entry.response.outcome in {"applied", "refused"}:
            original = entry.response
            current = self._current(target)
            return self._response(
                request,
                original.outcome,
                original.reason or f"request_{original.outcome}",
                original.detail or f"the job request was {original.outcome}",
                job_id=target.job_id,
                internal_request_id=target.internal_request_id,
                current_state=None if current is None else current.state,
                eject_destination=entry.request.destination if entry.request.action == "eject" else None,
            )
        return self._observe(request, entry, target)


__all__ = ["JobExecutor"]
