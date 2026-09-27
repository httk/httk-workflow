"""Resumable event-driven jobflow scheduling: JSON/MSON state records pending DAG jobs and reports those whose parents have settled, applies ``run_locally`` response semantics, makes replacement and detour flows block the responder's children until they settle, and is checkpointed with ``to_mapping()``/``from_mapping()`` while workers receive spooled pending jobs and write normal jobflow documents to the shared store."""

from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, cast

from jobflow.core.flow import Flow, get_flow
from jobflow.core.job import Job, JobConfig, Response
from jobflow.core.reference import OnMissing, find_and_get_references, find_and_resolve_references
from monty.json import MontyDecoder, jsanitize

_BLOB_SNAPSHOT_UUID = "__httk_jobflow_additional_stores__"


def _job_key(uuid: str, index: int) -> str:
    """Return the persistent execution key for a job attempt."""
    return f"{uuid}:{index}"


def spool_job(job: Job) -> dict[str, Any]:
    """Encode a pending job for a worker process.

    :param job: The job to encode.
    :return: An MSON-safe job mapping.
    """
    return jsanitize(job, strict=True, enum_values=True, allow_bson=True)


def load_spooled_job(mapping: Mapping[str, Any]) -> Job:
    """Decode a worker job and reject unavailable serialized functions.

    :param mapping: An MSON job mapping produced by :func:`spool_job`.
    :return: The decoded job.
    :raises TypeError: If the mapping does not decode to a job.
    :raises RuntimeError: If the job function cannot be imported in this process.
    """
    job = MontyDecoder().process_decoded(deepcopy(dict(mapping)))
    if not isinstance(job, Job):
        raise TypeError("spooled mapping did not decode to a jobflow Job")
    if isinstance(job.function, dict):
        raise RuntimeError("function could not be deserialized — is the module available in this environment?")
    return job


@dataclass
class _PendingJob:
    """Serializable pending-job record."""

    job: dict[str, Any]
    parents: set[str]
    barriers: set[str]


@dataclass
class DriverState:
    """Persistent state for a jobflow DAG.

    Jobs are keyed internally by ``uuid:index`` because jobflow replacements deliberately
    reuse a UUID at the next index.  The public state stores only MSON for jobs that have not
    finished; completed job outputs remain in the job store.
    """

    pending: dict[str, _PendingJob] = field(default_factory=dict)
    identities: dict[str, tuple[str, int]] = field(default_factory=dict)
    order: list[str] = field(default_factory=list)
    completed: set[str] = field(default_factory=set)
    running: set[str] = field(default_factory=set)
    errored: set[str] = field(default_factory=set)
    stopped: set[str] = field(default_factory=set)
    skipped: set[str] = field(default_factory=set)
    errors: dict[str, str] = field(default_factory=dict)
    stored_data: dict[str, Any] = field(default_factory=dict)
    root_output: Any = None
    stop_jobflow: bool = False

    @classmethod
    def from_flow(cls, flow: Flow | Job) -> "DriverState":
        """Create a new scheduler state from a root flow or job.

        :param flow: The root jobflow object.
        :return: State with all root jobs pending.
        """
        root = get_flow(flow)
        state = cls(root_output=jsanitize(root.output, strict=True, enum_values=True, allow_bson=True))
        state._add_flow(root)
        return state

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "DriverState":
        """Restore state from :meth:`to_mapping` output.

        :param mapping: Plain JSON-compatible state mapping.
        :return: Restored state.
        """
        pending = {
            item["key"]: _PendingJob(
                deepcopy(item["job"]),
                set(item["parents"]),
                set(item["barriers"]),
            )
            for item in mapping["pending"]
        }
        return cls(
            pending=pending,
            identities={key: (value["uuid"], value["index"]) for key, value in mapping["identities"].items()},
            order=list(mapping["order"]),
            completed=set(mapping["completed"]),
            running=set(mapping["running"]),
            errored=set(mapping["errored"]),
            stopped=set(mapping["stopped"]),
            skipped=set(mapping["skipped"]),
            errors=dict(mapping["errors"]),
            stored_data=deepcopy(mapping["stored_data"]),
            root_output=deepcopy(mapping["root_output"]),
            stop_jobflow=bool(mapping["stop_jobflow"]),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Return a JSON-compatible checkpoint.

        :return: A mapping containing MSON only for unfinished jobs.
        """
        return {
            "pending": [
                {
                    "key": key,
                    "job": record.job,
                    "parents": sorted(record.parents),
                    "barriers": sorted(record.barriers),
                }
                for key, record in self.pending.items()
            ],
            "identities": {key: {"uuid": uuid, "index": index} for key, (uuid, index) in self.identities.items()},
            "order": self.order,
            "completed": sorted(self.completed),
            "running": sorted(self.running),
            "errored": sorted(self.errored),
            "stopped": sorted(self.stopped),
            "skipped": sorted(self.skipped),
            "errors": self.errors,
            "stored_data": jsanitize(self.stored_data, strict=True, enum_values=True, allow_bson=True),
            "root_output": self.root_output,
            "stop_jobflow": self.stop_jobflow,
        }

    def ready(self) -> list[Job]:
        """Return pending jobs with no unsettled scheduling parents.

        :return: Freshly decoded jobs in deterministic insertion order.
        """
        self._settle_skips()
        settled = self._settled
        return [
            load_spooled_job(record.job)
            for key, record in self.pending.items()
            if key not in self.running and (record.parents | record.barriers).issubset(settled)
        ]

    def mark_running(self, uuid: str) -> None:
        """Mark one ready job as dispatched to a worker.

        :param uuid: A job UUID, or the internal ``uuid:index`` key when needed.
        :raises KeyError: If no matching pending job exists.
        """
        key = self._pending_key(uuid)
        self.running.add(key)

    def mark_pending(self, uuid: str) -> None:
        """Return an in-flight job to the pending queue after a worker crash.

        :param uuid: A job UUID, or the internal ``uuid:index`` key when needed.
        :raises KeyError: If no matching pending job exists.
        """
        key = self._pending_key(uuid)
        self.running.discard(key)

    def apply_success(self, uuid: str, response: Response) -> None:
        """Record a successful job response and its dynamic graph changes.

        :param uuid: The responding job UUID or execution key.
        :param response: The decoded response returned by ``Job.run``.
        :raises KeyError: If the job is not pending or running.
        """
        key = self._pending_key(uuid)
        children = [child for child, record in self.pending.items() if key in record.parents and child != key]
        barrier_dependents = [
            child for child, record in self.pending.items() if key in record.barriers and child != key
        ]
        self._finish(key)
        if response.stored_data is not None:
            self.stored_data[self.identities[key][0]] = response.stored_data
        if response.stop_jobflow:
            self.stop_jobflow = True
            self._stop_all_pending()
            return
        if response.stop_children:
            self.stopped.add(key)
            self._stop_descendants(children)
        for flow in (response.replace, response.detour):
            if flow is not None:
                added = self._add_flow(get_flow(cast(Flow | Job | list[Job], flow), allow_external_references=True))
                for child in (*children, *barrier_dependents):
                    if child in self.pending:
                        self.pending[child].barriers.update(added)
        if response.addition is not None:
            added = self._add_flow(
                get_flow(
                    cast(Flow | Job | list[Job], response.addition),
                    allow_external_references=True,
                )
            )
            for child in barrier_dependents:
                if child in self.pending:
                    self.pending[child].barriers.update(added)
        self._settle_skips()

    def apply_error(self, uuid: str, message: str) -> None:
        """Record a failed worker; dependent poisoning is performed lazily.

        :param uuid: The failed job UUID or execution key.
        :param message: A serializable failure message.
        :raises KeyError: If the job is not pending or running.
        """
        key = self._pending_key(uuid)
        self._finish(key)
        self.errored.add(key)
        self.errors[key] = message
        self._settle_skips()

    @property
    def is_complete(self) -> bool:
        """Whether no job remains pending or running."""
        self._settle_skips()
        return not self.pending and not self.running

    @property
    def succeeded(self) -> bool:
        """Whether all scheduled work completed without errors or stop directives."""
        return self.is_complete and not (self.errored or self.skipped or self.stopped or self.stop_jobflow)

    def is_settled(self, identifier: str) -> bool:
        """Return whether an execution key has already reached a terminal state.

        :param identifier: The internal execution key to inspect.
        :return: Whether the key is completed, errored, stopped, or skipped.
        """
        return identifier in self._settled

    def failure_summary(self) -> dict[str, Any]:
        """Return structured failure information.

        :return: Execution keys grouped by terminal failure category and reported messages.
        """
        return {
            "errored": sorted(self.errored),
            "stopped": sorted(self.stopped),
            "skipped": sorted(self.skipped),
            "messages": dict(self.errors),
        }

    @property
    def _settled(self) -> set[str]:
        return self.completed | self.errored | self.stopped | self.skipped

    def _pending_key(self, identifier: str) -> str:
        if identifier in self.pending:
            return identifier
        matches = [key for key, (uuid, _) in self.identities.items() if uuid == identifier and key in self.pending]
        if len(matches) != 1:
            raise KeyError(f"no unique pending job for {identifier!r}")
        return matches[0]

    def _finish(self, key: str) -> None:
        self.pending.pop(key)
        self.running.discard(key)
        self.completed.add(key)

    def _add_flow(self, flow: Flow) -> set[str]:
        jobs = list(flow.iterflow())
        new_keys: dict[str, str] = {}
        for job, _ in jobs:
            key = _job_key(job.uuid, job.index)
            if key in self.identities:
                raise ValueError(f"dynamic flow reused already scheduled job {key}")
            new_keys[job.uuid] = key
            self.identities[key] = (job.uuid, job.index)
            self.order.append(key)
        added: set[str] = set()
        for job, parent_uuids in jobs:
            key = new_keys[job.uuid]
            parents = {new_keys[parent] for parent in parent_uuids if parent in new_keys}
            external_uuids = {
                reference.uuid
                for reference in find_and_get_references((job.function_args, job.function_kwargs))
                if reference.uuid not in new_keys
            }
            parents.update(
                external_key
                for external_uuid in external_uuids
                if (external_key := self._latest_unsettled_key(external_uuid)) is not None
            )
            self.pending[key] = _PendingJob(spool_job(job), parents, set())
            added.add(key)
        return added

    def _latest_unsettled_key(self, uuid: str) -> str | None:
        for key in reversed(self.order):
            if self.identities[key][0] == uuid and key not in self._settled:
                return key
        return None

    def _settle_skips(self) -> None:
        if self.stop_jobflow:
            self._stop_all_pending()
            return
        changed = True
        while changed:
            changed = False
            for key, record in list(self.pending.items()):
                if not (record.parents | record.barriers).issubset(self._settled):
                    continue
                parent_uuids = {self.identities[parent][0] for parent in record.parents}
                if parent_uuids & self._status_uuids(self.stopped):
                    self.pending.pop(key)
                    self.stopped.add(key)
                    self.skipped.add(key)
                    changed = True
                elif parent_uuids & self._status_uuids(self.errored) and self._on_missing_is_error(record):
                    self.pending.pop(key)
                    self.errored.add(key)
                    self.skipped.add(key)
                    self.errors.setdefault(key, "skipped: parent job failed")
                    changed = True

    def _status_uuids(self, keys: Iterable[str]) -> set[str]:
        return {self.identities[key][0] for key in keys}

    @staticmethod
    def _on_missing_is_error(record: _PendingJob) -> bool:
        job = load_spooled_job(record.job)
        config = cast(JobConfig, job.config)
        return config.on_missing_references == OnMissing.ERROR

    def _stop_all_pending(self) -> None:
        for key in list(self.pending):
            if key not in self.running:
                self.pending.pop(key)
                self.stopped.add(key)
                self.skipped.add(key)

    def _stop_descendants(self, children: Iterable[str]) -> None:
        queue = list(children)
        while queue:
            key = queue.pop()
            record = self.pending.pop(key, None)
            if record is None:
                continue
            self.running.discard(key)
            self.stopped.add(key)
            self.skipped.add(key)
            queue.extend(child for child, child_record in self.pending.items() if key in child_record.parents)


def merge_documents(store: Any, docs: Iterable[Mapping[str, Any]]) -> None:
    """Idempotently merge raw worker documents and blobs into a jobflow store.

    :param store: A ``JobStore`` or compatible object.
    :param docs: Job documents, blob documents, or mappings with a ``blobs`` mapping.
    """
    main_docs: list[dict[str, Any]] = []
    blobs: dict[str, list[dict[str, Any]]] = {}
    for item in docs:
        value = dict(item)
        value.pop("_id", None)
        nested_stores = value.pop("_httk_additional_stores", None)
        if nested_stores:
            for name, entries in nested_stores.items():
                blobs.setdefault(name, []).extend(dict(entry) for entry in entries)
            continue
        nested_blobs = value.pop("blobs", None)
        if nested_blobs:
            for name, entries in nested_blobs.items():
                blobs.setdefault(name, []).extend(dict(entry) for entry in entries)
        if "blob_uuid" in value:
            blobs.setdefault(value.pop("store"), []).append(value)
        elif value:
            main_docs.append(value)
    if main_docs:
        store.docs_store.update(main_docs, key=["uuid", "index"])
    for name, entries in blobs.items():
        if name is None:
            raise ValueError("blob document is missing its store name")
        blob_store = store.additional_stores[name]
        blob_store.connect()
        blob_store.update(entries, key="blob_uuid")


def snapshot_documents(store: Any) -> list[dict[str, Any]]:
    """Return JSON-safe main and additional-store documents for a worker snapshot.

    :param store: A connected ``JobStore`` or compatible object.
    :return: Documents suitable for :func:`merge_documents`.
    """
    documents = [{key: value for key, value in doc.items() if key != "_id"} for doc in store.query()]
    additional: dict[str, list[dict[str, Any]]] = {}
    for name, blob_store in store.additional_stores.items():
        blob_store.connect()
        entries = [{key: value for key, value in doc.items() if key != "_id"} for doc in blob_store.query()]
        if entries:
            additional[name] = entries
    if additional:
        documents.append({"uuid": _BLOB_SNAPSHOT_UUID, "index": 0, "_httk_additional_stores": additional})
    return documents


def resolve_final_output(state: DriverState, store: Any) -> Any:
    """Resolve the root output, or the final completed job output when none was declared.

    :param state: A completed driver state.
    :param store: The jobflow store containing worker documents.
    :return: A plain JSON-safe resolved output.
    """
    if state.root_output is not None:
        value = find_and_resolve_references(state.root_output, store)
    else:
        completed = [key for key in state.order if key in state.completed]
        if not completed:
            return None
        uuid, index = state.identities[completed[-1]]
        document = store.query_one({"uuid": uuid, "index": index})
        value = None if document is None else document.get("output")
    return jsanitize(value, strict=True, enum_values=True, allow_bson=True)
