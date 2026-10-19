"""Fault boundaries use an isolated Firestore emulator and fake materializations."""

import signal
import threading
import time
from unittest.mock import Mock

import mongomock
import pytest
import test_web_worker as fixtures
from google.api_core.exceptions import ServiceUnavailable
from google.cloud import firestore
from google.cloud.firestore_v1.batch import WriteBatch
from google.cloud.firestore_v1.document import DocumentReference
from google.cloud.firestore_v1.transaction import Transaction
from pymongo.errors import AutoReconnect
from redis.exceptions import AuthenticationError, AuthorizationError
from redis.exceptions import ConnectionError as RedisConnectionError

import pebble.web_worker as module
from pebble.web_worker import (
    RunDeadlineExceeded,
    Worker,
    calculate_snapshot,
    run_deadline,
)

# Reuse the same isolated emulator fixture without duplicating its seed contract.
config = fixtures.config
emulator = fixtures.emulator

OUTPUT = {
    "players": [{"id": "test-player", "name": "Test"}],
    "nights": [],
    "weeks": [],
    "reportCount": 1,
    "reportSummaries": {},
    "warnings": [],
}


def queue(season, name):
    ref = season.collection("runs").document(name)
    ref.set(
        {
            "status": "queued",
            "requestedAt": firestore.SERVER_TIMESTAMP,
            "requestedBy": "test-officer",
        }
    )
    return ref


@pytest.fixture
def prepared(config, emulator, monkeypatch):
    client, season = emulator
    worker = Worker(config, client, mongomock.MongoClient())
    monkeypatch.setattr(module, "calculate_snapshot", Mock(return_value=OUTPUT))
    queue(season, "baseline")
    assert worker.process_next()
    pointer = client.document("publicSeasons/worker-tests")
    assert pointer.get().to_dict()["latestRunId"] == "baseline"
    return worker, season, pointer


@pytest.mark.parametrize(
    "boundary",
    [
        "claim-read",
        "claim-ack",
        "capture",
        "mongo",
        "metadata",
        "progress",
        "root",
        "upload",
        "finish",
    ],
)
def test_service_failure_at_each_boundary_preserves_complete_pointer(
    prepared, monkeypatch, boundary
):
    worker, season, pointer = prepared
    bad = queue(season, "fault")
    original_update = DocumentReference.update
    original_get = DocumentReference.get
    original_set = DocumentReference.set
    original_commit = WriteBatch.commit

    with monkeypatch.context() as patch:
        if boundary == "claim-read":

            def get(ref, *args, **kwargs):
                if ref.path == bad.path:
                    raise ServiceUnavailable("PRIVATE DATA")
                return original_get(ref, *args, **kwargs)

            patch.setattr(DocumentReference, "get", get)
        elif boundary == "claim-ack":
            claim = worker.claim

            def lost_ack(ref):
                claim(ref)
                raise ServiceUnavailable("PRIVATE DATA")

            patch.setattr(worker, "claim", lost_ack)
        elif boundary == "capture":
            patch.setattr(
                module,
                "capture_inputs",
                Mock(side_effect=ServiceUnavailable("PRIVATE DATA")),
            )
        elif boundary == "mongo":
            patch.setattr(
                worker.db.input_snapshots,
                "replace_one",
                Mock(side_effect=AutoReconnect("PRIVATE DATA")),
            )
        elif boundary in {"metadata", "progress"}:

            def update(ref, values, *args, **kwargs):
                field = "inputSnapshotHash" if boundary == "metadata" else "stage"
                if ref.path == bad.path and field in values:
                    raise ServiceUnavailable("PRIVATE DATA")
                return original_update(ref, values, *args, **kwargs)

            patch.setattr(DocumentReference, "update", update)
        elif boundary == "root":

            def set_root(ref, *args, **kwargs):
                if ref.path.endswith("/results/fault"):
                    raise ServiceUnavailable("PRIVATE DATA")
                return original_set(ref, *args, **kwargs)

            patch.setattr(DocumentReference, "set", set_root)
        elif boundary == "upload":

            def upload(batch, *args, **kwargs):
                if any(
                    "/results/fault/players/" in w.update.name for w in batch._write_pbs
                ):
                    raise ServiceUnavailable("PRIVATE DATA")
                return original_commit(batch, *args, **kwargs)

            patch.setattr(WriteBatch, "commit", upload)
        else:
            original = Transaction._commit

            def finish(tx):
                if any(
                    w.update.fields.get("status", {}).string_value == "succeeded"
                    for w in tx._write_pbs
                    if "status" in w.update.fields
                ):
                    raise ServiceUnavailable("PRIVATE DATA")
                return original(tx)

            patch.setattr(Transaction, "_commit", finish)
        with pytest.raises(Exception):
            worker.process_next()
    assert pointer.get().to_dict()["latestRunId"] == "baseline"
    worker.recover()
    assert bad.get().to_dict()["status"] == (
        "queued" if boundary == "claim-read" else "failed"
    )
    if boundary == "claim-read":
        assert worker.process_next()
    else:
        queue(season, "next")
        assert worker.process_next()
    assert not worker.process_next()


@pytest.mark.parametrize("lost_reply", [ServiceUnavailable, RuntimeError])
def test_success_is_not_downgraded_when_final_commit_reply_is_lost(
    prepared, monkeypatch, lost_reply
):
    worker, season, pointer = prepared
    ref = queue(season, "committed")
    original = Transaction._commit

    def commit(tx):
        final = any(
            w.update.fields["status"].string_value == "succeeded"
            for w in tx._write_pbs
            if "status" in w.update.fields
        )
        value = original(tx)
        if final:
            raise lost_reply("PRIVATE RESPONSE")
        return value

    with monkeypatch.context() as patch:
        patch.setattr(Transaction, "_commit", commit)
        try:
            worker.process_next()
        except ServiceUnavailable:
            pass
    worker.recover()
    assert ref.get().to_dict()["status"] == "succeeded"
    assert pointer.get().to_dict()["latestRunId"] == "committed"
    assert (
        season.collection("results").document("committed").get().to_dict()["complete"]
    )
    assert not worker.process_next()


@pytest.mark.parametrize("committed", [False, True])
def test_failure_recording_outage_is_recovered_without_losing_request(
    prepared, monkeypatch, caplog, committed
):
    worker, season, pointer = prepared
    ref = queue(season, "failure-record")
    with monkeypatch.context() as patch:
        patch.setattr(
            module,
            "calculate_snapshot",
            Mock(side_effect=ValueError("PRIVATE INPUT AND SECRET")),
        )
        fail_run = worker.fail_run

        def failed_ack(*args):
            if committed:
                fail_run(*args)
            raise ServiceUnavailable("PRIVATE INPUT AND SECRET")

        patch.setattr(worker, "fail_run", failed_ack)
        with pytest.raises(ServiceUnavailable):
            worker.process_next()
    worker.recover()
    assert ref.get().to_dict()["status"] == "failed"
    assert "PRIVATE" not in ref.get().to_dict()["error"]
    assert "PRIVATE" not in caplog.text
    assert pointer.get().to_dict()["latestRunId"] == "baseline"


def test_deadline_interrupts_and_restores_signal_handler():
    previous = signal.getsignal(signal.SIGALRM)
    start = time.monotonic()
    with pytest.raises(RunDeadlineExceeded):
        with run_deadline(1):
            time.sleep(10)
    assert time.monotonic() - start < 3
    assert signal.getsignal(signal.SIGALRM) == previous
    assert signal.getitimer(signal.ITIMER_REAL) == (0, 0)


def test_materializations_include_project_identity(config):
    mongo = mongomock.MongoClient()
    first = Worker(config, Mock(), mongo)
    other = config.model_copy(
        update={
            "firestore": config.firestore.model_copy(
                update={"project_id": "demo-another-project"}
            )
        }
    )
    second = Worker(other, Mock(), mongo)
    assert first.db.name != second.db.name


@pytest.mark.parametrize("error", [AuthenticationError, AuthorizationError])
def test_redis_access_errors_are_not_retried_as_network_outages(error):
    assert not module.retryable(error("PRIVATE CREDENTIAL"))


def test_input_validation_does_not_publish_or_log_private_values(
    prepared, monkeypatch, caplog
):
    worker, season, pointer = prepared
    season.collection("settings").document("calculation").set(
        {
            "version": 1,
            "updatedBy": "test-officer",
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "defaultMythicStart": "PRIVATE INPUT",
        }
    )
    monkeypatch.setattr(module, "calculate_snapshot", calculate_snapshot)
    ref = queue(season, "invalid-input")
    assert worker.process_next()
    assert ref.get().to_dict()["status"] == "failed"
    assert "Invalid inputs" in ref.get().to_dict()["error"]
    assert "PRIVATE" not in ref.get().to_dict()["error"]
    assert "PRIVATE" not in caplog.text
    assert pointer.get().to_dict()["latestRunId"] == "baseline"


@pytest.mark.parametrize("boundary", ["config", "mongo-init"])
def test_startup_errors_are_sanitized(config, monkeypatch, boundary):
    from click.testing import CliRunner

    from pebble import web_cli

    if boundary == "config":
        monkeypatch.setattr(
            web_cli, "resources", Mock(side_effect=ValueError("PRIVATE SECRET"))
        )
    else:
        monkeypatch.setattr(web_cli, "resources", Mock(return_value=(config, Mock())))
        monkeypatch.setattr(
            web_cli, "MongoClient", Mock(side_effect=ValueError("PRIVATE SECRET"))
        )
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    result = CliRunner().invoke(web_cli.web, ["worker"])
    assert result.exit_code == 1
    assert "PRIVATE" not in result.output
    assert "ValueError" in result.output
    assert {sig: signal.getsignal(sig) for sig in previous} == previous


def test_fatal_loop_stops_heartbeat_thread(config, monkeypatch):
    worker = Worker(config, Mock(), mongomock.MongoClient())
    monkeypatch.setattr(worker, "heartbeat", Mock(side_effect=RuntimeError("PRIVATE")))
    monkeypatch.setattr(worker, "listen", Mock(side_effect=RuntimeError("fatal loop")))
    with pytest.raises(RuntimeError, match="fatal loop"):
        worker.run()
    assert not any(t.name == "pebble-heartbeat" for t in threading.enumerate())


@pytest.mark.parametrize(
    "error", [AutoReconnect, RedisConnectionError, ServiceUnavailable]
)
def test_service_backoff_is_bounded_and_sanitized(config, monkeypatch, caplog, error):
    worker = Worker(config, Mock(), mongomock.MongoClient())
    stop = Mock()
    stop.is_set.return_value = False
    stop.wait.side_effect = [False] * 7 + [True]
    monkeypatch.setattr(
        worker, "recover", Mock(side_effect=error("PRIVATE SERVICE CREDENTIAL"))
    )
    worker.listen(stop)
    assert [c.args[0] for c in stop.wait.call_args_list] == [1, 2, 4, 8, 16, 32, 60, 60]
    assert "PRIVATE" not in caplog.text
