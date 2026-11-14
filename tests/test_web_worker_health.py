"""Worker health probes never contact production services in these tests."""

import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
import requests
from pydantic import ValidationError

from pebble import web_worker as module
from pebble.web_models import WorkerConfig

PING_URL = "https://hc-ping.com/00000000-0000-0000-0000-000000000001"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def health(monkeypatch):
    monkeypatch.delenv("FIRESTORE_EMULATOR_HOST", raising=False)
    config = WorkerConfig(
        firestore={"project_id": "demo-health", "season_id": "test"},
        mongo={"uri": "mongodb://127.0.0.1", "db": "test"},
        wcl={"client_id": "test", "client_secret": "test"},
        healthchecks_ping_url=PING_URL,
    )
    client, mongo = MagicMock(), MagicMock()
    worker = module.Worker(config, client, mongo)
    query = Mock()
    query.stream.return_value = []
    monkeypatch.setattr(worker, "pending_runs", Mock(return_value=query))
    cache = MagicMock()
    cache.__enter__.return_value = cache
    redis_factory = Mock(return_value=cache)
    monkeypatch.setattr(module.redis.Redis, "from_url", redis_factory)
    reply = MagicMock(status_code=200, text="OK")
    reply.__enter__.return_value = reply
    post = Mock(return_value=reply)
    monkeypatch.setattr(module.requests, "post", post)
    clock = [0.0]
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    return SimpleNamespace(
        worker=worker,
        client=client,
        mongo=mongo,
        query=query,
        cache=cache,
        redis_factory=redis_factory,
        post=post,
        reply=reply,
        clock=clock,
    )


def request(status, age, *, run_id="request"):
    ref = Mock(id=run_id)
    snapshot = Mock(id=run_id, reference=ref, read_time=NOW)
    snapshot.to_dict.return_value = {
        "status": status,
        "requestedAt": NOW - timedelta(seconds=age),
        "startedAt": NOW - timedelta(seconds=age),
    }
    return snapshot


def fake_listener(health, monkeypatch, *, until=601, notify=False):
    """Advance only the scheduling clock; service checks remain exercised."""
    stop = threading.Event()
    watch = Mock(is_active=True)

    class Wake:
        signaled = False

        def set(self):
            self.signaled = True

        def clear(self):
            self.signaled = False

        def wait(self, seconds):
            health.clock[0] += seconds
            if health.clock[0] >= until:
                stop.set()
            return self.signaled

    monkeypatch.setattr(module.threading, "Event", Wake)
    query = Mock()

    def attach(callback):
        if notify:
            callback([], [], None)
        return watch

    query.on_snapshot.side_effect = attach
    monkeypatch.setattr(health.worker, "queued_runs", lambda: query)
    monkeypatch.setattr(health.worker, "recover", Mock())
    return stop, watch


def test_idle_probes_run_on_main_thread_every_five_minutes(health, monkeypatch):
    stop, watch = fake_listener(health, monkeypatch)
    times = []
    main_thread = threading.get_ident()

    def sent(*_, **__):
        assert threading.get_ident() == main_thread
        times.append(health.clock[0])
        return health.reply

    health.post.side_effect = sent
    monkeypatch.setattr(health.worker, "process_next", Mock())
    health.worker.listen(stop)
    assert times == [0, 300, 600]
    assert health.query.stream.call_count == 3
    assert health.client.document.return_value.set.call_count == 3
    health.worker.process_next.assert_not_called()
    watch.unsubscribe.assert_called_once()


def test_probe_picks_up_work_even_without_listener_notification(health, monkeypatch):
    stop, watch = fake_listener(health, monkeypatch)
    health.query.stream.return_value = [request("queued", 0)]
    process = Mock(side_effect=lambda **_: stop.set() or True)
    monkeypatch.setattr(health.worker, "process_next", process)
    health.worker.listen(stop)
    process.assert_called_once_with(stop=stop)
    assert health.clock[0] == 1
    watch.unsubscribe.assert_called_once()


def test_busy_queue_does_not_starve_probes_between_jobs(health, monkeypatch):
    stop, _ = fake_listener(health, monkeypatch, until=1000, notify=True)
    times = []
    health.post.side_effect = (
        lambda *_, **__: times.append(health.clock[0]) or health.reply
    )
    jobs = []

    def process(**_):
        jobs.append(health.clock[0])
        health.clock[0] += 120
        if len(jobs) == 7:
            stop.set()
        return True

    monkeypatch.setattr(health.worker, "process_next", process)
    health.worker.listen(stop)
    assert len(jobs) == 7
    assert times == [0, 361, 721]


def test_probe_checks_dependencies_then_acknowledged_write_then_ping(health):
    events = []
    health.query.stream.side_effect = lambda **_: events.append("read") or []
    health.mongo.admin.command.side_effect = lambda _: events.append("mongo")
    health.cache.ping.side_effect = lambda: events.append("redis")
    health.client.document.return_value.set.side_effect = (
        lambda *_, **__: events.append("write")
    )
    health.post.side_effect = lambda *_, **__: events.append("ping") or health.reply

    assert not health.worker.check_health()
    assert events == ["read", "mongo", "redis", "write", "ping"]
    health.post.assert_called_once_with(PING_URL, timeout=(3, 5), allow_redirects=False)
    health.client.document.assert_called_once_with("bot/status")
    assert health.worker._next_health_at == 300
    assert health.redis_factory.call_args.kwargs["socket_timeout"] == 5
    health.cache.__exit__.assert_called_once()


@pytest.mark.parametrize("boundary", ["read", "mongo", "redis", "write"])
def test_dependency_failure_withholds_success_and_resets_observation(
    health, caplog, boundary
):
    calls = {
        "read": health.query.stream,
        "mongo": health.mongo.admin.command,
        "redis": health.cache.ping,
        "write": health.client.document.return_value.set,
    }
    calls[boundary].side_effect = RuntimeError("PRIVATE CREDENTIAL")
    health.worker._queued_observation = ("request", 0)
    health.worker.check_health()
    health.post.assert_not_called()
    assert health.worker._queued_observation is None
    assert "RuntimeError" in caplog.text
    assert "PRIVATE" not in caplog.text
    if boundary != "write":
        health.client.document.return_value.set.assert_not_called()


def test_old_queue_gets_startup_opportunity_then_fails_and_recovers(
    health, monkeypatch
):
    queued = request("queued", 3600)
    health.query.stream.return_value = [queued]
    assert health.worker.check_health()
    health.post.assert_not_called()

    # Use the real processing entry point to record a claim attempt that leaves
    # the request outstanding (e.g. concurrent status changes during claim).
    monkeypatch.setattr(health.worker, "queued_runs", lambda: health.query)
    monkeypatch.setattr(health.worker, "claim", Mock(return_value=False))
    assert health.worker.process_next()
    health.clock[0] = 299
    health.worker.check_health()
    health.post.assert_not_called()
    health.clock[0] = 300
    writes_before = health.client.document.return_value.set.call_count
    assert health.worker.check_health()
    assert health.post.call_args.args[0] == PING_URL + "/fail"
    assert health.client.document.return_value.set.call_count == writes_before

    health.query.stream.return_value = []
    health.clock[0] = 600
    assert not health.worker.check_health()
    assert health.post.call_args.args[0] == PING_URL
    assert health.worker._queued_observation is None


@pytest.mark.parametrize("age,attempted", [(599, True), (600, False)])
def test_queue_requires_both_age_and_processing_opportunity(health, age, attempted):
    health.query.stream.return_value = [request("queued", age)]
    health.worker.check_health()
    health.clock[0] = 300
    if attempted:
        health.worker._attempted_queued_id = "request"
    health.worker.check_health()
    if age < module.QUEUED_STALE_SECONDS:
        assert health.post.call_args.args[0] == PING_URL
    else:
        health.post.assert_not_called()


def test_repeated_restart_cannot_mask_an_old_queued_request(health):
    health.query.stream.return_value = [request("queued", 3600)]
    for tick in range(5):
        health.clock[0] = tick * 300
        health.worker._queued_observation = None
        health.worker._attempted_queued_id = None
        assert health.worker.check_health()
    health.post.assert_not_called()
    health.client.document.return_value.set.assert_not_called()


def test_dependency_outage_restarts_queue_allowance(health):
    health.query.stream.return_value = [request("queued", 3600)]
    health.worker.check_health()
    health.worker._attempted_queued_id = "request"
    health.clock[0] = 300
    health.cache.ping.side_effect = RuntimeError("unavailable")
    health.worker.check_health()
    health.cache.ping.side_effect = None
    health.clock[0] = 600
    health.worker.check_health()
    health.post.assert_not_called()
    health.clock[0] = 900
    health.worker.check_health()
    assert health.post.call_args.args[0] == PING_URL + "/fail"


@pytest.mark.parametrize("age,failed", [(179, False), (180, True), (3600, True)])
def test_running_age_uses_started_at_and_server_read_time(health, age, failed):
    snapshot = request("running", age)
    # Requested long ago is not proof that the calculation has been running long.
    snapshot.to_dict.return_value["requestedAt"] = NOW - timedelta(days=1)
    health.query.stream.return_value = [snapshot]
    assert not health.worker.check_health()
    assert health.post.call_args.args[0] == PING_URL + ("/fail" if failed else "")
    assert health.client.document.return_value.set.call_count == (0 if failed else 1)


def test_missing_request_timestamp_never_renews_health(health):
    snapshot = request("running", 0)
    del snapshot.to_dict.return_value["startedAt"]
    health.query.stream.return_value = [snapshot]
    health.worker.check_health()
    assert health.post.call_args.args[0] == PING_URL + "/fail"
    health.client.document.return_value.set.assert_not_called()


def test_healthchecks_outage_does_not_undo_site_heartbeat(health, caplog):
    health.post.side_effect = requests.Timeout("PRIVATE URL " + PING_URL)
    health.worker.check_health()
    health.client.document.return_value.set.assert_called_once()
    assert "Timeout" in caplog.text
    assert "PRIVATE" not in caplog.text
    assert PING_URL not in caplog.text


@pytest.mark.parametrize(
    "status,body",
    [
        (200, "OK (not found)"),
        (200, "OK (rate limited)"),
        (503, "PRIVATE"),
        (302, "PRIVATE"),
    ],
)
def test_unaccepted_ping_is_logged_without_private_response(
    health, caplog, status, body
):
    health.reply.status_code, health.reply.text = status, body
    health.worker.check_health()
    assert "did not accept" in caplog.text
    assert "PRIVATE" not in caplog.text


@pytest.mark.parametrize("disabled", ["unset", "emulator"])
def test_local_workers_can_probe_without_external_pings(health, monkeypatch, disabled):
    if disabled == "unset":
        health.worker.config.healthchecks_ping_url = None
    else:
        monkeypatch.setenv("FIRESTORE_EMULATOR_HOST", "127.0.0.1:8281")
    health.worker.check_health()
    health.post.assert_not_called()
    health.client.document.return_value.set.assert_called_once()


def test_one_shot_never_renews_continuous_worker_health(health, monkeypatch, tmp_path):
    monkeypatch.setattr(module.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(health.worker, "recover", Mock())
    monkeypatch.setattr(health.worker, "process_next", Mock(return_value=False))
    health.worker.run(once=True)
    health.post.assert_not_called()
    health.client.document.return_value.set.assert_not_called()


@pytest.mark.parametrize("value", [0, 121, 1200, True])
def test_run_deadline_cannot_exceed_two_minutes(health, value):
    data = health.worker.config.model_dump()
    data["run_timeout_seconds"] = value
    with pytest.raises(ValidationError):
        WorkerConfig.model_validate(data)
    assert health.worker.config.run_timeout_seconds == 120


def test_private_environment_loads_secret_ping_url(health, monkeypatch, tmp_path):
    import yaml

    monkeypatch.setattr(module, "load_dotenv", Mock())
    for key in (
        "FIREBASE_PROJECT_ID",
        "PEBBLE_SEASON_ID",
        "MONGODB_URI",
        "REDIS_URL",
        "WCL_CLIENT_ID",
        "WCL_CLIENT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("PEBBLE_HEALTHCHECKS_PING_URL", PING_URL)
    data = health.worker.config.model_dump(exclude={"healthchecks_ping_url"})
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    config = module.load_worker_config(str(path))
    assert config.healthchecks_ping_url.get_secret_value() == PING_URL
    assert PING_URL not in repr(config)


@pytest.mark.parametrize(
    "url",
    [
        "http://hc-ping.com/uuid",
        "https://example.com/secret",
        PING_URL + "/fail",
        PING_URL + "?secret=1",
    ],
)
def test_ping_url_rejects_non_check_destinations(health, url):
    data = health.worker.config.model_dump()
    data["healthchecks_ping_url"] = url
    with pytest.raises(ValidationError):
        WorkerConfig.model_validate(data)
