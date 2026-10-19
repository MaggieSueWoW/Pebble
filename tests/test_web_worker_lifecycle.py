"""Real worker processes, demo Firestore, disposable Mongo/Redis and local WCL.

Opt in with FIRESTORE_EMULATOR_HOST, PEBBLE_TEST_MONGO_URI and
PEBBLE_TEST_REDIS_URL, all pointing to loopback-only disposable services.
"""

from __future__ import annotations

import json
import os
import select
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest
import yaml
from google.cloud import firestore
from pymongo import MongoClient
from redis import Redis

import pebble
from pebble.web_models import wall_ms
from pebble.web_worker import firestore_client

ROOT = Path(pebble.__file__).resolve().parents[1]
PROJECT = "demo-pebble-bot1"
CODE = "aB3dE6gH9jK2mN5p"


def wait_for(predicate, *, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError("Timed out waiting for isolated worker state")


class FaultProxy:
    """Cut real TCP connections without stopping a shared service or emulator."""

    def __init__(self, host, port):
        assert host in {"127.0.0.1", "localhost"}
        self.online = threading.Event()
        self.online.set()
        self.dropped = threading.Event()
        proxy = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                if not proxy.online.is_set():
                    proxy.dropped.set()
                    return
                try:
                    with socket.create_connection((host, port), timeout=2) as upstream:
                        while proxy.online.is_set():
                            readable, _, _ = select.select(
                                [self.request, upstream], [], [], 0.1
                            )
                            for source in readable:
                                payload = source.recv(65536)
                                if not payload:
                                    return
                                target = (
                                    upstream if source is self.request else self.request
                                )
                                target.sendall(payload)
                        proxy.dropped.set()
                except OSError:
                    pass

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.online.clear()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)


class FakeWCL:
    def __init__(self):
        self.status = 200
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
        self.requests = 0
        wcl = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if self.path == "/token":
                    status = 200
                    value = {"access_token": "fake-token", "expires_in": 3600}
                else:
                    wcl.requests += 1
                    wcl.entered.set()
                    wcl.gate.wait(15)
                    status = wcl.status
                    start = wall_ms("2026-10-14T19:00")
                    report = {
                        "title": "Isolated test report",
                        "startTime": start,
                        "endTime": start + 4200000,
                        "masterData": {
                            "actors": [
                                {
                                    "id": 1,
                                    "name": "Example",
                                    "type": "Player",
                                    "subType": "Druid",
                                }
                            ]
                        },
                        "fights": [
                            {
                                "id": i,
                                "encounterID": 1,
                                "difficulty": 5,
                                "startTime": offset,
                                "endTime": offset + 600000,
                                "friendlyPlayers": [1],
                            }
                            for i, offset in [(1, 0), (2, 3600000)]
                        ],
                    }
                    value = (
                        {"data": {"reportData": {"report": report}}}
                        if status == 200
                        else {"error": "PRIVATE ERROR BODY"}
                    )
                encoded = json.dumps(value).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)


@pytest.fixture
def isolated(tmp_path):
    emulator = os.getenv("FIRESTORE_EMULATOR_HOST")
    mongo_uri = os.getenv("PEBBLE_TEST_MONGO_URI")
    redis_url = os.getenv("PEBBLE_TEST_REDIS_URL")
    if not all([emulator, mongo_uri, redis_url]):
        pytest.skip(
            "Provide isolated loopback Firestore/Mongo/Redis endpoints for lifecycle tests"
        )
    for value in ["http://" + emulator, mongo_uri, redis_url]:
        assert urlparse(value).hostname in {
            "localhost",
            "127.0.0.1",
        }, "Refusing non-loopback test services"
    proxies = {}
    for name, uri in [
        ("firestore", "http://" + emulator),
        ("mongo", mongo_uri),
        ("redis", redis_url),
    ]:
        parsed = urlparse(uri)
        proxies[name] = FaultProxy(parsed.hostname, parsed.port)
    wcl = FakeWCL()
    season_id = "lifecycle-" + uuid4().hex
    client = firestore_client(PROJECT)
    season = client.collection("seasons").document(season_id)
    season.set({"displayName": "Isolated lifecycle tests"})
    client.document("officers/test-officer").set({"enabled": True})
    metadata = {
        "version": 1,
        "updatedBy": "test-officer",
        "updatedAt": datetime.now(timezone.utc),
    }
    season.collection("players").document("example").set(
        {**metadata, "name": "Example", "className": "Druid", "active": True}
    )
    season.collection("reports").document(CODE).set({**metadata, "enabled": True})
    prefix = f"bot1:{season_id}:"
    cfg = {
        "firestore": {"project_id": PROJECT, "season_id": season_id},
        "mongo": {
            "uri": f"mongodb://127.0.0.1:{proxies['mongo'].port}",
            "db": "bot1_tests",
        },
        "redis": {
            "url": f"redis://127.0.0.1:{proxies['redis'].port}/0",
            "key_prefix": prefix,
        },
        "wcl": {
            "client_id": "fake",
            "client_secret": "fake",
            "base_url": f"http://127.0.0.1:{wcl.port}/api",
            "token_url": f"http://127.0.0.1:{wcl.port}/token",
        },
        "run_timeout_seconds": 90,
    }
    cfg_path = tmp_path / "worker.yaml"
    processes = []
    logs = []

    class Harness:
        def start(self, *, ready=True, timeout=90):
            cfg["run_timeout_seconds"] = timeout
            cfg_path.write_text(yaml.safe_dump(cfg))
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(
                    ("GOOGLE_", "FIREBASE_", "WCL_", "MONGODB_", "REDIS_", "PEBBLE_")
                )
            }
            env.update(
                {
                    "FIRESTORE_EMULATOR_HOST": f"127.0.0.1:{proxies['firestore'].port}",
                    "PYTHONPATH": str(ROOT),
                    "PYTHONUNBUFFERED": "1",
                    "PYTHON_DOTENV_DISABLED": "1",
                }
            )
            log_path = tmp_path / f"worker-{len(processes)}.log"
            stream = log_path.open("w")
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "pebble.cli",
                    "web",
                    "worker",
                    "--config",
                    str(cfg_path),
                ],
                cwd=tmp_path,
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            processes.append(process)
            logs.append((stream, log_path))
            if ready:
                wait_for(
                    lambda: "Listening for queued Pebble runs" in log_path.read_text()
                )
            return process

        def queue(self, name):
            ref = season.collection("runs").document(name)
            ref.set(
                {
                    "status": "queued",
                    "requestedBy": "test-officer",
                    "requestedAt": firestore.SERVER_TIMESTAMP,
                }
            )
            return ref

        def status(self, name):
            doc = season.collection("runs").document(name).get(timeout=5)
            return (doc.to_dict() or {}).get("status")

        def complete(self, name):
            wait_for(lambda: self.status(name) == "succeeded")
            result = (
                season.collection("results").document(name).get(timeout=5).to_dict()
            )
            assert result["complete"]

        def pointer(self):
            return (
                client.document(f"publicSeasons/{season_id}")
                .get(timeout=5)
                .to_dict()["latestRunId"]
            )

        def clear_cache(self):
            with Redis.from_url(redis_url, socket_timeout=2) as redis:
                redis.delete(prefix + CODE)

        def log_text(self):
            return "\n".join(path.read_text() for _, path in logs)

    harness = Harness()
    harness.proxies = proxies
    harness.wcl = wcl
    harness.season = season
    try:
        with MongoClient(mongo_uri, serverSelectionTimeoutMS=2000) as mongo:
            mongo.admin.command("ping")
        with Redis.from_url(redis_url, socket_timeout=2) as redis:
            redis.ping()
        yield harness
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=25)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for stream, _ in logs:
            stream.close()
        for proxy in proxies.values():
            proxy.close()
        wcl.close()
        client.close()


def test_idle_sigterm_and_shared_lock(isolated):
    process = isolated.start()
    second = isolated.start(ready=False)
    second.wait(timeout=15)
    assert second.returncode != 0
    started = time.monotonic()
    process.terminate()
    process.wait(timeout=10)
    assert process.returncode == 0, isolated.log_text()
    assert time.monotonic() - started < 5


def test_midrun_sigterm_finishes_only_current_run_then_restart_drains_queue(isolated):
    process = isolated.start()
    isolated.wcl.gate.clear()
    isolated.queue("drain")
    assert isolated.wcl.entered.wait(10)
    isolated.queue("next")
    process.send_signal(signal.SIGTERM)
    assert process.poll() is None
    isolated.wcl.gate.set()
    process.wait(timeout=15)
    assert process.returncode == 0, isolated.log_text()
    assert isolated.status("drain") == "succeeded"
    assert isolated.status("next") == "queued"
    isolated.start()
    isolated.complete("next")


def test_forced_termination_recovers_without_changing_previous_results(isolated):
    process = isolated.start()
    isolated.queue("baseline")
    isolated.complete("baseline")
    isolated.clear_cache()
    isolated.wcl.entered.clear()
    isolated.wcl.gate.clear()
    isolated.queue("killed")
    assert isolated.wcl.entered.wait(10)
    process.kill()
    process.wait(timeout=5)
    assert isolated.status("killed") == "running"
    assert isolated.pointer() == "baseline"
    isolated.wcl.gate.set()
    isolated.queue("after-kill")
    isolated.start()
    isolated.complete("after-kill")
    assert isolated.status("killed") == "failed"
    assert isolated.pointer() == "after-kill"


def test_run_deadline_interrupts_blocked_http_and_allows_next_run(isolated):
    isolated.start(timeout=2)
    isolated.wcl.gate.clear()
    started = time.monotonic()
    ref = isolated.queue("timeout")
    wait_for(lambda: isolated.status("timeout") == "failed", timeout=8)
    assert time.monotonic() - started < 6
    assert "time limit" in ref.get().to_dict()["error"]
    isolated.wcl.gate.set()
    isolated.queue("after-timeout")
    isolated.complete("after-timeout")


@pytest.mark.parametrize("dependency", ["mongo", "redis", "wcl"])
def test_real_dependency_outage_recovers_and_preserves_prior_publication(
    isolated, dependency
):
    isolated.start()
    isolated.queue("baseline")
    isolated.complete("baseline")
    isolated.clear_cache()
    if dependency == "wcl":
        isolated.wcl.status = 503
    else:
        isolated.proxies[dependency].online.clear()
    isolated.queue("outage")
    wait_for(lambda: "Worker dependency unavailable" in isolated.log_text())
    assert isolated.pointer() == "baseline"
    if dependency == "wcl":
        isolated.wcl.status = 200
    else:
        isolated.proxies[dependency].online.set()
    wait_for(lambda: isolated.status("outage") == "failed")
    isolated.queue("after-outage")
    isolated.complete("after-outage")
    assert "PRIVATE" not in isolated.log_text()


def test_firestore_offline_startup_and_stream_reconnect_receive_missed_work(isolated):
    proxy = isolated.proxies["firestore"]
    proxy.online.clear()
    isolated.start(ready=False)
    isolated.queue("offline-start")
    assert proxy.dropped.wait(10)
    proxy.online.set()
    isolated.complete("offline-start")
    proxy.dropped.clear()
    proxy.online.clear()
    isolated.queue("missed-event")
    assert proxy.dropped.wait(10)
    assert isolated.status("missed-event") == "queued"
    proxy.online.set()
    isolated.complete("missed-event")


def test_permanent_wcl_failure_is_actionable_and_sanitized(isolated):
    isolated.start()
    isolated.wcl.status = 403
    ref = isolated.queue("forbidden")
    wait_for(lambda: isolated.status("forbidden") == "failed")
    assert "WCL accessibility" in ref.get().to_dict()["error"]
    assert isolated.wcl.requests == 1
    assert "PRIVATE" not in isolated.log_text()
