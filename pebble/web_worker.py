"""Local Firestore worker using Pebble's existing calculation engine."""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
import signal
import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from google.api_core import exceptions as api_errors
from google.api_core.retry import Retry
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from pymongo.errors import ConnectionFailure, ExecutionTimeout
from redis.exceptions import AuthenticationError, AuthorizationError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from .ingest import WOW_CLASS_COLORS, persist_report_fights
from .mongo_client import ensure_indexes
from .pipeline import compute_results
from .utils.time import ms_to_pt_iso, night_id_from_ms
from .wcl_client import WCLClient, _is_retryable
from .web_identity import mapped_bundle
from .web_models import (
    INPUT_MODELS,
    CalculationInput,
    WorkerConfig,
    validate_snapshot,
    wall_ms,
)
from .web_publication import publication_batches

log = logging.getLogger(__name__)
RPC_OPTIONS = {"timeout": 10, "retry": Retry(initial=0.2, maximum=1, timeout=10)}


class InputError(ValueError):
    """An actionable message authored by the worker, without raw service data."""


class RunDeadlineExceeded(BaseException):
    """Interrupt even dependency retry wrappers when a run exceeds its budget."""


def retryable(exc: BaseException) -> bool:
    """Classify service outages without logging their potentially private text."""
    if isinstance(exc, (AuthenticationError, AuthorizationError)):
        return False
    return isinstance(
        exc,
        (
            api_errors.ServiceUnavailable,
            api_errors.DeadlineExceeded,
            api_errors.Aborted,
            api_errors.InternalServerError,
            api_errors.TooManyRequests,
            api_errors.RetryError,
            ConnectionFailure,
            ExecutionTimeout,
            RedisConnectionError,
            RedisTimeoutError,
        ),
    ) or _is_retryable(exc)


@contextmanager
def run_deadline(seconds: int):
    """Bound main-thread processing; SIGTERM still drains within this budget."""

    def expired(*_):
        raise RunDeadlineExceeded()

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


WORKING_COLLECTIONS = (
    "reports",
    "actors",
    "fights_all",
    "team_roster",
    "night_qa",
    "participation_m",
    "blocks",
    "bench_night_totals",
    "bench_week_totals",
    "bench_rankings",
)


def load_worker_config(path: str) -> WorkerConfig:
    """Read local secrets and project configuration without contacting Sheets."""
    load_dotenv(override=False)
    data = yaml.safe_load(Path(path).read_text()) or {}
    for section, fields in {
        "wcl": {"client_id": "WCL_CLIENT_ID", "client_secret": "WCL_CLIENT_SECRET"},
        "mongo": {"uri": "MONGODB_URI"},
        "redis": {"url": "REDIS_URL"},
        "firestore": {
            "project_id": "FIREBASE_PROJECT_ID",
            "season_id": "PEBBLE_SEASON_ID",
        },
    }.items():
        for key, env in fields.items():
            if os.getenv(env):
                data.setdefault(section, {})[key] = os.environ[env]
    return WorkerConfig.model_validate(data)


def firestore_client(project_id: str, credentials_file: str | None = None):
    """Use ADC in production and anonymous credentials only for the emulator."""
    kwargs = (
        {"credentials": AnonymousCredentials()}
        if os.getenv("FIRESTORE_EMULATOR_HOST")
        else {}
    )
    if credentials_file and not kwargs:
        from google.oauth2.service_account import Credentials

        kwargs["credentials"] = Credentials.from_service_account_file(credentials_file)
    return firestore.Client(project=project_id, **kwargs)


def capture_inputs(client, season_id: str) -> tuple[dict, dict]:
    """Read every input collection at one Firestore transaction snapshot."""
    season = client.collection("seasons").document(season_id)

    @firestore.transactional
    def read(transaction):
        result, versions = {}, {}
        for name in (*INPUT_MODELS, "settings"):
            result[name] = {}
            for document in season.collection(name).get(
                transaction=transaction, **RPC_OPTIONS
            ):
                value = document.to_dict()
                result[name][document.id] = value
                versions[f"{name}/{document.id}"] = value.get("version")
        return result, versions

    return read(client.transaction(read_only=True, max_attempts=3))


def calculate_snapshot(
    config: WorkerConfig, db, raw: dict, stage=lambda _: None
) -> dict:
    """Rebuild local materializations and return a public-safe result projection."""
    try:
        inputs = validate_snapshot(raw)
        calculation = raw.get("settings", {}).get("calculation")
        time_config = (
            CalculationInput.model_validate(calculation).time_config()
            if calculation
            else config.time
        )
    except ValueError:
        raise InputError(
            "Invalid inputs. Check roster, reports, availability and calculation settings."
        ) from None
    config = config.model_copy(update={"time": time_config})
    if config.time.tz != "America/Los_Angeles":
        raise InputError("The existing calculation engine requires America/Los_Angeles")
    enabled = [code for code, report in inputs["reports"].items() if report.enabled]
    if enabled and not inputs["players"]:
        raise InputError("Add the team roster before processing reports")
    # This database is dedicated to web runs; never clear the existing Sheets DB.
    for collection in WORKING_COLLECTIONS:
        db[collection].delete_many({})
    ensure_indexes(db)
    roster = [
        {
            "main": p.name,
            "join_night": p.joinDate,
            "leave_night": p.leaveDate,
            "active": p.active,
            "class_color": WOW_CLASS_COLORS.get(
                p.className.replace(" ", "").upper(), "#AAAAAA"
            ),
        }
        for p in inputs["players"].values()
    ]
    if roster:
        db.team_roster.insert_many(roster)
    stage("Fetching reports")
    wcl = WCLClient(
        config.wcl.client_id,
        config.wcl.client_secret,
        base_url=config.wcl.base_url,
        token_url=config.wcl.token_url,
        redis_url=config.redis.url,
        cache_prefix=config.redis.key_prefix,
    )
    summaries = {}
    try:
        for code in enabled:
            try:
                bundle = wcl.fetch_report_bundle(code)
                start, end = int(bundle["startTime"]), int(bundle["endTime"])
                if start <= 0 or end < start:
                    raise ValueError("Invalid report timestamps")
            except Exception as exc:
                if retryable(exc):
                    raise
                if isinstance(exc, (AuthenticationError, AuthorizationError)):
                    raise InputError(
                        "Redis access denied. Check worker cache credentials, then run again."
                    ) from None
                raise InputError(
                    f"Could not fetch report {code}. Check its code and WCL accessibility, then run again."
                ) from exc
            night = night_id_from_ms(start)
            doc = {
                "code": code,
                "title": bundle.get("title", ""),
                "start_ms": start,
                "end_ms": end,
                "night_id": night,
            }
            timing = inputs["nightOverrides"].get(night)
            for field, attribute in (
                ("break_override_start_ms", "breakStart"),
                ("break_override_end_ms", "breakEnd"),
                ("mythic_override_start_ms", "start"),
                ("mythic_override_end_ms", "end"),
            ):
                doc[field] = wall_ms(getattr(timing, attribute)) if timing else None
            db.reports.insert_one(doc)
            try:
                actors_bundle = mapped_bundle(bundle, inputs)
            except ValueError:
                raise InputError(
                    "Report region is missing. Refresh the report cache before resolving character mappings."
                ) from None
            persist_report_fights(db, code, actors_bundle, start, night)
            summaries[code] = {
                "title": doc["title"],
                "night": night,
                "checkedAt": datetime.now(timezone.utc),
            }
    finally:
        wcl.close()
    stage("Calculating results")
    overrides = {}
    for a in inputs["availabilityOverrides"].values():
        name = inputs["players"][a.playerId].name
        overrides.setdefault(a.nightId, {})[name] = {
            "pre": a.pre.effective(),
            "post": a.post.effective(),
        }
    compute_results(config, db, log, overrides_by_night=overrides)
    qa = {r["night_id"]: r for r in db.night_qa.find({}, {"_id": 0})}

    def player_metadata(pid, player):
        return {
            "id": pid,
            "name": player.name,
            "className": player.className,
            "role": player.role,
            "color": WOW_CLASS_COLORS.get(
                player.className.replace(" ", "").upper(), "#AAAAAA"
            ),
        }

    metadata = {p.name: player_metadata(pid, p) for pid, p in inputs["players"].items()}
    nightly = list(db.bench_night_totals.find({}, {"_id": 0}))

    def player_row(name, rows):
        return {
            **metadata[name],
            "played": sum(r["played_total_min"] for r in rows),
            "prePlayed": sum(r["played_pre_min"] for r in rows),
            "postPlayed": sum(r["played_post_min"] for r in rows),
            "bench": sum(r["bench_total_min"] for r in rows),
            "preBench": sum(r["bench_pre_min"] for r in rows),
            "postBench": sum(r["bench_post_min"] for r in rows),
            "nights": sum(
                r["played_total_min"] + r["bench_total_min"] > 0 for r in rows
            ),
            "overridden": any(
                inputs["availabilityOverrides"].get(
                    f"{r['night_id']}_{metadata[name]['id']}"
                )
                and (
                    inputs["availabilityOverrides"][
                        f"{r['night_id']}_{metadata[name]['id']}"
                    ].pre.mode
                    != "automatic"
                    or inputs["availabilityOverrides"][
                        f"{r['night_id']}_{metadata[name]['id']}"
                    ].post.mode
                    != "automatic"
                )
                for r in rows
            ),
        }

    totals = [
        player_row(p.name, [r for r in nightly if r["main"] == p.name])
        for p in inputs["players"].values()
        if p.active
    ]
    nights = []
    warnings = []
    for night, q in sorted(qa.items(), reverse=True):
        if q["mythic_end_ms"] <= q["mythic_start_ms"]:
            raise InputError(f"Mythic end must follow start on {night}")

        def wall(ms):
            return ms_to_pt_iso(ms)[:16] if ms is not None else ""

        timing = {
            "start": wall(q["mythic_start_ms"]),
            "end": wall(q["mythic_end_ms"]),
            "breakStart": wall(q["break_start_ms"]),
            "breakEnd": wall(q["break_end_ms"]),
            "notes": "",
        }
        rows = [
            player_row(
                p.name,
                [r for r in nightly if r["night_id"] == night and r["main"] == p.name],
            )
            for p in inputs["players"].values()
            if p.active
        ]
        nights.append(
            {
                "night": night,
                "duration": q["mythic_pre_min"] + q["mythic_post_min"],
                "preMinutes": q["mythic_pre_min"],
                "postMinutes": q["mythic_post_min"],
                "timing": timing,
                "players": rows,
                "reports": len(q["reports"]),
            }
        )
        if q["not_on_roster_mains"]:
            warnings.append(
                f"{night}: unknown characters: {', '.join(q['not_on_roster_mains'])}"
            )
    observed = {r["night_id"] for r in db.reports.find({})}
    for night in sorted(observed - set(qa)):
        warnings.append(
            f"{night}: no Mythic encounters; no bench totals for this night"
        )
    return {
        "players": totals,
        "nights": nights,
        "weeks": list(db.bench_week_totals.find({}, {"_id": 0})),
        "reportCount": len(enabled),
        "reportSummaries": summaries,
        "warnings": warnings,
    }


class Worker:
    """Process one season serially and atomically replace complete results."""

    def __init__(self, config, client, mongo):
        self.config, self.client = config, client
        self.season = client.collection("seasons").document(config.firestore.season_id)
        identity = f"{config.firestore.project_id}/{config.firestore.season_id}"
        suffix = hashlib.sha256(identity.encode()).hexdigest()[:12]
        self.db = mongo[f"{config.mongo.db}_web_{suffix}"]
        self.current_run = None

    def heartbeat(self):
        self.client.document("bot/status").set(
            {
                "lastHeartbeatAt": firestore.SERVER_TIMESTAMP,
                "currentRunId": self.current_run,
            },
            **RPC_OPTIONS,
        )

    def fail_run(self, ref, message):
        """Settle pending/active work; never downgrade a committed success."""

        @firestore.transactional
        def fail(tx):
            value = ref.get(transaction=tx, **RPC_OPTIONS).to_dict() or {}
            if value.get("status") in {"queued", "running"}:
                tx.update(
                    ref,
                    {
                        "status": "failed",
                        "completedAt": firestore.SERVER_TIMESTAMP,
                        "error": message,
                    },
                )

        fail(self.client.transaction(max_attempts=3))

    def recover(self):
        for run in (
            self.season.collection("runs")
            .where(filter=FieldFilter("status", "==", "running"))
            .stream(**RPC_OPTIONS)
        ):
            self.fail_run(
                run.reference,
                "Worker interrupted by shutdown or a service outage. Run Pebble again.",
            )

    def queued_runs(self):
        """Select the oldest queued request for both listening and claiming."""
        return (
            self.season.collection("runs")
            .where(filter=FieldFilter("status", "==", "queued"))
            .order_by("requestedAt")
            .limit(1)
        )

    def claim(self, ref, stop: threading.Event | None = None):
        """Transactionally claim a queued request after checking officer access."""

        @firestore.transactional
        def claim(tx):
            run = ref.get(transaction=tx, **RPC_OPTIONS).to_dict()
            if run.get("status") != "queued" or (stop is not None and stop.is_set()):
                return False
            officer = self.client.document(
                f"officers/{run.get('requestedBy', '')}"
            ).get(transaction=tx, **RPC_OPTIONS)
            # A stop may arrive while either transaction read is in flight.
            # Leave the request queued instead of accepting new work to drain.
            if stop is not None and stop.is_set():
                return False
            if not officer.exists or not officer.to_dict().get("enabled"):
                tx.update(
                    ref,
                    {
                        "status": "failed",
                        "completedAt": firestore.SERVER_TIMESTAMP,
                        "error": "Requester no longer has officer access.",
                    },
                )
                return False
            tx.update(
                ref,
                {
                    "status": "running",
                    "startedAt": firestore.SERVER_TIMESTAMP,
                    "stage": "Validating inputs",
                },
            )
            return True

        return claim(self.client.transaction(max_attempts=3))

    def process_next(self, stop: threading.Event | None = None) -> bool:
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError(
                "Worker processing requires the main thread for deadlines"
            )
        if stop is not None and stop.is_set():
            return False
        queued = list(self.queued_runs().stream(**RPC_OPTIONS))
        if not queued or (stop is not None and stop.is_set()):
            return False
        ref = queued[0].reference

        try:
            with run_deadline(self.config.run_timeout_seconds):
                if not self.claim(ref, stop=stop):
                    return True
                self.current_run = ref.id
                self.compute(ref)
        except (Exception, RunDeadlineExceeded) as exc:
            if retryable(exc):
                # Resolve an ambiguous claim/publication on reconnect before
                # accepting another request. Never recompute a completed run.
                raise
            log.warning(
                "Web run failed (%s)", type(exc).__name__, extra={"run_id": ref.id}
            )
            message = (
                "Run exceeded its time limit. Reduce the report set and Run Pebble again."
                if isinstance(exc, RunDeadlineExceeded)
                else (
                    str(exc)
                    if isinstance(exc, InputError)
                    else "Processing failed. Check worker access and inputs, then Run Pebble again."
                )
            )
            self.fail_run(ref, message)
        finally:
            self.current_run = None
        return True

    def compute(self, ref):
        """Capture, compute and publish one already-claimed request."""
        raw, versions = capture_inputs(self.client, self.config.firestore.season_id)
        self.db.input_snapshots.replace_one(
            {"run_id": ref.id}, {"run_id": ref.id, "inputs": raw}, upsert=True
        )
        digest = hashlib.sha256(
            json.dumps(raw, default=str, sort_keys=True).encode()
        ).hexdigest()
        ref.update(
            {
                "inputVersions": versions,
                "inputSnapshotHash": digest,
                "engineVersion": "0.1.2",
            },
            **RPC_OPTIONS,
        )
        output = calculate_snapshot(
            self.config,
            self.db,
            raw,
            stage=lambda stage: ref.update({"stage": stage}, **RPC_OPTIONS),
        )
        ref.update(
            {
                "stage": "Updating results",
                "reportSummaries": output["reportSummaries"],
                "warnings": output["warnings"],
            },
            **RPC_OPTIONS,
        )
        self.publish(ref, output)

    def publish(self, run_ref, output):
        root = self.season.collection("results").document(run_ref.id)
        writes = [
            (root.collection("players").document(p["id"]), p) for p in output["players"]
        ]
        writes += [
            (root.collection("nights").document(n["night"]), n)
            for n in output["nights"]
        ]
        weeks = {}
        for row in output["weeks"]:
            weeks.setdefault(row["game_week"], []).append(
                {
                    k: row[k]
                    for k in (
                        "game_week",
                        "main",
                        "played_min",
                        "bench_min",
                        "bench_pre_min",
                        "bench_post_min",
                    )
                }
            )
        writes += [
            (root.collection("weeks").document(week), {"players": rows})
            for week, rows in weeks.items()
        ]
        # Validate every document before creating staging data or uploading a batch.
        try:
            publication_batches(
                [
                    (
                        root,
                        {
                            "complete": True,
                            "reportCount": output["reportCount"],
                            "warnings": output["warnings"],
                        },
                    )
                ]
            )
            batches = publication_batches(writes)
        except ValueError:
            raise InputError(
                "Results exceed the supported Firestore size budget. Reduce the season or roster size."
            ) from None
        root.set({"complete": False}, **RPC_OPTIONS)
        for writes_batch in batches:
            batch = self.client.batch()
            for ref, value in writes_batch:
                batch.set(ref, value)
            batch.commit(**RPC_OPTIONS)

        @firestore.transactional
        def finish(tx):
            run = run_ref.get(transaction=tx, **RPC_OPTIONS).to_dict()
            if run["status"] == "succeeded":
                return
            if run["status"] != "running":
                raise ValueError("Run was interrupted before results were committed")
            season_data = self.season.get(transaction=tx, **RPC_OPTIONS).to_dict() or {}
            tx.set(
                root,
                {
                    "complete": True,
                    "completedAt": firestore.SERVER_TIMESTAMP,
                    "reportCount": output["reportCount"],
                    "warnings": output["warnings"],
                },
            )
            tx.set(
                self.client.document(
                    f"publicSeasons/{self.config.firestore.season_id}"
                ),
                {
                    "latestRunId": run_ref.id,
                    "displayName": season_data.get(
                        "displayName", self.config.firestore.season_id
                    ),
                    "completedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            tx.update(
                run_ref,
                {"status": "succeeded", "completedAt": firestore.SERVER_TIMESTAMP},
            )

        finish(self.client.transaction(max_attempts=3))

    def listen(self, stop: threading.Event):
        """Wake on queue changes; let the SDK resume transient stream failures."""
        wake = threading.Event()
        watch = None
        retry_delay = 1
        needs_recovery = True

        def changed(_snapshot, _changes, _read_time):
            # Never compute on the SDK callback thread. Coalesce notifications
            # while the main thread drains requests serially.
            wake.set()

        try:
            while not stop.is_set():
                try:
                    if needs_recovery:
                        self.recover()
                        needs_recovery = False
                    if watch is None:
                        watch = self.queued_runs().on_snapshot(changed)
                        log.info("Listening for queued Pebble runs")
                    if not watch.is_active:
                        log.warning("Queue listener stopped; reconnecting")
                        watch.unsubscribe()
                        watch = None
                    else:
                        # This timeout only checks local listener health; it
                        # makes no Firestore query when there is no notification.
                        if not wake.wait(1):
                            continue
                        wake.clear()
                        while not stop.is_set() and self.process_next(stop=stop):
                            pass
                        retry_delay = 1
                        continue
                except Exception as exc:
                    if not retryable(exc):
                        raise
                    log.warning(
                        "Worker dependency unavailable; reconnecting (%s)",
                        type(exc).__name__,
                    )
                    if watch is not None:
                        watch.unsubscribe()
                        watch = None
                    # A network failure during processing may leave a claimed
                    # run interrupted. Resolve it before accepting more work.
                    needs_recovery = True
                if stop.wait(retry_delay):
                    break
                retry_delay = min(retry_delay * 2, 60)
        finally:
            if watch is not None:
                watch.unsubscribe()

    def run(self, once=False, stop: threading.Event | None = None):
        identity = (
            f"{self.config.firestore.project_id}/{self.config.firestore.season_id}"
        )
        lock_path = (
            Path(tempfile.gettempdir())
            / f"pebble-{hashlib.sha256(identity.encode()).hexdigest()[:20]}.lock"
        )
        with lock_path.open("w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise InputError(
                    "A local worker is already running for this project and season"
                ) from exc
            if once:
                self.recover()
                self.heartbeat()
                self.process_next(stop=stop)
                return
            stop = stop if stop is not None else threading.Event()

            heartbeat_stop = threading.Event()

            def beat():
                while not heartbeat_stop.is_set():
                    try:
                        self.heartbeat()
                    except Exception:
                        log.warning("Could not update worker heartbeat")
                    heartbeat_stop.wait(60)

            thread = threading.Thread(target=beat, name="pebble-heartbeat", daemon=True)
            thread.start()
            try:
                self.listen(stop)
            finally:
                stop.set()
                heartbeat_stop.set()
                thread.join(timeout=21)
                if thread.is_alive():
                    raise RuntimeError("Heartbeat failed to stop within the RPC budget")
