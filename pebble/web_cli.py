"""Commands for administering Firestore inputs and the local worker."""

from __future__ import annotations

import json
import logging
import signal
import threading
from datetime import datetime, timezone

import click
from google.cloud import firestore
from pymongo import MongoClient

from .web_models import CalculationInput
from .web_worker import InputError, Worker, firestore_client, load_worker_config


@click.group()
def web():
    """Manage the Firestore web interface and run its local worker."""


def resources(config):
    settings = load_worker_config(config)
    client = firestore_client(
        settings.firestore.project_id, settings.firestore.credentials_file
    )
    return settings, client


@web.command("worker")
@click.option("--config", default="config-web.yaml", show_default=True)
@click.option("--once", is_flag=True, help="Process at most one queued run and exit.")
def worker(config, once):
    """Read queued runs, compute locally, and update complete public results."""
    logging.basicConfig(level=logging.INFO)
    try:
        settings, client = resources(config)
    except Exception as exc:
        raise click.ClickException(
            f"Worker configuration unavailable ({type(exc).__name__}). Check config and credentials."
        ) from None
    stop = threading.Event()
    previous = {
        sig: signal.signal(sig, lambda *_: stop.set())
        for sig in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        with MongoClient(
            settings.mongo.uri,
            timeoutMS=10000,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        ) as mongo:
            Worker(settings, client, mongo).run(once=once, stop=stop)
    except Exception as exc:
        raise click.ClickException(
            str(exc)
            if isinstance(exc, InputError)
            else f"Worker stopped ({type(exc).__name__}). Check worker configuration, service access and logs."
        ) from None
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        client.close()


@web.command("init-season")
@click.option("--config", default="config-web.yaml", show_default=True)
@click.option("--name", default="Midnight S2", show_default=True)
def init_season(config, name):
    """Create an empty season and calculation defaults without replacing inputs."""
    settings, client = resources(config)
    season = client.collection("seasons").document(settings.firestore.season_id)
    season.set({"displayName": name}, merge=True)
    ref = season.collection("settings").document("calculation")
    if not ref.get().exists:
        data = CalculationInput(
            version=1, updatedBy="local-admin", updatedAt=datetime.now(timezone.utc)
        ).model_dump()
        data["updatedAt"] = firestore.SERVER_TIMESTAMP
        ref.create(data)
    click.echo(f"Initialized {settings.firestore.project_id}/{season.id}")


@web.command("set-officer")
@click.option("--config", default="config-web.yaml", show_default=True)
@click.option(
    "--uid",
    required=True,
    help="Firebase Authentication UID, not the Google account ID.",
)
@click.option("--enabled/--disabled", default=True)
def set_officer(config, uid, enabled):
    """Grant or revoke officer membership using local administrator credentials."""
    if not uid or "/" in uid or len(uid) > 128:
        raise click.ClickException("Invalid Firebase Authentication UID")
    _, client = resources(config)
    client.collection("officers").document(uid).set({"enabled": enabled})
    click.echo(f"Officer {uid}: {'enabled' if enabled else 'disabled'}")


@web.command("import-inputs")
@click.option("--config", default="config-web.yaml", show_default=True)
@click.option("--file", "filename", required=True, type=click.Path(exists=True))
def import_inputs(config, filename):
    """Import JSON input collections; existing documents are left unchanged."""
    from pathlib import Path

    from .web_data import import_domain

    settings, client = resources(config)
    try:
        values = json.loads(Path(filename).read_text())
        import_domain(client, settings.firestore.season_id, values)
    except (ValueError, TypeError):
        raise click.ClickException(
            "Invalid import. Check schemas, references, dates and the 400-write limit; no inputs were written."
        ) from None
    finally:
        client.close()
    click.echo("Imported missing input documents. Run Pebble to compute results.")


@web.command("backup-firestore")
@click.option("--config", default="config-web.yaml", show_default=True)
@click.option("--file", "filename", required=True, type=click.Path(dir_okay=False))
def backup_firestore(config, filename):
    """Read an idle season into a new protected backup; never replace a file."""
    from .web_backup import RecoveryError
    from .web_backup import backup_firestore as backup

    client = None
    try:
        settings, client = resources(config)
        count = backup(client, settings.firestore.season_id, filename)
    except RecoveryError as exc:
        raise click.ClickException(str(exc)) from None
    except Exception as exc:  # noqa: BLE001 - redact provider/configuration secrets.
        raise click.ClickException(
            f"Firestore backup failed ({type(exc).__name__}). Check configuration, source access and the protected output directory."
        ) from None
    finally:
        if client is not None:
            client.close()
    click.echo(f"Backed up {count} Firestore documents to a protected local file.")


@web.command("restore-firestore")
@click.option("--config", required=True, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--file", "filename", required=True, type=click.Path(exists=True, dir_okay=False)
)
@click.option(
    "--target-project",
    required=True,
    help="Exact empty destination project ID; must match target config.",
)
def restore_firestore(config, filename, target_project):
    """Restore and verify in an empty different project using create-only writes."""
    from .web_backup import RecoveryError
    from .web_backup import restore_firestore as restore

    client = None
    try:
        _, client = resources(config)
        count = restore(client, filename, target_project)
    except RecoveryError as exc:
        raise click.ClickException(str(exc)) from None
    except Exception as exc:  # noqa: BLE001 - redact provider/configuration secrets.
        raise click.ClickException(
            f"Firestore restore failed ({type(exc).__name__}). Inspect the destination; retry only in a fresh empty target."
        ) from None
    finally:
        if client is not None:
            client.close()
    click.echo(
        f"Restored and verified {count} Firestore documents in the empty target. "
        "Invitation history is inactive; reissue any required invitations."
    )
