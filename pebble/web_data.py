"""Validated, atomic administrative input imports."""

from datetime import datetime, timezone

from google.cloud import firestore

from .web_models import (
    ID,
    INPUT_MODELS,
    CalculationInput,
    valid_input_id,
    validate_snapshot,
)

MAX_IMPORT_WRITES = 400


def import_domain(client, season_id: str, values: dict):
    """Validate the entire effective season, then atomically create missing inputs.

    Existing inputs win, so this is not a synchronization or overwrite command.
    Reads and writes share a transaction, including referenced existing players.
    New linked records advance their existing main's version to serialize with deletion.
    Oversized imports fail before writes; split only at valid reference boundaries.
    """
    if not ID.fullmatch(season_id):
        raise ValueError("Invalid season ID")
    if not isinstance(values, dict) or set(values) - set(INPUT_MODELS):
        raise ValueError("Import accepts only the five input collections")
    meta = dict(
        version=1, updatedBy="local-admin", updatedAt=datetime.now(timezone.utc)
    )
    prepared = {}
    for name, docs in values.items():
        if not isinstance(docs, dict):
            raise ValueError("Each input collection must be an object")
        prepared[name] = {}
        for key, value in docs.items():
            if not valid_input_id(name, key) or not isinstance(value, dict):
                raise ValueError("Invalid import document")
            # Import files contain domain fields; metadata is always authored here.
            if set(value) & set(meta):
                raise ValueError(
                    "Import must not supply version or attribution metadata"
                )
            record = {**value, **meta}
            INPUT_MODELS[name].model_validate(record)
            prepared[name][key] = record
    season = client.collection("seasons").document(season_id)

    @firestore.transactional
    def save(tx):
        existing = {
            name: {
                doc.id: doc.to_dict()
                for doc in season.collection(name).get(transaction=tx)
            }
            for name in (*INPUT_MODELS, "settings")
        }
        pending = {
            name: {
                key: value for key, value in docs.items() if key not in existing[name]
            }
            for name, docs in prepared.items()
        }
        effective = {
            name: {**existing[name], **pending.get(name, {})} for name in INPUT_MODELS
        }
        effective["settings"] = existing["settings"]
        validate_snapshot(effective)
        for value in existing["settings"].values():
            CalculationInput.model_validate(value)
        linked_mains = {
            record["playerId"]
            for family in ("characters", "availabilityOverrides")
            for record in pending.get(family, {}).values()
            if record["playerId"] in existing["players"]
        }
        if sum(map(len, pending.values())) + len(linked_mains) > MAX_IMPORT_WRITES:
            raise ValueError(
                "Import exceeds 400 writes; split into reference-complete groups"
            )
        for player_id in linked_mains:
            tx.update(
                season.collection("players").document(player_id),
                {
                    "version": existing["players"][player_id]["version"] + 1,
                    "updatedBy": "local-admin",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
        for name, docs in pending.items():
            for key, record in docs.items():
                tx.create(
                    season.collection(name).document(key),
                    {**record, "updatedAt": firestore.SERVER_TIMESTAMP},
                )
        return {name: len(docs) for name, docs in pending.items()}

    return save(client.transaction(max_attempts=3))
