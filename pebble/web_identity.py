"""Region-aware character identity at the web adapter boundary."""

from copy import deepcopy


def mapped_bundle(bundle: dict, inputs: dict) -> dict:
    """Resolve WCL actors without changing the cached bundle or Sheets resolver.

    Explicit mappings match region, realm and name case-insensitively. An empty
    realm is an intentional name-only mapping within that region. Main names
    remain season-wide identities, as in the Sheets roster. Unmapped characters
    retain a diagnostic identity that cannot shorten into a different main.
    """
    mappings = {
        (c.region, c.name.casefold(), c.realm.casefold()): inputs["players"][
            c.playerId
        ].name
        for c in inputs["characters"].values()
        if not c.ignored
    }
    ignored = {
        (c.region, c.name.casefold(), c.realm.casefold())
        for c in inputs["characters"].values()
        if c.ignored
    }
    region_data = (
        bundle.get("region")
        or ((bundle.get("guild") or {}).get("server") or {}).get("region")
        or {}
    )
    region = str(region_data.get("compactName", "")).casefold()
    if (mappings or ignored) and region not in {"us", "eu", "kr", "tw", "cn"}:
        raise ValueError("Report region is required to resolve character mappings")
    mains = {p.name.casefold(): p.name for p in inputs["players"].values()}
    result = deepcopy(bundle)
    ignored_ids = set()
    for actor in (result.get("masterData") or {}).get("actors", []):
        if str(actor.get("type", "")).casefold() != "player":
            continue
        name, realm = str(actor.get("name", "")), str(actor.get("server") or "")
        main = mappings.get((region, name.casefold(), realm.casefold()))
        if main is None:
            main = mappings.get((region, name.casefold(), ""))
        if main is None:
            main = mains.get(name.casefold())
        if main is None and (
            (region, name.casefold(), realm.casefold()) in ignored
            or (region, name.casefold(), "") in ignored
        ):
            ignored_ids.add(actor["id"])
            continue
        actor["name"] = main or f"{region or 'unknown'}:{realm}:{name}".replace(
            "-", " "
        )
        actor["server"] = None
    if ignored_ids:
        result["masterData"]["actors"] = [
            actor
            for actor in result["masterData"]["actors"]
            if actor["id"] not in ignored_ids
        ]
    actor_names = {
        actor["id"]: actor["name"]
        for actor in (result.get("masterData") or {}).get("actors", [])
        if "id" in actor and str(actor.get("type", "")).casefold() == "player"
    }
    # A main and its alt can both appear in a fight. Count that main once.
    for fight in result.get("fights", []):
        seen = set()
        participants = []
        for actor_id in fight.get("friendlyPlayers") or []:
            if actor_id in ignored_ids:
                continue
            identity = actor_names.get(actor_id, ("actor", actor_id))
            if identity not in seen:
                seen.add(identity)
                participants.append(actor_id)
        fight["friendlyPlayers"] = participants
    return result
