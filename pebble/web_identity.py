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
    }
    region_data = (
        bundle.get("region")
        or ((bundle.get("guild") or {}).get("server") or {}).get("region")
        or {}
    )
    region = str(region_data.get("compactName", "")).casefold()
    if mappings and region not in {"us", "eu", "kr", "tw", "cn"}:
        raise ValueError("Report region is required to resolve character mappings")
    mains = {p.name.casefold(): p.name for p in inputs["players"].values()}
    result = deepcopy(bundle)
    for actor in (result.get("masterData") or {}).get("actors", []):
        if str(actor.get("type", "")).casefold() != "player":
            continue
        name, realm = str(actor.get("name", "")), str(actor.get("server") or "")
        main = mappings.get((region, name.casefold(), realm.casefold()))
        if main is None:
            main = mappings.get((region, name.casefold(), ""))
        if main is None:
            main = mains.get(name.casefold())
        actor["name"] = main or f"{region or 'unknown'}:{realm}:{name}".replace(
            "-", " "
        )
        actor["server"] = None
    return result
