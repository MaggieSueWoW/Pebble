from pebble.utils.names import NameResolver


def test_resolver_shortens_unique_roster_names():
    resolver = NameResolver(["Alice-Illidan", "Bob-Illidan"])
    assert resolver.resolve("Alice-Illidan") == "Alice"
    # Sheets/log values without a realm suffix still resolve.
    assert resolver.resolve("Bob") == "Bob"
    # WCL style names map to their shortened display.
    assert resolver.resolve("Bob-Illidan") == "Bob"


def test_resolver_records_ambiguous_names():
    resolver = NameResolver(["Alice-Illidan", "Alice-Stormrage"])
    assert resolver.resolve("Alice-Illidan") is None
    assert resolver.resolve("Alice-Stormrage") is None
    assert resolver.not_on_roster == {"Alice-Illidan", "Alice-Stormrage"}


def test_resolver_uses_alt_mapping_for_unknown_main():
    resolver = NameResolver(["Alice-Illidan"], {"Alty-Illidan": "Bob-Illidan"})
    assert resolver.resolve("Alty-Illidan") is None
    assert resolver.not_on_roster == {"Bob-Illidan"}


def test_resolver_accepts_short_roster_mains():
    resolver = NameResolver(["Alice"])
    assert resolver.resolve("Alice") == "Alice"
    assert resolver.resolve("Alice-Illidan") == "Alice"


def test_resolver_maps_long_roster_map_targets_to_short_main():
    resolver = NameResolver(["Alice"], {"OldAlice-Illidan": "Alice-Illidan"})
    assert resolver.resolve("OldAlice-Illidan") == "Alice"


def test_resolver_records_unmapped_roster_map_target():
    resolver = NameResolver(["Alice"], {"BobAlt-Illidan": "Bob-Illidan"})
    assert resolver.resolve("BobAlt-Illidan") is None
    assert resolver.not_on_roster == {"Bob-Illidan"}
