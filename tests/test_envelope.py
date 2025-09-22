from pebble.envelope import split_pre_post


def test_split_pre_post_adds_post_extension():
    envelope = (0, 120_000)
    break_range = (60_000, 70_000)
    res = split_pre_post(envelope, break_range, post_extension_ms=30_000)
    assert res["pre_ms"] == 60_000
    # Base post duration would be 50_000ms; expect +30_000ms extension.
    assert res["post_ms"] == 80_000


def test_split_pre_post_extension_skipped_without_break():
    envelope = (0, 90_000)
    res = split_pre_post(envelope, None, post_extension_ms=45_000)
    assert res == {"pre_ms": 90_000, "post_ms": 0}
