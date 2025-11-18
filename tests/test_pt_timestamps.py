from datetime import datetime
import zoneinfo

from pebble.participation import build_mythic_participation
from pebble.blocks import build_blocks
from pebble.utils.time import ms_to_pt_iso, ms_to_pt_sheets, pt_time_to_ms

PT = zoneinfo.ZoneInfo("America/Los_Angeles")


def test_pt_fields_added():
    ms_start = 1719975600000  # 2024-07-02T20:00:00-07:00
    ms_end = ms_start + 60000
    fights = [
        {
            "report_code": "R1",
            "id": 1,
            "fight_abs_start_ms": ms_start,
            "fight_abs_end_ms": ms_end,
            "night_id": "2024-07-02",
            "participants": [{"name": "Alice"}],
        }
    ]
    rows = build_mythic_participation(fights)
    assert rows[0]["start_pt"] == ms_to_pt_iso(ms_start)
    blocks = build_blocks(rows, break_range=None, fights_all=[])
    assert blocks[0]["start_pt"] == ms_to_pt_iso(ms_start)
    assert blocks[0]["end_pt"] == ms_to_pt_iso(ms_end)


def test_pt_time_to_ms_formats():
    ref_ms = 1719975600000  # 2024-07-02T20:00:00-07:00
    assert pt_time_to_ms("21:15", ref_ms) == ref_ms + 75 * 60000
    assert pt_time_to_ms("9:15:00 PM", ref_ms) == ref_ms + 75 * 60000
    assert pt_time_to_ms("9:15", ref_ms) == ref_ms + 75 * 60000
    assert pt_time_to_ms("2024-07-02 21:15", ref_ms) == ref_ms + 75 * 60000
    assert pt_time_to_ms("7/2/24 9:15 PM", ref_ms) == ref_ms + 75 * 60000

    ref_dt = datetime(2025, 11, 12, 18, 40, 53, 288000, tzinfo=PT)
    ref_ms = int(ref_dt.timestamp() * 1000)
    expected_ms = int(
        ref_dt.replace(hour=19, minute=0, second=0, microsecond=0).timestamp()
        * 1000
    )
    assert pt_time_to_ms("7:00 PM", ref_ms) == expected_ms


def test_ms_to_pt_sheets_format():
    ms = 1719975600000  # 2024-07-02T20:00:00-07:00
    assert ms_to_pt_sheets(ms) == "2024-07-02 20:00:00"
