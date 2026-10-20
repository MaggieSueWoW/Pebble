"""Read-only numeric parity comparison for established Sheets result tables."""

from decimal import Decimal

from .utils.time import sheets_date_str


def compare_sheet_results(sheet_rows: dict, public: dict) -> dict:
    """Compare nightly, weekly and season totals without returning player names.

    Web UI documents include explicit zero rows that Sheets may omit. Treat an
    absent all-zero row as equivalent; count it separately for review. Numeric
    cells compare exactly as decimal minutes, independent of display formatting.
    """

    def records(name):
        rows = sheet_rows[name]
        return [
            dict(zip(rows[0], row))
            for row in rows[1:]
            if any(str(v).strip() for v in row)
        ]

    comparisons = {}
    sheets_nights = {
        (sheets_date_str(r["Night ID"]), r["Main"].casefold()): tuple(
            Decimal(str(r[k] or 0))
            for k in (
                "Played Total (min)",
                "Bench Pre (min)",
                "Bench Post (min)",
                "Bench Total (min)",
            )
        )
        for r in records("bench_night_totals")
    }
    web_nights = {
        (n["night"], p["name"].casefold()): tuple(
            Decimal(str(p[k])) for k in ("played", "preBench", "postBench", "bench")
        )
        for n in public["nights"]
        for p in n["players"]
    }
    comparisons["nightly"] = (sheets_nights, web_nights)
    sheets_weeks = {
        (sheets_date_str(r["Game Week"]), r["Main"].casefold()): tuple(
            Decimal(str(r[k] or 0))
            for k in (
                "Played Week (min)",
                "Bench Pre (min)",
                "Bench Post (min)",
                "Bench Week (min)",
            )
        )
        for r in records("bench_week_totals")
    }
    web_weeks = {
        (p["game_week"], p["main"].casefold()): tuple(
            Decimal(str(p[k]))
            for k in ("played_min", "bench_pre_min", "bench_post_min", "bench_min")
        )
        for w in public["weeks"]
        for p in w["players"]
    }
    comparisons["weekly"] = (sheets_weeks, web_weeks)
    sheets_season = {
        r["Main"].casefold(): tuple(
            Decimal(str(r[k] or 0))
            for k in ("Time Played (min)", "Bench Season-to-date (min)")
        )
        for r in records("bench_rankings")
    }
    web_season = {
        p["name"].casefold(): tuple(Decimal(str(p[k])) for k in ("played", "bench"))
        for p in public["players"]
    }
    comparisons["season"] = (sheets_season, web_season)
    summary = {}
    for name, (expected, actual) in comparisons.items():
        differences = zero_rows = 0
        for key in set(expected) | set(actual):
            a, b = expected.get(key), actual.get(key)
            if a is None or b is None:
                if any(a or b):
                    differences += 1
                else:
                    zero_rows += 1
            elif a != b:
                differences += 1
        summary[name] = {
            "sheetRows": len(expected),
            "webRows": len(actual),
            "differentRows": differences,
            "equivalentMissingZeroRows": zero_rows,
        }
    return summary
