from __future__ import annotations
from typing import Iterable, List, Optional, Tuple


def mythic_envelope(fights_mythic: List[dict]) -> Optional[Tuple[int, int]]:
    if not fights_mythic:
        return None
    s = min(f["fight_abs_start_ms"] for f in fights_mythic)
    e = max(f["fight_abs_end_ms"] for f in fights_mythic)
    return (s, e)


def _fight_half(fight: dict, break_range) -> str:
    start_ms = fight.get("fight_abs_start_ms", 0)
    end_ms = fight.get("fight_abs_end_ms", start_ms)
    if not break_range:
        return "pre"
    bs, _ = break_range
    mid = (start_ms + end_ms) // 2
    return "pre" if mid < bs else "post"


def non_mythic_boss_gap_intervals(
    fights_all: Iterable[dict],
    fights_mythic: Iterable[dict],
    break_range=None,
) -> List[Tuple[int, int]]:
    """Return full non-Mythic gap windows between Mythic fights within a half.

    The key detail is that halves are handled independently. If the night is
    ``Mythic -> Heroic -> break -> Heroic -> Mythic``, then pre- and post-break
    Mythic time are bounded by the Mythic activity in each half, and the break
    does not get swept into a cross-half exclusion window.
    """

    mythic_by_half = {"pre": [], "post": []}
    non_mythic_by_half = {"pre": [], "post": []}

    for fight in fights_mythic:
        start = fight.get("fight_abs_start_ms")
        end = fight.get("fight_abs_end_ms")
        if start is None or end is None:
            continue
        mythic_by_half[_fight_half(fight, break_range)].append((start, end))

    for fight in fights_all:
        start = fight.get("fight_abs_start_ms")
        end = fight.get("fight_abs_end_ms")
        if (
            fight.get("is_mythic")
            or fight.get("encounter_id", 0) <= 0
            or start is None
            or end is None
        ):
            continue
        non_mythic_by_half[_fight_half(fight, break_range)].append((start, end))

    excluded: List[Tuple[int, int]] = []
    for half in ("pre", "post"):
        mythic = sorted(mythic_by_half[half], key=lambda pair: (pair[0], pair[1]))
        non_mythic = non_mythic_by_half[half]
        for (_, prev_end), (next_start, _) in zip(mythic, mythic[1:]):
            if next_start <= prev_end:
                continue
            for nm_start, nm_end in non_mythic:
                if nm_start < next_start and nm_end > prev_end:
                    excluded.append((prev_end, next_start))
                    break
    return excluded


def split_pre_post_from_fights(
    fights_all: Iterable[dict],
    fights_mythic: Iterable[dict],
    break_range,
    *,
    envelope: Tuple[int, int] | None = None,
    post_extension_ms: int = 0,
):
    """Compute Mythic pre/post durations from the fights assigned to each half."""

    summary = mythic_half_summary_from_fights(
        fights_all,
        fights_mythic,
        break_range,
        envelope=envelope,
        post_extension_ms=post_extension_ms,
    )
    return {"pre_ms": summary["pre"]["duration_ms"], "post_ms": summary["post"]["duration_ms"]}


def mythic_half_summary_from_fights(
    fights_all: Iterable[dict],
    fights_mythic: Iterable[dict],
    break_range,
    *,
    envelope: Tuple[int, int] | None = None,
    post_extension_ms: int = 0,
):
    """Return per-half Mythic timing details used by the split calculation."""

    fights_mythic = list(fights_mythic)
    pre_fights = [f for f in fights_mythic if _fight_half(f, break_range) == "pre"]
    post_fights = [f for f in fights_mythic if _fight_half(f, break_range) == "post"]

    env_start = envelope[0] if envelope else None
    env_end = envelope[1] if envelope else None

    def _half_summary(half_fights: List[dict], half_name: str) -> dict:
        if not half_fights:
            return {
                "start_ms": None,
                "end_ms": None,
                "duration_ms": 0,
                "excluded_intervals": [],
            }
        start = min(f["fight_abs_start_ms"] for f in half_fights)
        end = max(f["fight_abs_end_ms"] for f in half_fights)
        if env_start is not None and _fight_half({"fight_abs_start_ms": env_start, "fight_abs_end_ms": env_start}, break_range) == half_name:
            start = min(start, env_start)
        if env_end is not None and _fight_half({"fight_abs_start_ms": env_end, "fight_abs_end_ms": env_end}, break_range) == half_name:
            end = max(end, env_end)
        duration = max(0, end - start)
        excluded_intervals = non_mythic_boss_gap_intervals(fights_all, half_fights, break_range)
        for ex_start, ex_end in excluded_intervals:
            overlap = max(0, min(ex_end, end) - max(ex_start, start))
            duration = max(0, duration - overlap)
        return {
            "start_ms": start,
            "end_ms": end,
            "duration_ms": duration,
            "excluded_intervals": excluded_intervals,
        }

    pre = _half_summary(pre_fights, "pre")
    post = _half_summary(post_fights, "post")
    if post["duration_ms"] > 0 and post_extension_ms > 0:
        post["duration_ms"] += post_extension_ms
        if post["end_ms"] is not None:
            post["end_ms"] += post_extension_ms
    return {"pre": pre, "post": post}


def split_pre_post(
    envelope: Tuple[int, int],
    break_range,
    *,
    post_extension_ms: int = 0,
    excluded_intervals: Iterable[Tuple[int, int]] | None = None,
):
    s, e = envelope
    excluded_intervals = list(excluded_intervals or [])
    if not break_range:
        pre = e - s
        for ex_start, ex_end in excluded_intervals:
            overlap = max(0, min(ex_end, e) - max(ex_start, s))
            pre = max(0, pre - overlap)
        return {"pre_ms": pre, "post_ms": 0}
    bs, be = break_range
    pre = max(0, min(bs, e) - s)
    post = max(0, e - max(be, s))
    pre_window = (s, min(bs, e))
    post_window = (max(be, s), e)
    for ex_start, ex_end in excluded_intervals:
        pre_overlap = max(0, min(ex_end, pre_window[1]) - max(ex_start, pre_window[0]))
        post_overlap = max(0, min(ex_end, post_window[1]) - max(ex_start, post_window[0]))
        pre = max(0, pre - pre_overlap)
        post = max(0, post - post_overlap)
    if post > 0 and post_extension_ms > 0:
        post += post_extension_ms
    return {"pre_ms": pre, "post_ms": post}
