"""Allowlisted result explanations from existing computed materializations."""


def availability_explanation(row: dict, override=None) -> dict:
    """Copy recorded half flags and captured modes without recalculating time.

    Parameters
    ----------
    row
        One computed bench-night row containing engine availability and source.
    override
        Validated captured availability input, or None for automatic modes.

    Returns
    -------
    dict
        Public-safe half flags/input modes and the whole-row engine source.
    """
    return {
        half: {
            "available": row[f"avail_{half}"],
            "mode": getattr(override, half).mode if override else "automatic",
        }
        for half in ("pre", "post")
    } | {"source": row["status_source"]}


def night_diagnostics(qa: dict, override=None) -> dict:
    """Allowlist private night counts and timing sources from computed QA.

    Parameters
    ----------
    qa
        Computed night QA document. Raw names, report IDs and candidates stay private.
    override
        Validated captured night override, if present.

    Returns
    -------
    dict
        Officer-only aggregate diagnostics with no raw names, notes or errors.
    """
    start = (
        "override"
        if override and override.start
        else "configured" if qa["mythic_override_start_ms"] is not None else "detected"
    )
    return {
        "reportCount": len(qa["reports"]),
        "mythicFights": qa["mythic_fights"],
        "unmappedCharacters": len(qa["not_on_roster_mains"]),
        "breakMinutes": qa["break_duration_min"],
        "timing": {
            "start": start,
            "end": (
                "override" if qa["mythic_override_end_ms"] is not None else "detected"
            ),
            "break": (
                "override"
                if qa["break_override_start_ms"] is not None
                else "detected" if qa["break_start_ms"] is not None else "none"
            ),
        },
    }
