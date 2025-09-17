from __future__ import annotations

from collections import Counter
from typing import Dict, Iterable, Optional, Set, Tuple


def _shorten(name: str) -> str:
    """Return the portion of ``name`` before the first realm suffix."""

    if not name:
        return ""
    parts = name.split("-", 1)
    return parts[0].strip()


class NameResolver:
    """Resolve player names from logs or Sheets to roster mains.

    The resolver shortens roster mains to their ``Name`` component (before the
    first ``-``) and only returns matches that are unambiguous.  When a name
    cannot be matched uniquely, the original ``name-realm`` string is recorded
    in :attr:`not_on_roster` so callers can surface it in Night QA.
    """

    def __init__(
        self,
        roster_mains: Iterable[str],
        alt_to_main: Optional[Dict[str, str]] = None,
    ) -> None:
        alt_to_main = alt_to_main or {}

        self._main_to_display: Dict[str, str] = {}
        display_counter: Counter[str] = Counter()

        # Track alt -> raw target and canonical roster mains once we normalize them.
        self._raw_alt_targets: Dict[str, str] = {}
        self._alt_to_canonical: Dict[str, Optional[str]] = {}

        for main in roster_mains:
            if not main:
                continue
            display = _shorten(main)
            if not display:
                continue
            self._main_to_display[main] = display
            display_counter[display] += 1

        self._ambiguous_displays: Set[str] = {
            display for display, count in display_counter.items() if count > 1
        }

        # Displays that uniquely identify a main.
        self._display_to_main: Dict[str, str] = {
            display: main
            for main, display in self._main_to_display.items()
            if display not in self._ambiguous_displays
        }

        # Map raw names (mains, alts, or already-shortened) to display names.
        self._aliases: Dict[str, str] = {}
        for main, display in self._main_to_display.items():
            if display in self._ambiguous_displays:
                continue
            self._aliases.setdefault(main, display)
            self._aliases.setdefault(display, display)

        for alt, raw_main in alt_to_main.items():
            alt = (alt or "").strip()
            raw_main = (raw_main or "").strip()
            if not alt or not raw_main:
                continue
            self._raw_alt_targets[alt] = raw_main
            canonical, display = self._canonical_main_for(raw_main)
            if canonical and display:
                self._alt_to_canonical[alt] = canonical
                self._aliases.setdefault(alt, display)
                base = _shorten(alt)
                if (
                    base
                    and base not in self._ambiguous_displays
                    and base not in self._aliases
                ):
                    self._aliases[base] = display
            else:
                self._alt_to_canonical[alt] = None

        self.not_on_roster: Set[str] = set()

    @property
    def active_displays(self) -> Set[str]:
        """Return the set of unambiguous roster main display names."""

        return set(self._display_to_main.keys())

    @staticmethod
    def _base(name: str) -> str:
        return _shorten(name)

    def resolve(self, name: str | None) -> Optional[str]:
        """Return the shortened roster main for ``name`` if unambiguous.

        ``name`` may originate from WCL logs or Sheets.  If the name cannot be
        mapped uniquely, the original string is recorded in
        :attr:`not_on_roster` and ``None`` is returned so callers can drop the
        associated data.
        """

        if not name:
            return None

        name = name.strip()
        if not name:
            return None

        alias = self._lookup_display(name)
        if alias:
            return alias

        if name in self._alt_to_canonical:
            canonical = self._alt_to_canonical.get(name)
            if canonical:
                display = self._main_to_display.get(canonical)
                if display and display not in self._ambiguous_displays:
                    return display
            raw_target = self._raw_alt_targets.get(name)
            if raw_target:
                self.not_on_roster.add(raw_target)
            else:
                self.not_on_roster.add(name)
            return None

        base = self._base(name)
        alias = self._lookup_display(base)
        if alias:
            return alias

        if base in self._ambiguous_displays:
            self.not_on_roster.add(name)
            return None

        if base in self._raw_alt_targets:
            raw_target = self._raw_alt_targets.get(base)
            if raw_target:
                self.not_on_roster.add(raw_target)
                return None

        self.not_on_roster.add(name)
        return None

    def _lookup_display(self, token: str | None) -> Optional[str]:
        if not token:
            return None
        token = token.strip()
        if not token:
            return None
        alias = self._aliases.get(token)
        if alias:
            return alias
        canonical = self._display_to_main.get(token)
        if canonical:
            display = self._main_to_display.get(canonical)
            if display and display not in self._ambiguous_displays:
                return display
        return None

    def _canonical_main_for(self, name: str) -> Tuple[Optional[str], Optional[str]]:
        if not name:
            return None, None
        name = name.strip()
        if not name:
            return None, None

        if name in self._main_to_display:
            display = self._main_to_display[name]
            if display in self._ambiguous_displays:
                return None, None
            return name, display

        display = self._lookup_display(name)
        if display:
            canonical = self._display_to_main.get(display)
            if canonical:
                return canonical, display

        base = _shorten(name)
        if not base:
            return None, None
        canonical = self._display_to_main.get(base)
        if canonical:
            display = self._main_to_display.get(canonical)
            if display and display not in self._ambiguous_displays:
                return canonical, display
        return None, None
