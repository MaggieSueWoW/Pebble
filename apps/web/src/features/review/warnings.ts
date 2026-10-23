/** Preserve warning indexes so source links always refer to the displayed run. */
export function warningsForNight(warnings: string[] = [], night?: string) {
  return warnings
    .map((text, index) => ({ text, index }))
    .filter(
      ({ text }) =>
        !night ||
        !/^\d{4}-\d{2}-\d{2}:/.test(text) ||
        text.startsWith(`${night}:`),
    );
}

/** The selected night supplies the date context on Review Night. */
export function terseWarning(text: string) {
  const message = text.replace(/^\d{4}-\d{2}-\d{2}:\s*/, "");
  if (message.startsWith("no Mythic encounters"))
    return "No Mythic encounters.";
  return message.replace(/^unknown characters:/, "Unknown players:");
}

/** Group published warnings by night without repeating general warnings per date. */
export function warningNights(warnings: string[] = []) {
  const counts = new Map<string, number>();
  for (const warning of warnings) {
    const night = warning.match(/^(\d{4}-\d{2}-\d{2}):/)?.[1] ?? "";
    counts.set(night, (counts.get(night) ?? 0) + 1);
  }
  return [...counts]
    .map(([night, count]) => ({ night, count }))
    .sort((a, b) => {
      if (!a.night) return 1;
      if (!b.night) return -1;
      return b.night.localeCompare(a.night);
    });
}
