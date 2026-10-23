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
