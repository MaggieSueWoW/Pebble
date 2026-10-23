export type RosterValue = {
  name: string;
  className: string;
  role: string;
  active: boolean;
  joinDate: string;
  leaveDate: string;
  notes: string;
};
export type RosterEntry = RosterValue & { id: string; version: number };
export const classes = [
  "Unknown",
  "Death Knight",
  "Demon Hunter",
  "Druid",
  "Evoker",
  "Hunter",
  "Mage",
  "Monk",
  "Paladin",
  "Priest",
  "Rogue",
  "Shaman",
  "Warlock",
  "Warrior",
];
export const roles = ["Unknown", "Tank", "Healer", "Damage"];
export const blankPlayer = (): RosterValue => ({
  name: "",
  className: "Unknown",
  role: "Unknown",
  active: true,
  joinDate: "",
  leaveDate: "",
  notes: "",
});
export function validatePlayer(
  value: RosterValue,
  roster: RosterEntry[],
  id: string,
): string | null {
  if (!/^\p{L}{1,32}$/u.test(value.name))
    return "Main name must contain 1–32 letters only. Use character mappings for realms and alts.";
  if (
    roster.some(
      (p) =>
        p.id !== id &&
        p.name.toLocaleLowerCase() === value.name.toLocaleLowerCase(),
    )
  )
    return "Main name is already used by another player.";
  if (!classes.includes(value.className)) return "Choose a listed class.";
  if (!roles.includes(value.role)) return "Choose a listed role.";
  for (const [label, day] of [
    ["Join", value.joinDate],
    ["Leave", value.leaveDate],
  ]) {
    if (
      day &&
      (!/^\d{4}-\d{2}-\d{2}$/.test(day) ||
        day.startsWith("0000-") ||
        !Number.isFinite(Date.parse(day)) ||
        new Date(day).toISOString().slice(0, 10) !== day)
    )
      return `${label} date must be a valid calendar date.`;
  }
  if (value.joinDate && value.leaveDate && value.joinDate > value.leaveDate)
    return "Join date must be on or before leave date.";
  if (value.notes.length > 1000)
    return "Private notes must be 1,000 characters or fewer.";
  return null;
}
