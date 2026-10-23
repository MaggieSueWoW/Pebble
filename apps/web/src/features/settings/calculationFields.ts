/** Editable labels and descriptions for the Settings form. */
export const calculationFields = [
  {
    key: "breakWindowStart",
    label: "Break window start",
    type: "time",
    description:
      "Earliest time at which the midpoint of a gap between boss pulls can qualify as the raid break. Pebble looks at boss encounters, not trash pulls. This window helps separate the planned break from pauses elsewhere in the night.",
  },
  {
    key: "breakWindowEnd",
    label: "Break window end",
    type: "time",
    description:
      "Latest time for a candidate break’s midpoint. Pebble chooses the longest gap whose midpoint falls between the window’s start and end, then checks its duration. The gap itself can start before the window or finish after it.",
  },
  {
    key: "minBreakMinutes",
    label: "Minimum break minutes",
    type: "number",
    description:
      "Shortest duration accepted for the detected break. The longest candidate gap in the break window must last at least this many minutes; shorter pauses do not qualify. Enter a whole number from 1 to 120.",
  },
  {
    key: "maxBreakMinutes",
    label: "Maximum break minutes",
    type: "number",
    description:
      "Longest duration accepted for the detected break. If the longest candidate exceeds this limit, Pebble detects no break rather than choosing a shorter gap. Enter a whole number from 1 to 120. A raid-night break override takes precedence.",
  },
  {
    key: "defaultMythicStart",
    label: "Default mythic start",
    type: "time",
    description:
      "Start time to use when the night’s first boss pull is Mythic and no start override is set for that night. Leave blank to use the detected first Mythic pull. An earlier start credits the added time to players in the first Mythic pull.",
  },
  {
    key: "postExtensionMinutes",
    label: "Post extension minutes",
    type: "number",
    description:
      "Extra minutes added to the after-break Mythic segment, when that segment exists. Players in the last Mythic pull receive matching played-time credit; other available players may gain bench time. Use this for end-of-night time absent from the pulls. Set 0 to disable; values up to 60, including decimals, are allowed.",
  },
] as const;
