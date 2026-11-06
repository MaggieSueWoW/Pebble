export type DropMode = "lowest" | "highest";

const oneDecimal = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

export function percent(rate: number, unit = "%") {
  // Remove floating-point noise before rounding the probability for display.
  return `${oneDecimal.format(Number((100 * rate).toPrecision(15)))}${unit}`;
}

/** Probability of at least k independent players attending, for each k. */
function atLeast(rates: number[]) {
  const distribution = Array<number>(rates.length + 1).fill(0);
  distribution[0] = 1;
  rates.forEach((rate, index) => {
    for (let count = index + 1; count > 0; count -= 1) {
      distribution[count] =
        distribution[count] * (1 - rate) + distribution[count - 1] * rate;
    }
    distribution[0] *= 1 - rate;
  });
  for (let count = rates.length - 1; count >= 0; count -= 1) {
    distribution[count] += distribution[count + 1];
  }
  return distribution;
}

/** Compare the uniform baseline to the roster modeled in the Attendance sheet. */
export function attendanceProbabilities(
  rates: number[],
  size: number,
  baseline: number,
  newPlayerRate: number | null = null,
  drop: DropMode = "lowest",
) {
  if (!rates.length) return [];
  const sorted = [...rates].sort((a, b) => b - a);
  const team = drop === "lowest" ? sorted.slice(0, size) : sorted.slice(-size);
  if (size > team.length) {
    const average = rates.reduce((sum, rate) => sum + rate, 0) / rates.length;
    team.push(
      ...Array<number>(size - team.length).fill(newPlayerRate ?? average),
    );
  }
  const actual = atLeast(team);
  const predicted = atLeast(Array<number>(size).fill(baseline));
  const minimum = size >= 18 ? 18 : 1;
  return actual.slice(minimum).map((probability, index) => {
    const players = index + minimum;
    return { players, actual: probability, predicted: predicted[players] };
  });
}
