# Attendance Probability Tables

This document explains the probability tables on the `Attendance` Google Sheet tab.

## What The Tables Are Trying To Answer

The sheet is trying to answer questions like:

- If we have a roster of $N$ players, what is the chance that at least 20 of them show up?
- Given our real player attendance rates, how does that compare to a simple "everyone attends at 90%" model?
- If we grow or shrink the roster, how does that change the probability of fielding a Mythic-sized group?

The two main columns are:

- **Predicted**: a baseline model where every player has the same attendance rate of 90%.
- **Actual**: a model using each player's real attendance rate.

## Plain-English Summary

We can treat each player as a coin flip:

- A player with 90% attendance is like a coin that lands "shows up" 90% of the time.
- A player with 75% attendance is a less reliable coin.

Instead of flipping one coin, we flip one coin per player and count how many players show up.

We can then answer:

- What is the chance that exactly $k$ players show up?
- More importantly for roster planning, what is the chance that **at least** $k$ players show up?

That second question is the one the sheet displays.

We use a Google Apps Script to compute this, with some variables for experimenting with different team sizes and 
attendance rates. The script is at the end of this doc.

## Step 1: Build The Simulated Team

The function starts by reading all numeric attendance rates from the range and sorting them from highest to lowest.

Then it adjusts the roster to the requested `teamSize`.

### If `teamSize` is smaller than the current roster

The script removes players:

- `dropMode = "lowest"` keeps the most reliable players and drops the least reliable ones.
- `dropMode = "highest"` does the opposite. **(Default)**

### If `teamSize` is larger than the current roster

The script adds synthetic players.

Those added players get:

- `newPlayerRate`, if provided
- otherwise the current roster's average attendance rate

This is a modeling choice, not a discovered fact. It is an assumption used to answer "what if we had a bigger roster?"

## Step 2: Compute Two Attendance Models

After the team is built, the script computes two different probability distributions.

### Actual model

This uses the real attendance rates for the players on the simulated roster:

$$p_1, p_2, ..., p_n$$

where $p_i$ is player $i$'s attendance probability.

### Predicted model

This uses a uniform roster where every player has the same attendance probability:

$$
u
$$

Usually $u = 0.9$.

So instead of modeling different players with different rates, the predicted model says:

- every player is a 90% attendance player

This gives a clean benchmark to compare against the real roster.

## Step 3: Compute The Probability Of Exactly $k$ Players Attending

Let:

- $X$ = number of players who show up
- $n$ = team size

The script first computes:

$$
P(X = k)
$$

for every $k$ from $0$ to $n$.

### Uniform case

If every player has the same attendance rate $u$, then:

$$
X \sim \mathrm{Binomial}(n, u)
$$

and:

$$
P(X = k) = \binom{n}{k} u^k (1-u)^{n-k}
$$

This is the usual binomial distribution.

### Actual case

If players have different attendance probabilities $p_1, p_2, ..., p_n$, then:

$$
X = B_1 + B_2 + \cdots + B_n
$$

where each:

$$
B_i \sim \mathrm{Bernoulli}(p_i)
$$

This makes $X$ a **Poisson-binomial** random variable.

There is no simple one-line binomial formula in the unequal-rate case, so the script uses dynamic programming instead.

## Step 4: Dynamic Programming Recurrence

The function `computeAttendanceDistribution(pList)` builds an array:

```text
dp[k] = P(exactly k players attend)
```

It starts with:

$$
dp_0(0) = 1
$$

meaning: before considering any players, the probability of zero attendees is 100%.

Then it processes players one at a time.

If the next player has attendance probability $p$, the recurrence is:

$$
dp_{\text{new}}(k) = dp_{\text{old}}(k)(1-p) + dp_{\text{old}}(k-1)p
$$

This works because there are only two ways to end up with exactly $k$ attendees after adding one more player:

- we already had $k$, and the new player does not attend
- we already had $k-1$, and the new player does attend

The script updates $k$ from high to low so it does not overwrite values it still needs during the same pass.

After all players are processed, the final array satisfies:

$$
dp[k] = P(X = k)
$$

for every $k = 0, 1, ..., n$.

## Step 5: Convert Exact Probabilities Into "At Least" Probabilities

The sheet usually cares about questions like:

- What is the chance at least 20 players show up?

That is:

$$
P(X \ge c)
$$

for some cutoff $c$.

The script computes this by summing the exact probabilities from the top down:

$$
P(X \ge c) = \sum_{k=c}^{n} P(X = k)
$$

This is what `cumulativeAtLeast(dp)` does.

So if the row says:

```text
20 | 83% | 74%
```

that means:

- under the uniform baseline, there is an 83% chance that at least 20 players attend
- under the actual player-by-player rates, there is a 74% chance that at least 20 players attend

## What The Two Table Shapes Usually Mean

The same function can be used in two slightly different ways.

### Table shape A: vary team size, hold cutoff fixed

Example question:

- If we had $18, 19, 20, ..., 28$ players on the roster, what is the chance at least 20 show up?

In that setup:

- the row label is roster size
- the cutoff is fixed at 20

This is useful for planning how large the roster needs to be.

### Table shape B: hold team size fixed, vary cutoff

Example question:

- Given our current roster size, what is the chance at least $18, 19, 20, ..., n$ players show up?

In that setup:

- the team size stays fixed
- the row label is the attendance threshold

This is useful for understanding how likely different staffing outcomes are with the current roster.

## Why The Baseline Uses 90%

The `Predicted` column is not a prediction learned from the team. It is a benchmark.

It answers:

- what would this look like if every player were a 90% attendance player?

That is helpful because it gives officers a stable comparison point:

- if `Actual` is below `Predicted`, the real roster is less reliable than the benchmark
- if `Actual` is above `Predicted`, the real roster is more reliable than the benchmark

## Assumptions Baked Into The Model

This is the most important section to keep in mind when interpreting the results.

### 1. Player attendance is treated as independent

The math assumes one player's attendance does not affect another's.

That is often only approximately true. Real teams can have correlated absences:

- holidays
- burnout waves
- patch timing
- weather or regional outages
- players who tend to miss together
- love

If absences are positively correlated, the model can be too optimistic.

### 2. Historical attendance rate is treated as true attendance probability

If a player has attended 82% of the time so far, the model uses 0.82 as that player's future attendance probability.

That is a practical simplification. In reality:

- the sample may be small
- the player's future behavior may change
- recent attendance may matter more than old attendance

### 3. Added players are synthetic

When the script simulates a larger roster than currently exists, it invents extra players and assigns them:

- the provided `newPlayerRate`, or
- the current average rate

That is useful for planning, but it is still hypothetical.

### 4. Smaller-roster scenarios depend on who gets dropped

If the script drops the lowest-attendance players, the smaller roster may look better than a random cut would.

That is fine if the goal is "what if we trimmed the least reliable players?", but it should not be mistaken for a neutral sample.

### 5. Attendance percentages are assumed to be valid probabilities

The algorithm assumes the input rates are already normalized to values between 0 and 1, and that they are the right summary of attendance behavior for this question.

## Why Dynamic Programming Is Used

For equal attendance rates, the binomial formula is enough.

For unequal rates, a direct formula for:

$$
P(X = k)
$$

would require summing over many combinations of players. That gets cumbersome fast.

Dynamic programming is used because it:

- is exact
- is easy to verify
- is efficient for roster sizes in this project

For a roster of size $n$, the runtime is roughly:

$$
O(n^2)
$$

which is completely reasonable at these team sizes.

## Sanity Checks

A few simple checks help verify the model:

- All probabilities in `dp` should sum to about 1.
- $P(X \ge 0)$ should be 1.
- $P(X \ge n)$ should equal the probability that every player attends.
- If all players have the same rate $u$, the `Actual` model should match the `Predicted` binomial model when `uniformRate = u`.

## How To Read The Output Responsibly

The tables are best understood as planning tools, not guarantees.

They are good for:

- comparing roster constructions
- seeing whether the current roster is more or less reliable than a benchmark
- understanding how much risk is attached to a given roster size

They are less reliable for:

- forecasting a specific raid night with unusual circumstances
- capturing social or seasonal attendance patterns
- predicting brand-new recruits with confidence

## Mapping Back To The Apps Script

The Apps Script functions correspond to the math like this:

- `TEAM_ATTENDANCE_PROB_TABLE(...)`
  - prepares the simulated roster
  - computes both models
  - returns rows of cutoff, predicted probability, actual probability
- `computeAttendanceDistribution(pList)`
  - computes $P(X = k)$ for all $k$
- `cumulativeAtLeast(dp)`
  - converts exact probabilities into $P(X \ge k)$

## Bottom Line

The algorithm is a roster attendance model.

It uses each player's attendance percentage as a probability, computes the full distribution of how many players will show up, and then reports the chance of meeting or beating key attendance thresholds.

The `Predicted` column is a clean benchmark where everyone is a 90% player.
The `Actual` column is the same calculation using the real roster.
The difference between them gives a quick read on whether the current team is more or less reliable than that benchmark.

# Appendix

## Google Apps Script

The Apps Script function is:

```javascript
function TEAM_ATTENDANCE_PROB_TABLE(range, teamSize, minCutoff, maxCutoff, uniformRate, newPlayerRate, dropMode) {
  // range: attendance % values (e.g. I8:I32)
  // teamSize: total players to simulate (e.g. 28)
  // minCutoff/maxCutoff: range of "at least X show up" (e.g. 18, 25)
  // uniformRate: comparison baseline (e.g. 0.9)
  // newPlayerRate: optional fixed rate for added players (blank = avg)
  // dropMode: "lowest" or "highest" for which players to drop if smaller team

  const values = range.flat()
    .filter(v => typeof v === 'number' && !isNaN(v));
  const n = values.length;
  if (n === 0) return [['No data']];

  // Sort descending so most reliable first
  const sorted = [...values].sort((a, b) => b - a);

  // Adjust team size
  let team;
  if (teamSize <= n) {
    if (dropMode && dropMode.toLowerCase() === 'highest') {
      team = sorted.slice(sorted.length - teamSize);
    } else {
      team = sorted.slice(0, teamSize);
    }
  } else {
    const avg = sorted.reduce((a, b) => a + b, 0) / n;
    const addCount = teamSize - n;
    const addRate = (newPlayerRate && !isNaN(newPlayerRate)) ? newPlayerRate : avg;
    team = sorted.concat(Array(addCount).fill(addRate));
  }

  // Compute distributions once
  const dpActual = computeAttendanceDistribution(team);
  const dpUniform = computeAttendanceDistribution(Array(teamSize).fill(uniformRate));

  // Precompute cumulative probabilities from the top down
  const cumActual = cumulativeAtLeast(dpActual);
  const cumUniform = cumulativeAtLeast(dpUniform);

  // const results = [['Cutoff (≥ Players)', 'Actual Prob', `Uniform ${uniformRate*100}% Prob`]];
  const results = [];
  for (let cutoff = minCutoff; cutoff <= maxCutoff; cutoff++) {
    const a = cumActual[cutoff] ?? 0;
    const u = cumUniform[cutoff] ?? 0;
    results.push([cutoff, u, a]);
  }

  return results;
}

function computeAttendanceDistribution(pList) {
  const n = pList.length;
  const dp = Array(n + 1).fill(0);
  dp[0] = 1;

  for (const p of pList) {
    for (let k = n; k > 0; k--) {
      dp[k] = dp[k] * (1 - p) + dp[k - 1] * p;
    }
    dp[0] *= (1 - p);
  }
  return dp;  // dp[k] = P(exactly k attend)
}

function cumulativeAtLeast(dp) {
  const n = dp.length;
  const result = Array(n).fill(0);
  let running = 0;
  for (let k = n - 1; k >= 0; k--) {
    running += dp[k];
    result[k] = running;
  }
  return result;  // result[k] = P(≥k attend)
}
```

The important inputs are:

- `range`: the attendance rates already calculated for players on the sheet.
- `teamSize`: the roster size we want to model.
- `minCutoff`, `maxCutoff`: the attendance thresholds we care about, such as 18 through 25, or just 20.
- `uniformRate`: the baseline attendance rate for the `Predicted` column, usually 0.9.
- `newPlayerRate`: the assumed rate for synthetic players if we simulate a larger roster than we currently have.
- `dropMode`: when simulating a smaller roster, whether we drop the `lowest` attendance players or the `highest`.

