import { useState } from "react";
import { DataTable } from "../../components/DataTable";
import { PanelHeader } from "../../components/PanelHeader";
import { SelectControl } from "../../components/SelectControl";
import {
  attendanceProbabilities,
  percent,
  type DropMode,
} from "./attendanceProbability";

const defaultBaseline = 85;
const maxScenarioSize = 100;
const newPlayerRates = Array.from({ length: 11 }, (_, index) => 50 + 5 * index);

function ProbabilityTable({
  title,
  rows,
}: {
  title: string;
  rows: ReturnType<typeof attendanceProbabilities>;
}) {
  return (
    <DataTable className="attendance-probabilities">
      <caption className="sr-only">{title}</caption>
      <thead>
        <tr>
          <th scope="col">At least</th>
          <th scope="col">Predicted</th>
          <th scope="col">Actual</th>
          <th scope="col">Delta</th>
        </tr>
      </thead>
      <tbody>
        {rows.map(({ players, predicted, actual }) => (
          <tr
            key={players}
            className={players === 20 ? "mythic-cutoff" : undefined}
          >
            <th scope="row">{players}</th>
            <td>{percent(predicted)}</td>
            <td>{percent(actual)}</td>
            <td>{percent(actual - predicted, " pp")}</td>
          </tr>
        ))}
      </tbody>
    </DataTable>
  );
}

export function AttendanceModels({ rates }: { rates: number[] }) {
  const [size, setSize] = useState("25");
  const [baseline, setBaseline] = useState(String(defaultBaseline));
  const [newRate, setNewRate] = useState("100");
  const [drop, setDrop] = useState<DropMode>("lowest");
  const sizeValue = Number(size);
  const baselineValue = Number(baseline);
  const validSize =
    Number.isInteger(sizeValue) &&
    sizeValue >= 1 &&
    sizeValue <= maxScenarioSize;
  const validBaseline =
    baseline !== "" && baselineValue >= 0 && baselineValue <= 100;

  return (
    <section
      className="panel panel-body"
      aria-labelledby="attendance-modeling-title"
    >
      <PanelHeader
        id="attendance-modeling-title"
        title="Attendance Modeling"
        description={
          <>
            Chance of reaching each player count. Predicted uses one baseline
            rate for everyone. Actual uses each player’s attendance rate and
            assumes players attend independently. Delta is the difference in
            percentage points. Read the{" "}
            <a
              className="attendance-model-reference"
              href="https://github.com/MaggieSueWoW/Pebble/blob/main/Attendance_Probability.md"
            >
              attendance probability guide
            </a>{" "}
            for the math behind the models.
          </>
        }
      />
      {rates.length === 0 ? (
        <p className="empty-state">
          Attendance rates will appear after eligible raid time is recorded.
        </p>
      ) : (
        <div className="attendance-models">
          <section aria-labelledby="current-roster-model">
            <h3 id="current-roster-model">Current roster</h3>
            <p className="muted small">
              {rates.length} modeled players · {defaultBaseline}% baseline
            </p>
            <ProbabilityTable
              title="Current roster probabilities"
              rows={attendanceProbabilities(
                rates,
                rates.length,
                defaultBaseline / 100,
              )}
            />
          </section>
          <section aria-labelledby="scenario-roster-model">
            <h3 id="scenario-roster-model">What-if roster</h3>
            <fieldset
              className="attendance-model-controls"
              aria-label="What-if roster settings"
            >
              <label className="field">
                <span>Roster size</span>
                <input
                  type="number"
                  min="1"
                  max={maxScenarioSize}
                  value={size}
                  aria-invalid={!validSize}
                  onChange={(event) => setSize(event.target.value)}
                />
              </label>
              <label className="field">
                <span>Baseline attendance (%)</span>
                <input
                  type="number"
                  min="0"
                  max="100"
                  step="any"
                  value={baseline}
                  aria-invalid={!validBaseline}
                  onChange={(event) => setBaseline(event.target.value)}
                />
              </label>
              <label className="field">
                <span>New player attendance</span>
                <SelectControl
                  value={newRate}
                  onChange={(event) => setNewRate(event.target.value)}
                >
                  <option value="average">Average</option>
                  {newPlayerRates.map((rate) => (
                    <option key={rate} value={rate}>
                      {rate}%
                    </option>
                  ))}
                </SelectControl>
              </label>
              <label className="field">
                <span>When shrinking, drop</span>
                <SelectControl
                  value={drop}
                  onChange={(event) => setDrop(event.target.value as DropMode)}
                >
                  <option value="lowest">Lowest rate</option>
                  <option value="highest">Highest rate</option>
                </SelectControl>
              </label>
            </fieldset>
            {validSize && validBaseline ? (
              <ProbabilityTable
                title="What-if roster probabilities"
                rows={attendanceProbabilities(
                  rates,
                  sizeValue,
                  baselineValue / 100,
                  newRate === "average" ? null : Number(newRate) / 100,
                  drop,
                )}
              />
            ) : (
              <p className="field-error" role="status">
                Enter a roster size from 1 to {maxScenarioSize} and attendance
                rates from 0 to 100%.
              </p>
            )}
          </section>
        </div>
      )}
    </section>
  );
}
