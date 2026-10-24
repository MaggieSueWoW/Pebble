import { describe, expect, it } from "vitest";
import { demoCode, freshState } from "../../src/demo/model";
import { parseReports, stableJSON } from "../../src/shared/model";

describe("report entry", () => {
  it("normalizes report URLs and skips duplicate URLs/codes without accepting other hosts", () => {
    const rows = parseReports(
      `https://www.warcraftlogs.com/reports/${demoCode}#fight=last\n${demoCode}\nhttps://evil.example/reports/${demoCode}`,
      [],
    );
    expect(rows.map((r) => r.state)).toEqual(["ready", "duplicate", "invalid"]);
    expect(rows[0].code).toBe(demoCode);
  });
  it("distinguishes included, excluded, and malformed reports", () => {
    const { inputs } = freshState();
    expect(
      parseReports(
        inputs.reports.map((r) => r.code).join("\n") + "\ntoo-short",
        inputs.reports,
      ).map((r) => r.state),
    ).toEqual(["existing", "existing", "existing", "disabled", "invalid"]);
  });
});

describe("Firestore values", () => {
  it("compares documents independently of Firestore map key ordering", () => {
    expect(
      stableJSON({ pre: { mode: "partial", minutes: 45 }, notes: "" }),
    ).toBe(stableJSON({ notes: "", pre: { minutes: 45, mode: "partial" } }));
  });
});
