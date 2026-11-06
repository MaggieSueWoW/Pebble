import { describe, expect, it } from "vitest";
import { parseReports, stableJSON } from "../../src/shared/model";

const reportCode = "AAAAAAAAAAAAAAAA";
const disabledCode = "BBBBBBBBBBBBBBBB";

describe("report entry", () => {
  it("normalizes report URLs and skips duplicate URLs/codes without accepting other hosts", () => {
    const rows = parseReports(
      `https://www.warcraftlogs.com/reports/${reportCode}#fight=last\n${reportCode}\nhttps://evil.example/reports/${reportCode}`,
      [],
    );
    expect(rows.map((r) => r.state)).toEqual(["ready", "duplicate", "invalid"]);
    expect(rows[0].code).toBe(reportCode);
  });
  it("distinguishes included, excluded, and malformed reports", () => {
    const details = { night: "", title: "", fetched: false };
    expect(
      parseReports(`${reportCode}\n${disabledCode}\ntoo-short`, [
        { ...details, code: reportCode, enabled: true },
        { ...details, code: disabledCode, enabled: false },
      ]).map((r) => r.state),
    ).toEqual(["existing", "disabled", "invalid"]);
  });
});

describe("Firestore values", () => {
  it("compares documents independently of Firestore map key ordering", () => {
    expect(
      stableJSON({ pre: { mode: "partial", minutes: 45 }, notes: "" }),
    ).toBe(stableJSON({ notes: "", pre: { minutes: 45, mode: "partial" } }));
  });
});
