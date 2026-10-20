import { test, expect } from "@playwright/test";
import {
  initializeTestEnvironment,
  type RulesTestEnvironment,
} from "@firebase/rules-unit-testing";
import { doc, setDoc, Timestamp } from "firebase/firestore";
import { readFileSync } from "node:fs";
let env: RulesTestEnvironment;
const season = "seasons/test-reports";
const attendance = {
  played: 60,
  bench: 20,
  possible: 160,
  rate: 0.5,
  weeks: [
    { week: "2026-10-13", status: "PBO" },
    { week: "2026-10-06", status: "B" },
    { week: "2026-10-20", status: "" },
  ],
};
const row = (id: string, name: string) => ({
  id,
  name,
  className: "Druid",
  role: "Healer",
  color: "#b9c7d8",
  played: 90,
  bench: 30,
  preBench: 10,
  postBench: 20,
  overridden: false,
  nights: 1,
});
test.beforeAll(async () => {
  env = await initializeTestEnvironment({
    projectId: "demo-pebble-reports",
    firestore: {
      host: "127.0.0.1",
      port: 8180,
      rules: readFileSync(
        new URL("../../../firebase/firestore.rules", import.meta.url),
        "utf8",
      ),
    },
  });
});
test.beforeEach(async () => {
  await env.clearFirestore();
  await env.withSecurityRulesDisabled(async (ctx) => {
    const db = ctx.firestore(),
      root = `${season}/results/published`;
    await setDoc(doc(db, root), {
      complete: true,
      completedAt: Timestamp.now(),
      reportCount: 1,
    });
    await setDoc(doc(db, `${root}/players/eligible`), {
      ...row("eligible", "Example"),
      attendance,
    });
    await setDoc(doc(db, `${root}/players/absent`), {
      ...row("absent", "Absent"),
      played: 0,
      bench: 0,
      preBench: 0,
      postBench: 0,
      nights: 0,
      attendance: {
        played: 0,
        bench: 0,
        possible: 160,
        rate: 0,
        weeks: [{ week: "2026-10-13", status: "O" }],
      },
    });
    await setDoc(doc(db, `${root}/players/older`), row("older", "Earlier"));
    await setDoc(doc(db, `${root}/players/zero`), {
      ...row("zero", "Zero"),
      attendance: {
        ...attendance,
        played: 0,
        bench: 0,
        possible: 0,
        rate: null,
        weeks: [],
      },
    });
    await setDoc(doc(db, `${root}/nights/2026-10-15`), {
      night: "2026-10-15",
      duration: 120,
      reports: 1,
      timing: {
        start: "2026-10-15T19:00",
        breakStart: "",
        breakEnd: "",
        end: "2026-10-15T21:00",
        notes: "",
      },
      players: [row("eligible", "Example")],
    });
    await setDoc(doc(db, `${root}/weeks/2026-10-13`), {
      players: [
        {
          main: "Example",
          played_min: 90,
          bench_min: 30,
          bench_pre_min: 10,
          bench_post_min: 20,
        },
      ],
    });
    await setDoc(doc(db, "publicSeasons/test-reports"), {
      latestRunId: "published",
    });
  });
});
test.afterAll(async () => {
  await env.cleanup();
});
test("presents published season attendance separately, orders P/B/O weeks and does not fetch on expansion", async ({
  page,
}) => {
  const resultReads: string[] = [];
  page.on("request", (request) => {
    if (
      request.method() === "POST" &&
      [...new URLSearchParams(request.postData() ?? "").values()].some(
        (value) => value.includes(`${season}/results/`),
      )
    )
      resultReads.push(request.url());
  });
  await page.goto("/");
  const toggle = page.getByRole("button", {
    name: "Details for Example",
    exact: true,
  });
  await expect(toggle).toBeVisible();
  const initialReads = resultReads.length;
  expect(initialReads).toBeGreaterThan(0);
  await toggle.focus();
  await page.keyboard.press("Enter");
  const details = page.getByRole("region", { name: "Attendance", exact: true });
  await expect(details.getByText("50%", { exact: true })).toBeVisible();
  await expect(
    details.getByText("Possible minutes", { exact: true }),
  ).toBeVisible();
  await expect(details.getByText("160 min", { exact: true })).toBeVisible();
  await expect(details.getByText("60 min", { exact: true })).toBeVisible();
  await expect(details.getByText("20 min", { exact: true })).toBeVisible();
  await expect(page.getByText("75% played", { exact: true })).toHaveCount(3);
  const weeks = details
    .getByRole("table", { name: "Weekly attendance" })
    .getByRole("row");
  await expect(weeks.nth(1)).toContainText("October 6");
  await expect(weeks.nth(2).getByLabel("played, benched, out")).toHaveText(
    "PBO",
  );
  await expect(weeks.nth(3)).toContainText("No applicable event");
  await expect(details.getByText(/P = played/)).toContainText("O = out");
  expect(resultReads.length).toBe(initialReads);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await page
    .getByLabel("Result period", { exact: true })
    .selectOption("2026-10-15");
  await page
    .getByRole("button", { name: "Details for Example", exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Availability breakdown for Example" }),
  ).toBeVisible();
  await expect(details).toHaveCount(0);
  await page
    .getByLabel("Result period", { exact: true })
    .selectOption("week:2026-10-13");
  await page
    .getByRole("button", { name: "Details for Example", exact: true })
    .click();
  await expect(
    page.getByRole("region", { name: "Availability breakdown for Example" }),
  ).toBeVisible();
  await expect(details).toHaveCount(0);
  await expect(
    page.getByRole("button", { name: "Details for Example", exact: true }),
  ).toBeVisible();
});
test("keeps old publications and zero denominators unavailable without fabricating eligibility or zero rates", async ({
  page,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Details for Earlier", exact: true })
    .click();
  let details = page.getByRole("region", { name: "Attendance", exact: true });
  await expect(details).toContainText("Attendance unavailable");
  await expect(details).not.toContainText("0%");
  await expect(details).not.toContainText("ineligible");
  await page
    .getByRole("button", { name: "Details for Zero", exact: true })
    .click();
  details = page.getByRole("region", { name: "Attendance", exact: true });
  await expect(details.getByText("Unavailable", { exact: true })).toBeVisible();
  await expect(details.getByText("0 min", { exact: true })).toHaveCount(3);
  await expect(details).toContainText("No possible time was recorded");
  await expect(details).not.toContainText("0%");
});

test("makes fully absent published attendance reachable outside the fairness ranking", async ({
  page,
}) => {
  await page.goto("/");
  const additional = page.getByRole("region", {
    name: "Additional attendance",
  });
  await expect(additional).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Details for Absent", exact: true }),
  ).toHaveCount(0);
  await additional.getByText("Attendance for Absent", { exact: true }).focus();
  await page.keyboard.press("Enter");
  const details = additional.getByRole("region", {
    name: "Attendance",
    exact: true,
  });
  await expect(details.getByText("0%", { exact: true })).toBeVisible();
  await expect(details.getByText("160 min", { exact: true })).toBeVisible();
  await expect(details.getByLabel("out", { exact: true })).toHaveText("O");
  await page
    .getByLabel("Result period", { exact: true })
    .selectOption("2026-10-15");
  await expect(additional).toHaveCount(0);
});
