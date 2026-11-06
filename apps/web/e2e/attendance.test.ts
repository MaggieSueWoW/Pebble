import { expect, test } from "@playwright/test";
import { doc, getDoc, setDoc, Timestamp } from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { signIn } from "./support/officer";
const emulator = setupEmulator();
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
test.beforeEach(async () => {
  await emulator.admin(async (ctx) => {
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
    const night = {
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
    };
    await setDoc(doc(db, `${root}/nights/2026-10-15`), night);
    await setDoc(doc(db, `${root}/nights/2026-10-13`), {
      ...night,
      night: "2026-10-13",
    });
    await setDoc(doc(db, `${root}/officer/attendance`), {
      byPlayer: {
        absent: { "2026-10-13": "O", "2026-10-15": "O" },
        eligible: { "2026-10-15": "PBO" },
        zero: {},
      },
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
  await expect(details.locator("details")).toHaveCount(0);
  await expect(details).toContainText(
    "(1h 00m eligible played + 20m eligible bench) / 2h 40m possible",
  );
  await expect(page.getByText("75%", { exact: true })).toHaveCount(3);
  const weeks = details
    .getByRole("table", { name: "Weekly attendance" })
    .getByRole("row");
  await expect(weeks.nth(1)).toContainText("October 6");
  await expect(weeks.nth(2)).toContainText("played, benched, out");
  await expect(weeks.nth(3)).toContainText("No applicable event");
  await expect(details.getByText(/P = played/)).toHaveCount(0);
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
  await expect(page.getByRole("option", { name: /^Week of/ })).toHaveCount(0);
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
  await expect(details).toContainText("Attendance not recorded");
  await expect(details).not.toContainText("0%");
  await expect(details).not.toContainText("ineligible");
  await page
    .getByRole("button", { name: "Details for Zero", exact: true })
    .click();
  details = page.getByRole("region", { name: "Attendance", exact: true });
  await expect(details.getByText("Unavailable", { exact: true })).toBeVisible();
  await expect(details).toContainText(
    "(0m eligible played + 0m eligible bench) / 0m possible",
  );
  await expect(details).toContainText("No possible time recorded");
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
  await expect(details.getByText("2h 40m", { exact: true })).toBeVisible();
  await expect(details.getByText("out", { exact: true })).toHaveText("out");
  await expect(page).toHaveURL(/player=absent/);
  await page.reload();
  await expect(details.getByText("0%", { exact: true })).toBeVisible();
  await expect(page.getByText(/This player is not available/)).toHaveCount(0);
  await additional.getByText("Attendance for Absent", { exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(page).not.toHaveURL(/player=absent/);
  await expect(details).not.toBeVisible();
  await page
    .getByLabel("Result period", { exact: true })
    .selectOption("2026-10-15");
  await expect(additional).toHaveCount(0);
});

test("officers see nightly attendance and can model a smaller roster", async ({
  page,
}) => {
  const uid = await signIn(page);
  const attendanceLink = page.getByRole("button", {
    name: "Attendance",
    exact: true,
  });
  await expect(attendanceLink).toHaveCount(0);
  await emulator.admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  await attendanceLink.click();
  const table = page.getByRole("table", {
    name: "Season attendance by player",
  });
  await expect(table.getByRole("columnheader")).toHaveText([
    "Player",
    "Attendance %",
    "Played",
    "Bench",
    "Total possible",
    "2026-10-13",
    "2026-10-15",
  ]);
  await expect(table.getByRole("rowheader")).toHaveText([
    "Absent",
    "Example",
    "Zero",
  ]);
  const players = table.getByRole("rowheader");
  const header = (name: string) =>
    table.getByRole("columnheader", { name, exact: true });
  await expect(header("Player")).toHaveAttribute("aria-sort", "ascending");
  await header("Player").getByRole("button").click();
  await expect(players).toHaveText(["Zero", "Example", "Absent"]);
  for (const [name, ascending, descending] of [
    [
      "Attendance %",
      ["Absent", "Example", "Zero"],
      ["Example", "Absent", "Zero"],
    ],
    ["Played", ["Absent", "Zero", "Example"], ["Example", "Absent", "Zero"]],
    ["Bench", ["Absent", "Zero", "Example"], ["Example", "Absent", "Zero"]],
    [
      "Total possible",
      ["Zero", "Absent", "Example"],
      ["Absent", "Example", "Zero"],
    ],
  ] as const) {
    await header(name).getByRole("button").click();
    await expect(header(name)).toHaveAttribute("aria-sort", "ascending");
    await expect(players).toHaveText(ascending);
    await header(name).getByRole("button").click();
    await expect(header(name)).toHaveAttribute("aria-sort", "descending");
    await expect(players).toHaveText(descending);
  }
  const example = table.getByRole("row").filter({ hasText: "Example" });
  await expect(example.getByText("Example", { exact: true })).toHaveCSS(
    "color",
    "rgb(185, 199, 216)",
  );
  const legend = page.getByRole("list", { name: "Nightly attendance legend" });
  await expect(legend.getByRole("listitem")).toHaveCount(4);
  await expect(example.getByText("50.0%", { exact: true })).toBeVisible();
  await expect(example.getByText("1h 00m", { exact: true })).toBeVisible();
  await expect(example.getByText("20m", { exact: true })).toBeVisible();
  await expect(example.getByText("2h 40m", { exact: true })).toBeVisible();
  await expect(example.getByTitle("No applicable status")).toBeVisible();
  await expect(example.getByText("PBO", { exact: true })).toBeVisible();
  await expect(
    table
      .getByRole("row")
      .filter({ hasText: "Absent" })
      .getByText("O", { exact: true }),
  ).toHaveCount(2);
  const current = page.getByRole("table", {
    name: "Current roster probabilities",
  });
  await expect(current.getByRole("row").nth(1).getByRole("cell")).toHaveText([
    "97.8%",
    "50.0%",
    "-47.8 pp",
  ]);
  const scenario = page.getByRole("table", {
    name: "What-if roster probabilities",
  });
  await expect(
    scenario.getByRole("rowheader", { name: "20", exact: true }),
  ).toBeVisible();
  await page.getByRole("spinbutton", { name: "Roster size" }).fill("1");
  await expect(scenario.getByRole("row").nth(1).getByRole("cell")).toHaveText([
    "85.0%",
    "50.0%",
    "-35.0 pp",
  ]);
  await page.getByLabel("When shrinking, drop").selectOption("highest");
  await expect(scenario.getByRole("row").nth(1).getByRole("cell")).toHaveText([
    "85.0%",
    "0.0%",
    "-85.0 pp",
  ]);
  await page.getByRole("spinbutton", { name: "Roster size" }).fill("3");
  const newPlayer = page.getByRole("combobox", {
    name: "New player attendance",
  });
  await expect(newPlayer.getByRole("option")).toHaveText([
    "Average",
    "50%",
    "55%",
    "60%",
    "65%",
    "70%",
    "75%",
    "80%",
    "85%",
    "90%",
    "95%",
    "100%",
  ]);
  const chanceOfTwo = scenario.getByRole("row").nth(2).getByRole("cell").nth(1);
  await expect(chanceOfTwo).toHaveText("50.0%");
  await newPlayer.selectOption("average");
  await expect(chanceOfTwo).toHaveText("12.5%");
  await newPlayer.selectOption("50");
  await expect(chanceOfTwo).toHaveText("25.0%");
  await page.setViewportSize({ width: 390, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  await emulator.admin(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: false });
  });
  await expect(table).toHaveCount(0);
  await expect(attendanceLink).toHaveCount(0);
});

test("keeps summary columns pinned while scrolling across 14 raid nights on desktop", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const uid = await signIn(page);
  await emulator.admin(async (ctx) => {
    const db = ctx.firestore();
    const nights = `${season}/results/published/nights`;
    const sample = (await getDoc(doc(db, `${nights}/2026-10-15`))).data();
    await Promise.all([
      setDoc(doc(db, `officers/${uid}`), { enabled: true }),
      ...Array.from({ length: 12 }, (_, index) => {
        const night = `2026-10-${16 + index}`;
        return setDoc(doc(db, `${nights}/${night}`), { ...sample, night });
      }),
    ]);
  });
  await page.reload();
  await page.getByRole("button", { name: "Attendance", exact: true }).click();
  const grid = page.getByRole("region", { name: "Attendance grid" });
  await expect(grid.getByRole("columnheader")).toHaveCount(19);

  const measureScroll = () =>
    grid.evaluate((region) => {
      region.scrollLeft = 0;
      const headers = [...region.querySelectorAll("thead th")];
      const firstRow = [...region.querySelectorAll("tbody tr:first-child > *")];
      const cells = [...headers.slice(0, 5), ...firstRow.slice(0, 5)];
      const before = cells.map((cell) => cell.getBoundingClientRect().left);
      region.scrollLeft = region.scrollWidth;
      return {
        distance: region.scrollLeft,
        offsets: cells.map(
          (cell, index) => cell.getBoundingClientRect().left - before[index],
        ),
        latestDateVisible:
          headers.at(-1)!.getBoundingClientRect().right <=
          region.getBoundingClientRect().right + 1,
      };
    });
  const desktop = await measureScroll();
  expect(desktop.distance).toBeGreaterThan(0);
  expect(desktop.latestDateVisible).toBe(true);
  for (const offset of desktop.offsets)
    expect(Math.abs(offset)).toBeLessThan(1);

  await page.setViewportSize({ width: 390, height: 844 });
  const mobile = await measureScroll();
  expect(mobile.latestDateVisible).toBe(true);
  expect(Math.abs(mobile.offsets[0])).toBeLessThan(1);
  expect(mobile.offsets[1]).toBeLessThan(-1);
  await expect(
    page.getByRole("link", { name: "attendance probability guide" }),
  ).toHaveAttribute(
    "href",
    "https://github.com/MaggieSueWoW/Pebble/blob/main/Attendance_Probability.md",
  );
});
