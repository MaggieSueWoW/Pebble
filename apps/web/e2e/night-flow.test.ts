import { expect, test } from "@playwright/test";

test("guides an officer through four steps, recalculates edits, and finishes on the selected night", async ({
  page,
}) => {
  await page.goto("/?demo=1");
  const flow = page.getByRole("navigation", { name: "Process a raid night" });
  await expect(flow.getByRole("listitem")).toHaveCount(4);
  await expect(page.getByText(/Selected raid night:/)).toHaveCount(0);
  await page
    .getByRole("button", { name: "Next: Run Pebble and go to Review Night" })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Reports");
  await expect(flow.locator('[aria-current="step"]')).toContainText(
    "Add report",
  );
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Review Night",
    { timeout: 20000 },
  );
  const night = page.getByRole("combobox", { name: "Raid night", exact: true });
  await night.selectOption("2026-10-14");
  await page
    .getByRole("textbox", { name: "Timing notes" })
    .fill("Reviewed break and raid times");
  await page.getByRole("button", { name: "Save timing", exact: true }).click();
  await page
    .getByRole("button", { name: "Recalculate and review overrides →" })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Overrides",
    { timeout: 20000 },
  );
  await expect(night).toHaveValue("2026-10-14");
  await page
    .getByRole("textbox", { name: "Availability notes" })
    .fill("Reviewed availability");
  await page
    .getByRole("button", { name: "Save adjustment", exact: true })
    .click();
  await page
    .getByRole("button", { name: "Recalculate and view results →" })
    .click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Results", {
    timeout: 20000,
  });
  await expect(
    page.getByRole("combobox", { name: "Result period" }),
  ).toHaveValue("2026-10-14");
  await expect(
    page.getByText("You’ve reached the results. Night processing is complete."),
  ).toBeVisible();
  await page.getByRole("button", { name: "Process another night" }).click();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Reports");
  await expect(
    page.getByRole("navigation", { name: "Main navigation" }),
  ).toBeVisible();
});
