import { expect, test } from "@playwright/test";
import { doc, updateDoc, Timestamp } from "firebase/firestore";
import { setupEmulator } from "./support/emulator";
import { officer, signIn } from "./support/officer";

const emulator = setupEmulator();

test("officers invite by email, a new Google account accepts, and removal revokes the open session", async ({
  page,
  browser,
}) => {
  await officer(page, emulator.environment);
  await page.getByRole("button", { name: "Officers", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Current officers (1)" }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Leave", exact: true }),
  ).toBeDisabled();
  await page
    .getByRole("textbox", { name: "Google account email" })
    .fill("recipient@example.test");
  await page.getByRole("button", { name: "Add officer", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Add officer?" });
  await expect(dialog).toContainText("manage officer access");
  await dialog.getByRole("button", { name: "Confirm", exact: true }).click();
  const link = await page
    .getByRole("textbox", { name: "Invitation link", exact: true })
    .inputValue();
  await expect(
    page.getByRole("button", { name: "Copy invitation link", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: "/tmp/pebble-officers-desktop.png",
    fullPage: true,
  });

  const context = await browser.newContext();
  const recipient = await context.newPage();
  await signIn(recipient, { email: "recipient@example.test", path: link });
  await expect(
    recipient.getByRole("region", { name: "Officer invitations" }),
  ).toContainText("invited recipient@example.test");
  await recipient
    .getByRole("button", { name: "Accept officer access" })
    .click();
  await expect(
    recipient
      .getByRole("navigation")
      .getByRole("button", { name: "Officers", exact: true }),
  ).toBeVisible();
  await recipient
    .getByRole("navigation")
    .getByRole("button", { name: "Officers", exact: true })
    .click();
  await expect(
    recipient.getByRole("heading", { name: "Current officers (2)" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", { name: "Current officers (2)" }),
  ).toBeVisible();
  await expect(page.getByText("No pending invitations.")).toBeVisible();
  await page
    .getByRole("row")
    .filter({ hasText: "recipient@example.test" })
    .getByRole("button", { name: "Remove access" })
    .click();
  await page
    .getByRole("dialog", { name: "Remove officer access?" })
    .getByRole("button", { name: "Confirm", exact: true })
    .click();
  await expect(
    recipient.getByRole("region", { name: "Officer access not enabled" }),
  ).toBeVisible();
  await expect(
    recipient
      .getByRole("navigation")
      .getByRole("button", { name: "Officers", exact: true }),
  ).toHaveCount(0);
  await context.close();
});

test("wrong-account, cancelled and expired invitations have clear recovery", async ({
  page,
  browser,
}) => {
  await officer(page, emulator.environment);
  await page.getByRole("button", { name: "Officers", exact: true }).click();
  await page
    .getByRole("textbox", { name: "Google account email" })
    .fill("intended@example.test");
  await page.getByRole("button", { name: "Add officer", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Confirm", exact: true })
    .click();
  const link = await page
    .getByRole("textbox", { name: "Invitation link", exact: true })
    .inputValue();
  const context = await browser.newContext();
  const recipient = await context.newPage();
  await signIn(recipient, { email: "wrong@example.test", path: link });
  await expect(
    recipient.getByRole("button", { name: "Switch Google account" }),
  ).toBeVisible();
  await expect(
    recipient.getByRole("button", { name: "Accept officer access" }),
  ).toHaveCount(0);
  const popupPromise = recipient.waitForEvent("popup");
  await recipient
    .getByRole("button", { name: "Switch Google account" })
    .click();
  const popup = await popupPromise;
  await popup.getByRole("button", { name: "Add new account" }).click();
  await popup.locator("#email-input").fill("intended@example.test");
  await popup.locator("#display-name-input").fill("Invited Officer");
  await popup.locator("#sign-in").click();
  await expect(
    recipient.getByRole("button", { name: "Accept officer access" }),
  ).toBeVisible();
  await page
    .getByRole("button", { name: "Cancel invitation", exact: true })
    .click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Confirm", exact: true })
    .click();
  await recipient.reload();
  await expect(
    recipient.getByRole("region", { name: "Officer invitations" }),
  ).toContainText("This invitation is cancelled");

  await page
    .getByRole("textbox", { name: "Google account email" })
    .fill("intended@example.test");
  await page.getByRole("button", { name: "Add officer", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Confirm", exact: true })
    .click();
  const renewed = await page
    .getByRole("textbox", { name: "Invitation link", exact: true })
    .inputValue();
  expect(renewed).not.toEqual(link);
  const id = new URL(renewed).searchParams.get("invite")!;
  await emulator.admin(async (ctx) =>
    updateDoc(doc(ctx.firestore(), `officerInvitations/${id}`), {
      expiresAt: Timestamp.fromMillis(1),
    }),
  );
  await recipient.goto(renewed);
  await expect(
    recipient.getByRole("region", { name: "Officer invitations" }),
  ).toContainText("This invitation is expired");
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({
    path: "/tmp/pebble-officers-mobile.png",
    fullPage: true,
  });
  await context.close();
});

test("matching pending invitation appears on normal Google sign-in without the invitation link", async ({
  page,
  browser,
}) => {
  await officer(page, emulator.environment);
  await page.getByRole("button", { name: "Officers", exact: true }).click();
  await page
    .getByRole("textbox", { name: "Google account email" })
    .fill("normal@example.test");
  await page.getByRole("button", { name: "Add officer", exact: true }).click();
  await page
    .getByRole("dialog")
    .getByRole("button", { name: "Confirm", exact: true })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Invitation link", exact: true }),
  ).toBeVisible();
  const context = await browser.newContext();
  const recipient = await context.newPage();
  await signIn(recipient, { email: "normal@example.test" });
  await expect(
    recipient.getByRole("button", { name: "Accept officer access" }),
  ).toBeVisible();
  await context.close();
});
