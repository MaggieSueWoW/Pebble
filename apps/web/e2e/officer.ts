import { expect, type Page } from "@playwright/test";
import type { RulesTestEnvironment } from "@firebase/rules-unit-testing";
import { doc, setDoc } from "firebase/firestore";

export async function signIn(page: Page) {
  await page.goto("/");
  const popupPromise = page.waitForEvent("popup");
  await page.getByRole("button", { name: "Officer sign-in" }).click();
  const popup = await popupPromise;
  await popup.waitForLoadState("load");
  await popup.getByRole("button", { name: "Add new account" }).click();
  await popup
    .locator("#email-input")
    .fill(`officer-${Date.now()}@example.test`);
  await popup.locator("#display-name-input").fill("Example Officer");
  await popup.locator("#sign-in").click();
  const notice = page.getByRole("region", {
    name: "Officer access not enabled",
  });
  await expect(notice).toBeVisible();
  await notice.getByText("Account details", { exact: true }).click();
  const uid = (await notice.locator("code").innerText()).trim();
  return uid;
}

export async function officer(page: Page, env: RulesTestEnvironment) {
  const uid = await signIn(page);
  await env.withSecurityRulesDisabled(async (ctx) => {
    await setDoc(doc(ctx.firestore(), `officers/${uid}`), { enabled: true });
  });
  await page
    .getByRole("navigation")
    .getByRole("button", { name: /^Reports/ })
    .click();
  await expect(
    page.getByRole("textbox", { name: "Warcraft Logs reports" }),
  ).toBeVisible();
}
