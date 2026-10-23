/** Recovery text authored here; never display an SDK payload or account token. */
export function authMessage(error: unknown): string {
  const code =
    typeof error === "object" && error !== null && "code" in error
      ? String(error.code)
      : "";
  switch (code) {
    case "auth/popup-blocked":
      return "Your browser blocked Google sign-in. Allow popups for this site, then try Officer sign-in again in Chrome.";
    case "auth/popup-closed-by-user":
      return "Google sign-in was cancelled. Try again and choose the intended officer account.";
    case "auth/cancelled-popup-request":
      return "Another Google sign-in is already open. Finish or close that window, then try again.";
    case "auth/unauthorized-domain":
      return "Google sign-in is not enabled for this site. Ask the administrator to authorize this hostname in Firebase Authentication.";
    case "auth/operation-not-allowed":
      return "Google sign-in is not enabled. Ask the administrator to enable the Google provider in Firebase Authentication.";
    case "auth/network-request-failed":
      return "Couldn’t reach Google sign-in. Check your connection, then try again in Chrome.";
    case "auth/account-exists-with-different-credential":
      return "This account uses a different sign-in method. Choose the intended Google account or ask the administrator for help.";
    case "auth/user-disabled":
      return "This sign-in account is disabled. Ask the administrator to review its Firebase Authentication status.";
    default:
      return "Couldn’t complete the account action. Try again in Chrome, or ask the administrator for help.";
  }
}
