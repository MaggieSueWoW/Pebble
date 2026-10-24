import { httpsCallable } from "firebase/functions";
import { functions } from "../../data/firebase";

export type Invitation = {
  id: string;
  email: string;
  invitedByName: string;
  expiresAt: string;
  status: string;
  matchesAccount?: boolean;
};
export type Session = {
  invitations: Invitation[];
  invitation: Invitation | null;
};
export async function officerAction<T>(
  data: Record<string, string>,
): Promise<T> {
  if (!functions) throw new Error("Firebase is not configured.");
  const result = await httpsCallable<Record<string, string>, T>(
    functions,
    "officerAccess",
  )(data);
  return result.data;
}
export function accessMessage(error: unknown): string {
  if (
    error instanceof Error &&
    "code" in error &&
    typeof error.code === "string" &&
    [
      "functions/permission-denied",
      "functions/unauthenticated",
      "functions/invalid-argument",
      "functions/failed-precondition",
      "functions/already-exists",
      "functions/not-found",
      "functions/resource-exhausted",
    ].includes(error.code)
  )
    return error.message;
  return "Couldn’t complete the officer action. Check your connection and try again.";
}
export function invitationLink(id: string): string {
  const url = new URL(location.pathname, location.origin);
  url.searchParams.set("invite", id);
  return url.href;
}
