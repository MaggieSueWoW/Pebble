// @vitest-environment jsdom
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import type { User } from "firebase/auth";
import { afterEach, expect, it, vi } from "vitest";
import { InvitationNotice } from "../../../src/features/officers/InvitationNotice";
import { officerAction } from "../../../src/features/officers/api";

vi.mock("../../../src/features/officers/api", async (importOriginal) => ({
  ...(await importOriginal<
    typeof import("../../../src/features/officers/api")
  >()),
  officerAction: vi.fn(),
}));
afterEach(() => {
  cleanup();
  vi.mocked(officerAction).mockReset();
  history.replaceState(null, "", "/");
});

it("hides an accepted invitation when membership arrives before the acceptance response", async () => {
  const id = "a".repeat(48);
  history.replaceState(null, "", `/?invite=${id}`);
  const invitation = {
    id,
    email: "recipient@example.test",
    invitedByName: "Officer",
    status: "pending",
    matchesAccount: true,
    expiresAt: "2026-10-30T00:00:00Z",
  };
  let releaseAcceptance!: (value: { accepted: boolean }) => void;
  const acceptance = new Promise<{ accepted: boolean }>((resolve) => {
    releaseAcceptance = resolve;
  });
  vi.mocked(officerAction)
    .mockResolvedValueOnce({ invitations: [invitation], invitation })
    .mockReturnValueOnce(acceptance)
    .mockResolvedValueOnce({
      invitations: [],
      invitation: { ...invitation, status: "accepted" },
    });
  const props = {
    user: { uid: "recipient", email: invitation.email } as User,
    switchAccount: vi.fn(),
    authPending: false,
  };
  const view = render(
    <InvitationNotice key="public" {...props} officer={false} />,
  );
  fireEvent.click(
    await screen.findByRole("button", { name: "Accept officer access" }),
  );
  await act(async () => {
    view.rerender(<InvitationNotice key="officer" {...props} officer />);
  });
  expect(
    screen.queryByRole("region", { name: "Officer invitations" }),
  ).toBeNull();
  await act(async () => {
    releaseAcceptance({ accepted: true });
  });
  expect(screen.queryByText(/Loading officer access/)).toBeNull();
});
