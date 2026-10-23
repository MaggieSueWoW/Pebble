// @vitest-environment jsdom
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it } from "vitest";
import {
  readResultLocation,
  resultLink,
  useResultNavigation,
} from "../src/useResultNavigation";

beforeEach(() => history.replaceState(null, "", "/"));
afterEach(cleanup);

it("reads public result IDs and copies only public selection for the configured season", () => {
  history.replaceState(
    null,
    "",
    "/?demo=1&privateOption=ignored&view=results&period=week%3A2026-10-13&player=public%2Fid#old",
  );
  expect(readResultLocation("reports")).toEqual({
    page: "results",
    period: "week:2026-10-13",
    player: "public/id",
  });
  const params = new URL(
    resultLink({ period: "week:2026-10-13", player: "public/id" }),
  ).searchParams;
  expect([...params.keys()]).toEqual(["view", "period", "player"]);
  expect(params.get("player")).toBe("public/id");
  expect(
    new URL(
      resultLink({ period: "season", player: null }, true),
    ).searchParams.get("demo"),
  ).toBe("1");
});

it("restores period and player through back/forward without rewriting history", async () => {
  const { result } = renderHook(() =>
    useResultNavigation(
      "reports",
      () => false,
      (action) => action(),
    ),
  );
  act(() =>
    result.current.navigate({
      page: "results",
      period: "2026-10-14",
      player: "player-a",
    }),
  );
  act(() =>
    result.current.navigate({ period: "week:2026-10-13", player: null }),
  );
  act(() => history.back());
  await waitFor(() => expect(result.current.period).toBe("2026-10-14"));
  expect(result.current.player).toBe("player-a");
  act(() => history.forward());
  await waitFor(() => expect(result.current.period).toBe("week:2026-10-13"));
  expect(result.current.player).toBeNull();
});

it("restores the history cursor before guarded back, allowing cancellation and a later discard", async () => {
  let blocked = false;
  let pending: (() => void) | undefined;
  const { result } = renderHook(() =>
    useResultNavigation(
      "reports",
      () => blocked,
      (action) => {
        pending = action;
      },
    ),
  );
  act(() => result.current.navigate({ page: "results", period: "2026-10-14" }));
  act(() => result.current.navigate({ page: "reports" }));
  blocked = true;
  const previous = location.href;
  act(() => history.back());
  await waitFor(() => expect(pending).toBeTypeOf("function"));
  expect(location.href).toBe(previous);
  expect(result.current.page).toBe("reports");
  pending = undefined; // Existing Keep editing action cancels the pending navigation.
  act(() => history.back());
  await waitFor(() => expect(pending).toBeTypeOf("function"));
  act(() => pending!());
  await waitFor(() => expect(result.current.page).toBe("results"));
  expect(result.current.period).toBe("2026-10-14");
});
