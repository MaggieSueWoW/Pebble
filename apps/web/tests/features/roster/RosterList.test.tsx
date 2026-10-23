// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { RosterList } from "../../../src/features/roster/RosterList";
import {
  blankPlayer,
  type RosterEntry,
} from "../../../src/features/roster/roster";
import type { CharacterEntry } from "../../../src/features/roster/characters";

afterEach(cleanup);

it("sorts every roster column, marks the direction, and keeps details attached", () => {
  const roster: RosterEntry[] = [
    {
      ...blankPlayer(),
      id: "b",
      name: "Beta",
      joinDate: "2026-09-10",
      leaveDate: "2026-10-20",
      version: 1,
    },
    { ...blankPlayer(), id: "missing", name: "Missing", version: 1 },
    {
      ...blankPlayer(),
      id: "a",
      name: "Alpha",
      joinDate: "2026-10-01",
      leaveDate: "2026-10-10",
      version: 1,
    },
    {
      ...blankPlayer(),
      id: "c",
      name: "Gamma",
      joinDate: "2026-10-01",
      version: 1,
    },
  ];
  const characters: CharacterEntry[] = [
    {
      id: "a1",
      playerId: "a",
      name: "Zulu",
      realm: "",
      region: "us",
      version: 1,
    },
    {
      id: "b1",
      playerId: "b",
      name: "Apple",
      realm: "",
      region: "us",
      version: 1,
    },
    // A retained main mapping is hidden and must not affect the Alts sort.
    {
      id: "self",
      playerId: "a",
      name: "Alpha",
      realm: "",
      region: "us",
      version: 1,
    },
  ];
  const editMain = vi.fn();
  const close = vi.fn();
  render(
    <RosterList
      roster={roster}
      characters={characters}
      colors={{}}
      editMain={editMain}
      addMain={vi.fn()}
      editAlt={vi.fn()}
      close={close}
      detailPlayerId="a"
      detail={<div>Alpha editor</div>}
    />,
  );
  const order = () =>
    [...document.querySelectorAll(".roster-main-button strong")].map(
      (el) => el.textContent,
    );
  const active = (label: string, direction: string) => {
    expect(
      screen
        .getByRole("columnheader", { name: label })
        .getAttribute("aria-sort"),
    ).toBe(direction);
    expect(document.querySelectorAll("th[aria-sort]").length).toBe(1);
  };
  expect(order()).toEqual(["Alpha", "Beta", "Gamma", "Missing"]);
  active("Player", "ascending");
  fireEvent.click(screen.getByRole("button", { name: "Player" }));
  expect(order()).toEqual(["Missing", "Gamma", "Beta", "Alpha"]);
  active("Player", "descending");
  for (const [label, ascending, descending] of [
    [
      "Joined",
      ["Beta", "Alpha", "Gamma", "Missing"],
      ["Alpha", "Gamma", "Beta", "Missing"],
    ],
    [
      "Left",
      ["Alpha", "Beta", "Gamma", "Missing"],
      ["Beta", "Alpha", "Gamma", "Missing"],
    ],
    [
      "Alts",
      ["Beta", "Alpha", "Gamma", "Missing"],
      ["Alpha", "Beta", "Gamma", "Missing"],
    ],
  ] as const) {
    fireEvent.click(screen.getByRole("button", { name: label }));
    expect(order()).toEqual(ascending);
    active(label, "ascending");
    fireEvent.click(screen.getByRole("button", { name: label }));
    expect(order()).toEqual(descending);
    active(label, "descending");
  }
  const detailRow = screen.getByText("Alpha editor").closest("tr")!;
  expect(detailRow.previousElementSibling?.textContent).toContain("Alpha");
  expect(editMain).not.toHaveBeenCalled();
  fireEvent.change(screen.getByRole("textbox", { name: "Search roster" }), {
    target: { value: "Apple" },
  });
  expect(order()).toEqual(["Alpha", "Beta"]);
  active("Alts", "descending");
  fireEvent.click(screen.getByRole("button", { name: "Collapse Alpha" }));
  expect(close).toHaveBeenCalledOnce();
  expect(roster.map((player) => player.id)).toEqual(["b", "missing", "a", "c"]);
});
