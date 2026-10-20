// @vitest-environment jsdom
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { useCharacterEditor } from "./useCharacterEditor";
import { blankPlayer } from "./roster";
import { blankCharacter } from "./characters";
afterEach(cleanup);
const roster = [{ ...blankPlayer(), id: "main", name: "Example", version: 1 }];
const mapping = {
  ...blankCharacter(),
  id: "stable",
  name: "Alt",
  playerId: "main",
  version: 3,
};
it("keeps a draft bound to its original mapping version and cancels to updated remote data", async () => {
  const save = vi
    .fn()
    .mockRejectedValue(new Error("Another officer changed this mapping"));
  const { result, rerender } = renderHook(
    ({ characters }) =>
      useCharacterEditor(characters, roster, save, false, true),
    { initialProps: { characters: [mapping] } },
  );
  act(() => result.current.change((old) => ({ ...old, realm: "Draft Realm" })));
  rerender({ characters: [{ ...mapping, version: 4, realm: "Remote Realm" }] });
  await act(() => result.current.save());
  expect(save).toHaveBeenCalledWith(
    "stable",
    {
      ...blankCharacter(),
      name: "Alt",
      playerId: "main",
      realm: "Draft Realm",
    },
    3,
  );
  expect(result.current.error).toContain("Another officer");
  act(() => result.current.cancel());
  expect(result.current.value.realm).toBe("Remote Realm");
});
it("keeps stable identity through reordered snapshots and creates a separate mapping ID", async () => {
  let release!: (version: number) => void;
  const save = vi.fn(
    () =>
      new Promise<number>((resolve) => {
        release = resolve;
      }),
  );
  const other = { ...mapping, id: "other", name: "Beta" };
  const { result, rerender } = renderHook(
    ({ characters }) =>
      useCharacterEditor(characters, roster, save, false, true),
    { initialProps: { characters: [mapping, other] } },
  );
  act(() => result.current.change((old) => ({ ...old, name: "Zed" })));
  let pending!: Promise<void>;
  act(() => {
    pending = result.current.save();
  });
  rerender({ characters: [other, { ...mapping, name: "Zed", version: 4 }] });
  expect(result.current.id).toBe("stable");
  await act(async () => {
    release(4);
    await pending;
  });
  expect(result.current.id).toBe("stable");
  act(() => result.current.add());
  const id = result.current.id;
  expect(id).not.toBe("stable");
  rerender({ characters: [other, mapping] });
  expect(result.current.id).toBe(id);
});
