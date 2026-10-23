import { useRef, useState } from "react";
import type { LiveConnection } from "../../data/useFirestore";
import type { MissingPlayer } from "../results/resultProvenance";
import { blankPlayer, classes } from "../roster/roster";

export type Resolution = "ignore" | "main" | "alt";
export type MissingPlayerDraft = {
  id: string;
  player: MissingPlayer;
  night: string;
  mode: Resolution;
  className: string;
  mainId: string;
};
export function useMissingPlayerEditor(connection?: LiveConnection) {
  const [draft, setDraft] = useState<MissingPlayerDraft | null>(null);
  const [saving, setSaving] = useState(false);
  const pending = useRef(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  function cancel() {
    if (pending.current) return;
    setDraft(null);
    setError("");
  }
  function open(player: MissingPlayer, night: string, mode: Resolution) {
    const value: MissingPlayerDraft = {
      id: crypto.randomUUID(),
      player,
      night,
      mode,
      className:
        classes.find(
          (name) =>
            name.replaceAll(" ", "").toLowerCase() ===
            player.className.replaceAll(" ", "").toLowerCase(),
        ) ?? "Unknown",
      mainId: "",
    };
    setDraft(value);
    setError("");
    setMessage("");
  }
  async function commit(value: MissingPlayerDraft) {
    if (!connection?.officer || connection.busy || pending.current) return;
    pending.current = true;
    setSaving(true);
    setError("");
    try {
      if (value.mode === "main") {
        await connection.savePlayer(
          value.id,
          {
            ...blankPlayer(),
            name: value.player.name,
            className: value.className,
            joinDate: value.night,
          },
          0,
        );
      } else if (value.mode === "alt") {
        await connection.saveCharacter(
          value.id,
          {
            name: value.player.name,
            realm: value.player.realm,
            region: value.player.region,
            playerId: value.mainId,
          },
          0,
        );
      } else {
        await connection.ignoreCharacter(value.id, {
          name: value.player.name,
          realm: value.player.realm,
          region: value.player.region,
        });
      }
      setDraft(null);
      setMessage(
        `${value.player.name}: ${value.mode === "ignore" ? "ignored for this season" : value.mode === "main" ? `main added from ${value.night}` : "alt added"}. Run Pebble to update results.`,
      );
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Could not save. Try again.",
      );
    } finally {
      pending.current = false;
      setSaving(false);
    }
  }
  return {
    draft,
    saving,
    dirty: Boolean(draft),
    error,
    message,
    open,
    cancel,
    change: (value: Partial<MissingPlayerDraft>) =>
      setDraft((current) => (current ? { ...current, ...value } : current)),
    save: () => (draft ? commit(draft) : Promise.resolve()),
  };
}
export type MissingPlayerEditor = ReturnType<typeof useMissingPlayerEditor>;
