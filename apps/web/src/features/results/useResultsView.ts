import { useEffect, useState } from "react";
import type { Player, Results } from "../../shared/model";
import {
  orderResultRows,
  sortResultRows,
  type ResultSort,
} from "./resultRanking";
import {
  resultLink,
  type useResultNavigation,
} from "../../app/useResultNavigation";

export type ResultLinkNotice = {
  text: string;
  period: string;
  player: string | null;
};
/** Keep result selection and sorting alive while another workspace page is open. */
export function useResultsView({
  results,
  players,
  navigation,
  selectionReady,
  demo,
  notify,
}: {
  results: Results;
  players: Player[];
  navigation: ReturnType<typeof useResultNavigation>;
  selectionReady: boolean;
  demo: boolean;
  notify: (text: string, error?: boolean) => void;
}) {
  const { page, navigate } = navigation;
  const [resultSort, setResultSort] = useState<ResultSort | null>(null);
  const activeResultSort: ResultSort = resultSort ?? {
    key: "bench",
    direction: "ascending",
  };
  function changeResultSort(key: ResultSort["key"]) {
    setResultSort((selection) => {
      const current: ResultSort = selection ?? {
        key: "bench",
        direction: "ascending",
      };
      return {
        key,
        direction:
          current.key === key
            ? current.direction === "ascending"
              ? "descending"
              : "ascending"
            : key === "name" || key === "bench"
              ? "ascending"
              : "descending",
      };
    });
  }
  const resultNight = navigation.period;
  const expandedPlayer = navigation.player;
  const setResultNight = (period: string) => navigate({ period, player: null });
  const setExpandedPlayer = (player: string | null) => navigate({ player });
  const [resultLinkNotice, setResultLinkNotice] =
    useState<ResultLinkNotice | null>(null);
  const periodAvailable = results.nights.some((n) => n.night === resultNight);
  const visibleResultNight = periodAvailable ? resultNight : "season";
  const resultRows = sortResultRows(
    orderResultRows(
      visibleResultNight === "season"
        ? results.players
        : (results.nights.find((n) => n.night === visibleResultNight)
            ?.players ?? []),
      visibleResultNight === "season" ? "season" : "night",
      (row) => row.name ?? players.find((p) => p.id === row.id)?.name ?? row.id,
    ),
    resultSort,
    (row) =>
      row.name ??
      players.find((player) => player.id === row.id)?.name ??
      row.id,
  );
  const additionalAttendance =
    visibleResultNight === "season"
      ? results.players.filter(
          (row) =>
            row.attendance &&
            !resultRows.some((ranked) => ranked.id === row.id),
        )
      : [];
  const resultSelectionReady = selectionReady;
  const playerAvailable =
    !expandedPlayer ||
    resultRows.some((row) => row.id === expandedPlayer) ||
    additionalAttendance.some((row) => row.id === expandedPlayer);
  useEffect(() => {
    if (page !== "results" || !resultSelectionReady) return;
    if (resultNight !== "season" && !periodAvailable) {
      setResultLinkNotice({
        text: "This result period is not available in the latest completed results. Showing season totals.",
        period: "season",
        player: null,
      });
      navigate({ period: "season", player: null }, true);
    } else if (expandedPlayer && !playerAvailable) {
      setResultLinkNotice({
        text: "This player is not available for this result period. Showing all players.",
        period: resultNight,
        player: null,
      });
      navigate({ player: null }, true);
    }
  }, [
    page,
    resultNight,
    expandedPlayer,
    resultSelectionReady,
    periodAvailable,
    playerAvailable,
    navigate,
  ]);
  async function copyResultLink() {
    try {
      await navigator.clipboard.writeText(
        resultLink(
          { period: visibleResultNight, player: expandedPlayer },
          demo,
        ),
      );
      notify("Result link copied.");
    } catch {
      notify(
        "Couldn’t copy the link. Copy the address from your browser.",
        true,
      );
    }
  }
  return {
    resultRows,
    additionalAttendance,
    visibleResultNight,
    resultNight,
    expandedPlayer,
    resultLinkNotice,
    setResultLinkNotice,
    setResultNight,
    setExpandedPlayer,
    resultSelectionReady,
    copyResultLink,
    activeResultSort,
    changeResultSort,
  };
}
export type ResultsView = ReturnType<typeof useResultsView>;
