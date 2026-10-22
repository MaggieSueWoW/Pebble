import {
  doc,
  serverTimestamp,
  type DocumentData,
  type DocumentSnapshot,
  type Firestore,
  type Transaction,
} from "firebase/firestore";

/** Read every affected main before any writes, including both ends of an alt move. */
export async function readLinkedPlayers(
  transaction: Transaction,
  db: Firestore,
  season: string,
  playerIds: string[],
) {
  const players = await Promise.all(
    [...new Set(playerIds)].map((id) =>
      transaction.get(doc(db, "seasons", season, "players", id)),
    ),
  );
  if (players.some((player) => !player.exists()))
    throw new Error(
      "A linked main is no longer in the roster. Reload and try again.",
    );
  return players;
}

/** Make linked writes conflict with a concurrent main-deletion transaction. */
export function advanceLinkedPlayers(
  transaction: Transaction,
  players: DocumentSnapshot<DocumentData>[],
  uid: string,
) {
  players.forEach((player) =>
    transaction.set(player.ref, {
      ...player.data(),
      version: player.data()!.version + 1,
      updatedBy: uid,
      updatedAt: serverTimestamp(),
    }),
  );
}
