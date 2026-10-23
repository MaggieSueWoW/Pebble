import type { User } from "firebase/auth";
import type { Inputs, Player, Saved } from "../../model";
import type { CalculationSettings } from "../../calculation";
import type { CharacterEntry, CharacterValue } from "../../characters";
import type { RosterEntry, RosterValue } from "../../roster";
import type { RecordedNightDiagnostics } from "../../resultProvenance";
export type Versions = Record<string, number>;
export type LiveConnection = {
  saved: Saved;
  players: Player[];
  roster: RosterEntry[];
  characters: CharacterEntry[];
  saveCharacter: (
    id: string,
    value: CharacterValue,
    expected: number,
  ) => Promise<number>;
  calculation: CalculationSettings;
  saveCalculation: (
    value: CalculationSettings,
    expected: number,
  ) => Promise<number>;
  deletePlayer: (id: string, expected: Versions) => Promise<void>;
  deleteCharacter: (id: string, expected: number) => Promise<void>;
  swapMain: (
    playerId: string,
    altId: string,
    playerVersion: number,
    altVersion: number,
  ) => Promise<void>;
  savePlayer: (
    id: string,
    value: RosterValue,
    expected: number,
  ) => Promise<number>;
  nights: string[];
  user: User | null;
  officer: boolean;
  accessGeneration: number;
  label: string;
  online: boolean;
  active: { id: string; status: "queued" | "running"; stage: number } | null;
  versions: Versions;
  unprocessed: boolean;
  error: string;
  accessError: string;
  retryAccess: () => void;
  busy: boolean;
  hasResults: boolean;
  submitting: boolean;
  queueReady: boolean;
  queueError: string;
  retryQueue: () => void;
  resultSources: { code: string; title: string; warningIndexes?: number[] }[];
  resultDiagnostics: Record<string, RecordedNightDiagnostics>;
  resultsPending: boolean;
  resultsError: string;
  retryResults: () => void;
  saveInputs: (next: Inputs, expected?: Versions) => Promise<Versions>;
  requestRun: () => Promise<void>;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
};
