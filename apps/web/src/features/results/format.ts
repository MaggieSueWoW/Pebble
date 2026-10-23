import { minutes } from "../../shared/model";

/** Round presentation only; calculations and sorting retain published precision. */
export const resultMinutes = (value: number) => minutes(Math.round(value));
