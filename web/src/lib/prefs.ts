import { useSyncExternalStore } from "react";

export type ThemePref = "light" | "dark" | "system";
export type AutoRefresh = "off" | "5s" | "10s" | "30s";
export type Density = "compact" | "comfortable";

export interface Prefs {
  theme: ThemePref;
  autoRefresh: AutoRefresh;
  density: Density;
  relativeTime: boolean;
  apiBase: string;
}

const KEY = "forgequeue-ui:prefs";
const THEME_KEY = "forgequeue-ui:theme";

const defaults: Prefs = {
  theme: "dark",
  autoRefresh: "5s",
  density: "compact",
  relativeTime: true,
  apiBase: import.meta.env["VITE_API_BASE_URL"] || "/api",
};

let state: Prefs = defaults;
let loaded = false;
const listeners = new Set<() => void>();

function load() {
  if (loaded || typeof window === "undefined") return;
  loaded = true;
  try {
    const raw = localStorage.getItem(KEY);
    if (raw) state = { ...defaults, ...JSON.parse(raw) };
    const t = localStorage.getItem(THEME_KEY) as ThemePref | null;
    if (t) state = { ...state, theme: t };
  } catch {
    /* ignore */
  }
}

export function getPrefs(): Prefs {
  load();
  return state;
}

export function setPrefs(patch: Partial<Prefs>) {
  load();
  state = { ...state, ...patch };
  try {
    localStorage.setItem(KEY, JSON.stringify(state));
    localStorage.setItem(THEME_KEY, state.theme);
  } catch {
    /* ignore */
  }
  listeners.forEach((l) => l());
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => listeners.delete(l);
};

export function usePrefs(): Prefs {
  return useSyncExternalStore(subscribe, getPrefs, () => defaults);
}

export function refreshMs(a: AutoRefresh): number | false {
  return a === "off" ? false : Number(a.replace("s", "")) * 1000;
}
