import { useSyncExternalStore } from "react";

export type Permission =
  | "system:read"
  | "queues:read"
  | "queues:write"
  | "jobs:read"
  | "jobs:write"
  | "workers:read"
  | "dlq:read"
  | "dlq:write"
  | "delayed:read"
  | "delayed:write"
  | "metrics:read";

export interface SessionUser {
  username: string;
  roles: string[];
  permissions: Permission[];
}

export interface Session {
  token: string;
  expiresAt: string;
  user: SessionUser;
}

const KEY = "forgequeue-ui:session";

let state: Session | null = null;
let loaded = false;
const listeners = new Set<() => void>();

function isExpired(s: Session): boolean {
  const t = Date.parse(s.expiresAt);
  return Number.isFinite(t) && t <= Date.now();
}

function load() {
  if (loaded || typeof window === "undefined") return;
  loaded = true;
  try {
    const raw = localStorage.getItem(KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as Session;
      if (parsed?.token && !isExpired(parsed)) state = parsed;
      else localStorage.removeItem(KEY);
    }
  } catch {
    /* ignore */
  }
}

function persist() {
  try {
    if (state) localStorage.setItem(KEY, JSON.stringify(state));
    else localStorage.removeItem(KEY);
  } catch {
    /* ignore */
  }
}

function emit() {
  listeners.forEach((l) => l());
}

/** The current session, or null when unauthenticated/expired. */
export function getSession(): Session | null {
  load();
  return state;
}

/** The stored bearer token, if any. */
export function getToken(): string | null {
  return getSession()?.token ?? null;
}

export function setSession(next: Session) {
  load();
  state = next;
  persist();
  emit();
}

export function clearSession() {
  load();
  state = null;
  persist();
  emit();
}

/** Whether the current session holds permission p. */
export function hasPermission(p: Permission): boolean {
  const s = getSession();
  return !!s && s.user.permissions.includes(p);
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => listeners.delete(l);
};

export function useSession(): Session | null {
  return useSyncExternalStore(subscribe, getSession, () => null);
}

export function useHasPermission(p: Permission): boolean {
  const s = useSession();
  return !!s && s.user.permissions.includes(p);
}
