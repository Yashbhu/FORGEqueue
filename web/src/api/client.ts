import { useSyncExternalStore } from "react";
import { getPrefs } from "@/lib/prefs";
import { clearSession, getToken } from "@/lib/auth";

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public body?: unknown,
  ) {
    super(message);
  }
}

export type ConnStatus = "connected" | "connecting" | "disconnected" | "error";
interface ConnState {
  status: ConnStatus;
  lastSuccessAt: number | null;
  lastError: string | null;
}

let conn: ConnState = { status: "connecting", lastSuccessAt: null, lastError: null };
const subs = new Set<() => void>();
function setConn(p: Partial<ConnState>) {
  conn = { ...conn, ...p };
  subs.forEach((s) => s());
}
const serverConn: ConnState = { status: "connecting", lastSuccessAt: null, lastError: null };
export const useConnection = () =>
  useSyncExternalStore(
    (l) => (subs.add(l), () => subs.delete(l)),
    () => conn,
    () => serverConn,
  );

export const getApiBase = () => getPrefs().apiBase.replace(/\/$/, "");

// Auth header is injected here from the session store so components never
// deal with tokens directly.
function headers(): HeadersInit {
  const h: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  const token = getToken();
  if (token) h["Authorization"] = `Bearer ${token}`;
  return h;
}

export async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 10_000);
  try {
    const res = await fetch(`${getApiBase()}${path}`, {
      method,
      headers: headers(),
      body: body === undefined ? null : JSON.stringify(body),
      signal: ctrl.signal,
    });
    const text = await res.text();
    const data = text ? JSON.parse(text) : undefined;
    if (!res.ok) {
      // A rejected token means the session is no longer valid; drop it so the
      // root guard sends the user back to the login page.
      if (res.status === 401) clearSession();
      throw new ApiError(data?.message ?? res.statusText, res.status, data);
    }
    setConn({ status: "connected", lastSuccessAt: Date.now(), lastError: null });
    return data as T;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    setConn({ status: e instanceof ApiError ? "error" : "disconnected", lastError: msg });
    throw e;
  } finally {
    clearTimeout(t);
  }
}
