import type { Account, SearchEntry } from "./types";

export function readStorage<T>(key: string, fallback: T, session = false): T {
  try {
    return JSON.parse((session ? sessionStorage : localStorage).getItem(key) || "null") ?? fallback;
  } catch {
    return fallback;
  }
}

export function writeStorage(key: string, value: unknown, session = false): boolean {
  try {
    (session ? sessionStorage : localStorage).setItem(key, JSON.stringify(value));
    return true;
  } catch {
    return false;
  }
}

export function readAccount(): Account | null {
  const account = readStorage<Account | null>("er-account", null, true);

  return account?.token && Date.parse(account.expires_at) > Date.now() ? account : null;
}

export function guestHistory(): SearchEntry[] {
  const entries = readStorage<SearchEntry[]>("er-history", []);

  return Array.isArray(entries) ? entries.filter((item) => item.id && item.response).slice(0, 30) : [];
}
