import type { Account, SearchEntry, SearchResponse } from "../types";

interface SavedSearch {
  id: string;
  query: string;
  created_at: string;
  response: SearchResponse;
}

export async function userRequest<T>(
  path: string, token?: string, method = "GET", body?: unknown,
): Promise<T> {
  const response = await fetch(`/api/user${path}`, {
    method, signal: AbortSignal.timeout(15_000),
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  if (!response.ok) {
    const messages: Record<number, string> = {
      401: "Проверьте почту и пароль или войдите заново.",
      409: "Пользователь с такой почтой уже зарегистрирован.",
      422: "Проверьте поля формы. Пароль должен содержать не менее 8 символов.",
    };
    throw new Error(messages[response.status] || "Сервис аккаунтов недоступен. Повторите позже.");
  }

  return response.status === 204 ? undefined as T : response.json() as Promise<T>;
}

export async function loadHistory(account: Account): Promise<SearchEntry[]> {
  const entries = await userRequest<SavedSearch[]>("/me/searches?limit=50", account.token);

  return entries.map((entry) => ({
    ...entry, remoteId: entry.id,
    state: entry.response.client_state || "done", stage: "generating",
  }));
}

export async function saveSearch(account: Account, entry: SearchEntry): Promise<string> {
  const saved = await userRequest<SavedSearch>("/me/searches", account.token, "POST", {
    query: entry.query, response: { ...entry.response, client_state: entry.state },
  });

  return saved.id;
}
