import type { Account, SearchOptions, StreamEvent } from "../types";

/** Mark controlled search failures whose messages are safe to display. */
export class SearchStreamError extends Error {}

/** Decode a supported SSE event without exposing malformed payloads. */
export function parseEvent(block: string): StreamEvent | null {
  const lines = block.split(/\r?\n/);
  const event = lines.find((line) => line.startsWith("event:"))?.slice(6).trim();
  const data = lines.filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).replace(/^ /, "")).join("\n");

  if (!event || !data) return null;

  if (!["status", "sources", "token", "done", "error"].includes(event)) return null;

  try {
    return { event, data: JSON.parse(data) } as StreamEvent;
  } catch {
    throw new SearchStreamError("Сервер вернул некорректный потоковый ответ. Повторите запрос.");
  }
}

/** Own the reader until a terminal event, an error, or cancellation. */
export async function readEvents(
  body: ReadableStream<Uint8Array>, onEvent: (event: StreamEvent) => void,
): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { value, done } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });

      const blocks = buffer.split(/\r?\n\r?\n/);
      buffer = blocks.pop() || "";

      for (const block of blocks) {
        const message = parseEvent(block);
        if (!message) continue;

        onEvent(message);

        if (message.event === "done" || message.event === "error") return;
      }

      if (done) {
        throw new SearchStreamError("Соединение прервалось. Повторите запрос.");
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

/** Request an SSE search and release rejected response bodies. */
export async function searchStream(
  query: string, options: SearchOptions, account: Account | null,
  signal: AbortSignal, onEvent: (event: StreamEvent) => void,
): Promise<void> {
  const response = await fetch("/api/retriever/search/stream", {
    method: "POST", signal,
    headers: { "Content-Type": "application/json", Accept: "text/event-stream" },
    body: JSON.stringify({
      query, ...options, filename_filter: options.filename_filter.trim() || null,
      only_tables: options.only_tables || null,
      user_id: account?.user.id, session_id: account?.session_id,
    }),
  });

  const error = responseError(response);
  if (error) {
    await response.body?.cancel().catch(() => undefined);

    throw new SearchStreamError(error);
  }

  await readEvents(response.body!, onEvent);
}

function responseError(response: Response): string | null {
  if (!response.ok) {
    return response.status === 422
      ? "Проверьте вопрос и параметры поиска."
      : "Сервис поиска недоступен. Попробуйте ещё раз через несколько секунд.";
  }

  if (!response.body || !response.headers.get("content-type")?.includes("text/event-stream")) {
    return "Сервер не поддерживает потоковый ответ. Обновите Retriever API.";
  }

  return null;
}
