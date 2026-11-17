import { SearchStreamError, searchStream } from "../api/stream";
import type { Account, SearchEntry, SearchOptions, StreamEvent } from "../types";


function createId(): string {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }

  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));

  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10, 16).join(""),
  ].join("-");
}


/** Create a search entry before any network operation starts. */
export function createSearchEntry(query: string): SearchEntry {
  return {
    id: createId(),
    query,
    created_at: new Date().toISOString(),
    state: "streaming",
    stage: "accepted",
    response: {
      answer: "",
      results: [],
      effective_query: query,
      was_rewritten: false,
    },
  };
}


function applyEvent(entry: SearchEntry, message: StreamEvent): void {
  switch (message.event) {
    case "status":
      entry.stage = message.data.stage;
      break;

    case "token":
      entry.response.answer = (entry.response.answer ?? "") + message.data.text;
      break;

    case "sources":
      entry.response.results = message.data.items;
      break;

    case "done":
      entry.response = message.data;
      entry.state = "done";
      break;

    case "error":
      entry.error = message.data.message;
      entry.state = "error";
      break;
  }
}


/** Publish frame-batched snapshots and return a final entry without mutating input. */
export async function executeSearch(
  initial: SearchEntry,
  options: SearchOptions,
  account: Account | null,
  signal: AbortSignal,
  onUpdate: (entry: SearchEntry) => void,
): Promise<SearchEntry> {
  const entry = { ...initial, response: { ...initial.response } };
  let frame = 0;

  const publish = () => {
    frame = 0;

    onUpdate({ ...entry, response: { ...entry.response } });
  };

  try {
    await searchStream(entry.query, options, account, signal, (message) => {
      applyEvent(entry, message);

      if (!frame) {
        frame = requestAnimationFrame(publish);
      }
    });
  } catch (error) {
    entry.state = signal.aborted ? "stopped" : "error";
    entry.error = signal.aborted
      ? undefined
      : error instanceof SearchStreamError
        ? error.message
        : "Не удалось получить ответ. Повторите запрос.";
  } finally {
    cancelAnimationFrame(frame);
    publish();
  }

  return entry;
}
