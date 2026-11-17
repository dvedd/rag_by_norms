import { describe, expect, it } from "vitest";
import { parseEvent, readEvents } from "./stream";
import type { StreamEvent } from "../types";

function byteStream(text: string, size = 1): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text);

  return new ReadableStream({ start(controller) {
    for (let index = 0; index < bytes.length; index += size) {
      controller.enqueue(bytes.slice(index, index + size));
    }
    controller.close();
  } });
}

describe("SSE reader", () => {
  it("decodes Cyrillic across arbitrary byte and CRLF boundaries", async () => {
    const events: StreamEvent[] = [];
    const body = byteStream(': keep-alive\r\n\r\nevent: token\r\ndata: {"text":"Арматура 🏗"}\r\n\r\nevent: done\r\ndata: {}\r\n\r\n');

    await readEvents(body, (event) => events.push(event));

    expect(events).toEqual([{ event: "token", data: { text: "Арматура 🏗" } }, { event: "done", data: {} }]);
  });

  it("delivers a token before the producer sends done", async () => {
    let producer!: ReadableStreamDefaultController<Uint8Array>;
    const events: StreamEvent[] = [];
    const body = new ReadableStream<Uint8Array>({ start(controller) { producer = controller; } });
    const reading = readEvents(body, (event) => events.push(event));
    producer.enqueue(new TextEncoder().encode('event: token\ndata: {"text":"Сразу"}\n\n'));

    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(events).toEqual([{ event: "token", data: { text: "Сразу" } }]);
    producer.enqueue(new TextEncoder().encode('event: done\ndata: {}\n\n'));
    await reading;
  });

  it("rejects a truncated stream without a terminal event", async () => {
    await expect(readEvents(byteStream('event: token\ndata: {"text":"Часть"}\n\n'), () => {})).rejects.toThrow("Соединение прервалось");
  });

  it("stops on an error event and ignores trailing data", async () => {
    const events: StreamEvent[] = [];
    await readEvents(byteStream('event: error\ndata: {"message":"Ошибка"}\n\nevent: done\ndata: {}\n\n', 1024), (event) => events.push(event));

    expect(events).toHaveLength(1);
    expect(events[0].event).toBe("error");
  });

  it("ignores comments and supports multiline data", () => {
    expect(parseEvent(": heartbeat")).toBeNull();
    expect(parseEvent('event: token\ndata: {\ndata: "text": "текст"}')).toEqual({ event: "token", data: { text: "текст" } });
  });
});
