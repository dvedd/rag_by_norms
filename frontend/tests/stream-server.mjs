import { createServer } from "node:http";

const source = {
  id: "example", filename: "Тестовый документ", section_path: "Раздел 1",
  text: "Фрагмент для проверки отображения источников.", headings: [],
  is_table: false, score: 12,
};
let disconnected = 0;

function send(response, event, data) {
  response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function streamAnswer(response, query) {
  response.writeHead(200, { "Content-Type": "text/event-stream" });
  send(response, "status", { stage: "searching" });
  send(response, "sources", { items: [source] });
  send(response, "status", { stage: "generating" });
  const tokens = ["### Краткий вывод\n\n", "Тестовый ответ", " появляется", " постепенно", ", по мере", " генерации", " модели.", "\n\n", "### Основание\n\n", "Ссылка", " на", " найденный", " фрагмент", " [1]."];
  let index = 0;

  const timer = setInterval(() => {
    if (query.includes("ошибка") && index === 3) {
      send(response, "error", { message: "Тестовая ошибка соединения. Повторите запрос." });
      clearInterval(timer); response.end(); return;
    }
    send(response, "token", { text: tokens[index++] });
    if (index === tokens.length) {
      send(response, "done", { answer: tokens.join(""), results: [source], effective_query: query, was_rewritten: false, latency_ms: 2800 });
      clearInterval(timer); response.end();
    }
  }, 200);

  response.on("close", () => { if (!response.writableEnded) disconnected++; clearInterval(timer); });
}

createServer(async (request, response) => {
  if (request.url === "/stats") { response.end(JSON.stringify({ disconnected })); return; }
  if (request.url !== "/search/stream") { response.writeHead(404).end(); return; }
  let body = "";
  for await (const chunk of request) body += chunk;
  const { query } = JSON.parse(body);

  streamAnswer(response, query);
}).listen(9138, "127.0.0.1");
