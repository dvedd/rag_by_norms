# EngineeringRAG Frontend

React + TypeScript + Vite. Интерфейс на русском: поиск, потоковый Markdown-ответ,
источники, остановка и повтор запроса, параметры поиска, светлая/тёмная темы,
история, регистрация и вход через существующий User API.
Шрифты поставляются в сборке; браузеру не нужны внешние CDN.

## Запуск

Из корня репозитория, если инфраструктура уже работает:

```bash
docker compose up -d --build --no-deps retriever_api frontend
```

Интерфейс: **http://localhost:3000**. Retriever нужно пересобрать: старый образ
не содержит `/search/stream`. Для запуска всего окружения:

```bash
docker compose --profile base --profile search up -d --build
```

Для разработки (Node.js 20.19+ или 22.12+):

```bash
cd frontend
npm ci
npm run dev
```

Интерфейс разработки: **http://localhost:5173**. Vite проксирует `/api/retriever`
на `http://127.0.0.1:9123`, `/api/user` на `http://127.0.0.1:9130`.
Другие адреса можно задать в `frontend/.env.local`:

```env
RETRIEVER_API_URL=http://127.0.0.1:9123
USER_API_URL=http://127.0.0.1:9130
```

`npm run preview` проверяет только статическую сборку; API-прокси настроен
для dev-сервера и production Nginx. Дополнительные CORS-настройки не нужны:
браузер работает через один origin. При доступе извне используйте HTTPS.

## Потоковый протокол

`POST /api/retriever/search/stream` принимает JSON:

```json
{"query":"Как располагать стыки арматуры внахлестку?"}
```

Системные промпты и параметры поиска по умолчанию находятся на backend.
Также принимаются параметры существующего `SearchRequest`.
Старый `POST /search` продолжает возвращать обычный JSON для Streamlit.

Сервер возвращает `text/event-stream`:

```text
event: status
data: {"stage":"searching"}

event: sources
data: {"items":[]}

event: token
data: {"text":"Часть ответа"}

event: done
data: {"answer":"Полный ответ","results":[],"effective_query":"...","was_rewritten":false,"query_id":"...","latency_ms":1234}

```

Стадии: `accepted`, `rewriting`, `searching`, `preparing`, `generating`.
`sources` и `done.results` содержат фрагменты в порядке контекста модели,
поэтому номера совпадают со ссылками `[1]`, `[2]` в ответе.
Пустая выдача заканчивается `done` с `answer: null` и пустыми источниками.
Ошибки после начала потока передаются событием `error` с полем `message`;
после него `done` не отправляется. HTTP-ошибки валидации возвращаются до потока.

Каждые 15 секунд ожидания отправляется SSE-комментарий heartbeat.
Nginx отключает буферизацию, кеширование и сжатие потока. Кнопка остановки
закрывает fetch через AbortController; сервер отменяет задачу и закрывает
поток модели. Уже выполняющийся синхронный поиск в thread pool может закончиться
в фоне, но дальнейшая генерация после отмены не запускается.
Частичный ответ остаётся виден и помечается как незавершённый.

Используется настоящий `stream=True` локальной vLLM, а не анимация готового текста.
Streaming уменьшает ожидание начала ответа, но не ускоряет поиск и подготовку
контекста: до первого токена пользователь видит статусы этапов.
Протокол опирается на [документацию потоковых ответов OpenAI-compatible API](https://developers.openai.com/api/docs/guides/streaming-responses).

## История и аккаунты

Для гостя последние 30 запросов сохраняются в localStorage этого браузера.
После входа история загружается и сохраняется через User API; гостевая история
автоматически в аккаунт не переносится. Токен хранится в sessionStorage,
до закрытия вкладки или истечения сессии. Если сохранение в User API не удалось,
интерфейс предлагает скопировать ответ. Тема хранится локально в браузере.
Каждый вопрос — отдельный поиск; история диалога не добавляется в контекст LLM.

## Проверка

```bash
cd frontend
npm run build
npm test
npx playwright install chromium
npm run test:e2e
```

Проверки backend, из корня репозитория:

```bash
PYTHONPATH=services/retriever venv/bin/pytest services/retriever/stream_tests -q
```

Новые тесты изолированы от старого набора `services/retriever/tests`, в котором
остались импорты удалённого пакета `app.services`. Существующие тесты не изменены.
