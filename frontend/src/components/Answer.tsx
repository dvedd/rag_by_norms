import { Check, CheckCheck, ChevronDown, Copy, FileText, LoaderCircle, Pencil, RotateCcw, Table2 } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type { SearchEntry, Source, Stage } from "../types";

const stages: Record<Stage, string> = {
  accepted: "Запрос принят", rewriting: "Уточняем поисковый запрос",
  searching: "Ищем в нормативной базе", preparing: "Проверяем найденные фрагменты",
  generating: "Формируем ответ по источникам",
};

const markdownComponents = { table: MarkdownTable };

function MarkdownTable({ children }: { children?: ReactNode }) {
  return <div className="markdown-table" role="region" aria-label="Таблица" tabIndex={0}><table>{children}</table></div>;
}

export function Answer({ entry, busy, onRetry, onEdit }: {
  entry: SearchEntry; busy: boolean; onRetry: () => void; onEdit: () => void;
}) {
  const end = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  useEffect(() => {
    const onScroll = () => { following.current = window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 240; };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);
  useEffect(() => {
    if (entry.state === "streaming" && following.current) end.current?.scrollIntoView({ block: "end" });
  }, [entry.response.answer, entry.state]);

  return <article className="answer-view">
    <div className="question-heading"><span>Ваш вопрос</span><button className="icon-button" onClick={onEdit} disabled={busy} aria-label="Уточнить вопрос"><Pencil size={16} /></button></div>
    <h1 className="question-title">{entry.query}</h1>
    <AnswerStatus entry={entry} />
    {entry.response.answer ? <div className={`answer-content markdown ${entry.state === "streaming" ? "is-streaming" : ""}`}><Markdown remarkPlugins={[remarkGfm]} components={markdownComponents}>{entry.response.answer}</Markdown></div>
      : entry.state === "streaming" ? <div className="answer-skeleton" aria-hidden="true"><i /><i /><i /></div>
        : entry.state === "done" && <p className="empty-answer">{entry.response.results.length ? "Ответ не сформирован. Изучите найденные источники." : "Подходящие фрагменты не найдены. Уточните вопрос или измените параметры поиска."}</p>}
    {entry.error && <div role="alert" className="error-notice">{entry.error}</div>}
    {entry.state !== "streaming" && <AnswerActions entry={entry} disabled={busy} onRetry={onRetry} />}
    <Sources sources={entry.response.results} />
    <div ref={end} className="answer-end" />
  </article>;
}

function AnswerStatus({ entry }: { entry: SearchEntry }) {
  const streaming = entry.state === "streaming";
  const label = streaming ? stages[entry.stage] : entry.state === "stopped" ? "Генерация остановлена"
    : entry.state === "error" ? "Ответ не завершён" : "Поиск завершён";

  return <div className="answer-status" role="status" aria-live="polite">
    {streaming ? <LoaderCircle className="spin" size={16} /> : <CheckCheck size={17} />}
    <span>{label}</span>
    {entry.state === "done" && entry.response.latency_ms != null && <small>{(entry.response.latency_ms / 1000).toFixed(1)} с</small>}
  </div>;
}

function AnswerActions({ entry, disabled, onRetry }: {
  entry: SearchEntry; disabled: boolean; onRetry: () => void;
}) {
  const [copy, setCopy] = useState("");
  async function copyAnswer() {
    try { await navigator.clipboard.writeText(entry.response.answer || ""); setCopy("Скопировано"); }
    catch { setCopy("Выделите текст и скопируйте вручную"); }
  }

  return <div className="answer-actions">
    {entry.response.answer && <button onClick={() => void copyAnswer()}>{copy === "Скопировано" ? <Check size={15} /> : <Copy size={15} />}{copy || "Копировать"}</button>}
    <button onClick={onRetry} disabled={disabled}><RotateCcw size={15} />Повторить поиск</button>
  </div>;
}

function Sources({ sources }: { sources: Source[] }) {
  if (!sources.length) return null;

  return <details className="sources">
    <summary className="sources-heading">
      <span className="sources-title"><FileText size={17} />Источники ответа</span>
      <span className="sources-count">{sources.length} фрагм.</span>
      <ChevronDown className="sources-chevron" size={17} />
    </summary>
    <div className="sources-list">
      {sources.map((source, index) => <details className="source" key={`${source.id}-${index}`}>
        <summary><span className="source-number">{index + 1}</span><span className="source-title">{source.filename}<small>{source.section_path || source.headings?.join(" / ") || "Раздел не указан"}</small></span>
          {source.is_table ? <Table2 size={16} /> : <FileText size={16} />}<ChevronDown className="source-chevron" size={16} /></summary>
        <div className="source-excerpt markdown"><Markdown remarkPlugins={[remarkGfm]} components={markdownComponents}>{source.text}</Markdown></div>
      </details>)}
      <p className="source-note">Раскройте документ, чтобы проверить фрагмент, на котором основан ответ.</p>
    </div>
  </details>;
}
