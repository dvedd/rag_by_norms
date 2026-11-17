import { LogIn, LogOut, MessageSquare, Moon, Plus, Search, Sun, Trash2, X } from "lucide-react";
import { useState } from "react";
import type { Account, SearchEntry } from "../types";
import { Brand } from "./Brand";

interface Props {
  entries: SearchEntry[]; activeId: string | null; busy: boolean; open: boolean;
  onSelect: (id: string | null) => void; onClose: () => void;
  onRemove: (entry: SearchEntry) => void; account: Account | null;
  onAccount: () => void; dark: boolean; onTheme: () => void;
}

export function Sidebar(props: Props) {
  const [filter, setFilter] = useState("");
  const entries = props.entries.filter((entry) => entry.query.toLowerCase().includes(filter.toLowerCase()));

  return <>
    {props.open && <button className="sidebar-backdrop" onClick={props.onClose} aria-label="Закрыть меню" />}
    <aside className={`sidebar ${props.open ? "is-open" : ""}`} aria-label="История запросов">
      <div className="sidebar-brand"><Brand /><button className="icon-button mobile-only" onClick={props.onClose} aria-label="Закрыть меню"><X size={19} /></button></div>
      <button className="new-search" disabled={props.busy} onClick={() => props.onSelect(null)}><Plus size={18} />Новый вопрос</button>
      <div className="history-label">История запросов<span>{props.entries.length || ""}</span></div>
      {props.entries.length > 0 && <label className="history-filter"><Search size={15} /><input value={filter} onChange={(event) => setFilter(event.target.value)} placeholder="Найти в истории" aria-label="Найти в истории" /></label>}
      <nav className="history-list">
        {entries.map((entry) => <HistoryItem key={entry.id} entry={entry} active={props.activeId === entry.id} disabled={props.busy} onSelect={props.onSelect} onRemove={props.onRemove} />)}
        {!entries.length && <div className="history-empty"><MessageSquare size={23} strokeWidth={1.3} /><p>{filter ? "Ничего не найдено" : "Здесь будет ваша история"}</p><span>{filter ? "Попробуйте другие слова" : "Возвращайтесь к вопросам и найденным источникам"}</span></div>}
      </nav>
      <div className="sidebar-bottom">
        <div className="storage-note">{props.account ? "История в вашем аккаунте" : "История хранится в этом браузере"}</div>
        <button className="theme-button" onClick={props.onTheme}>{props.dark ? <Sun size={17} /> : <Moon size={17} />}{props.dark ? "Светлая тема" : "Тёмная тема"}</button>
        <button className="account-button" disabled={props.busy} onClick={props.onAccount}>
          <span className="avatar">{props.account ? props.account.user.display_name.slice(0, 1).toUpperCase() : <LogIn size={17} />}</span>
          <span>{props.account?.user.display_name || "Войти в аккаунт"}<small>{props.account ? "Выйти из аккаунта" : "История на всех устройствах"}</small></span>
          {props.account && <LogOut size={15} />}
        </button>
      </div>
    </aside>
  </>;
}

function HistoryItem({ entry, active, disabled, onSelect, onRemove }: {
  entry: SearchEntry; active: boolean; disabled: boolean;
  onSelect: (id: string) => void; onRemove: (entry: SearchEntry) => void;
}) {
  const date = new Date(entry.created_at).toLocaleDateString("ru-RU", { day: "numeric", month: "short" });

  return <div className={`history-item ${active ? "active" : ""}`}>
    <button className="history-select" disabled={disabled} onClick={() => onSelect(entry.id)} title={entry.query}>
      <span>{entry.query}</span><small>{date}{entry.state === "stopped" ? " · Остановлено" : ""}</small>
    </button>
    <button className="history-delete icon-button" disabled={disabled} onClick={() => onRemove(entry)} aria-label={`Удалить: ${entry.query}`}><Trash2 size={14} /></button>
  </div>;
}
