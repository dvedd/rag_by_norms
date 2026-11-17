import { ArrowUp, SlidersHorizontal, Square, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { SearchOptions } from "../types";

interface Props {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  onStop: () => void;
  busy: boolean;
  options: SearchOptions;
  setOptions: (options: SearchOptions) => void;
}

export function Composer(props: Props) {
  const ref = useAutoSize(props.value);
  const [settings, setSettings] = useState(false);

  return <div className="composer-wrap">
    {settings && <SearchSettings options={props.options} onChange={props.setOptions} onClose={() => setSettings(false)} />}
    <form className="composer" onSubmit={(event) => { event.preventDefault(); props.onSubmit(); }}>
      <textarea ref={ref} value={props.value} onChange={(event) => props.onChange(event.target.value)} rows={1}
        maxLength={4000} aria-label="Ваш вопрос" placeholder="Какое требование нужно найти?" disabled={props.busy}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
            event.preventDefault(); if (!props.busy && props.value.trim()) props.onSubmit();
          }
        }} />
      <div className="composer-toolbar">
        <button className={`settings-button ${settings ? "selected" : ""}`} type="button" onClick={() => setSettings(!settings)} aria-expanded={settings}>
          <SlidersHorizontal size={15} /><span>Параметры поиска</span>{(props.options.only_tables || props.options.filename_filter) && <i />}
        </button>
        <span className="enter-hint">Enter — отправить</span>
        {props.busy ? <button type="button" className="send-button stop-button" onClick={props.onStop} aria-label="Остановить генерацию"><Square size={16} fill="currentColor" /></button>
          : <button type="submit" className="send-button" disabled={!props.value.trim()} aria-label="Отправить вопрос"><ArrowUp size={22} /></button>}
      </div>
    </form>
    <p className="composer-note">Ответ опирается на найденные документы. Проверяйте первоисточники перед применением.</p>
  </div>;
}

function useAutoSize(value: string) {
  const ref = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    ref.current.style.height = "auto";
    ref.current.style.height = `${Math.min(ref.current.scrollHeight, 180)}px`;
  }, [value]);

  return ref;
}

function SearchSettings({ options, onChange, onClose }: {
  options: SearchOptions; onChange: (value: SearchOptions) => void; onClose: () => void;
}) {
  return <div className="search-settings">
    <div className="section-title"><strong>Область поиска</strong><button className="icon-button" onClick={onClose} aria-label="Закрыть параметры"><X size={16} /></button></div>
    <label className="field">Документ<input placeholder="Название файла в базе (необязательно)" value={options.filename_filter}
      onChange={(event) => onChange({ ...options, filename_filter: event.target.value })} /></label>
    <div className="settings-checks">
      <label><input type="checkbox" checked={options.only_tables} onChange={(event) => onChange({ ...options, only_tables: event.target.checked })} />Только таблицы</label>
      <label><input type="checkbox" checked={options.use_rewriter} onChange={(event) => onChange({ ...options, use_rewriter: event.target.checked })} />Уточнять запрос для поиска</label>
    </div>
  </div>;
}
