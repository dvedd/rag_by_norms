import { BookOpen, ChevronRight, Menu } from "lucide-react";
import { useEffect, useState } from "react";
import { userRequest } from "./api/account";
import { AccountDialog } from "./components/AccountDialog";
import { Answer } from "./components/Answer";
import { Composer } from "./components/Composer";
import { Sidebar } from "./components/Sidebar";
import { Welcome } from "./components/Welcome";
import { useSearch } from "./hooks/useSearch";
import { readAccount, readStorage, writeStorage } from "./storage";
import type { Account, SearchOptions } from "./types";

export function App() {
  const [account, setAccount] = useState<Account | null>(readAccount);
  const [authOpen, setAuthOpen] = useState(false);
  const [dark, setDark] = useState(() => readStorage("er-dark", false));
  const [error, setError] = useState("");
  useEffect(() => { document.documentElement.dataset.theme = dark ? "dark" : "light"; writeStorage("er-dark", dark); }, [dark]);

  function login(value: Account) {
    writeStorage("er-account", value, true);
    setAccount(value); setAuthOpen(false); setError("");
  }
  async function accountAction() {
    if (!account) { setAuthOpen(true); return; }
    try { await userRequest("/auth/logout", account.token, "POST"); }
    catch { setError("Не удалось завершить серверную сессию. Вход в этом браузере сброшен."); }
    writeStorage("er-account", null, true); setAccount(null);
  }

  return <>
    <Workspace key={account?.user.id || "guest"} account={account} dark={dark} onTheme={() => setDark(!dark)} onAccount={() => void accountAction()} accountError={error} />
    {authOpen && <AccountDialog onClose={() => setAuthOpen(false)} onLogin={login} />}
  </>;
}

interface WorkspaceProps {
  account: Account | null; dark: boolean; onTheme: () => void;
  onAccount: () => void; accountError: string;
}

function Workspace(props: WorkspaceProps) {
  const search = useSearch(props.account);
  const [draft, setDraft] = useState("");
  const [sidebar, setSidebar] = useState(false);
  const [options, setOptions] = useState<SearchOptions>({ filename_filter: "", only_tables: false, use_rewriter: true });
  function submit(query = draft) { if (!query.trim() || search.busy) return; setDraft(""); void search.run(query, options); }
  function select(id: string | null) { search.setActiveId(id); setSidebar(false); setDraft(""); window.scrollTo(0, 0); }

  return <div className="app-layout">
    <Sidebar entries={search.entries} activeId={search.activeId} busy={search.busy} open={sidebar} onSelect={select} onClose={() => setSidebar(false)} onRemove={(entry) => void search.remove(entry)} account={props.account} onAccount={props.onAccount} dark={props.dark} onTheme={props.onTheme} />
    <main className={`workspace ${search.active ? "has-answer" : ""}`}>
      <WorkspaceHeader onMenu={() => setSidebar(true)} />
      <div className="workspace-content">
        {(search.notice || props.accountError) && <div className="error-notice" role="alert">{search.notice || props.accountError}</div>}
        {search.active ? <Answer key={search.active.id} entry={search.active} busy={search.busy} onRetry={() => submit(search.active!.query)} onEdit={() => { setDraft(search.active!.query); document.querySelector<HTMLTextAreaElement>("textarea")?.focus(); }} />
          : <Welcome onSelect={submit} />}
      </div>
      <div className="composer-dock"><Composer value={draft} onChange={setDraft} onSubmit={() => submit()} onStop={search.stop} busy={search.busy} options={options} setOptions={setOptions} /></div>
    </main>
  </div>;
}

function WorkspaceHeader({ onMenu }: { onMenu: () => void }) {
  return <header className="workspace-header">
    <button className="icon-button mobile-only" onClick={onMenu} aria-label="Открыть меню"><Menu size={21} /></button>
    <div className="breadcrumbs"><span>Рабочее пространство</span><ChevronRight size={13} /><strong>Нормативный поиск</strong></div>
    <span className="header-caption"><BookOpen size={15} />Для инженеров и проектировщиков</span>
  </header>;
}
