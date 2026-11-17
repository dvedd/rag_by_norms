import { ArrowRight, LoaderCircle, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { userRequest } from "../api/account";
import type { Account } from "../types";
import { Brand } from "./Brand";

interface Props {
  onClose: () => void; onLogin: (account: Account) => void;
}

export function AccountDialog({ onClose, onLogin }: Props) {
  const ref = useRef<HTMLDialogElement>(null);
  const [register, setRegister] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => { ref.current?.showModal(); }, []);

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const fields = Object.fromEntries(new FormData(event.currentTarget));
    setBusy(true); setError("");
    try {
      const account = await userRequest<Account>(register ? "/auth/register" : "/auth/login", undefined, "POST", fields);
      onLogin(account);
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Не удалось войти."); }
    finally { setBusy(false); }
  }

  return <dialog ref={ref} className="account-dialog" onCancel={onClose} onClick={(event) => { if (event.target === event.currentTarget) onClose(); }} aria-labelledby="account-title">
    <div className="dialog-body"><button className="dialog-close icon-button" onClick={onClose} aria-label="Закрыть"><X size={20} /></button><Brand />
      <h2 id="account-title">{register ? "Создать аккаунт" : "С возвращением"}</h2><p>Ваши вопросы и источники — на любом устройстве.</p>
      <form onSubmit={(event) => void submit(event)}><AccountFields register={register} />
        {error && <div className="error-notice" role="alert">{error}</div>}
        <button type="submit" className="primary-button" disabled={busy}>{busy ? <LoaderCircle className="spin" size={17} /> : <ArrowRight size={17} />}{register ? "Зарегистрироваться" : "Войти"}</button>
      </form>
      <button className="auth-switch" disabled={busy} onClick={() => { setRegister(!register); setError(""); }}>{register ? "Уже есть аккаунт? Войти" : "Нет аккаунта? Зарегистрироваться"}</button>
    </div>
  </dialog>;
}

function AccountFields({ register }: { register: boolean }) {
  return <>
    {register && <label className="field">Имя<input name="display_name" autoComplete="name" required maxLength={120} placeholder="Как к вам обращаться" /></label>}
    <label className="field">Электронная почта<input name="email" type="email" autoComplete="email" required placeholder="name@company.ru" /></label>
    <label className="field">Пароль<input name="password" type="password" autoComplete={register ? "new-password" : "current-password"} minLength={register ? 8 : 1} required placeholder={register ? "Не менее 8 символов" : "Введите пароль"} /></label>
  </>;
}
