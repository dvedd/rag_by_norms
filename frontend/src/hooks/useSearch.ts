import { useEffect, useRef, useState } from "react";
import type { Dispatch, SetStateAction } from "react";

import { loadHistory, saveSearch, userRequest } from "../api/account";
import { guestHistory, writeStorage } from "../storage";
import type {
  Account,
  SearchEntry,
  SearchOptions,
} from "../types";
import { createSearchEntry, executeSearch } from "./searchExecution";


/** Coordinate search execution, selection, and account history. */
export function useSearch(account: Account | null) {
  const [entries, setEntries] = useState<SearchEntry[]>(
    account ? [] : guestHistory,
  );
  const [activeId, setActiveId] = useState<string | null>(null);
  const [notice, setNotice] = useState("");

  const { busy, run, stop } = useRunner(
    account,
    setEntries,
    setActiveId,
    setNotice,
  );

  useHistory(
    account,
    setEntries,
    setActiveId,
    setNotice,
  );

  useEffect(() => {
    if (
      !account
      && !busy
      && !writeStorage("er-history", entries.slice(0, 30))
    ) {
      setNotice(
        "История не сохранилась: хранилище браузера недоступно или заполнено.",
      );
    }
  }, [entries, busy, account]);

  const remove = createRemoveHandler(
    account,
    setEntries,
    activeId,
    setActiveId,
    setNotice,
  );

  return {
    entries,
    activeId,
    setActiveId,
    busy,
    notice,
    run,
    remove,
    stop,
    active: entries.find((entry) => entry.id === activeId),
  };
}


type EntriesSetter = Dispatch<SetStateAction<SearchEntry[]>>;


function useRunner(
  account: Account | null,
  setEntries: EntriesSetter,
  setActiveId: (id: string | null) => void,
  setNotice: (message: string) => void,
) {
  const [busy, setBusy] = useState(false);
  const controller = useRef<AbortController | null>(null);

  useEffect(() => () => controller.current?.abort(), []);

  const run = async (query: string, options: SearchOptions): Promise<void> => {
    if (controller.current || !query.trim()) return;

    const initial = createSearchEntry(query.trim());
    const requestController = new AbortController();
    controller.current = requestController;

    setBusy(true);
    setNotice("");
    setActiveId(initial.id);
    setEntries((previous) => [initial, ...previous].slice(0, 50));

    let entry: SearchEntry;

    try {
      entry = await executeSearch(
        initial,
        options,
        account,
        requestController.signal,
        (update) => replaceEntry(setEntries, update),
      );
    } finally {
      controller.current = null;
      setBusy(false);
    }

    if (account) {
      await persistRemote(
        entry,
        account,
        setEntries,
        setNotice,
      );
    }
  };

  return {
    busy,
    run,
    stop: () => controller.current?.abort(),
  };
}


function useHistory(
  account: Account | null,
  setEntries: EntriesSetter,
  select: (id: string | null) => void,
  notify: (message: string) => void,
) {
  useEffect(() => {
    let cancelled = false;

    select(null);

    if (!account) {
      setEntries(guestHistory());
      return;
    }

    setEntries([]);

    loadHistory(account)
      .then((entries) => {
        if (!cancelled) {
          setEntries((current) => [...current, ...entries]);
        }
      })
      .catch(() => {
        if (!cancelled) {
          notify("Не удалось загрузить историю аккаунта.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [account, setEntries, select, notify]);
}


async function persistRemote(
  entry: SearchEntry,
  account: Account,
  setEntries: EntriesSetter,
  notify: (message: string) => void,
): Promise<void> {
  try {
    const remoteId = await saveSearch(account, entry);

    setEntries((all) =>
      all.map((item) =>
        item.id === entry.id
          ? { ...item, remoteId }
          : item,
      ),
    );
  } catch {
    notify(
      "Ответ доступен, но не сохранился в аккаунте. "
      + "Скопируйте его перед закрытием страницы.",
    );
  }
}


function createRemoveHandler(
  account: Account | null,
  setEntries: EntriesSetter,
  activeId: string | null,
  select: (id: string | null) => void,
  notify: (message: string) => void,
) {
  return async (entry: SearchEntry): Promise<void> => {
    try {
      if (account && entry.remoteId) {
        await userRequest(
          `/me/searches/${entry.remoteId}`,
          account.token,
          "DELETE",
        );
      }

      setEntries((all) =>
        all.filter((item) => item.id !== entry.id),
      );

      if (activeId === entry.id) {
        select(null);
      }
    } catch {
      notify("Не удалось удалить запрос. Попробуйте ещё раз.");
    }
  };
}


function replaceEntry(setEntries: EntriesSetter, update: SearchEntry): void {
  setEntries((entries) => entries.map((entry) => entry.id === update.id ? update : entry));
}
