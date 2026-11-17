import { expect, test } from "@playwright/test";

test("welcome, theme and mobile layout", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: /У каждого решения/ })).toBeVisible();
  await page.screenshot({ path: "test-results/welcome-desktop.png", fullPage: true });
  await page.getByRole("button", { name: "Тёмная тема" }).click();
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.screenshot({ path: "test-results/welcome-dark.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Открыть меню" }).click();
  await page.getByRole("button", { name: "Светлая тема" }).click();
  await page.locator("aside").getByRole("button", { name: "Закрыть меню" }).click();
  await expect(page.getByRole("textbox", { name: "Ваш вопрос" })).toBeVisible();
  const fits = await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth);
  expect(fits).toBe(true);
  await page.screenshot({ path: "test-results/welcome-mobile.png", fullPage: true });
});

test("renders tokens before done, sources and persistent guest history", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("textbox", { name: "Ваш вопрос" }).fill("Как стыковать арматуру?");
  await page.getByRole("button", { name: "Отправить вопрос" }).click();
  await expect(page.locator(".answer-content")).toContainText("Тестовый ответ");
  await expect(page.getByRole("button", { name: "Остановить генерацию" })).toBeVisible();
  await expect(page.getByRole("status")).toContainText("Поиск завершён");
  await expect(page.locator(".source").first()).not.toBeVisible();
  await page.locator(".sources > summary").click();
  await page.locator(".source summary").click();
  await expect(page.locator(".source-excerpt")).toContainText("Фрагмент для проверки");
  await page.screenshot({ path: "test-results/answer-desktop.png", fullPage: true });
  await page.reload();
  await page.getByRole("button", { name: /Как стыковать арматуру/ }).first().click();
  await expect(page.locator(".answer-content")).toContainText("генерации модели");
  await page.getByRole("button", { name: "Удалить: Как стыковать арматуру?" }).click();
  await expect(page.getByRole("heading", { name: /У каждого решения/ })).toBeVisible();
});

test("stop retains partial text and closes upstream", async ({ page, request }) => {
  const before = await (await request.get("http://127.0.0.1:9138/stats")).json();
  await page.goto("/");
  await page.getByRole("textbox", { name: "Ваш вопрос" }).fill("Остановить поток");
  await page.getByRole("button", { name: "Отправить вопрос" }).click();
  await expect(page.locator(".answer-content")).toContainText("Тестовый ответ");
  await page.getByRole("button", { name: "Остановить генерацию" }).click();
  await expect(page.getByRole("status")).toContainText("Генерация остановлена");
  await expect(page.locator(".answer-content")).toContainText("Тестовый ответ");
  await expect.poll(async () => (await (await request.get("http://127.0.0.1:9138/stats")).json()).disconnected).toBeGreaterThan(before.disconnected);
});

test("partial errors have retry and preserve the answer", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("textbox", { name: "Ваш вопрос" }).fill("Проверить ошибка");
  await page.getByRole("button", { name: "Отправить вопрос" }).click();
  await expect(page.getByRole("alert")).toContainText("Тестовая ошибка");
  await expect(page.locator(".answer-content")).toContainText("Тестовый ответ");
  await expect(page.getByRole("button", { name: "Повторить поиск" })).toBeEnabled();
});

test("login connects account history and logout restores guest history", async ({ page }) => {
  const account = { token: "test-only", session_id: "session", expires_at: "2099-01-01", user: { id: "engineer", display_name: "Инженер", email: "test@example.com" } };
  await page.route("**/api/user/**", (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("login")) return route.fulfill({ json: account });
    if (path.endsWith("logout")) return route.fulfill({ status: 204 });
    return route.fulfill({ json: [{ id: "saved", query: "Вопрос из аккаунта", created_at: "2026-01-01", response: { answer: "Сохранённый ответ", results: [], effective_query: "Вопрос", was_rewritten: false } }] });
  });
  await page.goto("/");
  await page.getByRole("button", { name: /Войти в аккаунт/ }).click();
  await page.getByLabel("Электронная почта").fill("test@example.com");
  await page.getByLabel("Пароль", { exact: true }).fill("password");
  await page.getByRole("button", { name: "Войти", exact: true }).click();
  await page.getByRole("button", { name: /Вопрос из аккаунта/ }).first().click();
  await expect(page.locator(".answer-content")).toContainText("Сохранённый ответ");
  await page.getByRole("button", { name: /Выйти из аккаунта/ }).click();
  await expect(page.getByText("Здесь будет ваша история")).toBeVisible();
});
