// First-time user walkthrough on a fresh server. Disposable servers only (REQUIREMENTS.md, Browser
// end-to-end checks): the walk adds a client key and uploads an account.
//
//   <page-url>       the dashboard served by `vite preview` over `tools/mock/server.mjs --accounts 0`
//                    (a server that lists models for any client key, as the mock does; a fresh
//                    real server has no model until an account signs in, so the test step stays open).
//   <management-key> the server's management key (`mock` for the mock).
//
// The walk: a server whose management API is off is explained on the sign-in page; on the empty
// server the overview's Get started card starts at 0 of 3 in both themes, on desktop and phone,
// without horizontal overflow; its first step is done by keyboard alone (Add key, type a key, Enter)
// and the card then reads 1 of 3; Use with tools explicitly reveals the selected resource's key,
// shows each setup with this server's address and the key shortened, and tests the key (2 of 3);
// an account on the server and its traffic close the card, after which overview shows traffic.
//
// What the mock cannot do, so it is not walked: a provider sign-in (the account is uploaded through
// the Management API instead) and a proxied request (the mock's accounts come with traffic), and the
// mock's /v1/models always lists models, so the "no models yet" wording is not reached. The server's
// own answer for a server without a management key is faked in the browser: a 403 that names
// CK_MANAGEMENT_PASSWORD.
//
// Usage: CHROME_PATH=... node scripts/first-run-check.mjs <page-url> <management-key> [capture-dir]
import { assertPrivateCredentialStore } from "./private-credential-store.mjs";
import { chromium } from "playwright-core";
import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import assert from "node:assert/strict";

const [pageUrl, secret, dir] = process.argv.slice(2);
assert.ok(pageUrl && secret, "usage: first-run-check.mjs <page-url> <management-key> [capture-dir]");
const root = pageUrl.endsWith("/") ? pageUrl : `${pageUrl}/`;
const out = resolve(dir || "first-run");
mkdirSync(out, { recursive: true });

/** One Management API call from this script, as a client of the same server the page talks to. */
async function api(method, path, body) {
  const r = await fetch(new URL(path.startsWith("admin/v1/") ? path : `v8/management/${path}`, root), {
    method,
    headers: { Authorization: `Bearer ${secret}`, ...(typeof body === "string" ? { "Content-Type": "application/json" } : {}) },
    body,
  });
  if (!r.ok) throw new Error(`${method} ${path}: HTTP ${r.status}`);
  return { headers: r.headers, json: await r.json() };
}

// A real server signs in and stores accounts: its credential store must be private, not a real database.
// The mock keeps accounts in memory and says so in X-Ck-Version.
const probe = await api("GET", "admin/v1/effective/config");
if (probe.headers.get("x-ck-version") !== "cooker-mock") await assertPrivateCredentialStore(root, secret);
const present = (await api("GET", "credentials")).json.files.length;
assert.equal(present, 0, `not an empty server: it already has ${present} accounts (restart the mock with --accounts 0)`);

const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH });
const desktop = { viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2 };
const phone = { viewport: { width: 390, height: 844 }, deviceScaleFactor: 2, isMobile: true, hasTouch: true };
const pass = (t) => console.log(`PASS ${t}`);
const errors = [];
const secretExports = [];

async function open(device, theme) {
  const context = await browser.newContext({ ...device, colorScheme: theme });
  await context.addInitScript((t) => localStorage.setItem("cooker-theme", t), theme);
  const page = await context.newPage();
  page.on("pageerror", (e) => errors.push(String(e)));
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.endsWith("/admin/v1/export") && url.searchParams.get("include_secrets") === "true") secretExports.push(request.url());
  });
  await page.goto(root);
  return page;
}
const keyField = (page) => page.getByRole("textbox", { name: /Management key/ });
const connect = (page) => page.getByRole("button", { name: "Connect", exact: true });
async function signIn(page) {
  await keyField(page).fill(secret);
  await connect(page).click();
  await page.getByRole("heading", { name: "Overview", level: 1 }).waitFor();
}
/** Wait for something the walk depends on, and say what it was when it never came. */
async function appears(locator, what, timeout = 15_000) {
  try {
    await locator.waitFor({ timeout });
  } catch {
    throw new Error(`expected ${what}, but it did not appear within ${timeout / 1000} s`);
  }
}
const shot = (page, name) => page.screenshot({ path: `${out}/${name}.png`, fullPage: true });
const overflow = (page) => page.evaluate(() => document.documentElement.scrollWidth > innerWidth);
const startHeading = (page) => page.getByRole("main").getByRole("heading", { name: "Get started" });
const startSteps = (page) => page.getByRole("main").getByRole("list", { name: "Get started" });
const mask = (key) => `${key.slice(0, 3)}…${key.slice(-4)}`;

try {
  // 1. A server started without a management key: the dashboard explains its 403.
  {
    const page = await open(desktop, "light");
    await page.route(/\/(?:v8\/management|admin\/v1)\//, (route) =>
      route.fulfill({
        status: 403,
        contentType: "application/json",
        body: JSON.stringify({ error: "management key not set: start the server with CK_MANAGEMENT_PASSWORD" }),
      }),
    );
    await keyField(page).fill("anything");
    await connect(page).click();
    // Other alerts, an empty live region among them, may exist; this is the one with the text.
    const alert = page.getByRole("alert").filter({ hasText: /\S/ });
    await appears(alert, "the sign-in page to explain that the management API is off");
    const message = await alert.textContent();
    assert.match(message, /management API is off/i);
    assert.match(message, /secret-key/);
    assert.equal(await page.getByRole("region", { name: "Notifications" }).getByRole("status").count(), 0, "no second message as a toast");
    await shot(page, "01-login-management-off-desktop-light");
    pass(`management off: "${message.trim()}"`);
    await page.context().close();
  }

  // 2. An empty server: nothing connected yet. Start from no client keys.
  const originalKeys = probe.json.access?.["api-keys"] ?? [];
  await api("POST", "admin/v1/apply", JSON.stringify({ ops: [{ op: "set", path: "access/api-keys", value: [], expect: originalKeys }] }));
  const page = await open(desktop, "light");
  const main = page.getByRole("main");
  await signIn(page);
  await appears(startHeading(page), "the Get started card on an empty server");
  await main.getByText("0 of 3 done").waitFor();
  assert.equal(await startSteps(page).getByRole("listitem").count(), 3, "three steps");
  assert.equal(await startSteps(page).getByText("completed").count(), 0, "no step is done yet");
  assert.equal(await main.getByRole("button", { name: "Connect account" }).first().isVisible(), true);
  await shot(page, "02-overview-fresh-desktop-light");
  for (const [theme, device, name] of [
    ["dark", desktop, "03-overview-fresh-desktop-dark"],
    ["light", phone, "04-overview-fresh-phone-light"],
    ["dark", phone, "05-overview-fresh-phone-dark"],
  ]) {
    const p = await open(device, theme);
    await signIn(p);
    await appears(startHeading(p), `${name}: the Get started card`);
    assert.equal(await overflow(p), false, `${name}: no horizontal overflow`);
    await shot(p, name);
    await p.context().close();
  }
  pass("empty server: Get started shows 0 of 3 in both themes, desktop and phone, without overflow");

  // 3. Step 1 by keyboard: Add key opens the dialog with focus in the key field, Enter adds the key.
  const key = `sk-first-run-check-${Date.now().toString(16)}`;
  await startSteps(page).getByRole("link", { name: "Add key" }).focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Add client key" });
  await appears(dialog, "the Add client key dialog");
  // The title can take focus between the field's initial focus and the dialog's next frame.
  await page.waitForFunction(
    async (input) => {
      if (document.activeElement !== input) return false;
      await new Promise(requestAnimationFrame);
      if (document.activeElement !== input) return false;
      await new Promise(requestAnimationFrame);
      return document.activeElement === input;
    },
    await dialog.locator("input").elementHandle(),
    { polling: "raf", timeout: 15_000 },
  );
  await page.keyboard.type(key);
  await page.keyboard.press("Enter");
  await dialog.waitFor({ state: "detached" });
  const clients = (await api("GET", "admin/v1/r/client-key")).json.items;
  assert.equal(clients.length, 1, "one resource was created");
  const clientId = clients[0].id;
  assert.equal(clients[0].spec.key, "$secret", "ordinary client reads stay masked");
  await appears(page.getByRole("row").filter({ hasText: clientId }), "the new resource ID in the client keys table");
  assert.equal(await main.getByText(key, { exact: true }).count(), 0, "the table does not reveal the new key automatically");
  await page.evaluate(() => (location.hash = "#overview"));
  await appears(main.getByText("1 of 3 done"), "Get started at 1 of 3 after adding the key");
  assert.equal(await startSteps(page).getByText("completed").count(), 1, "one step is done");
  assert.equal(await startSteps(page).getByRole("listitem").first().getByText("completed").count(), 1, "the key step is the done one");
  pass("Get started: Add key by keyboard adds the key (1 of 3)");

  // 4. Use with tools selects the stable resource ID; only explicit Reveal fetches plaintext.
  assert.equal(secretExports.length, 0, "ordinary pages never request plaintext export");
  await page.evaluate((id) => (location.hash = `#clients?open=use&key=${encodeURIComponent(id)}`), clientId);
  const use = page.getByRole("dialog", { name: "Use with tools" });
  await appears(use, "the Use with tools dialog");
  const origin = new URL(root).origin;
  assert.equal(secretExports.length, 0, "opening Use with tools does not reveal a key");
  for (const tool of ["Claude Code", "Codex CLI", "Cursor", "OpenAI SDK", "Anthropic SDK", "curl"]) {
    await use.getByRole("radio", { name: new RegExp(`^${tool}`) }).check();
    await use.getByRole("button", { name: "Continue" }).click();
    if (tool === "Claude Code") {
      assert.equal(secretExports.length, 0, "opening settings does not reveal a key");
      await use.getByRole("button", { name: /^(?:Reveal|Show key)$/ }).click();
      await use.getByRole("button", { name: /^(?:Reveal|Show key)$/ }).waitFor({ state: "detached" });
      assert.equal(secretExports.length, 1, "explicit Reveal fetches one plaintext export");
    }
    const code = await use.locator("pre").textContent();
    if (tool === "Cursor") assert.ok(code.includes("https://your-public-address/v1"), "Cursor gets a public-address placeholder on localhost");
    else assert.ok(code.includes(origin), `${tool}: uses ${origin}`);
    assert.ok(!code.includes(key) && code.includes(mask(key)), `${tool}: the key is shown shortened`);
    await use.getByRole("button", { name: "Back" }).click();
  }
  await use.getByRole("button", { name: "Continue" }).click();
  await use.getByRole("button", { name: "Continue" }).click();
  await appears(use.getByText(/Connection works|no models are available yet/), "the key test result");
  await shot(page, "06-use-test-desktop-light");
  await use.getByRole("button", { name: "Done" }).click();
  pass(`Use with tools: all six setups point at ${origin} with the key shortened, and the key test answers`);

  // 5. A passing key test ticks the last step. An account then arrives and the step turns done by
  //    itself (the poll, every 10 s); with the key and the account's traffic the card closes.
  await page.evaluate(() => (location.hash = "#overview"));
  await appears(main.getByText("2 of 3 done"), "Get started at 2 of 3 after the key test passed");
  assert.equal(await startSteps(page).getByRole("listitem").nth(2).getByText("completed").count(), 1, "the test step is done");
  const form = new FormData();
  form.append("file", new Blob([JSON.stringify({ type: "claude", email: "first-run@example.invalid" })], { type: "application/json" }), "first-run-check.json");
  const upload = await fetch(new URL("v8/management/credentials", root), { method: "POST", headers: { Authorization: `Bearer ${secret}` }, body: form });
  assert.ok(upload.ok, `account upload: HTTP ${upload.status}`);
  await startHeading(page).waitFor({ state: "detached", timeout: 30_000 }).catch(() => {
    throw new Error("expected Get started to close once the server has a key, an account and traffic, but it is still shown");
  });
  await appears(main.getByRole("heading", { name: "Traffic" }), "the overview to show traffic after setup");
  await shot(page, "07-overview-ready-desktop-light");
  for (const [theme, device, name] of [
    ["dark", desktop, "08-overview-ready-desktop-dark"],
    ["light", phone, "09-overview-ready-phone-light"],
    ["dark", phone, "10-accounts-ready-phone-dark"],
  ]) {
    const p = await open(device, theme);
    await signIn(p);
    if (name.includes("accounts")) {
      await p.evaluate(() => (location.hash = "#accounts"));
      await appears(p.getByRole("heading", { name: "Accounts", level: 1 }), `${name}: the Accounts page`);
    }
    await p.waitForTimeout(500);
    assert.equal(await startHeading(p).count(), 0, `${name}: Get started is gone`);
    assert.equal(await overflow(p), false, `${name}: no horizontal overflow`);
    await shot(p, name);
    await p.context().close();
  }
  pass("after setup: Get started is gone and the overview shows traffic, in both themes and on a phone");
  assert.deepEqual(errors, []);
  pass("no page errors");
} finally {
  await browser.close();
}
