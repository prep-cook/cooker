# Dashboard performance harness

The harness measures the dashboard in a real browser against a mock Management API with many accounts. The mock and the measurements do not depend on the UI framework, so another implementation can repeat the same run and compare its numbers.

```text
ui/tools/mock/server.mjs        mock /v8/management API (Node built-ins only)
ui/tools/perf/run.mjs           the harness
ui/tools/perf/adapters/<ui>.mjs every selector for one UI implementation
```

## Run

Build the UI somewhere other than `ui/dist`, then run the harness from `ui/`:

```sh
cd ui
node_modules/.bin/vite build --outDir "${TMPDIR:-/tmp}/cooker-perf/dist" --emptyOutDir
node tools/perf/run.mjs --accounts 1000 --minutes 5 --dist "${TMPDIR:-/tmp}/cooker-perf/dist"
```

The harness starts the mock on a free port, starts `vite preview` of `--dist` (its `/v8` proxy points at the mock through `CK_BACKEND`), opens headless Chrome, signs in through the real login form and measures. It stops both servers on exit and builds or writes nothing inside the repository.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--accounts N` | 1000 | accounts the mock serves |
| `--minutes M` | 5 | how long to keep polling for M2 and M3 |
| `--ui NAME` | react | adapter in `adapters/` |
| `--dist DIR` | `ui/dist` | built UI to serve |
| `--out DIR` | `<os tmpdir>/cooker-perf/<timestamp>/` | where `report.json`, `report.md`, `mock.log` and `preview.log` go |
| `--seed N` | 1 | mock seed; the same seed gives the same accounts |
| `--idle S` | 60 | seconds of M4 idle |
| `--polls N` | 6 | polls M2 takes its median over |
| `--loads N` | 3 | sign-ins per page for M1 |
| `--reps N` | 5 | repetitions of each M5 interaction |
| `--chrome PATH` | system Chrome | browser binary |
| `--headed` | off | show the window |
| `--strict-coverage` | off | count unexpected failed `/v8` requests as a harness failure |

Exit status: `0` when the harness ran to the end, whatever the numbers; `2` when it could not (it cannot sign in, a page never renders, a server does not start, or a polled request never appears). The harness still writes a partial `report.json`. Run nothing else heavy at the same time; the numbers are wall-clock times and CPU shares on a shared machine.

To run the mock alone, run `node tools/mock/server.mjs --port 8317 --accounts 1000 --seed 1 --key mock`, then `CK_BACKEND=http://127.0.0.1:8317 npm run dev`.

## What the mock serves

The mock serves every route the dashboard calls, with response shapes taken from `crates/ck-server/src/management/` and the fields the dashboard's `src/lib/` modules read. `X-Ck-Version: cooker-mock` makes the UI treat it as cooker. Accounts spread over claude, codex, gemini, kimi, antigravity, xai, devin and vertex, with twenty `recent_requests` buckets each, about 5% disabled, 5% cooling, 2% signed out, 3% with a model cooldown, and passive quota signals on most Claude, Codex and Devin accounts. Each `GET /credentials` gives about 5% of the accounts new request counts; the rest serialise to the same bytes. All buckets shift once, 10 minutes after the server starts. Write routes validate and answer like the real ones and keep state in memory. Unknown routes answer an empty 404.

The mock is not faithful in these places: `config.yaml` is kept as text and not parsed back into the JSON config; OAuth sign-in never completes; the usage queue returns random events; and logs are synthetic.

## Metrics

All times are from the page's own clock. Main-thread numbers are Chrome DevTools `Performance.getMetrics` deltas (`TaskDuration`, `ScriptDuration`, `LayoutDuration`, `RecalcStyleDuration`), so they count the renderer main thread only, not compositing or the GPU.

- **M1 load.** From the moment the login form is submitted until the page lists all N rows, and until the next frame after that (`paint`, the value to report). Run on a cold browser context `--loads` times for Accounts and for Overview. The report also gives the main-thread time spent in that span and when the `/credentials` response finished, which separates fetch time from render time.
- **M2 poll.** The cost of one 10 s refresh on Accounts. The window runs from the request start to 1.5 s after its body arrived. `gross` is the main-thread time in the window; `net` subtracts the idle rate measured in the gap before the poll. The long-task count and the longest long task (over 50 ms, from a `PerformanceObserver`) come from the same window. The median covers the first `--polls` polls.
- **M3 memory.** `JSHeapUsedSize` after a forced garbage collection, once after load and once after `--minutes` of polling; DOM node and listener counts alongside; a series with one point per minute. Growth that does not level off points to a leak.
- **M4 idle CPU.** Main-thread `TaskDuration` over `--idle` seconds on Accounts with no interaction, as a percent of one core. The 10 s polls run in that window; `between polls` is the same figure without them. Per-process CPU (renderer, GPU, browser) from `SystemInfo.getProcessInfo` is listed too.
- **M5 interactions.** On Accounts: select all, deselect all, select one row, unselect it, expand a row, collapse it. Time from the click event to the frame after the page reached the expected state. Median and maximum over `--reps`; a 10 s poll landing inside a step shows up as the maximum.
- **Pages.** Client-side navigation to each page the adapter lists: time until its heading shows and nothing is loading, the `/v8` requests it made, and any with status 400 or higher. The empty-body requests the dashboard sends to learn which routes a server has are expected to fail with 400; the harness counts them as probes and does not list them (`isExpectedProbe` in the adapter). Console errors are listed.

## Add another UI

Copy `adapters/react.mjs`, change the selectors and expressions, and run with `--ui <name>`. The adapter supplies the login selectors, the page headings, a "ready" expression per page (and one that holds once every account is listed), the M5 steps (a target to click and a condition that must hold), and `isExpectedProbe`. Keep the measured behaviour the same, or the numbers will not compare.
