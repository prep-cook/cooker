---
name: cooker Rosé Pine
description: A quiet operator console. Grey carries hierarchy; hue carries state.
---

# Design language: Rosé Pine (Moon/Dawn)

The dashboard lets one operator keep many provider accounts healthy behind one local endpoint. The operator opens it to answer three questions, in this order: whether the proxy can serve now, which account needs attention, and what the tools just sent. Every screen supports scanning a long list and acting on one row; no screen is designed for reading.

The product rules in `PRODUCT.md` apply. Where that file and this one disagree on visual or size clauses, this file takes precedence.

## Principles

1. **Grey carries hierarchy, hue carries state.** Surfaces, text and controls use the palette's neutral roles. A coloured pixel marks a state (ready, cooling, failing, info) or, in iris only, the focus ring. Links use body text colour, as the card-v2 admin draws them: a chevron, the hover state and the button shape show what is clickable.
2. **One way to draw each thing.** Use one row, one list, one detail panel, one dialog, one form field and one notice. Use Astryx components first; a local component exists only when two screens need it.
3. **Dense, not cramped.** Row height and type follow Astryx's console scale. Numbers use tabular figures; identifiers (model IDs, keys, paths, file names) use the mono family.
4. **State is never colour alone.** Every lamp has a word or an accessible label; active navigation changes weight and background, not only hue.
5. **Motion reports; it does not decorate.** Use one motion module and Astryx durations, and disable all motion under `prefers-reduced-motion`.

## Colour

The default palette is **Neutral**, described below. The other two palettes are [Rosé Pine](https://rosepinetheme.com): **Moon** (Moon in dark mode, Dawn in light mode) and **Rosé Pine** (Main) (Main in dark mode, Dawn in light mode). The Rosé Pine values come unmodified from [rose-pine/palette](https://github.com/rose-pine/palette) (MIT): the named roles from `palette.json`, the highlight roles from `dist/css/rose-pine.css`; they live in `src/themes/site/rosePine.ts`. Every colour is an official value; pairs that miss a contrast limit are listed in `REQUIREMENTS.md` Exceptions instead of being adjusted.

**Neutral** is Astryx's neutral theme with its own colours in both modes, added for anyone who finds Rosé Pine too colourful. `siteNeutralTheme.ts` builds `site-neutral` (in the entry) with the site's sizes, type scale and motion rule, and a few colours so it reads monochrome: bar fills take neutral's greys (#919191 light, #777777 dark) instead of core's blue-grey; chart volume takes Astryx's categorical blue #0171E3 in both modes, as BoothDex's trend charts draw it (a grey volume series reads as dead); and warning takes neutral's orange family (soft #ffc7a1 / #503424 fill, #622700 / #ffb37e ink; light icons and dots core's #d66100) instead of its bright yellow #ffce2f. Accent, focus ring, error, success and info stay neutral's. Each override and its source value is commented in the file. The role table below describes the Rosé Pine palettes.

- Theme: the three site themes extend Astryx's official neutral theme (`defineTheme({extends: neutralTheme})`, as Astryx's setup guide does) and set only tokens. Fonts, component styles, icons (lucide), radius and motion come from neutral. `src/themes/site/siteNeutralTheme.ts` builds `site-neutral` (Neutral, the default, in the entry); `siteTheme.ts` builds `site` (Moon) and `siteMainTheme.ts` builds `site-main`, and both load on demand when chosen. `astryx theme build` names its outputs after the theme, and they keep those names, so the import example in each generated header stays true; the Rosé Pine colour and type tokens are shared (`roseTokens`). Both modes are first-class; the theme follows the system by default and provides a manual switch.
- Palette and mode are separate choices. Mode takes one click: a ghost icon button flips light and dark relative to what is shown (BoothDex's ColorModeToggle), and until the first press the page follows the system. Palette, mode (with "follow the system") and language are set in the settings dialog (see Preferences). The choice is stored and applied before the first render.
- Roles are the same for both palettes and follow Rosé Pine's own usage table:

  | Dashboard role | Rosé Pine role |
  | --- | --- |
  | Page background | base |
  | Card, side nav surface | surface |
  | Popover, menu, palette, toast; status and accent muted fills | overlay |
  | Hover and selected nav item, neutral badge | highlight med (low measures 1.01–1.12:1 on base and surface) |
  | Hover tint mixed into radios, checkboxes and switches | highlight low |
  | Borders, dividers, skeleton, track | highlight med |
  | Emphasized border (inputs) | highlight high |
  | Body text | text |
  | Secondary text | subtle |
  | Tertiary, placeholder, disabled text | muted |
  | Primary button fill / its label | text / base (ink) |
  | Links | text (the text accent is set to text) |
  | Focus ring | iris |
  | Ready / success | pine |
  | Info | foam |
  | Warning (cooling), yellow Token (warning log level) | gold |
  | Error (failing), red Token (error log level) | love |
  | rose | unused |

- Status vocabulary is a closed set: `ready`, `cooling`, `failing`, `info`, `disabled` (muted). A lamp is Astryx's `StatusDot` (`success`, `warning`, `error`, `neutral`; `info` is for banners and badges only) with its word in secondary text beside it, as the BoothDex console draws it. Icons, lamps and status borders take the hue; banner and badge fills are overlay with the hue as their text, so no fill is mixed from a hue. Contrast limits stay: body text ≥ 10:1, secondary and status text ≥ 5:1, measured on the composited colour; the official colours miss some of them; see `REQUIREMENTS.md`.
- The dialog scrim, the shadow, and the hover and pressed overlays drawn over a fill keep Astryx's translucent defaults; they are not palette colours, and Rosé Pine defines none.
- Text tokens are not fill tokens. Traffic strips use the neutral roles for volume, gold for a bucket where 5% to 50% of the requests failed, and love from 50% (fewer failures stay neutral).

## Type

- Fonts are the neutral theme's own and unchanged: fonts and icons use the component library's defaults. Figtree, then the system sans stack, for body and headings; the system monospace stack for code. Neutral's `fonts.css` loads Figtree from Google Fonts at run time; the app imports the same four weights from `@fontsource/figtree` instead (in `src/themes/site/index.ts`), so it makes no third-party request, as BoothDex does. There is no `typography` block and no per-locale font rule, so CJK text falls to the system fonts the browser picks for the page language.
- Scale: the card-v2 admin's (card-v2 `panelScale`, MIT): `typography.scale` base 15, ratio 1.2, in `siteTypography` (`src/themes/site/siteTheme.ts`). Body 15, card title in a list 18 (h3), section 22 (h2), page title 26 (h1), metric value 31 (`display-3`); supporting text 13 on a 20 px line (`--text-supporting-size`, as card-v2 `panelShared` sets it). Weights are 400, 500 and 600 only: headings and the metric value are semibold in every locale.
- Controls follow `panelShared`: `--size-element-sm` 32 (row actions, icon buttons with a 16 px glyph), `md` 36 (buttons, inputs, nav rows), `lg` 40. Radii 8, 12 and 16 (inner, element, card).

## Layout

Every screen starts from an Astryx template and keeps its frame: build from the templates and their components, and take page content from the component library. Each page file names its template in its header comment and lists every deviation with its reason.

### Routes

Six hash routes are defined. Not every screen needs its own page, so the remaining screens are views of one route (`?view=`) or dialogs (`?open=`). The side nav groups the pages in titled sections that are never collapsed, as the card-v2 console does: Monitor (Overview, Usage), Accounts (Accounts, Provider keys), Models (Models, Payload rules), Server (Settings, Client keys, Logs). The table is `navSections` in `src/app/routes.ts`; a view's URL stays on its route whatever section lists it. Pages carry no tab strip; switching through header tabs reads as excessive. Sign-in is not a route. Each tab and dialog hosts a feature page from `src/features/` through a thin host in `src/app/pages/`.

| Route (`#id`) | Answers first | Template | Views (side nav) and dialogs |
| --- | --- | --- | --- |
| Overview (`overview`) | Can the proxy serve now, and what needs me? | `dashboard-alert-rail` | Three tiles (can serve, requests, failure share); "Needs attention", one card per reason with accounts, each linking to where it is fixed, or one all-clear line; two chart cards. The first attention card is inside the first screen on a phone |
| Accounts (`accounts`) | Which accounts cannot serve, and when do they recover? | `table-inbox` | Views: Accounts, Provider keys (`?view=providers`) |
| Clients (`clients`) | Which key do I use, and how do I revoke one? | `table-page` (minimal) | Dialog: Use with a tool (`?open=use`), from the header and from a key's row menu |
| Models (`models`) | Which models can clients ask for, and where do they go? | `table-filter` | Views: Models, Payload rules (`?view=payload`) |
| Activity (`activity`) | Where does traffic come from, and what happened to a request? | `dashboard-comparison`, `table-filter` | Views: Usage, Logs (`?view=logs`) |
| Settings (`settings`) | Which server setting do I change, and what does it affect? | `settings-sidebar` | Dialog: System (`?open=about`) |
| Sign-in (not a route) | Which server do I connect to, and what do I enter? | `login-card` | — |
| Connect an account (dialog over any page, `?open=connect`) | Which kind of account, and how far is the authorization? | `form-wizard-dialog` | After the type is chosen only that flow shows |
| About (preferences dialog, `?open=about`) | Which build is this, and is it current? | `settings-dialog` | The dialog's About panel (below) |

- One hash form: `#<route>[/<arg>][?<query>]`. A tab is `?view=<tab>` (the first tab has none); a dialog is `?open=<dialog>`; a filter is its own key (`#accounts?show=<reason>`, which Accounts reads). Logs keeps its own `?tab=`, which is why the route tab is `view`. A hash that names no route opens the first page; there are no redirects from older hashes.
- A hosted dialog is `PageDialog` on the `form-wizard-dialog` frame, 800 wide, not the template's 620: the hosted pages were laid out for a reading width (`roles.content.formWidth`). Closing it drops its own query keys, so the next opening starts clean.
- Filters, tabs, paging and the open item live in the hash query, so Back restores them.
- The sign-in page renders none of the signed-in app: the shell and its pages are their own chunks (`src/app/chunks.ts`). Every load starts the shell and the current page's chunk while the session cookie is checked. recharts loads once a chart has its data and the page has painted, so it is never on a page's critical path.
- After sign-in, the browser fetches every route's chunk when it is idle, so the first visit to a page does not wait for its chunks. Queries are fresh for 30 s (`src/app/queryClient.ts`); a page entered again shows its cached data while it refetches, and polling queries keep their own interval.

### Shell (`shell-side-nav`, as BoothDex's admin shell)

The precedent is the BoothDex admin console, itself built from the same template.

- Astryx `AppShell` with the side nav only; there is no top bar. `SideNav` is `collapsible` and `resizable` (260 px default, 220 to 360).
- Header: `SideNavHeading` with the brand mark and "cooker", linking to the first page.
- Body: one `SideNavSection` per entry of `navSections`, with its title as a small heading over its `SideNavItem`s, each with its lucide icon; there are no collapsible groups. The current page uses Astryx's own selected item. `Network` is the brand mark and never a nav icon.
- Collapsed rail: section headings hide and every page becomes one icon that navigates on click, with its name as `SideNavItem`'s own tooltip on hover and keyboard focus; no flyout or submenu exists. Tab reaches each icon in order, and Enter navigates. The width and the collapsed state persist through `resizable.autoSaveId` (Astryx reads and writes localStorage in try/catch), a per-viewer convenience.
- Footer (`SideNavSection isHeaderHidden`): the connection row (host with its `StatusDot`; a popover gives the lamp, version and commit), "Go to page" (`CommandPalette`, also Ctrl/Cmd+K, lists the pages and, under them, their tabs and dialogs; SideNav has no search slot), GitHub (new tab) and Sign out.
- Footer icon row (`SideNav footerIcons`, beside the collapse button, the slot SideNav keeps in the collapsed rail): the colour-mode toggle and the settings button, each a ghost icon button with a tooltip.
- Phones (below Astryx `md`): AppShell's own phone bar and drawer, with nothing hand-built; the bar carries the heading and the footer icon row.
- After sign-in and after every page change, focus moves to the page's h1 without a ring.

### Preferences

- The settings button opens the `settings-dialog` template. Appearance: palette (with a preview), mode (light, dark, follow the system) and language. Keyboard shortcuts: the template's own panel (`settings-dialog/page.tsx:413`). Options that affect only this browser sit in the dialog too.
- The About panel (below) is the dialog's last panel, opened directly by `?open=about`.
- Server settings, which affect everyone, stay on the Settings page and never enter the dialog.
- Sign-in keeps its own mode, palette and language controls at the page's top end, on the page gutter, because the side nav is not there yet.

### Sign-in (`login-card`)

- One flat card, centred (the template's: no `elevation`), with the preference controls above. Under the card, one link to the repository. The template's third-party sign-in, forgotten password, sign-up and terms do not exist for cooker and are left out.
- Inside the card, top to bottom: the brand mark and name; the h1 "Connect to this server"; a status line (lamp, host, and whether `GET /healthz` answers, rechecked every 10 s while the tab is shown); the key field with a show/hide button (`InputGroup`); the primary Connect button; the help as a closed `Collapsible`.
- Connect is never greyed out. With an empty field it focuses the field and shows the field's error; while signing in it shows Astryx's busy state, and one press sends one request. Enter in the field submits through `onEnter`, which ignores Enter that commits an IME conversion.
- Sign-in errors are the field's error status, so the field describes them (`aria-describedby`).

### Pages

- Every page renders in one frame component, `src/app/PageFrame.tsx`, after the card-v2 admin's (card-v2 `PageFrame.tsx`, MIT): `Layout height="fill"` with the page padding set once on the `Layout`, `LayoutHeader`, `LayoutContent` and, for a detail or a rail, `LayoutPanel` at the end. A page puts its template's body inside it and never draws its own header, so the h1 does not jump between pages. The header and the content share one line, so the h1 sits at the same place on every page; the content line stops at the content width, and a reading-width page caps only its content, aligned to the header's start, never centred.
- The header holds the h1; one supporting line, held empty while the page's line waits for its data so nothing moves; the page's controls; and at most one primary action. Controls that do not fit beside the title (a phone with two labelled buttons) take their own row under the supporting line, decided by the title and the controls alone, so a count arriving never moves them. A list's search and filters sit in the `LayoutHeader` (as `table-filter` does), so they stay while the content scrolls; on phones they fold as the template's narrow layout folds them (`table-filter:2034-2088`). A rarely used action (Clear) goes into a menu. A route's views are side-nav items, never a tab strip in the header.
- A page hosted by a route renders through `PageHostContext` (`src/app/pageHost.ts`): as a tab, its frame takes the route's title, tabs and controls; in a dialog, it draws only its controls and body under the dialog's title.
- The body is a header plus one or two sections. More content goes into header tabs, a 340 px side column (`layout.sideColumn`) or paging at 20 rows.
- Headings: h1 is the page; h2 is `SectionHeading` (`src/components/`) for a section outside cards; h3 is `CardHeading` inside a card; h4 is a sub-block. Never stack two headings. A key number is a `MetricCard` (label row, value at the metric size, optional badge, one supporting line; with `href` the whole card is the link, marked by a chevron). All three follow card-v2's components.
- Cards have one style (radius 16, hairline border, the card padding) and hold only a few related facts or a chart. Lists, tables, settings and logs are flat with hairline dividers and are never boxed.
- The outcome of a page action is a toast, or a banner directly under the header for a read.
- Collections are rows, not cards. Accounts and Logs are virtualized with `@tanstack/react-virtual` (as `LogRows.tsx` does; the templates do not virtualize).
- Low-frequency detail (raw JSON, empty groups, import forms) starts collapsed or lives in the detail panel.
- The preferences dialog's About panel lists the build and the server in one `MetadataList`: version, commit, build time, config layout, routing strategy, auth directory, remote management (a lamp with its word) and dashboard source. A value the server does not send reads "Not reported". The panel ends with the release check, whose outcomes are: up to date (`success`); update available with a link to the releases (`info`); cannot compare (`info`); no release published yet (`info`; the server's 502 `invalid_response`); checks disabled (`info`); anything else an error banner with Retry.

### Roles

A role that repeats across pages has one value. Sizes, spacing, type, radii, control heights, heading levels, card use and link colour come from the card-v2 admin (MIT): one size for each role keeps a dense page from looking cluttered. Colours stay Rosé Pine. The Astryx templates settle the rest. Spacing and sizes inside one template's own composition (its gaps, a toolbar's `size="sm"`) stay as that template writes them. Values are in `src/themes/site/roles.ts` and `layout.ts`; `check:ui` enforces the type rows, and `npm run measure` enforces the rendered values. A template's typography value (`Text size="sm"`, for example) enters a page only after it is added to `roles.ts` with a named use and its source, so text stays one scale across pages.

| Role | Value | Source |
| --- | --- | --- |
| Page frame | `Layout` / `LayoutHeader` / `LayoutContent` / `LayoutPanel`, padding set once on `Layout` | card-v2 `PageFrame.tsx`; `dashboard-alert-rail:1446-1567`, `table-filter:4333-4480` |
| Header | Every route has a `LayoutHeader` with a divider holding the h1 (26/600) and one 13 px `Text type="supporting"` line; the h1 is at the same place on every route. `table-page` and `settings-sidebar` have no header (`:856-869`, `:301`); their pages add one, a recorded deviation | card-v2 `PageFrame.tsx` |
| Page padding | 6 (24 px), 4 (16 px) below Astryx `md`; the body ends with 10 (40 px) | card-v2 `PageFrame.tsx` |
| Content width | The content line takes the full width (no 1440 cap, unlike card-v2: it left empty bands on wide screens). Settings pages cap their content at 1200, single-column forms at 800; lists and dashboards take the full line | card-v2 `PageFrame.tsx` `CONTENT_WIDTH`; `settings-sidebar:303`, `form-wizard-inline:610` |
| Section gap | 6 (24 px) between a page's sections | card-v2 `layout.ts` `SECTION_GAP` |
| Card gap | 4 (16 px) between cards and between blocks inside one card | card-v2 `layout.ts` `CARD_GAP` |
| Card | One style: `Card` with radius 16, hairline border, padding 5 (20 px). Only for a few related facts or a chart | card-v2 `layout.ts` `CARD_PADDING` |
| Side column | 340 px beside the main column | card-v2 `layout.ts` `SIDE_WIDTH` |
| Table density | `balanced`; `table-filter`'s density switch (`:3896`) is removed, a recorded deviation | `table-inbox:2628`, `table-grouped:953`, `table-page:933` |
| Heading levels | 1 page title, 2 `SectionHeading` outside cards, 3 `CardHeading` inside, 4 a sub-block; nothing deeper; one level 1 per route; never two headings stacked | card-v2 page rules |
| Text types | `body` content and cells (15); `supporting` the line under a title and hints (13); `label` a field or metric label; `display-3` a metric value (31/600); `code` identifiers (model IDs, keys, paths) as plain mono text; versions and other numbers take `body` with tabular figures. The `Code` chip (filled, padded) is only for code inside a sentence or a snippet, never in a table cell, list row or metadata value; `inherit` text inside another text. A `Heading` takes no `type`: its level sets the scale | card-v2 `MetricCard.tsx`, `panelScale`; `table-grouped:777`, `dashboard-alert-rail:1461`, `:863`, `dashboard-comparison:389` |
| Text size and weight | Size: `lg` for the brand name only. Weight: `semibold` for emphasis, `bold` for the brand and a group name, `normal` as the plain branch of a conditional emphasis. Rendered weights are 400/500/600 | `login-card:94`, `dashboard-alert-rail:1098`, `table-grouped:1001`, `table-inbox:1059` |
| Primary action | At most one `Button variant="primary"` per page, default size (36 px), at the header's end; the other header controls are 36 too | card-v2 button rules, `panelShared` |
| Row actions | `IconButton variant="ghost" size="sm"`: 32 × 32 with a 16 px glyph | card-v2; `table-inbox:1008`, `table-filter:2159` |
| Nav row | 36 px, 16 px icon and text | card-v2 admin nav |
| Links | Body text colour, no underline; a chevron, the hover and the button shape show what is clickable; navigation is a link, an action is a button | card-v2 `siteTheme.ts` (text accent is the ink) |
| Numbers | Every numeric column and metric value: `Text hasTabularNumbers`; a metric value is `Text type="display-3" hasTabularNumbers`, never a heading | card-v2 `MetricCard.tsx`; `dashboard-comparison:703`, `table-page:503` |
| Charts | Two series at most, 2 px lines or bars, horizontal grid only, no dots; plot 14rem, 12rem on a phone; the legend is one text row | card-v2 `TrendChart.tsx` |
| Detail panel | `LayoutPanel` behind a `ResizeHandle`, 380 px (320 to 560), padding 0. Below the compact query (`(max-width: 768px), (max-width: 1024px) and (pointer: coarse) and (hover: none)`) a `BottomSheet height="tall" padding={0}` | `table-filter:2229`, `:4300`, `:4313`, `:1129` |
| Skeleton | Each region renders `Skeleton`s in the shape and size of its content, as `table-filter`'s loading rows do; a refetch keeps the previous data on screen | `table-filter:1882-1945` |

Measured limits (`npm run measure`): CLS during load ≤ 0.02; the first region moves ≤ 2 px from its skeleton to its content; each route's answer element (Overview's first attention card or its all-clear line, for example) is inside 1440×900 and 390×844. Every measured route declares its answer selector and the measurements its page draws (table rows, header buttons, row actions, skeleton); a missing declaration or a declared measurement with no sample fails, and so do buttons of one role whose heights differ within a route.

### Keyboard

Follow the templates: each row is one tab stop, and Enter or Space opens its detail (`table-inbox:1109-1125`, `table-filter:2754-2762`). Astryx Table has no arrow-key movement between rows, and none is added. Ctrl/Cmd+K opens "Go to page"; the settings dialog lists the shortcuts.

## Components

| Need | Use |
|---|---|
| Account, key, model, log rows | One `Row` pattern on Astryx `List`/`Table`: status lamp and label, name and secondary line, metrics, row menu |
| Detail | Astryx `LayoutPanel` with `MetadataList`; `BottomSheet` on phones (Roles) |
| Edits | Astryx `Dialog purpose="form"`; config edits show a diff and reread before write |
| Quota windows | Astryx `ProgressBar` with label and reset time |
| Traffic per source (20 × 10 min) | The `dashboard-alert-rail` template's own `UptimeStrip`: one cell per bucket, neutral volume, gold or love by failure share, accessible summary |
| Charts | See Charts |
| Writes | Busy button, one write at a time, success toast, failure toast that stays with the server's text; deletes confirm in `AlertDialog` |
| Notices | See Notifications |

## Notifications

| Level | Meaning | Where |
|---|---|---|
| `success` | An action the user started finished | Toast, auto-dismiss |
| `info` | Neutral fact the user should know now | Toast, auto-dismiss |
| `warning` | Something works but needs attention (account cooling, setting ignored by this server) | Banner on the page. A lasting state (an account cooling) is rendered by the page from its data and stays while the state lasts; a warning sent through `notify()` is an event and stays until dismissed or the page changes |
| `error` | An action failed or a read failed | Banner for reads; toast that stays for a failed action |

`notify()` accepts only these four levels. Toasts sit at the bottom end, one at a time.

## Motion

Motion covers press feedback, toast entry and exit, panel open and close, and a lamp pulse on live elements (log tail, OAuth waiting, live usage). There are no page transitions and no row entrance stagger: a list that refreshes every 10 s must not move.

## Charts

- Charts use recharts, as nine Astryx templates do (`dashboard/page.tsx:20`, `dashboard-alert-rail/page.tsx:60-68`); Astryx has no chart component. All charts live in one module (`src/components/charts/`), structured after card-v2's chart set (`charts.tsx`, `TrendChart.tsx`, `LegendDot.tsx`, `MetricCard.tsx`, `focusRing.ts`).
- Colours are theme tokens passed as CSS variables (`dashboard-alert-rail:697-730`): series take `--color-data-categorical-*`, defined in the theme as Rosé Pine colours; failure stays gold and love. Chart code has no hex or px fallbacks.
- `isAnimationActive` is off on every series: the 10 s poll must not replay an animation.
- Hover or focus shows a point's tooltip, and the arrow keys step through points; a mouse click draws no browser outline, and keyboard focus draws Astryx's focus ring.
- Geometry follows the card-v2 admin (card-v2 `TrendChart.tsx`): two series at most, 2 px lines or bars, grid lines across the value axis only (horizontal in a time chart), no dots. The plot is 14rem high, 12rem on a phone (`chartPlot` in `src/themes/site/charts.ts`); a bar list grows with its rows instead. The legend is one text row. Two chart cards side by side align at the top and keep their own heights.
- Charts draw only what the API has: total and failed requests over 200 minutes (Overview), and source comparison and failure rate (Activity, Usage tab). The templates' 1h, 1d and 7d windows are dropped. Each chart has a one-line written conclusion as its screen-reader description, not shown: the tiles carry the same numbers, and a visible sentence would only repeat them.
- KPI tiles (`MetricCard`) appear where the page's template has them: Overview and Activity's Usage tab.

## Not doing

- No gradients, glass or glow.
- No top bar: the brand is the side nav's heading, and the page title is the page's h1.
- No custom selected-item decoration and no style overrides on Astryx navigation components, or on any component: the theme sets tokens only (gate: `check:ui`; a `components` block in a theme source fails).
- No disabled-grey primary buttons: a primary action stays pressable and explains itself (with focus and a field error, or a banner) instead of greying out.
- No long explanatory paragraphs above data: one supporting line under the title; anything more is a collapsed section or one muted note after the data.
- No accent hue beyond iris in its two places (links, focus ring); the primary button stays ink.
- Icons are the component library's defaults and there is no app registry: use a semantic name from the theme where one fits the meaning (`calendar`, `wrench`, `search`, `copy`, `check`, `eyeSlash` and similar; the list is `npx astryx docs icons available-names`), otherwise a lucide component passed straight to `Icon` or an `icon` prop and typed with Astryx's `IconProps["icon"]`. There is no `registerIcons` and no second icon set, and template icons (heroicons) become lucide. The brand mark is lucide `Network` in the text colour with no tile behind it, in the side nav, on the sign-in card and in `public/favicon.svg` (gate: `check:ui`; any `<svg>`, `<path>` or `registerIcons` fails).
- No per-page colours, spacings or font sizes: the Roles table holds the one value per role.
