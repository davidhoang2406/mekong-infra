# Mekong Web — Front-End Design

UI design specs and wireframes for `mekong-web`, the React SPA that consumes
`mekong-api` (REST) and `mekong-ws` (WebSocket). See
[`../services/DATA-PLATFORM.md`](../services/DATA-PLATFORM.md) for the full
data-platform architecture this UI sits on top of.

## Files

| File | Purpose |
|---|---|
| `design-system.excalidraw` | Color palette, typography, spacing, component library |
| `dashboard.excalidraw` | Landing page — live ticker + gainers/losers/volume |
| `symbol-detail.excalidraw` | Per-symbol page — candlestick + indicators + live header |
| `screener.excalidraw` | Weekly fundamental screener — sortable table |
| `digest.excalidraw` | Daily market digest — tabbed top movers |
| `auth.excalidraw` | Login + register pair (Phase 4) |
| `mobile.excalidraw` | Mobile (< 768px) layout adaptations |

Open in [excalidraw.com](https://excalidraw.com) (File → Open) or via the
[VS Code Excalidraw extension](https://marketplace.visualstudio.com/items?itemName=pomdtr.excalidraw-editor).

## Visual language

Strict black/white wireframes for now — no brand color yet. Same convention
as the platform architecture diagrams:

- `#1e1e1e` strokes on `#ffffff` background
- Solid lines for built / current state
- Dashed lines for to-be-built or alternate state
- Dotted rectangles for grouping / namespace boundaries

When the platform reaches Phase 4, swap the placeholder grays for the agreed
brand palette (TBD — single accent color recommended, leave price up/down as
green/red regardless of theme).

## Design system

### Typography

| Token | Size | Weight | Usage |
|---|---|---|---|
| `display` | 32 | 600 | Page titles (rare) |
| `h1` | 24 | 600 | Page H1 |
| `h2` | 20 | 600 | Section heads |
| `h3` | 16 | 600 | Card titles |
| `body` | 14 | 400 | Body text, table cells |
| `small` | 12 | 400 | Captions, meta |
| `mono` | 14 | 500 | Prices, symbols, code |

Font: **Inter** for UI, **JetBrains Mono** for prices and symbols.

### Spacing

4 / 8 / 12 / 16 / 24 / 32 / 48 / 64 px. Tailwind's default scale.

### Color (Phase 1 — wireframe)

| Token | Hex | Usage |
|---|---|---|
| `bg` | `#ffffff` | App background |
| `bg-muted` | `#f5f5f5` | Card background, hover row |
| `fg` | `#1e1e1e` | Primary text |
| `fg-muted` | `#737373` | Secondary text |
| `border` | `#e5e5e5` | Hairline dividers |
| `up` | `#16a34a` | Positive price change |
| `down` | `#dc2626` | Negative price change |
| `live` | `#16a34a` | Live connection indicator |

Dark mode swaps `bg ↔ fg` and `bg-muted ↔ #262626`; price up/down colors stay
the same so the meaning never changes.

### Components

Built on **shadcn/ui** (Radix primitives + Tailwind). Required in Phase 2:

- `Button` (default, ghost, outline, destructive)
- `Input` (text, number, date)
- `Select` (single + multi)
- `Tabs`
- `Table` (with TanStack Table for sort/filter)
- `Card`
- `Badge` (used for LIVE indicator, asset class chips)
- `Tooltip`
- `Skeleton` (loading placeholders)
- `Dialog` / `Sheet`
- `Toast` (for error notifications)
- `DropdownMenu`

Custom Mekong components:

- `PriceDisplay` — formatted price with up/down color and arrow
- `CandlestickChart` — TradingView Lightweight Charts wrapper
- `SparklineChart` — minimal trend line
- `RSIChart`, `MACDChart` — indicator sub-panes
- `LiveTickerBar` — horizontal scrolling marquee, WS-backed
- `DateRangePicker` — preset + custom
- `WatchlistPicker` — quick-add/remove dropdown

## Layout shell

```
┌──────────────────────────────────────────────────────────────────────────┐
│ [Mekong]   Search... 🔍              [⌘K] [🌗] [🔔] [user ▾]            │ ← Navbar (60px)
├─────────┬────────────────────────────────────────────────────────────────┤
│         │                                                                 │
│ ◐ Dash  │                                                                 │
│ ★ Watch │                  Main content                                   │
│ ⌕ Sym   │                  (route-specific)                               │
│ ⊞ Scrn  │                                                                 │
│ ◫ Dgst  │                                                                 │
│ ⚙ Sett  │                                                                 │
│         │                                                                 │
│ ● LIVE  │                                                                 │
└─────────┴────────────────────────────────────────────────────────────────┘
   ↑ Sidebar (240px, collapses to icon-only at < 1024px)
```

The sidebar's `● LIVE` indicator at the bottom is driven by the WebSocket
connection state (green / yellow pulsing / red).

## Page specs

### Dashboard (`/`)

Landing page. Visible without auth in Phase 1, gated behind login in Phase 4.

```
┌────────────────────────────────────────────────────────────────────────┐
│ Live Ticker Bar (horizontal scroll, subscribed to *)                   │
│ VCB 85,800 ▲0.59%  │  FPT 128,000 ▲2.1%  │  BTC 68,500 ▲1.7%  │ ... │
├────────────────────────────────┬───────────────────────────────────────┤
│ Top Gainers                    │ Top Losers                            │
│ ┌──────────────────────────┐   │ ┌─────────────────────────────────┐  │
│ │ FPT   +6.67%   128,000   │   │ │ HPG   -3.2%   25,400            │  │
│ │ VNM   +4.12%    76,500   │   │ │ MSN   -2.8%   62,100            │  │
│ │ ...                       │   │ │ ...                              │  │
│ └──────────────────────────┘   │ └─────────────────────────────────┘  │
├────────────────────────────────┼───────────────────────────────────────┤
│ Volume Leaders                 │ My Watchlist                          │
│ ┌──────────────────────────┐   │ ┌──────────┐ ┌──────────┐            │
│ │ VCB   2,345,678          │   │ │ VCB ╱╲╱  │ │ BTC ╲╱╲  │            │
│ │ HPG   1,890,123          │   │ │ 85,800   │ │ 68,500   │            │
│ │ ...                       │   │ │ +0.59%   │ │ +1.78%   │            │
│ └──────────────────────────┘   │ └──────────┘ └──────────┘            │
└────────────────────────────────┴───────────────────────────────────────┘
```

**Data:**
- Ticker bar: WS subscribe `*`
- Gainers / Losers / Volume: `GET /api/v1/digest?date=today`
- Watchlist: `GET /api/v1/watchlists` (Phase 4) + `useOHLCV(symbol)` per item

**Interactions:**
- Row click → `/symbol/:symbol`
- Watchlist card click → `/symbol/:symbol`
- Live ticker click → `/symbol/:symbol`

### Symbol detail (`/symbol/:symbol`)

The most data-dense page. Combines historical chart, live updates, and
indicator panes.

```
┌────────────────────────────────────────────────────────────────────────┐
│ ← Back   VCB ▸ HOSE ▸ Stock        [+ Watchlist] [⇄ Compare] [⤓ CSV] │
├────────────────────────────────────────────────────────────────────────┤
│ VCB                                                                     │
│ ₫85,800   ▲ +500  +0.59%   ● LIVE                                      │
├────────────────────────────────────────────────────────────────────────┤
│ [1W] [1M] [3M] [6M] [1Y] [ALL]                              [+ SMA ▾] │
├────────────────────────────────────────────────────────┬───────────────┤
│                                                         │ Price         │
│   ▕ ▆▇▅▄▆▇█▇▆▅▄▃▄▅▆▇█▇▆▅▆▇▆▕                          │ Open  85,000  │
│   ▕   ╱╲       ╱╲                                       │ High  86,200  │
│   ▕ ╱  ╲╱╲   ╱  ╲   ←  SMA20                          │ Low   84,500  │
│   ▕      ╲ ╱     ╲                                      │ Close 85,800  │
│   ▕       ╲                                             │ Vol   2.34M   │
│                                                         │               │
│   Volume                                                │ Indicators    │
│   ▁▂▃▅▆▄▃▂▁▂▃▄▅▆▇█▇▆▅▄▃▂▁▂                            │ SMA20  85,100 │
├────────────────────────────────────────────────────────┤ SMA50  84,200 │
│ RSI(14)                                                 │ SMA200 82,500 │
│         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 70 (overbought)│ RSI14   58.3 │
│   ╱╲      ╱╲      ╱╲╱╲╱    ╱╲  ╱╲                       │ MACD   120.5  │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 30 (oversold)│ MACD-s  95.2 │
├────────────────────────────────────────────────────────┤ BB-upper 87.2k│
│ MACD                                                    │ BB-mid   85.1k│
│ ─────── ╱╲    histogram bars                            │ BB-lower 83.0k│
│         ╲╱                                              │               │
│                                                         │               │
└────────────────────────────────────────────────────────┴───────────────┘
```

**Data:**
- Header price: WS subscribe `[symbol]`, initial snapshot on connect
- Candlestick + volume: `GET /api/v1/ohlcv?symbol&from&to`
- RSI / MACD / BB: `GET /api/v1/indicators?symbol&from&to`

**Live behavior:**
- On WS tick: update header price (animate up/down briefly), call
  `series.update()` to mutate the last candle.
- Time range selector changes `from` → triggers refetch, re-renders chart.

### Screener (`/screener`)

Sortable table for weekly fundamental analysis. Lightweight — single table.

```
┌────────────────────────────────────────────────────────────────────────┐
│ Screener                          Year [2026 ▾]  Week [21 ▾]  [⤓ CSV] │
├────────────────────────────────────────────────────────────────────────┤
│ Search... 🔍                            Sector [All ▾]  Industry [All ▾]│
├────────────────────────────────────────────────────────────────────────┤
│ Symbol │ P/E ↓ │ P/B  │ ROE % │ EPS   │ D/E  │ Curr Ratio │  Action  │
├────────┼───────┼──────┼───────┼───────┼──────┼───────────┼──────────┤
│ VCB    │ 14.2  │ 2.1  │ 22.5  │ 6,200 │ 0.8  │ 1.4       │  [View]  │
│ FPT    │ 18.5  │ 3.0  │ 19.8  │ 4,800 │ 1.2  │ 1.6       │  [View]  │
│ VNM    │ 22.1  │ 4.5  │ 28.0  │ 5,100 │ 0.5  │ 2.0       │  [View]  │
│ ...    │       │      │       │       │      │           │          │
├────────────────────────────────────────────────────────────────────────┤
│  ◀ 1 2 3 ... 12 ▶                                  Showing 1-25 of 287│
└────────────────────────────────────────────────────────────────────────┘
```

**Data:** `GET /api/v1/screener?year&week`

**Interactions:**
- Click column header → sort asc/desc with arrow indicator
- Search box → client-side filter on symbol name
- Sector / industry filters → reduce visible rows
- [View] button → `/symbol/:symbol`
- Year/Week change → refetch

### Digest (`/digest`)

Daily snapshot of market movers. Three tabs, one table per tab.

```
┌────────────────────────────────────────────────────────────────────────┐
│ Daily Digest                     Date [2026-05-25 ▾]  Asset [All ▾]   │
├────────────────────────────────────────────────────────────────────────┤
│ [▼ Top Gainers] [Top Losers] [Volume Leaders]                          │
├────────────────────────────────────────────────────────────────────────┤
│  # │ Symbol │ Exch.  │ Open    │ Close   │ Volume    │ % Change ▾   │
├────┼────────┼────────┼─────────┼─────────┼───────────┼──────────────┤
│  1 │ FPT    │ HOSE   │ 120,000 │ 128,000 │ 5,678,901 │ ▲ +6.67 %    │
│  2 │ VNM    │ HOSE   │  73,500 │  76,500 │ 3,210,000 │ ▲ +4.12 %    │
│  3 │ HPG    │ HOSE   │  24,500 │  25,200 │ 8,900,123 │ ▲ +2.86 %    │
│  ...                                                                    │
└────────────────────────────────────────────────────────────────────────┘
```

**Data:** `GET /api/v1/digest?date&category&limit=10`

### Auth (Phase 4)

Login and register forms — centered card, minimal chrome.

```
┌───────────────────────────────────────┐
│                                       │
│        Mekong                         │
│        ──────                         │
│        Sign in to your account        │
│                                       │
│        Email                          │
│        ┌─────────────────────────┐    │
│        │ you@example.com         │    │
│        └─────────────────────────┘    │
│                                       │
│        Password                       │
│        ┌─────────────────────────┐    │
│        │ ●●●●●●●●●●              │    │
│        └─────────────────────────┘    │
│                                       │
│        ┌─────────────────────────┐    │
│        │       Sign in           │    │
│        └─────────────────────────┘    │
│                                       │
│        Don't have an account?         │
│        → Register                     │
│                                       │
└───────────────────────────────────────┘
```

After successful login, redirect to `/` (or to the original `next` param).

### Settings (`/settings`)

Tabbed settings page. Each tab is a single column form.

| Tab | Contents |
|---|---|
| Profile | Name, email, change password |
| API Keys | List with label / last-used / rate-limit, generate new, revoke |
| Watchlists | CRUD per watchlist (name + symbols multi-select) |
| Preferences | Theme (light / dark / system), default date range, default asset class |

## Mobile adaptation (< 768px)

- Sidebar collapses behind a hamburger menu in the navbar.
- Symbol detail right-sidebar moves below the chart (stacked).
- Dashboard cards stack into a single column.
- Tables become horizontally scrollable; pin the first column (symbol).
- Charts switch to touch-friendly crosshair (long-press to read values).

See `mobile.excalidraw` for stacked layout sketches.

## Accessibility

- All charts have a tabular fallback (toggle in chart toolbar) for screen readers.
- Color is never the only signal — up/down also use ▲/▼ arrows.
- Focus rings visible on all interactive elements.
- Skip-to-content link in the navbar.
- WCAG AA contrast minimum (text-fg-muted on bg-muted = 4.5:1 minimum).

## Open questions

Tracked in `../services/DATA-PLATFORM.md` §13. Front-end specific:

1. Should symbol search live in the navbar (`⌘K`) or only on the symbols
   route? Recommend navbar — it's the most common navigation.
2. Should the user be able to pin arbitrary charts to the dashboard, or are
   the gainers/losers/volume blocks fixed?
3. Should the candlestick chart support drawing tools (trendlines, fib levels)?
   That implies user-state persistence per symbol per user — material work.
4. Should the watchlist support sorting / grouping? Likely yes; defer to
   Phase 4 when watchlists are introduced.
