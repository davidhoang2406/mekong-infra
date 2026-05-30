# Mekong Web — HTML Prototype

Hand-coded Tailwind HTML mockups for every page in the [front-end design
spec](../README.md). Single-file per page, no build step, no framework —
open any `.html` in a browser and click around.

These are **clickable mockups** (no real data, no API calls). They serve two
purposes:

1. **Visual review** — show what the React SPA will look like before writing
   any React.
2. **Figma import source** — open in a browser, screenshot at high DPI, then
   drag into Figma or use the [html.to.design](https://www.figma.com/community/plugin/1159123024924461424/html-to-design)
   plugin to convert the DOM into editable Figma layers.

## Files

| File | Page |
|---|---|
| `index.html` | Dashboard |
| `symbol.html` | Symbol detail (VCB) |
| `screener.html` | Fundamental screener |
| `digest.html` | Daily digest |
| `login.html` | Login (Phase 4) |
| `register.html` | Register (Phase 4) |
| `styles.css` | Shared overrides (sparkline + candlestick polish) |

## How to view

```bash
cd design/front-end/mock
open index.html             # or any other .html
# or serve so relative imports work:
python3 -m http.server 8000 && open http://localhost:8000
```

No `npm install`, no Vite, no React — Tailwind is loaded via CDN, fonts via
Google Fonts, icons via inline SVG.

## How to import to Figma

1. Open the desired `.html` in Chrome / Safari at exactly 1600 × 1100 viewport
   (Dashboard / Symbol / Screener / Digest) or 1360 × 920 (Auth).
2. Install [html.to.design](https://www.figma.com/community/plugin/1159123024924461424/html-to-design)
   in Figma.
3. In Figma: Plugins → html.to.design → paste the page URL (or upload the
   HTML file directly).
4. The plugin converts every element into a Figma layer with the right
   colors, fonts, and sizing — fully editable.

Alternative: screenshot each page at 2× DPI and `Place Image` in Figma if
you only need pixel-perfect reference, not editable layers.

## Design tokens

Defined inline in each page's `tailwind.config` block — copy/paste-able to
your real Tailwind config:

```js
{
  fontFamily: {
    sans:  ['"Inter"',          'system-ui', 'sans-serif'],
    mono:  ['"JetBrains Mono"', 'ui-monospace', 'monospace'],
  },
  colors: {
    bg:         '#ffffff',
    'bg-muted': '#f5f5f5',
    fg:         '#1e1e1e',
    'fg-muted': '#737373',
    border:     '#e5e5e5',
    up:         '#16a34a',
    down:       '#dc2626',
    live:       '#16a34a',
  },
}
```

## Limitations

- Charts are SVG placeholders, not real Lightweight-Charts renders.
- No real data — every price/percentage is hand-typed.
- No theme toggle (single light theme only; dark mode is in the React build).
- No routing — links between pages use plain `<a href>` to other HTML files.

## What's next (React build)

These mockups are the visual contract for `mekong-web`. The React build
should:

1. Re-create every layout in JSX using the same Tailwind classes.
2. Replace SVG placeholders with TradingView Lightweight Charts +
   Recharts components.
3. Wire all data via TanStack Query → `mekong-api` and Zustand → `mekong-ws`.

See `../README.md` for the design system + per-page data sources.
