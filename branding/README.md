# swarmqtt brand

| File | Use |
|------|-----|
| `logo.svg` | Horizontal logo (mark + wordmark): README, docs, slides |
| `mark.svg` | Icon only: avatar, favicon, social preview |

Both switch to light ink automatically on dark backgrounds (`prefers-color-scheme`).

**Concept**: a swarm of devices (dots) flies into the hive (the broker), which keeps broadcasting.
The wordmark highlights `mqtt` inside `swarmqtt`.

## Colors

| Token | Light bg | Dark bg |
|-------|----------|---------|
| Hive (mark) | `#F59E0B` | `#F59E0B` |
| Wordmark accent `mqtt` | `#B45309` | `#F59E0B` |
| Ink (`swar`, dots, signal) | `#111827` | `#F3F4F6` |

`#B45309` is used for `mqtt` on light backgrounds because `#F59E0B` text on white is too low-contrast.

## Type

Bold geometric sans: Inter 700, falling back to Segoe UI / Helvetica / Arial.
The wordmark is live text, so outline it to paths before using it in print or merch.

## Don't

- Recolor the hive or stretch the mark
- Put the mark on amber/orange backgrounds
- Separate the dots from the hive (the swarm is the idea)
