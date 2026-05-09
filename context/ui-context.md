# UI Context

## Theme

The visual language is a **Security Operations Center (SOC) Interface**. It prioritizes high-contrast visibility for critical alerts and forensic investigations. The theme is **Dark-Only**, utilizing a "Deep Space" palette to ensure that "Fraud-Alert Red" and "Safe-System Green" are immediately identifiable. The design is dense and data-rich, optimized for analysts who need to cross-reference multiple streaming metrics simultaneously.

## Colors

| Role            | CSS Variable       | Value     |
| --------------- | ------------------ | --------- |
| Page background | `--bg-base`        | `#050505` |
| Surface         | `--bg-surface`     | `#121212` |
| Primary text    | `--text-primary`   | `#e2e8f0` |
| Muted text      | `--text-muted`     | `#64748b` |
| Primary accent  | `--accent-primary` | `#38bdf8` |
| Border          | `--border-default` | `#262626` |
| Error (Fraud)   | `--state-error`    | `#ff4b4b` |
| Success (Valid) | `--state-success`  | `#22c55e` |

## Typography

| Role      | Font               | Variable      |
| --------- | ------------------ | ------------- |
| UI text   | Inter / Sans-Serif | `--font-sans` |
| Code/mono | JetBrains Mono     | `--font-mono` |

## Border Radius

| Context           | Class            | Value    |
| ----------------- | ---------------- | -------- |
| Inline / small UI | `rounded-none`   | `0rem`    |
| Cards / panels    | `rounded-sm`     | `0.125rem` |
| Modals / overlays | `rounded-md`     | `0.375rem` |

## Component Library

Standardized on **shadcn/ui** with heavy customization for "Industrial" aesthetics. All components feature hard edges (low border radius) to reinforce the technical, military-grade nature of the application. High-frequency updates are handled via custom **React / Tailwind** components optimized for minimal re-renders during 25K events/sec peaks.

## Layout Patterns

- **Command Center:** A fixed three-column layout. Left: Live Alert Feed; Center: Real-time Transaction Map/Graph; Right: Detailed Forensics Panel.
- **Velocity Counters:** Mini-metric cards at the top of the viewport displaying "Fraud Rate %" and "System Latency (ms)" in real-time.
- **Forensic Table:** A high-density data grid (via DuckDB) with mono-spaced fonts for comparing transaction metadata (IPs, Device IDs, Latitudes).
- **The "Kill Switch" Overlay:** A high-visibility modal used for system-wide adjustments to fraud thresholds, featuring a distinct backdrop blur to focus the analyst's attention.

## Icons

**Lucide React** (Stroke weight: 1.5px). 
- **Threat Levels:** Shield icons for safe events; Danger/Alert icons for flagged fraud.
- **System Health:** Pulse/Activity icons for Kafka and Flink status.
- **Sizes:** `h-4 w-4` for data tables; `h-6 w-6` for primary dashboard status indicators.