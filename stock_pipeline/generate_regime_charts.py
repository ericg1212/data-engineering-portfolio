"""
Regime Chart Generator — run after `make regime` produces live CSVs.

Reads:
    stock_pipeline/regime_analysis.csv   — one row per month: regime + rolling Sharpe premium
    stock_pipeline/regime_summary.csv    — one row per regime combo: avg premium + month count

Outputs (stock_pipeline/regime_charts/):
    heatmap.png         — rate regime × inflation regime grid, cell = avg Sharpe premium
    bar_chart.png       — one bar per regime combo, sorted by premium, month count labeled
    time_series.png     — rolling Sharpe premium over time, background shaded by regime
    decision_matrix.png — table: if X regime → lean Builders vs Integrators

Usage:
    make regime_charts          # requires regime_analysis.csv + regime_summary.csv
    python stock_pipeline/generate_regime_charts.py

Note: run `make regime` first to generate the input CSVs (requires live AWS + S3 FRED data).
"""

import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(SCRIPT_DIR, 'regime_charts')
ANALYSIS_CSV = os.path.join(SCRIPT_DIR, 'regime_analysis.csv')
SUMMARY_CSV = os.path.join(SCRIPT_DIR, 'regime_summary.csv')

RATE_REGIMES = ['falling', 'rising', 'high']
INFLATION_REGIMES = ['normal', 'elevated', 'high']

# Regime background colors for time series shading
REGIME_COLORS = {
    'Low Rate / Low Inflation': '#d4e6f1',
    'Low Rate / Moderate Inflation': '#a9cce3',
    'Low Rate / High Inflation': '#7fb3d3',
    'Rising Rate / Low Inflation': '#fdebd0',
    'Rising Rate / Moderate Inflation': '#fad7a0',
    'Rising Rate / High Inflation': '#f8c471',
    'High Rate / Low Inflation': '#fadbd8',
    'High Rate / Moderate Inflation': '#f1948a',
    'High Rate / High Inflation': '#ec7063',
}


def _load_csvs():
    if not os.path.exists(ANALYSIS_CSV):
        raise FileNotFoundError(
            f"regime_analysis.csv not found at {ANALYSIS_CSV}\n"
            "Run `make regime` first to generate input data."
        )
    if not os.path.exists(SUMMARY_CSV):
        raise FileNotFoundError(
            f"regime_summary.csv not found at {SUMMARY_CSV}\n"
            "Run `make regime` first to generate input data."
        )
    analysis = pd.read_csv(ANALYSIS_CSV, parse_dates=['month'])
    summary = pd.read_csv(SUMMARY_CSV)
    # Normalize summary columns to match script expectations
    summary = summary.rename(columns={
        'builder_premium': 'avg_builder_premium',
        'n_months': 'month_count'
    })
    split = summary['combined_regime'].str.split('_', expand=True)
    summary['rate_regime'] = split[0]
    summary['inflation_regime'] = split[1]
    return analysis, summary


def generate_heatmap(summary: pd.DataFrame):
    """Rate regime × inflation regime grid; cell = avg Sharpe premium, annotated with month count."""
    pivot = summary.pivot_table(
        index='rate_regime', columns='inflation_regime',
        values='avg_builder_premium', aggfunc='mean',
    )
    counts = summary.pivot_table(index='rate_regime', columns='inflation_regime', values='month_count', aggfunc='sum')

    pivot = pivot.reindex(index=RATE_REGIMES, columns=INFLATION_REGIMES)
    counts = counts.reindex(index=RATE_REGIMES, columns=INFLATION_REGIMES)

    fig, ax = plt.subplots(figsize=(9, 5))
    im = ax.imshow(pivot.values.astype(float), cmap='RdYlGn', aspect='auto', vmin=-50, vmax=200)

    ax.set_xticks(range(len(INFLATION_REGIMES)))
    ax.set_yticks(range(len(RATE_REGIMES)))
    ax.set_xticklabels(INFLATION_REGIMES)
    ax.set_yticklabels(RATE_REGIMES)

    for i in range(len(RATE_REGIMES)):
        for j in range(len(INFLATION_REGIMES)):
            val = pivot.values[i, j]
            n = counts.values[i, j]
            if not np.isnan(val):
                label = f"{val:+.1f}%\n(n={int(n)})"
                ax.text(j, i, label, ha='center', va='center', fontsize=9, fontweight='bold')
            else:
                ax.text(j, i, 'No data', ha='center', va='center', fontsize=8, color='gray')

    plt.colorbar(im, ax=ax, label='Avg Builder Sharpe Premium (%)')
    ax.set_title('AI Builder Sharpe Premium by Macro Regime\n(green = builders outperform, red = underperform)',
                 fontsize=11, pad=12)
    ax.set_xlabel('Inflation Regime')
    ax.set_ylabel('Rate Regime')

    plt.tight_layout()
    out = os.path.join(CHARTS_DIR, 'heatmap.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print("  heatmap.png saved")


def generate_bar_chart(summary: pd.DataFrame):
    """One bar per regime combo, sorted by avg premium; month count labeled on each bar."""
    df = summary.copy()
    df['regime_label'] = df['rate_regime'] + '\n' + df['inflation_regime']
    df = df.sort_values('avg_builder_premium', ascending=True)

    colors = ['#e74c3c' if v < 0 else '#27ae60' for v in df['avg_builder_premium']]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['regime_label'], df['avg_builder_premium'], color=colors, edgecolor='white')

    for bar, (_, row) in zip(bars, df.iterrows()):
        x = bar.get_width()
        label = f"n={int(row['month_count'])}"
        ax.text(x + (3 if x >= 0 else -3), bar.get_y() + bar.get_height() / 2,
                label, va='center', ha='left' if x >= 0 else 'right', fontsize=8)

    ax.axvline(0, color='black', linewidth=0.8)
    ax.set_xlabel('Avg Builder Sharpe Premium (%)')
    ax.set_title('AI Builder Sharpe Premium by Macro Regime\n(sorted by premium; n = months in regime)',
                 fontsize=11)
    ax.set_xlim(df['avg_builder_premium'].min() - 30, df['avg_builder_premium'].max() + 30)

    plt.tight_layout()
    out = os.path.join(CHARTS_DIR, 'bar_chart.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print("  bar_chart.png saved")


def generate_time_series(analysis: pd.DataFrame):
    """Rolling Sharpe premium over time; background shaded by combined regime."""
    fig, ax = plt.subplots(figsize=(12, 5))

    # Shade background by regime
    if 'combined_regime' in analysis.columns:
        prev_regime = None
        start_date = None
        for _, row in analysis.iterrows():
            regime = row.get('combined_regime', '')
            if regime != prev_regime:
                if prev_regime is not None and start_date is not None:
                    color = REGIME_COLORS.get(prev_regime, '#f0f0f0')
                    ax.axvspan(start_date, row['month'], alpha=0.25, color=color, label=prev_regime)
                start_date = row['month']
                prev_regime = regime

    ax.plot(analysis['month'], analysis['builder_premium'], color='#2c3e50',
            linewidth=1.5, label='Rolling Builder Premium')
    ax.axhline(0, color='gray', linewidth=0.8, linestyle='--')
    ax.axhline(analysis['builder_premium'].mean(), color='#e67e22',
               linewidth=1.0, linestyle=':', label=f"Mean: {analysis['builder_premium'].mean():+.1f}%")

    ax.set_xlabel('Date')
    ax.set_ylabel('Builder Sharpe Premium (%)')
    ax.set_title('Rolling AI Builder Sharpe Premium Over Time\n(background shaded by macro regime)',
                 fontsize=11)
    ax.legend(loc='upper left', fontsize=7, ncol=2)

    plt.tight_layout()
    out = os.path.join(CHARTS_DIR, 'time_series.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print("  time_series.png saved")


def generate_decision_matrix(summary: pd.DataFrame):
    """Simple table: if X regime → recommended stance."""
    df = summary.copy()
    df['combined_regime'] = df['rate_regime'] + ' / ' + df['inflation_regime']
    df = df.sort_values('avg_builder_premium', ascending=False)

    def stance(premium):
        if premium > 50:
            return 'Strong Builders'
        elif premium > 10:
            return 'Lean Builders'
        elif premium > -10:
            return 'Neutral'
        else:
            return 'Lean Integrators'

    df['stance'] = df['avg_builder_premium'].apply(stance)
    df['premium_fmt'] = df['avg_builder_premium'].apply(lambda x: f"{x:+.1f}%")

    fig, ax = plt.subplots(figsize=(9, max(3, len(df) * 0.5 + 1.5)))
    ax.axis('off')

    table_data = [['Macro Regime', 'Avg Premium', 'Months', 'Stance']]
    for _, row in df.iterrows():
        table_data.append([
            row['combined_regime'],
            row['premium_fmt'],
            str(int(row['month_count'])),
            row['stance'],
        ])

    colors_map = {'Strong Builders': '#27ae60', 'Lean Builders': '#82e0aa',
                  'Neutral': '#f0f0f0', 'Lean Integrators': '#e74c3c'}
    row_colors = [['#2c3e50', '#2c3e50', '#2c3e50', '#2c3e50']]
    for _, row in df.iterrows():
        c = colors_map.get(row['stance'], '#f0f0f0')
        row_colors.append(['white', 'white', 'white', c])

    tbl = ax.table(cellText=table_data, loc='center', cellLoc='center')
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 1.6)

    for (i, j), cell in tbl.get_celld().items():
        if i == 0:
            cell.set_facecolor('#2c3e50')
            cell.set_text_props(color='white', fontweight='bold')
        elif j == 3:
            stance_val = table_data[i][3]
            cell.set_facecolor(colors_map.get(stance_val, '#f0f0f0'))

    ax.set_title('Macro Regime Decision Matrix\n(Builder vs Integrator Stance by Regime)',
                 fontsize=11, pad=20)

    plt.tight_layout()
    out = os.path.join(CHARTS_DIR, 'decision_matrix.png')
    plt.savefig(out, dpi=150, bbox_inches='tight')
    plt.close()
    print("  decision_matrix.png saved")


def main():
    os.makedirs(CHARTS_DIR, exist_ok=True)
    print("Loading regime CSVs...")
    analysis, summary = _load_csvs()

    print(f"  regime_analysis.csv: {len(analysis)} rows")
    print(f"  regime_summary.csv:  {len(summary)} rows")
    print("\nGenerating charts...")

    generate_heatmap(summary)
    generate_bar_chart(summary)
    generate_time_series(analysis)
    generate_decision_matrix(summary)

    print(f"\nAll charts saved to {CHARTS_DIR}/")


if __name__ == '__main__':
    main()
