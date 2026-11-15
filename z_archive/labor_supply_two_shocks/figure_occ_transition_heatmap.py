"""Occupation-transition heatmaps: where does the shock move households?

Reads the grid-regression coefficients from 15_build_occ_transition_grid.do
(reduced form: 1{occ type x income tercile at t} on the t-1 -> t shock,
conditional on the occupation type at t-1). One figure per baseline type,
two panels (original male-preferred cell shock | t-1-hours-weighted
working-head shock). Cells are pp per $10k of shock: red = the shock moves
households INTO the cell, blue = out. The dashed box marks the baseline
occupation's own row (movement along it = richer/poorer within the same
occupation). Coefficients sum to zero within a panel by construction.

Outputs (figs/labor_supply/):
  occ_transition_heatmap_blue.png / _white.png / _service.png
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
from matplotlib.patches import Rectangle
from pathlib import Path

COEFS = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
             ) / 'interim' / 'labor_supply' / 'occ_transition_coefs.csv'
FIG_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition'
               ) / 'figs' / 'labor_supply'

OCC_LABELS = {1: 'White collar', 2: 'Blue collar', 3: 'Service', 4: 'Not employed'}
BIN_LABELS = {1: 'Low', 2: 'Middle', 3: 'High'}
SHOCKS = [('orig', 'Original shock'), ('whrs', 'Hours-weighted shock')]
BASELINES = {2: 'blue', 1: 'white', 3: 'service'}

# diverging: cool pole -> neutral -> warm pole (matches the paper's accent red)
CMAP = LinearSegmentedColormap.from_list(
    'shock_div', ['#3b6b9a', '#f7f7f5', '#a63d40'])


def stars(p):
    return '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''


def draw_panel(ax, sub, vmax, title, show_ylabels):
    """One 4x3 heatmap; sub has occ_type (rows) x inc_bin (cols)."""
    grid = sub.pivot(index='occ_type', columns='inc_bin', values='b') * 100
    grid = grid.reindex(index=[1, 2, 3, 4], columns=[1, 2, 3])

    norm = TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax)
    im = ax.imshow(grid.values, cmap=CMAP, norm=norm, aspect='auto')

    for i, t in enumerate(grid.index):
        for j, c in enumerate(grid.columns):
            row = sub[(sub['occ_type'] == t) & (sub['inc_bin'] == c)].iloc[0]
            b, se = row['b'] * 100, row['se'] * 100
            ink = 'white' if abs(b) > 0.62 * vmax else '#262626'
            ax.text(j, i - 0.13, f"{b:+.2f}{stars(row['p'])}",
                    ha='center', va='center', fontsize=13,
                    fontweight='bold', color=ink)
            ax.text(j, i + 0.24, f"({se:.2f})",
                    ha='center', va='center', fontsize=10, color=ink)

    ax.set_xticks(range(3))
    ax.set_xticklabels([BIN_LABELS[c] for c in grid.columns], fontsize=13)
    ax.set_yticks(range(4))
    if show_ylabels:
        ax.set_yticklabels([OCC_LABELS[t] for t in grid.index], fontsize=13)
    else:
        ax.set_yticklabels([])
    ax.set_xlabel('Income tercile at $t$', fontsize=14)
    n = int(sub['N'].iloc[0])
    ax.set_title(f"{title}\n(N = {n:,})", fontsize=14)
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    return im


def main():
    df = pd.read_csv(COEFS)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    for base_code, base_name in BASELINES.items():
        d = df[df['base_type'] == base_code]
        vmax = np.abs(d['b']).max() * 100 * 1.05

        fig, axes = plt.subplots(1, 2, figsize=(11, 5.6))
        for k, (shock, title) in enumerate(SHOCKS):
            sub = d[d['shock'] == shock]
            im = draw_panel(axes[k], sub, vmax, title, show_ylabels=(k == 0))
            # dashed box on the baseline occupation's own row
            axes[k].add_patch(Rectangle(
                (-0.5, base_code - 1 - 0.5), 3, 1, fill=False,
                edgecolor='#262626', linestyle='--', linewidth=1.8))
            # sanity: coefficients over an exhaustive grid sum to ~0
            print(f"baseline {base_name}, {shock}: coef sum = "
                  f"{sub['b'].sum() * 100:+.4f} pp")

        axes[0].set_ylabel('Occupation type at $t$', fontsize=14)
        cbar = fig.colorbar(im, ax=axes, fraction=0.035, pad=0.02)
        cbar.set_label('pp per \\$10k shock', fontsize=13)
        cbar.ax.tick_params(labelsize=11)

        out = FIG_DIR / f'occ_transition_heatmap_{base_name}.png'
        plt.savefig(out, bbox_inches='tight', dpi=150)
        plt.close(fig)
        print(f"saved {out}")


if __name__ == '__main__':
    main()
