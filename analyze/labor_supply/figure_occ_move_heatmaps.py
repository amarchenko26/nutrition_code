"""Occupation-move heatmaps from the clean-sample grid regressions.

Reads interim/labor_supply/occ_move_coefs.csv (15_build_occ_move_grids.do;
reduced form of destination dummies on the original male-preferred shock,
clean sample = employed shock cell at t-1 and t) and renders:

1. occ_transition_3x3.png -- occupation at t-1 (x-axis: service, blue,
   white; employed origins) vs occupation at t (y-axis, including Not
   employed -- the clean sample keeps young unemployed). Each column is a
   separate conditional sample; the four destinations are exhaustive, so
   each column's coefficients sum to zero. Stayers marked with dashed boxes.
2. occ_move_ranks.png -- pooled 5x5 grid of (change in occupation rank) x
   (change in income tercile), t-1 -> t. Ranks: service (1) < blue (2) <
   white (3), by mean household income. (0,0) = stayers, dashed box.

Cells are pp per $10k of shock: red = the shock moves households INTO the
cell, blue = out.

Outputs (figs/labor_supply/): occ_transition_3x3.png, occ_move_ranks.png
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
             ) / 'interim' / 'labor_supply' / 'occ_move_coefs.csv'
FIG_DIR = Path('/Users/anyamarchenko/Documents/GitHub/nutrition_overleaf'
               ) / 'figs' / 'labor_supply'

OCC_LABELS = {1: 'White collar', 2: 'Blue collar', 3: 'Service', 4: 'Not employed'}
OCC_ORDER = [3, 2, 1]   # service, blue, white -- left-to-right and bottom-to-top

CMAP = LinearSegmentedColormap.from_list(
    'shock_div', ['#3b6b9a', '#f7f7f5', '#a63d40'])


def stars(p):
    return '***' if p < 0.01 else '**' if p < 0.05 else '*' if p < 0.10 else ''


def annotate(ax, i, j, b, se, p, vmax):
    ink = 'white' if abs(b) > 0.62 * vmax else '#262626'
    ax.text(j, i - 0.16, f"{b:+.2f}{stars(p)}", ha='center', va='center',
            fontsize=13, fontweight='bold', color=ink)
    ax.text(j, i + 0.26, f"({se:.2f})", ha='center', va='center',
            fontsize=10, color=ink)


def style(ax):
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)


def occ3x3_figure(sub):
    """Origin occupation (cols) x destination occupation (rows)."""
    rows = [1, 2, 3, 4]                # top-to-bottom: white, blue, service, not employed
    cols = OCC_ORDER                   # left-to-right: service, blue, white
    grid = sub.pivot(index='row', columns='origin_occ', values='b') * 100
    grid = grid.reindex(index=rows, columns=cols)
    vmax = np.abs(grid.values).max() * 1.05

    fig, ax = plt.subplots(figsize=(7.2, 7.2))
    im = ax.imshow(grid.values, cmap=CMAP,
                   norm=TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax), aspect='auto')
    for i, d in enumerate(rows):
        for j, o in enumerate(cols):
            r = sub[(sub['row'] == d) & (sub['origin_occ'] == o)].iloc[0]
            annotate(ax, i, j, r['b'] * 100, r['se'] * 100, r['p'], vmax)
            if d == o:   # stayers
                ax.add_patch(Rectangle((j - 0.5, i - 0.5), 1, 1, fill=False,
                                       edgecolor='#262626', linestyle='--',
                                       linewidth=2))
    ns = {o: int(sub[sub['origin_occ'] == o]['N'].iloc[0]) for o in cols}
    ax.set_xticks(range(3))
    ax.set_xticklabels([f"{OCC_LABELS[o]}\n(N = {ns[o]:,})" for o in cols],
                       fontsize=13)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([OCC_LABELS[d] for d in rows], fontsize=13)
    ax.set_xlabel('Occupation type at $t-1$', fontsize=14)
    ax.set_ylabel('Occupation type at $t$', fontsize=14)
    ax.set_title('Occupation transitions per \\$10k shock', fontsize=14)
    style(ax)
    cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
    cbar.set_label('pp per \\$10k shock', fontsize=13)
    cbar.ax.tick_params(labelsize=11)

    out = FIG_DIR / 'occ_transition_3x3.png'
    plt.savefig(out, bbox_inches='tight', dpi=150)
    plt.close(fig)
    for o in cols:
        s = sub[sub['origin_occ'] == o]['b'].sum() * 100
        print(f"  origin {OCC_LABELS[o]}: coef sum {s:+.4f} pp")
    print(f"saved {out}")


def ranks_figure(sub):
    rows = [2, 1, 0, -1, -2]           # d_occ, top to bottom
    cols = [-2, -1, 0, 1, 2]           # d_inc, left to right
    grid = sub.pivot(index='row', columns='col', values='b') * 100
    grid = grid.reindex(index=rows, columns=cols)
    vmax = np.abs(grid.values).max() * 1.05

    fig, ax = plt.subplots(figsize=(8.2, 7))
    im = ax.imshow(grid.values, cmap=CMAP,
                   norm=TwoSlopeNorm(vmin=-vmax, vcenter=0, vmax=vmax), aspect='auto')
    for i, r_val in enumerate(rows):
        for j, c_val in enumerate(cols):
            r = sub[(sub['row'] == r_val) & (sub['col'] == c_val)].iloc[0]
            annotate(ax, i, j, r['b'] * 100, r['se'] * 100, r['p'], vmax)
    ax.add_patch(Rectangle((cols.index(0) - 0.5, rows.index(0) - 0.5), 1, 1,
                           fill=False, edgecolor='#262626', linestyle='--',
                           linewidth=2))
    ax.set_xticks(range(5))
    ax.set_xticklabels([f"{c:+d}" if c else "0" for c in cols], fontsize=13)
    ax.set_yticks(range(5))
    ax.set_yticklabels([f"{r:+d}" if r else "0" for r in rows], fontsize=13)
    ax.set_xlabel('Change in income tercile, $t-1 \\to t$', fontsize=14)
    ax.set_ylabel('Change in occupation rank, $t-1 \\to t$', fontsize=14)
    n = int(sub['N'].iloc[0])
    ax.set_title(f"Rank moves per \\$10k shock (N = {n:,})", fontsize=14)
    style(ax)
    cbar = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
    cbar.set_label('pp per \\$10k shock', fontsize=13)
    cbar.ax.tick_params(labelsize=11)

    out = FIG_DIR / 'occ_move_ranks.png'
    plt.savefig(out, bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f"saved {out}  (coef sum {sub['b'].sum() * 100:+.4f} pp)")


def main():
    df = pd.read_csv(COEFS)
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    occ3x3_figure(df[df['gridtype'] == 'occ3x3'])
    ranks_figure(df[df['gridtype'] == 'ranks'])


if __name__ == '__main__':
    main()
