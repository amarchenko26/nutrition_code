"""Exhibits for build_family_events.do: diet around changes in who lives in the household (2026-11-05).

Reads interim/what_moves/family_events_coefs.csv and family_events_summary.csv.
Outputs: figs/what_moves/family/family_events.png (events in rows, outcomes in columns), tabs/what_moves/family_events.tex
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, GREY = '#2c5f8a', '#7a7a7a'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 12, 'xtick.labelsize': 11, 'ytick.labelsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})
EVENTS = [('emptynest', 'The last child leaves'), ('birth', 'A young child arrives'), ('partner', 'A second head joins'),
          ('split', 'A couple separates'), ('widowed', 'A head dies')]
OUT = [('hei_usda', 'HEI (points)'), ('upf_share', 'Ultra-processed share of calories (pct. points)'), ('cals_ae_day', 'Calories per adult equivalent per day'),
       ('produce_share', 'Produce share of calories (pct. points)'), ('sugar_1000', 'Sugar, g per 1,000 kcal'), ('real_income', 'Household income ($10k)')]


def main():
    c = pd.read_csv(idir / 'what_moves' / 'family_events_coefs.csv')
    s = pd.read_csv(idir / 'what_moves' / 'family_events_summary.csv')
    fig, axes = plt.subplots(len(EVENTS), len(OUT), figsize=(26, 19), sharex=True)
    fig.subplots_adjust(hspace=0.42, wspace=0.28)
    for i, (ev, evt) in enumerate(EVENTS):
        for j, (o, ot) in enumerate(OUT):
            ax = axes[i, j]
            d = c[(c.event == ev) & (c.outcome == o)].sort_values('r')
            r = s[(s.event == ev) & (s.outcome == o)]
            if d.empty or r.empty:
                ax.set_visible(False)
                continue
            r = r.iloc[0]
            ax.plot(d.r, d.b, color=BLUE, lw=2.2, marker='o', ms=4)
            ax.fill_between(d.r, d.b - 1.96 * d.se, d.b + 1.96 * d.se, color=BLUE, alpha=0.15, lw=0)
            ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
            ax.set_title(f'{evt if j == 0 else ""}{" · " if j == 0 else ""}{ot}\npost {r.jump:+.2f} ({r.jump_se:.2f}), before-move p = {r.p_pretrend:.2f}', fontsize=10.5)
    for ax in axes[-1]:
        ax.set_xlabel('Year relative to the change')
    out = fdir / 'what_moves' / 'family' / 'family_events.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=130)
    print(f'saved {out}')
    names = dict(EVENTS)
    cols = ['hei_usda', 'upf_share', 'cals_ae_day', 'produce_share', 'sugar_1000', 'real_income']
    lines = ['\\begin{tabular}{lrr' + 'c' * len(cols) + '}', '\\toprule',
             'Change & Households & Comparison & HEI & Ultra-proc. & Calories & Produce & Sugar & Income \\\\',
             ' & & households & (points) & (pct. pts) & per AE/day & (pct. pts) & (g/1,000 kcal) & (\\$10k) \\\\', '\\midrule']
    for ev, evt in EVENTS:
        x = s[s.event == ev].set_index('outcome')
        if x.empty:
            continue
        fmt = {'cals_ae_day': 0}
        row = [f'{x.loc[o, "jump"]:+.{fmt.get(o, 2)}f}' for o in cols]
        se = [f'({x.loc[o, "jump_se"]:.{fmt.get(o, 2)}f})' for o in cols]
        lines.append(f'{evt} & {int(x.n_events.iloc[0]):,} & {int(x.n_controls.iloc[0]):,} & ' + ' & '.join(row) + ' \\\\')
        lines.append(' & & & ' + ' & '.join(se) + ' \\\\')
    lines += ['\\bottomrule', '\\end{tabular}']
    (tdir / 'what_moves' / 'family_events.tex').write_text('\n'.join(lines) + '\n')
    print(s.round(3).to_string(index=False))


if __name__ == '__main__':
    main()
