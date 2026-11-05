"""Figure and table for the health-diagnosis event studies (build_diagnosis_events.do).

Reads interim/what_moves/diagnosis_events_coefs.csv and diagnosis_events_summary.csv.
Outputs: figs/what_moves/health/diagnosis_events.png (rows: conditions; columns: outcomes; the
event-study coefficients with 95% bands, r = -1 normalized to zero, never-reporters as controls),
tabs/what_moves/diagnosis_events.tex (post-event mean of r = 1..3 with SE, pre-trend p, by condition
and outcome) and diagnosis_events_regime.tex (the same for first reports in 2013-2015 vs 2017-2019).
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

BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 12, 'axes.labelsize': 12, 'axes.titlesize': 11, 'xtick.labelsize': 11, 'ytick.labelsize': 11,
                     'axes.spines.top': False, 'axes.spines.right': False})
CONDS = [('diabetes', 'Type-2 diabetes'), ('hypertension', 'Hypertension'), ('cholesterol', 'High cholesterol'),
         ('heart', 'Heart disease'), ('obesity', 'Obesity')]
OUTS = [('hei_usda', 'HEI', 'points'), ('sugar_1000', 'Sugar per 1,000 kcal', 'g'), ('sodium_1000', 'Sodium per 1,000 kcal', 'mg'),
        ('soda_share', 'Soda share of spending', 'pct. pts'), ('cals_ae_day', 'Calories per adult per day', 'kcal'),
        ('upf_share', 'Ultra-processed share', 'pct. pts'), ('produce_share', 'Produce share of calories', 'pct. pts')]
FIG = fdir / 'what_moves' / 'health'
TAB = tdir / 'what_moves'


def main():
    coefs = pd.read_csv(idir / 'what_moves' / 'diagnosis_events_coefs.csv')
    summ = pd.read_csv(idir / 'what_moves' / 'diagnosis_events_summary.csv')
    fig, axes = plt.subplots(len(CONDS), len(OUTS), figsize=(27, 17))
    fig.subplots_adjust(wspace=0.32, hspace=0.55)
    for i, (c, clab) in enumerate(CONDS):
        for j, (o, olab, unit) in enumerate(OUTS):
            ax = axes[i, j]
            d = coefs[(coefs['condition'] == c) & (coefs['regime'] == 'all') & (coefs['outcome'] == o)].sort_values('r')
            s = summ[(summ['condition'] == c) & (summ['regime'] == 'all') & (summ['outcome'] == o)]
            if d.empty or s.empty:
                ax.set_visible(False); continue
            s = s.iloc[0]
            ax.plot(d['r'], d['b'], color=BLUE, lw=2.2, marker='o', ms=5)
            ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=BLUE, alpha=0.15, lw=0)
            ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
            ax.set_xticks(range(-3, 4))
            ax.set_title(f'{clab} · {olab}\n{int(s["n_events"]):,} households · post {s["post"]:+.2f} ({s["post_se"]:.2f}) · pre p {s["p_pretrend"]:.2f}', fontsize=10)
            if j == 0:
                ax.set_ylabel(f'Change ({unit})')
            else:
                ax.set_ylabel(unit, fontsize=10)
            if i == len(CONDS) - 1:
                ax.set_xlabel('Year relative to the first report')
    FIG.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG / 'diagnosis_events.png', bbox_inches='tight', dpi=130)
    plt.close(fig)
    print(f'saved {FIG / "diagnosis_events.png"}')

    TAB.mkdir(parents=True, exist_ok=True)
    # main table: rows conditions, columns outcomes, cell = post (SE) over pre p
    out = ['\\begin{tabular}{l' + 'c' * len(OUTS) + '}', '\\toprule',
           'Condition (households) & ' + ' & '.join(olab for _, olab, _ in OUTS) + ' \\\\',
           ' & ' + ' & '.join(unit for _, _, unit in OUTS) + ' \\\\', '\\midrule']
    for c, clab in CONDS:
        cells, nev = [], None
        for o, _, _ in OUTS:
            s = summ[(summ['condition'] == c) & (summ['regime'] == 'all') & (summ['outcome'] == o)]
            if s.empty:
                cells.append('--'); continue
            s = s.iloc[0]; nev = int(s['n_events'])
            cells.append(f'{s["post"]:.2f} ({s["post_se"]:.2f})')
        out.append(f'{clab} ({nev:,}) & ' + ' & '.join(cells) + ' \\\\')
        pre = []
        for o, _, _ in OUTS:
            s = summ[(summ['condition'] == c) & (summ['regime'] == 'all') & (summ['outcome'] == o)]
            pre.append('--' if s.empty else f'\\footnotesize pre $p$ = {s.iloc[0]["p_pretrend"]:.2f}')
        out.append('\\quad & ' + ' & '.join(pre) + ' \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'diagnosis_events.tex').write_text('\n'.join(out) + '\n')
    # regime table
    rows = [('early', 'first report 2013--2015'), ('late', 'first report 2017--2019')]
    outs2 = [o for o in OUTS if o[0] in ('hei_usda', 'sugar_1000', 'cals_ae_day', 'soda_share')]
    out = ['\\begin{tabular}{ll' + 'c' * len(outs2) + '}', '\\toprule',
           'Condition & Survey regime (households) & ' + ' & '.join(olab for _, olab, _ in outs2) + ' \\\\', '\\midrule']
    for c, clab in CONDS:
        for reg, rlab in rows:
            cells, nev = [], None
            for o, _, _ in outs2:
                s = summ[(summ['condition'] == c) & (summ['regime'] == reg) & (summ['outcome'] == o)]
                if s.empty:
                    cells.append('--'); continue
                s = s.iloc[0]; nev = int(s['n_events'])
                cells.append(f'{s["post"]:.2f} ({s["post_se"]:.2f})')
            if nev is None:
                continue
            out.append(f'{clab} & {rlab} ({nev:,}) & ' + ' & '.join(cells) + ' \\\\')
    out += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'diagnosis_events_regime.tex').write_text('\n'.join(out) + '\n')
    print(f'saved {TAB / "diagnosis_events.tex"} and diagnosis_events_regime.tex')
    print(summ[summ['regime'] == 'all'].pivot(index='condition', columns='outcome', values='post').round(3).to_string())


if __name__ == '__main__':
    main()
