"""Figures and table for the soda-tax event studies (build_soda_tax_did.do).
Reads interim/what_moves/soda_tax_coefs.csv and soda_tax_summary.csv.
Outputs (Overleaf clone): figs/what_moves/prices/soda_tax_philadelphia.png (treated households, six outcomes),
soda_tax_cities.png (pooled city taxes, three outcomes); tabs/what_moves/soda_tax.tex (post-tax changes by design).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / 'paths.py').exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
import pandas as pd, numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 13, 'axes.titlesize': 12, 'axes.spines.top': False, 'axes.spines.right': False})
IN = idir / 'what_moves'
FIG = fdir / 'what_moves' / 'prices'
TAB = tdir / 'what_moves'
FIG.mkdir(parents=True, exist_ok=True); TAB.mkdir(parents=True, exist_ok=True)
OUT = [('soda_oz', 'Sugary soda bought, oz per household-year', 'oz'), ('home_share', 'Share of soda bought at stores in own zip3', 'pct. points'),
       ('soda_share', 'Soda share of grocery spending', 'pct. points'), ('sugar_per_1000cal_usda', 'Sugar per 1,000 kcal', 'grams'),
       ('hei_usda', 'HEI', 'points'), ('upf_share', 'Ultra-processed share of calories', 'pct. points')]
LAB = {'philadelphia': 'Philadelphia, 1.5 cents per oz from January 2017', 'cities': 'Berkeley, Albany, Oakland, Boulder, San Francisco, Seattle (2015-18)', 'cook': 'Cook County, 1 cent per oz, August-November 2017 only'}


def panel(ax, c, s, group, color, label):
    d = c[(c['group'] == group)].sort_values('r')
    r = s[s['group'] == group].iloc[0]
    ax.plot(d['r'], d['b'], color=color, lw=2.5, marker='o', ms=6, label=f'{label}: post {r["post"]:+.2f} (SE {r["post_se"]:.2f}), pre-trend p = {r["p_pre"]:.2f}')
    ax.fill_between(d['r'], d['b'] - 1.96 * d['se'], d['b'] + 1.96 * d['se'], color=color, alpha=0.15, lw=0)


def fig_design(coefs, summ, design, outs, name, ncol):
    nrow = int(np.ceil(len(outs) / ncol))
    fig, axes = plt.subplots(nrow, ncol, figsize=(6.5 * ncol, 5 * nrow))
    for ax, (o, title, unit) in zip(np.atleast_1d(axes).flat, outs):
        c = coefs[(coefs['design'] == design) & (coefs['outcome'] == o)]; s = summ[(summ['design'] == design) & (summ['outcome'] == o)]
        if c.empty:
            ax.set_visible(False); continue
        panel(ax, c, s, 'treated', BLUE, 'taxed')
        panel(ax, c, s, 'adjacent', GREY, 'adjacent counties')
        ax.axhline(0, color=GREY, lw=1, ls='--'); ax.axvline(-0.5, color=GREY, lw=1, ls=':')
        ax.set_xticks(range(-4, 4)); ax.set_xlabel('Year relative to the first taxed year'); ax.set_ylabel(f'Change ({unit})')
        ax.set_title(title); ax.legend(loc='upper left', frameon=False, fontsize=9)
    fig.suptitle(f'{LAB[design]}: {int(summ[summ.design == design].n_treated.iloc[0]):,} treated households, '
                 f'{int(summ[summ.design == design].n_control.iloc[0]):,} controls in the same state', y=1.0, fontsize=13)
    fig.tight_layout(); fig.savefig(FIG / name, bbox_inches='tight', dpi=150); plt.close(fig); print('saved', FIG / name)


def table(summ):
    rows = ['\\begin{tabular}{llrcccccc}', '\\toprule', 'Tax & Group & Treated HHs & Soda oz & Own-zip3 share & Soda spend share & Sugar/1,000 kcal & HEI & UPF share \\\\', '\\midrule']
    for design, lab in [('philadelphia', 'Philadelphia 2017'), ('cities', 'Six cities 2015--18'), ('cook', 'Cook County 2017 (4 months)')]:
        for group, glab in [('treated', 'taxed'), ('adjacent', 'adjacent')]:
            s = summ[(summ.design == design) & (summ.group == group)].set_index('outcome')
            cells = [f'{s.loc[o, "post"]:.2f} ({s.loc[o, "post_se"]:.2f})' if o in s.index else '' for o in ['soda_oz', 'home_share', 'soda_share', 'sugar_per_1000cal_usda', 'hei_usda', 'upf_share']]
            cells[0] = f'{s.loc["soda_oz", "post"]:.0f} ({s.loc["soda_oz", "post_se"]:.0f})' if 'soda_oz' in s.index else ''
            n = int(s.n_treated.iloc[0]) if group == 'treated' else (int(s.n_adjacent.iloc[0]) if 'n_adjacent' in s.columns else None)
            rows.append(f'{lab if group == "treated" else ""} & {glab} & {f"{n:,}" if n is not None else ""} & ' + ' & '.join(cells) + ' \\\\')
    rows += ['\\bottomrule', '\\end{tabular}']
    (TAB / 'soda_tax.tex').write_text('\n'.join(rows) + '\n'); print('saved', TAB / 'soda_tax.tex')


coefs = pd.read_csv(IN / 'soda_tax_coefs.csv'); summ = pd.read_csv(IN / 'soda_tax_summary.csv')
fig_design(coefs, summ, 'philadelphia', OUT, 'soda_tax_philadelphia.png', 3)
fig_design(coefs, summ, 'cities', [OUT[0], OUT[3], OUT[4]], 'soda_tax_cities.png', 3)
table(summ)
print(summ[summ.group == 'treated'][['design', 'outcome', 'n_treated', 'post', 'post_se', 'pre', 'pre_se', 'p_pre']].round(3).to_string(index=False))
