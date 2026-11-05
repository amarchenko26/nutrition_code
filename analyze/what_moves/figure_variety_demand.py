"""Exhibits for the Jaravel-by-healthiness regressions (2026-11-05). Reads interim/what_moves/variety_demand_coefs_<part>.csv
(build_variety_demand_regs.do) and variety_buying_coefs.csv (build_variety_buying_regs.do).
All demand coefficients are shown per 10 log points (about 10 percent) of county growth: percent for log outcomes,
percentage points for shares.
Outputs: figs/what_moves/variety/demand_shock_by_group.png, tabs/what_moves/variety_demand.tex, tabs/what_moves/variety_buying.tex
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY, ORANGE = '#2c5f8a', '#a63d40', '#999999', '#fc8d59'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 25, 'xtick.labelsize': 21, 'ytick.labelsize': 22,
                     'legend.fontsize': 21, 'axes.spines.top': False, 'axes.spines.right': False})
OUTCOMES = [('dlog_spend', 'Spending'), ('dlog_real', 'Real spending'), ('dlog_nupcs', 'UPCs sold'), ('d_new_county', 'New-to-county UPC share'),
            ('d_ssnp', 'New-to-nation spending share'), ('dlog_ces', 'Continuing-product price'), ('dlog_va', 'Variety-adjusted price')]
PLOT_OUT = [('dlog_spend', 'Spend'), ('dlog_nupcs', 'UPCs'), ('d_new_county', 'New to\ncounty'), ('d_ssnp', 'New to\nnation'), ('dlog_ces', 'Cont.\nprice'), ('dlog_va', 'Var.-adj.\nprice')]
GL = {'hi_group': ('Healthiness', {1: ('Unhealthy', RED), 2: ('Middle', ORANGE), 3: ('Healthy', BLUE)}),
      'upf_group': ('Ultra-processed', {2: ('Mostly UPF', RED), 1: ('Mostly not', BLUE)}),
      'prep_group': ('Preparation', {1: ('Open and eat', RED), 2: ('Little prep', ORANGE), 3: ('Cooking', BLUE)})}


def load():
    fs = glob.glob(str(idir / 'what_moves' / 'variety_demand_coefs_*.csv'))
    d = pd.concat([pd.read_csv(f) for f in fs], ignore_index=True)
    d['term'] = d['term'].astype(str)
    d['F'] = pd.to_numeric(d['F'], errors='coerce')
    # each observation is one county x module, so two-way variance = V(county) + V(module) - V(robust), which can be negative
    # (reghdfe shows SE = 0) or too small: report the largest of the two-way, county and module SEs (build_variety_demand_se.do)
    se = pd.concat([pd.read_csv(f) for f in glob.glob(str(idir / 'what_moves' / 'variety_demand_se_*.csv'))], ignore_index=True)
    se['term'] = se['term'].astype(str)
    d = d.merge(se, on=['spec', 'shock', 'grouping', 'term', 'outcome'], how='left')
    d['se_twoway'] = d['se']
    d['se'] = d[['se_twoway', 'se_county', 'se_module']].max(axis=1)
    rep = d[d['se_county'].notna()]
    print(f"{len(rep)} rows with one-way SEs; two-way below the larger one-way in {(rep.se_twoway < rep[['se_county', 'se_module']].max(axis=1)).sum()}, two-way = 0 in {(rep.se_twoway == 0).sum()}")
    miss = d[d['se_county'].isna() & d['spec'].isin(['S0', 'S1', 'S4', 'S2', 'S1g', 'S3g']) & d['shock'].isin(['wb', 'bk', 'hat', 'inc'])]
    print('no one-way SEs, not used in the exhibits:', miss.groupby(['spec', 'shock']).size().to_dict())
    return d


def figure(d):
    """Both rows have county and module fixed effects. Top: gap between module groups in the same county (base group = 0).
    Bottom: county earnings growth x richer buyers, separately by group. The per-group slopes without county fixed effects are not
    shown: they pick up retailers entering and leaving the RMS sample in a county."""
    fig, axes = plt.subplots(2, 3, figsize=(32, 17))
    fig.subplots_adjust(wspace=0.2, hspace=0.42)
    gaps = {'hi_group': [(1, 'Unhealthy vs healthy', RED), (2, 'Middle vs healthy', ORANGE)], 'upf_group': [(2, 'Mostly UPF vs mostly not', RED)],
            'prep_group': [(1, 'Open and eat vs cooking', RED), (2, 'Little prep vs cooking', ORANGE)]}
    def dots(ax, spec, g, terms):
        n = len(terms)
        for q, (k, lab, col) in enumerate(terms):
            xs, ys, es = [], [], []
            for o, (y, _) in enumerate(PLOT_OUT):
                r = d[(d.spec == spec) & (d.shock == 'wb') & (d.grouping == g) & (d.term == str(k)) & (d.outcome == y)]
                if r.empty:
                    continue
                xs.append(o + (q - (n - 1) / 2) * 0.22); ys.append(10 * r.b.iloc[0]); es.append(19.6 * r.se.iloc[0])
            ax.errorbar(xs, ys, yerr=es, fmt='o', color=col, ms=11, capsize=5, lw=3, label=lab)
        ax.axhline(0, color=GREY, lw=1.5, ls='--')
        ax.set_xticks(range(len(PLOT_OUT))); ax.set_xticklabels([l for _, l in PLOT_OUT])
        ax.legend(frameon=False, loc='best')
    for j, (g, (gname, gl)) in enumerate(GL.items()):
        dots(axes[0, j], 'S2', g, gaps[g])
        axes[0, j].set_title(f'{gname}: gap in the same county', fontsize=23)
        dots(axes[1, j], 'S1g', g, [(k, v[0], v[1]) for k, v in gl.items()])
        axes[1, j].set_title(f'{gname}: growth x richer buyers', fontsize=23)
    axes[0, 0].set_ylabel('% or pct. pts per 10%'); axes[1, 0].set_ylabel('% or pct. pts per 10%')
    out = fdir / 'what_moves' / 'variety' / 'demand_shock_by_group.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=110)
    print(f'saved {out}')


def table(d):
    cols = [y for y, _ in OUTCOMES]
    head = ['\\begin{tabular}{l' + 'c' * len(cols) + '}', '\\toprule',
            ' & Spending & Real & UPCs & New to & New to & Continuing- & Variety- \\\\',
            ' & & spending & sold & county & nation & product price & adj. price \\\\', '\\midrule']
    def row(lab, sel, F=False):
        z = d.loc[sel].set_index('outcome')
        z = z[~z.index.duplicated()]
        if z.empty:
            return []
        l1 = lab + ' & ' + ' & '.join(f"{10 * z.b[y]:.2f}" if y in z.index else '' for y in cols) + ' \\\\'
        l2 = ' & ' + ' & '.join(f"({10 * z.se[y]:.2f})" if y in z.index else '' for y in cols) + ' \\\\'
        return [l1, l2]
    S = lambda spec, sh: (d.spec == spec) & (d.shock == sh)
    L = list(head)
    L += ['\\multicolumn{8}{l}{\\textit{A. County growth, same module in other counties (no county FE, so retailers entering or leaving RMS enter)}} \\\\']
    for sh, lab in [('wb', 'Private wage bill (QCEW)'), ('bk', 'Wage bill predicted from industry mix'), ('hat', 'Homescan predicted income'), ('inc', 'Homescan income')]:
        L += row(lab, S('S0', sh))
    L += ['\\midrule', '\\multicolumn{8}{l}{\\textit{B. County growth $\\times$ module bought by richer households (1 SD), county and module FE}} \\\\']
    for sh, lab in [('wb', 'Private wage bill (QCEW)'), ('bk', 'Wage bill predicted from industry mix'), ('hat', 'Homescan predicted income'), ('inc', 'Homescan income')]:
        L += row(lab, S('S1', sh))
    iv = d[d.spec == 'IV1']
    if len(iv):
        L += row(f"Wage bill instrumented (F = {iv.F.iloc[0]:.0f})", d.spec == 'IV1')
    L += ['\\midrule', '\\multicolumn{8}{l}{\\textit{C. County growth $\\times$ module income elasticity (pipeline), county and module FE}} \\\\']
    for sh, lab in [('wb', 'Private wage bill (QCEW)'), ('hat', 'Homescan predicted income')]:
        L += row(lab, S('S4', sh))
    L += ['\\midrule', '\\multicolumn{8}{l}{\\textit{D. Gap between module groups in the same county (private wage bill), county and module FE}} \\\\']
    for g, k, lab in [('hi_group', 1, 'Unhealthy vs healthy'), ('hi_group', 2, 'Middle vs healthy'), ('upf_group', 2, 'Mostly UPF vs mostly not'), ('prep_group', 1, 'Open and eat vs cooking'), ('prep_group', 2, 'Little prep vs cooking')]:
        L += row(lab, S('S2', 'wb') & (d.grouping == g) & (d.term == str(k)))
    L += ['\\midrule', '\\multicolumn{8}{l}{\\textit{E. The same gaps, wage bill predicted from industry mix}} \\\\']
    for g, k, lab in [('hi_group', 1, 'Unhealthy vs healthy'), ('hi_group', 2, 'Middle vs healthy'), ('upf_group', 2, 'Mostly UPF vs mostly not'), ('prep_group', 1, 'Open and eat vs cooking'), ('prep_group', 2, 'Little prep vs cooking')]:
        L += row(lab, S('S2', 'bk') & (d.grouping == g) & (d.term == str(k)))
    L += ['\\bottomrule', '\\end{tabular}']
    out = tdir / 'what_moves' / 'variety_demand.tex'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text('\n'.join(L) + '\n')
    print(f'saved {out}')


def table_buying():
    f = idir / 'what_moves' / 'variety_buying_coefs.csv'
    if not f.exists():
        return
    b = pd.read_csv(f); b['term'] = b['term'].astype(str)
    print(b.round(3).to_string(index=False))
    L = ['\\begin{tabular}{lccc}', '\\toprule', ' & Log change in spending & Log change in spending & Log change in spending \\\\',
         ' & on log change in UPCs & on log change in & on log change in UPCs, \\\\', ' & (OLS) & variety-adj. price (OLS) & launches instrument \\\\', '\\midrule']
    rows = [('all', 'all', 'All modules')]
    for g, (_, gl) in GL.items():
        rows += [(g, str(k), v[0]) for k, v in sorted(gl.items())]
    for g, k, lab in rows:
        cells, ses = [], []
        for spec, x in [('OLS', 'dlog_nupcs'), ('OLS', 'dlog_va'), ('IV_launch', 'dlog_nupcs')]:
            r = b[(b.spec == spec) & (b.regressor == x) & (b.grouping == g) & (b.term == k)]
            cells.append(f'{r.b.iloc[0]:.3f}' if len(r) else ''); ses.append(f'({r.se.iloc[0]:.3f})' if len(r) else '')
        L += [f'{lab} & ' + ' & '.join(cells) + ' \\\\', ' & ' + ' & '.join(ses) + ' \\\\']
    iv = b[(b.spec == 'IV_launch') & (b.grouping == 'all')]
    if len(iv):
        L += ['\\midrule', f'First-stage F, all modules & & & {iv.F.iloc[0]:.0f} \\\\', f'County-modules & {int(b[(b.spec == "OLS") & (b.regressor == "dlog_nupcs") & (b.grouping == "all")].N.iloc[0]):,} & {int(b[(b.spec == "OLS") & (b.regressor == "dlog_va") & (b.grouping == "all")].N.iloc[0]):,} & {int(iv.N.iloc[0]):,} \\\\']
    L += ['\\bottomrule', '\\end{tabular}']
    out = tdir / 'what_moves' / 'variety_buying.tex'
    out.write_text('\n'.join(L) + '\n')
    print(f'saved {out}')


def main():
    d = load()
    figure(d)
    table(d)
    table_buying()


if __name__ == '__main__':
    main()
