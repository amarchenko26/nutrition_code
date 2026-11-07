"""The income-nutrition gradients of fig1a-d with the normal controls, then holding education fixed, and the education-nutrition
gradients with the normal controls, then holding income fixed (2026-11-06, for 0_main.tex).

Sample and normal controls as analyze/hi/usda/replicate_figure1_usda.py: household-years 2004-2020, income trimmed at the 1st and 99th
percentiles, age-bin, year and household-size controls, projection weights. Outcomes: sugar, whole-grain share of bread calories and
fruit and vegetable share of calories (USDA labels), HEI in standard deviations.

figs/gradients/income_gradients_education.png: 15-bin binscatter on income, normal controls and with education bins added.
figs/gradients/education_gradients_income.png: mean by education bin (the heads' average years of schooling), normal controls and with
twenty income bins added. Slopes and group means are printed, standard errors clustered on household.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd
import pyreadstat
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

FIG = fdir / 'gradients'
BLUE, ORANGE, GREY = '#2c5f8a', '#fc8d59', '#999999'
YEARS = range(2004, 2021)
NQ = 15
OUTCOMES = [('sugar', 'Sugar (g per 1,000 kcal)'), ('whole', 'Whole grain (% of bread)'), ('produce', 'Produce (% of calories)'), ('hei_z', 'HEI (standard deviations)')]
EDUC = ['Under 12', '12', '13-15', '16+']
plt.rcParams.update({'font.size': 17, 'axes.labelsize': 19, 'xtick.labelsize': 16, 'ytick.labelsize': 16, 'legend.fontsize': 15,
                     'axes.spines.top': False, 'axes.spines.right': False})


def load():
    p = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=['household_code', 'panel_year', 'hh_real_income_avg', 'avg_age_hh_head', 'household_size', 'projection_factor'])
    u = pd.read_parquet(idir / 'panel_dataset' / 'hi_usda_panel.parquet', columns=['household_code', 'panel_year', 'sugar_per_1000cal_usda', 'produce_usda', 'whole_usda'])
    h = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda'])
    r, _ = pyreadstat.read_dta(str(gdir / 'labor_supply_reg_data.dta'), usecols=['hhid', 'year', 'hh_avg_yrsofschool'])
    r = r.rename(columns={'hhid': 'household_code', 'year': 'panel_year'})
    r['household_code'] = r['household_code'].astype('int64'); r['panel_year'] = r['panel_year'].astype(int)
    d = p[p['panel_year'].isin(YEARS)].merge(u, on=['household_code', 'panel_year']).merge(h, on=['household_code', 'panel_year']).merge(r, on=['household_code', 'panel_year'])
    lo, hi = d['hh_real_income_avg'].quantile([0.01, 0.99])
    d = d[d['hh_real_income_avg'].between(lo, hi)].dropna(subset=['hh_real_income_avg', 'avg_age_hh_head', 'household_size', 'projection_factor', 'hh_avg_yrsofschool']).copy()
    d['sugar'] = d['sugar_per_1000cal_usda']
    d['whole'] = 100 * d['whole_usda']
    d['produce'] = 100 * d['produce_usda']
    w = d['projection_factor']
    m = np.average(d['hei_usda'], weights=w)
    d['hei_z'] = (d['hei_usda'] - m) / np.sqrt(np.average((d['hei_usda'] - m) ** 2, weights=w))
    d['educ_b'] = pd.cut(d['hh_avg_yrsofschool'], [0, 11.9, 12.9, 15.9, np.inf], labels=False)
    d['inc_v'] = pd.qcut(d['hh_real_income_avg'].rank(method='first'), 20, labels=False)
    d['age_b'] = pd.cut(d['avg_age_hh_head'], bins=[0, 35, 45, 55, 65, 100], labels=False)
    d = d.dropna(subset=['educ_b', 'age_b']).reset_index(drop=True)
    print(f"{len(d):,} household-years, {d['household_code'].nunique():,} households; shares by education bin {d.groupby('educ_b')['projection_factor'].sum().div(w.sum()).round(3).tolist()}")
    return d


def dummies(s, prefix):
    return pd.get_dummies(s.astype(int), prefix=prefix, drop_first=True, dtype=float)


def resid(y, X, w):
    sw = np.sqrt(w)
    Xc = np.column_stack([np.ones(len(y)), X])
    b = np.linalg.lstsq(Xc * sw[:, None], y * sw, rcond=None)[0]
    return y - Xc @ b


def binscatter(d, y, C):
    ok = d[y].notna().values
    w = d['projection_factor'].values[ok].astype(float)
    Y, X, Cc = d[y].values[ok].astype(float), d['hh_real_income_avg'].values[ok].astype(float), C.values[ok]
    ry = resid(Y, Cc, w) + np.average(Y, weights=w)
    rx = resid(X, Cc, w) + np.average(X, weights=w)
    edges = np.percentile(rx, np.linspace(0, 100, NQ + 1)); edges[-1] += 1
    b = np.digitize(rx, edges)
    return np.array([np.average(rx[b == i], weights=w[b == i]) for i in range(1, NQ + 1)]), np.array([np.average(ry[b == i], weights=w[b == i]) for i in range(1, NQ + 1)])


def fit(d, y, X):
    ok = d[y].notna()
    return sm.WLS(d.loc[ok, y].astype(float), sm.add_constant(X[ok]), weights=d.loc[ok, 'projection_factor']).fit(cov_type='cluster', cov_kwds={'groups': d.loc[ok, 'household_code'].values})


def finish(fig, axes, name):
    h, l = axes.flat[0].get_legend_handles_labels()
    fig.legend(h, l, loc='lower center', ncol=2, frameon=False, fontsize=18, bbox_to_anchor=(0.5, 0.0))
    fig.savefig(FIG / name, dpi=150, bbox_inches='tight'); plt.close(fig)
    print('saved', name)


def income_gradients(d, base, edu):
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.subplots_adjust(wspace=0.25, hspace=0.27, bottom=0.12)
    for ax, (y, lab) in zip(axes.flat, OUTCOMES):
        slopes = []
        for C, col, mark, name in [(base, BLUE, 'o', 'Age, year, household size'), (pd.concat([base, edu], axis=1), ORANGE, 'D', 'Adding education')]:
            xb, yb = binscatter(d, y, C)
            r = fit(d, y, pd.concat([d[['hh_real_income_avg']].astype(float) / 10, C], axis=1))
            b, se = r.params['hh_real_income_avg'], r.bse['hh_real_income_avg']
            print(f'{y} on income ($10k), {name}: {b:.4f} ({se:.4f}) N {int(r.nobs):,}')
            slopes.append(b)
            ax.scatter(xb, yb, color=col, s=80, marker=mark, zorder=5, edgecolors='white', linewidth=0.5, label=name)
        down = slopes[0] < 0
        ax.text(0.97 if down else 0.03, 0.96, f'Per \\$10k: {slopes[0]:+.3f}, {slopes[1]:+.3f}', transform=ax.transAxes, ha='right' if down else 'left', va='top', fontsize=17)
        ax.set_xlabel('Income (\\$000s)'); ax.set_ylabel(lab); ax.grid(True, alpha=0.3)
    finish(fig, axes, 'income_gradients_education.png')


def education_gradients(d, base, inc):
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.subplots_adjust(wspace=0.25, hspace=0.27, bottom=0.12)
    E = pd.get_dummies(d['educ_b'].astype(int), prefix='e', dtype=float)
    w = d['projection_factor']
    for ax, (y, lab) in zip(axes.flat, OUTCOMES):
        gaps = []
        for C, col, mark, name, off in [(base, BLUE, 'o', 'Age, year, household size', -0.07), (pd.concat([base, inc], axis=1), ORANGE, 'D', 'Adding income', 0.07)]:
            ok = d[y].notna()
            # controls in deviations from their means, no constant: each coefficient is the group's mean at the sample's average controls
            Cd = C[ok] - np.average(C[ok].values, axis=0, weights=w[ok])
            X = pd.concat([E[ok], Cd], axis=1)
            r = sm.WLS(d.loc[ok, y].astype(float), X, weights=w[ok]).fit(cov_type='cluster', cov_kwds={'groups': d.loc[ok, 'household_code'].values})
            level = np.array([r.params[f'e_{i}'] for i in range(4)])
            ses = np.array([r.bse[f'e_{i}'] for i in range(4)])
            V = r.cov_params()
            gap, gse = level[3] - level[0], np.sqrt(V.loc['e_3', 'e_3'] + V.loc['e_0', 'e_0'] - 2 * V.loc['e_3', 'e_0'])
            print(f'{y} by education, {name}: ' + ', '.join(f'{EDUC[i]} {level[i]:.2f}' for i in range(4)) + f'; 16+ minus under 12 {gap:.3f} ({gse:.3f})')
            gaps.append(gap)
            ax.errorbar(np.arange(4) + off, level, yerr=1.96 * ses, color=col, marker=mark, ms=10, lw=0, elinewidth=2, capsize=4, label=name)
        down = gaps[0] < 0
        ax.text(0.97 if down else 0.03, 0.96, f'16+ minus under 12: {gaps[0]:+.2f}, {gaps[1]:+.2f}', transform=ax.transAxes, ha='right' if down else 'left', va='top', fontsize=17)
        ax.set_xticks(range(4)); ax.set_xticklabels(EDUC); ax.set_xlabel("Heads' average years of schooling"); ax.set_ylabel(lab); ax.grid(True, alpha=0.3)
    finish(fig, axes, 'education_gradients_income.png')


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    d = load()
    base = pd.concat([dummies(d['age_b'], 'a'), dummies(d['panel_year'], 'y'), d[['household_size']].astype(float)], axis=1)
    income_gradients(d, base, dummies(d['educ_b'], 'e'))
    education_gradients(d, base, dummies(d['inc_v'], 'v'))


if __name__ == '__main__':
    main()
