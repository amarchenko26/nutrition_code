"""
figure_hei_gradients_income_education.py -- the HEI-income gradient with and without
education held fixed, and within each education bin. For notes/obesity.tex.
Reads interim/movers/occupation_movers_panel.parquet (household-years, geographic
non-movers 2004-2020, USDA-label HEI; built by analyze/movers/1_).

Left: HEI and household income (2013 dollars) both residualized on 5-year age-bin, kids and
year dummies (means added back), averaged within twenty income bins; the second series adds
education-bin dummies to the controls, so it is the income gradient among households with
the same education. Slopes from the household-level regressions, SE clustered on household.
Right: the same binscatter within each education bin (ten bins each), controls as in the
first series.
Output: figs/health/hei_gradients_income_education.png
"""
import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
FIG = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/Apps/Overleaf/nutrition/figs') / 'health'
FIG.mkdir(parents=True, exist_ok=True)
BLUE, RED, GREY = '#2c5f8a', '#a63d40', '#7a7a7a'
EDU_COL = {1: '#d73027', 2: '#fc8d59', 3: '#3b6b9a', 4: '#1a5e33'}
EDU = {1: 'Under 12 years', 2: '12 years', 3: '13-15 years', 4: '16+ years'}
plt.rcParams.update({'font.size': 13, 'axes.labelsize': 14, 'axes.titlesize': 13, 'legend.fontsize': 10.5,
                     'axes.spines.top': False, 'axes.spines.right': False})


def dummies(sub, cols, w):
    C = pd.concat([pd.get_dummies(sub[c].astype(int), prefix=c, drop_first=True).astype(float) for c in cols], axis=1)
    return C - np.average(C.values, weights=w, axis=0)


def resid(y, X, w):
    sw = np.sqrt(w)
    beta = np.linalg.lstsq(X * sw[:, None], y * sw, rcond=None)[0]
    return y - X @ beta + np.average(y, weights=w)


def binscatter(sub, controls, nq):
    w = sub['projection_factor'].values
    X = np.column_stack([np.ones(len(sub)), dummies(sub, controls, w).values])
    y = resid(sub['hei_usda'].values.astype(float), X, w)
    x = resid(sub['real_income'].values.astype(float) * 10, X, w)        # $000s
    edges = np.percentile(x, np.linspace(0, 100, nq + 1)); edges[-1] += 1
    b = np.digitize(x, edges)
    pts = pd.DataFrame([(np.average(x[b == i], weights=w[b == i]), np.average(y[b == i], weights=w[b == i])) for i in range(1, nq + 1)], columns=['x', 'y'])
    fit = sm.WLS(y, sm.add_constant(x / 10), weights=w).fit(cov_type='cluster', cov_kwds={'groups': sub['hhid'].values})
    return pts, fit.params[1], fit.bse[1], fit.params[0]


def main():
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet',
                         columns=['hhid', 'year', 'analysis', 'hei_usda', 'real_income', 'educ_bin', 'age5', 'kids', 'projection_factor'])
    sub = df[(df['analysis'] == 1) & df['hei_usda'].notna() & df['real_income'].notna() & df['educ_bin'].notna()].copy()
    p1, p99 = sub['real_income'].quantile([0.01, 0.99])
    sub = sub[sub['real_income'].between(p1, p99)].reset_index(drop=True)
    print(f'{len(sub):,} household-years')

    fig, axes = plt.subplots(1, 2, figsize=(17, 6.5), sharey=True)
    ax = axes[0]
    for controls, color, lab in [(['age5', 'kids', 'year'], BLUE, 'Controls: age bins, kids, year'),
                                 (['age5', 'kids', 'year', 'educ_bin'], RED, '+ education bins')]:
        pts, b, se, a = binscatter(sub, controls, 20)
        ax.scatter(pts['x'], pts['y'], s=50, color=color, zorder=4, label=f'{lab}: {b:.2f} HEI points per \\$10k (SE {se:.2f})')
        xx = np.linspace(pts['x'].min(), pts['x'].max(), 10)
        ax.plot(xx, a + b * xx / 10, color=color, lw=2)
        print(f'{lab}: slope {b:.3f} (SE {se:.3f}); bottom bin {pts.y.iloc[0]:.2f}, top bin {pts.y.iloc[-1]:.2f}')
    ax.set_xlabel('Household income (\\$000s, 2013\\$)')
    ax.set_ylabel('HEI (0–100), net of controls')
    ax.set_title('The income gradient, with and without education held fixed')
    ax.legend(loc='upper left', frameon=False)
    ax = axes[1]
    for e in [1, 2, 3, 4]:
        s = sub[sub['educ_bin'] == e].reset_index(drop=True)
        pts, b, se, a = binscatter(s, ['age5', 'kids', 'year'], 10)
        ax.plot(pts['x'], pts['y'], color=EDU_COL[e], lw=2, marker='o', ms=5, label=f'{EDU[e]} (n = {len(s):,}): {b:.2f} per \\$10k (SE {se:.2f})')
        print(f'education bin {e}: slope {b:.3f} (SE {se:.3f}), n = {len(s):,}')
    ax.set_xlabel('Household income (\\$000s, 2013\\$)')
    ax.set_title('The income gradient within each education bin')
    ax.legend(loc='upper left', frameon=False)
    fig.savefig(FIG / 'hei_gradients_income_education.png', bbox_inches='tight', dpi=150)
    plt.close(fig)
    print(f'saved {FIG / "hei_gradients_income_education.png"}')


if __name__ == '__main__':
    main()
