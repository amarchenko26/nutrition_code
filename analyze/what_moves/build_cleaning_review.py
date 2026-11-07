"""Before and after the cleaning of 2026-11-05: national series and the main income estimates, for the Data cleaning
section of 0_main.tex (2026-11-06).

Before = nielsen_data/z_backup_2026-11-05 (panels of 2026-09-16; hei_usda_panel and the calorie panel of 2026-11-04, after the
fatty-acid fix and before the rest); after = interim/ and final/ today. All series are projection-weighted household means by panel year.
Reads interim/what_moves/cleaning_estimates.csv (build_cleaning_estimates.do) and nhanes_sugar.parquet.
Outputs: figs/what_moves/cleaning/before_after.png, tabs/what_moves/cleaning_estimates.tex; every series is printed.
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
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

B = data / 'z_backup_2026-11-05'
BLUE, RED, ORANGE, GREY = '#2c5f8a', '#a63d40', '#fc8d59', '#999999'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 26, 'xtick.labelsize': 21, 'ytick.labelsize': 21,
                     'legend.fontsize': 19, 'axes.spines.top': False, 'axes.spines.right': False})


def wm(df, c, w='projection_factor'):
    x = df[['panel_year', c, w]].dropna()
    return x.groupby('panel_year').apply(lambda s: np.average(s[c], weights=s[w]))


def nova(path, weights):
    c = pd.read_parquet(path)
    c = c.merge(weights, on=['household_code', 'panel_year'], how='inner')
    c = c[c['ae_complete'] == 1].copy()
    c['upf'] = 100 * c['cals_nova4'] / c[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    c['cal_all'] = (c['cals_all'] / c['ae_total'] / 365).clip(upper=4000)
    c['cal_lab'] = (c['cals_matched'] / c['ae_total'] / 365).clip(upper=4000)
    c['lab_share'] = 100 * c['cals_matched'] / c['cals_all']
    c['upf_own'] = 100 * c['cals_ownnova4'] / c[['cals_ownnova1', 'cals_ownnova2', 'cals_ownnova3', 'cals_ownnova4']].sum(axis=1) if 'cals_ownnova4' in c else np.nan
    return c


def main():
    pcols = ['household_code', 'panel_year', 'hi', 'sugar_per_1000cal', 'produce', 'projection_factor']
    ucols = ['household_code', 'panel_year', 'hi_usda', 'sugar_per_1000cal_usda', 'produce_usda']
    new = pd.read_parquet(idir / 'panel_dataset' / 'panel_hh_year.parquet', columns=pcols)
    old = pd.read_parquet(B / 'interim_panel_dataset' / 'panel_hh_year.parquet', columns=pcols)
    newu = pd.read_parquet(idir / 'panel_dataset' / 'hi_usda_panel.parquet', columns=ucols).merge(new[['household_code', 'panel_year', 'projection_factor', 'hi']], on=['household_code', 'panel_year'])
    oldu = pd.read_parquet(B / 'interim_panel_dataset' / 'hi_usda_panel.parquet', columns=ucols).merge(old[['household_code', 'panel_year', 'projection_factor', 'hi']], on=['household_code', 'panel_year'])
    newe = pd.read_parquet(idir / 'panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda', 'hei_c_fattyacid', 'projection_factor'])
    nov4 = pd.read_parquet(B / 'interim_panel_dataset' / 'hei_usda_panel.parquet', columns=['household_code', 'panel_year', 'hei_usda', 'hei_c_fattyacid', 'projection_factor'])
    sep, _ = pyreadstat.read_dta(str(B / 'final' / 'labor_supply_reg_data.dta'), usecols=['year', 'hei_usda', 'projection_factor'])
    sep = sep.rename(columns={'year': 'panel_year'}); sep['panel_year'] = sep['panel_year'].astype(int)
    w_new = new[['household_code', 'panel_year', 'projection_factor']]
    na = nova(idir / 'health' / 'nova_calorie_panel.parquet', w_new)
    nb = nova(B / 'interim_health' / 'nova_calorie_panel.parquet', w_new)
    nh = pd.read_parquet(idir / 'what_moves' / 'nhanes_sugar.parquet')

    s = pd.DataFrame({'hei_sep': wm(sep, 'hei_usda'), 'hei_nov4': wm(nov4, 'hei_usda'), 'hei_after': wm(newe, 'hei_usda'),
                      'fa_nov4': wm(nov4, 'hei_c_fattyacid'), 'fa_after': wm(newe, 'hei_c_fattyacid'),
                      'sug_syn_before': wm(old, 'sugar_per_1000cal'), 'sug_syn_after': wm(new, 'sugar_per_1000cal'),
                      'sug_usda_before': wm(oldu, 'sugar_per_1000cal_usda'), 'sug_usda_after': wm(newu, 'sugar_per_1000cal_usda'),
                      'upf_before': wm(nb, 'upf'), 'upf_after': wm(na, 'upf'), 'upf_own_after': wm(na, 'upf_own'),
                      'cal_before': wm(nb, 'cal_all'), 'cal_lab_before': wm(nb, 'cal_lab'), 'cal_after': wm(na, 'cal_all'),
                      'lab_share_before': wm(nb, 'lab_share'),
                      'prod_before': 100 * wm(old, 'produce'), 'prod_after': 100 * wm(new, 'produce'),
                      'corr_before': oldu.groupby('panel_year').apply(lambda x: x['hi'].corr(x['hi_usda'])),
                      'corr_after': newu.groupby('panel_year').apply(lambda x: x['hi'].corr(x['hi_usda'])),
                      'sd_hiu_before': oldu.groupby('panel_year')['hi_usda'].std(), 'sd_hiu_after': newu.groupby('panel_year')['hi_usda'].std()})
    pd.set_option('display.width', 250); pd.set_option('display.max_columns', 40)
    print(s.round(2).T.to_string())
    print(nh.round(2).to_string())

    fig, ax = plt.subplots(2, 3, figsize=(28, 16))
    fig.subplots_adjust(wspace=0.24, hspace=0.34)
    y = s.index

    def line(a, col, c, lab, ls='-', lw=3.5):
        a.plot(y, s[col], color=c, ls=ls, lw=lw, label=lab)

    a = ax[0, 0]
    line(a, 'hei_sep', RED, 'Before', '--'); line(a, 'hei_nov4', ORANGE, 'Fatty-acid fix only', '--'); line(a, 'hei_after', BLUE, 'After')
    a.set_title('HEI (points)'); a.set_ylim(44, 51); a.legend(frameon=False, loc='lower right')
    a = ax[0, 1]
    line(a, 'sug_syn_before', RED, 'Syndigo, before', '--'); line(a, 'sug_usda_before', ORANGE, 'USDA, before', '--')
    line(a, 'sug_syn_after', BLUE, 'Syndigo, after'); line(a, 'sug_usda_after', GREY, 'USDA, after')
    ycol = [c for c in nh.columns if 'year' in c.lower()][0]
    a.plot(nh[ycol], nh['store_sugar_1000'] if 'store_sugar_1000' in nh else nh.iloc[:, 1], color='#555555', ls='none', marker='o', ms=11, label='NHANES, store-bought')
    a.set_title('Sugar (g per 1,000 kcal)'); a.legend(frameon=False, loc='lower left')
    a = ax[0, 2]
    line(a, 'upf_before', RED, 'Before', '--'); line(a, 'upf_after', BLUE, 'After'); line(a, 'upf_own_after', ORANGE, 'After, own ingredient lists only', ':')
    a.set_title('Ultra-processed share (%)'); a.set_ylim(66, 82); a.legend(frameon=False, loc='lower left')
    a = ax[1, 0]
    line(a, 'cal_lab_before', ORANGE, 'Before, labelled products only', '--'); line(a, 'cal_before', RED, 'Before, all purchases', '--'); line(a, 'cal_after', BLUE, 'After')
    a.set_title('Calories per adult eq. per day'); a.set_ylim(100, 1400); a.legend(frameon=False, loc='lower right')
    a = ax[1, 1]
    line(a, 'prod_before', RED, 'Before', '--'); line(a, 'prod_after', BLUE, 'After')
    a.set_title('Produce share of calories (%)'); a.set_ylim(4, 8.5); a.legend(frameon=False, loc='center right')
    a = ax[1, 2]
    line(a, 'corr_before', RED, 'Before', '--'); line(a, 'corr_after', BLUE, 'After')
    a.set_title('Correlation of the household score,\nSyndigo labels with USDA labels'); a.set_ylim(0, 1.05); a.legend(frameon=False, loc='lower right')
    for a in ax.ravel():
        a.set_xlabel('Panel year'); a.set_xticks([2004, 2008, 2012, 2016, 2020])
    out = fdir / 'what_moves' / 'cleaning' / 'before_after.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=110)
    print(f'saved {out}')

    # ---- estimates before and after
    e = pd.read_csv(idir / 'what_moves' / 'cleaning_estimates.csv', na_values='.')
    rows = [('main', 'Income IV, change the year after'), ('before', 'Same income change, purchases $t-3$ to $t-2$'),
            ('ols', 'OLS, same regression and sample as the first row'), ('unemp', 'Income IV, not employed at $t-1$, under 67')]

    def stars(b, se):
        p = 2 * (1 - __import__('scipy.stats').stats.norm.cdf(abs(b / se)))
        return '***' if p < .01 else '**' if p < .05 else '*' if p < .10 else ''

    def c(ver, out_, row):
        x = e[(e['version'] == ver) & (e['outcome'] == out_) & (e['row'] == row)]
        if not len(x):
            return ''
        b, se = float(x['b'].iloc[0]), float(x['se'].iloc[0])
        return f'{b:.3f}{stars(b, se)} ({se:.3f})'
    tex = [r'\begin{tabular}{lcccccc}', r'\toprule', r' & \multicolumn{2}{c}{Simple index, Syndigo} & \multicolumn{2}{c}{Simple index, USDA} & \multicolumn{2}{c}{HEI} \\',
           r'\cmidrule(lr){2-3} \cmidrule(lr){4-5} \cmidrule(lr){6-7}', r' & Before & After & Before & After & Before & After \\', r'\midrule']
    for r, lab in rows:
        tex.append(lab + ' & ' + ' & '.join(c(v, o, r) for o in ('hi', 'hi_usda', 'hei_usda') for v in ('before', 'after')) + r' \\')
    f = lambda v, o: e[(e['version'] == v) & (e['outcome'] == o) & (e['row'] == 'main')]['f'].iloc[0]
    tex += [r'\midrule', 'First-stage F, first row & ' + ' & '.join(f'{f(v, o):.0f}' for o in ('hi', 'hi_usda', 'hei_usda') for v in ('before', 'after')) + r' \\', r'\bottomrule', r'\end{tabular}']
    (tdir / 'what_moves' / 'cleaning_estimates.tex').write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(e[e['outcome'] == 'hei_nov4'].round(4).to_string(index=False))
    print('saved', tdir / 'what_moves' / 'cleaning_estimates.tex')


if __name__ == '__main__':
    main()
