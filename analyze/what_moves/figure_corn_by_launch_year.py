"""Ingredients of food products by the year they first sold (2026-11-05, Anya: do firms use ingredients like corn in more things?).

interim/usda_nielsen_merged/usda_nova.parquet holds ingredient text per UPC from the USDA Branded file, a 2024 snapshot:
a product launched in 2008 appears only if it survived to the snapshot, with its 2024 recipe. So this is the composition of
SURVIVING products by launch year, not a time series of recipes; the coverage line shows how selected each cohort is.
Launch year: first_year_national (interim/what_moves/upc_first_year_national.parquet; 2007 = on the shelf in 2007).
Flags from the ingredient text: corn-derived = corn syrup, high fructose corn syrup, corn starch, modified corn starch,
dextrose, maltodextrin, corn oil (corn flour, corn meal, whole corn excluded: these are the cheap processing ingredients);
hfcs = high fructose corn syrup; sugar = cane/beet sugar word; soy oil = soybean oil.
Output: figs/what_moves/variety/ingredients_by_launch_year.png, tabs/what_moves/ingredients_by_group.tex; prints
within-module slopes on launch year (module fixed effects, clustered on module).
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
import statsmodels.formula.api as smf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BLUE, RED, GREY, ORANGE, LGREY = '#2c5f8a', '#a63d40', '#999999', '#fc8d59', '#cccccc'
plt.rcParams.update({'font.size': 24, 'axes.labelsize': 24, 'axes.titlesize': 25, 'xtick.labelsize': 22, 'ytick.labelsize': 22,
                     'legend.fontsize': 21, 'axes.spines.top': False, 'axes.spines.right': False})
CORN = r'CORN SYRUP|HIGH FRUCTOSE|HIGH-FRUCTOSE|HFCS|CORN STARCH|CORNSTARCH|MODIFIED CORN|DEXTROSE|MALTODEXTRIN|CORN OIL'
HFCS = r'HIGH FRUCTOSE|HIGH-FRUCTOSE|HFCS'
SUGAR = r'(?<![A-Z])SUGAR(?![A-Z])'
SOY = r'SOYBEAN OIL|SOY OIL'


def main():
    fy = pd.read_parquet(idir / 'what_moves' / 'upc_first_year_national.parquet')
    groups = pd.read_parquet(idir / 'what_moves' / 'module_groups.parquet', columns=['product_module_code', 'hi_group', 'upf_group', 'prep_group', 'upf_share', 'product_module_descr'])
    fy = fy[fy['product_module_code'].isin(groups['product_module_code'])].drop_duplicates('upc').copy()
    fy['upc13'] = '0' + fy['upc']
    n = pd.read_parquet(idir / 'usda_nielsen_merged' / 'usda_nova.parquet', columns=['upc13', 'ingredients', 'nova', 'n_ingredients', 'modified_date', 'available_date'])
    n = n.sort_values('modified_date').drop_duplicates('upc13', keep='last')
    d = fy.merge(n, on='upc13', how='left').merge(groups[['product_module_code', 'hi_group', 'upf_group', 'prep_group']], on='product_module_code', how='left')
    d['listed'] = d['ingredients'].notna() & (d['ingredients'].str.len() > 2)
    print(f'{len(d):,} food UPCs in RMS; with an ingredient list: {d.listed.mean():.3f}')
    x = d[d['listed']].copy()
    ing = x['ingredients'].str.upper()
    x['corn'] = ing.str.contains(CORN, regex=True).astype(float) * 100
    x['hfcs'] = ing.str.contains(HFCS, regex=True).astype(float) * 100
    x['sugar'] = ing.str.contains(SUGAR, regex=True).astype(float) * 100
    x['soy'] = ing.str.contains(SOY, regex=True).astype(float) * 100
    x['upf'] = (x['nova'] == 4).astype(float) * 100
    print('all listed UPCs: ', x[['corn', 'hfcs', 'sugar', 'soy', 'upf']].mean().round(1).to_dict(), 'n_ingredients', round(x.n_ingredients.mean(), 1))
    # cohorts
    cov = d.groupby('first_year_national')['listed'].mean() * 100
    print('coverage by launch year:', cov.round(1).to_dict())
    fig, axes = plt.subplots(1, 3, figsize=(30, 10))
    fig.subplots_adjust(wspace=0.25)
    gl = {1.0: ('Mostly not ultra-processed', BLUE), 2.0: ('Mostly ultra-processed', RED)}
    panels = [('corn', 'Contains a corn-derived ingredient'), ('hfcs', 'Contains high fructose corn syrup')]
    for ax, (v, title) in zip(axes, panels):
        for g, (lab, col) in gl.items():
            t = x[x['upf_group'] == g].groupby('first_year_national')[v].mean()
            ax.plot(t.index, t.values, color=col, lw=3, marker='o', ms=6, label=lab)
        ax.set_title(title); ax.set_ylabel('% of listed UPCs'); ax.set_xlabel('Launch year (2007 = 2007 or earlier)')
        ax.set_ylim(bottom=0); ax.set_xticks([2007, 2010, 2013, 2016, 2019])
    axes[0].legend(frameon=False, loc='center')
    ax = axes[2]
    for g, (lab, col) in gl.items():
        t = d[d['upf_group'] == g].groupby('first_year_national')['listed'].mean() * 100
        ax.plot(t.index, t.values, color=col, lw=3, marker='o', ms=6, label=lab)
    ax.set_title('Has a list in the 2024 USDA file'); ax.set_ylabel('% of all UPCs in the cohort'); ax.set_xlabel('Launch year (2007 = 2007 or earlier)')
    ax.set_ylim(bottom=0); ax.set_xticks([2007, 2010, 2013, 2016, 2019])
    out = fdir / 'what_moves' / 'variety' / 'ingredients_by_launch_year.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, bbox_inches='tight', dpi=150)
    print(f'saved {out}')
    # table: by group, stock vs new
    x['cohort'] = np.where(x['first_year_national'] == 2007, 'stock', 'new')
    rows = []
    lab = {('upf_group', 1.0): 'Mostly not ultra-processed', ('upf_group', 2.0): 'Mostly ultra-processed',
           ('hi_group', 1.0): 'Unhealthy (score 0--3)', ('hi_group', 2.0): 'Middle (4--5)', ('hi_group', 3.0): 'Healthy (6--10)',
           ('prep_group', 1.0): 'Open and eat', ('prep_group', 2.0): 'Little preparation', ('prep_group', 3.0): 'Cooking'}
    for (g, k), name in lab.items():
        z = x[x[g] == k]
        a, b = z[z.cohort == 'stock'], z[z.cohort == 'new']
        rows.append((name, len(a), a.corn.mean(), b.corn.mean(), a.hfcs.mean(), b.hfcs.mean(), a.sugar.mean(), b.sugar.mean(), a.upf.mean(), b.upf.mean(), a.n_ingredients.mean(), b.n_ingredients.mean()))
    t = pd.DataFrame(rows, columns=['group', 'n_stock', 'corn_s', 'corn_n', 'hfcs_s', 'hfcs_n', 'sugar_s', 'sugar_n', 'upf_s', 'upf_n', 'ni_s', 'ni_n'])
    print(t.round(1).to_string(index=False))
    lines = ['\\begin{tabular}{lrrrrrrrr}', '\\toprule',
             ' & \\multicolumn{2}{c}{Corn-derived (\\%)} & \\multicolumn{2}{c}{HFCS (\\%)} & \\multicolumn{2}{c}{Sugar (\\%)} & \\multicolumn{2}{c}{NOVA 4 (\\%)} \\\\',
             '\\cmidrule(lr){2-3} \\cmidrule(lr){4-5} \\cmidrule(lr){6-7} \\cmidrule(lr){8-9}',
             'Module group & 2007 & 2008--20 & 2007 & 2008--20 & 2007 & 2008--20 & 2007 & 2008--20 \\\\', '\\midrule']
    for i, r in t.iterrows():
        if i in (2, 5):
            lines.append('\\midrule')
        lines.append(f"{r['group']} & {r.corn_s:.1f} & {r.corn_n:.1f} & {r.hfcs_s:.1f} & {r.hfcs_n:.1f} & {r.sugar_s:.1f} & {r.sugar_n:.1f} & {r.upf_s:.1f} & {r.upf_n:.1f} \\\\")
    lines += ['\\bottomrule', '\\end{tabular}']
    (tdir / 'what_moves' / 'ingredients_by_group.tex').write_text('\n'.join(lines) + '\n')
    # within-module slope on launch year (new cohorts only), per 10 years
    z = x[x['first_year_national'] >= 2008].copy()
    z['ly'] = (z['first_year_national'] - 2014) / 10
    z['m'] = z['product_module_code']
    for v in ['corn', 'hfcs', 'sugar', 'upf']:
        for g, name in [(1.0, 'not UPF'), (2.0, 'UPF')]:
            zz = z[z['upf_group'] == g].copy()
            for c in [v, 'ly']:
                zz[c + '_w'] = zz[c] - zz.groupby('m')[c].transform('mean')
            r = smf.ols(f'{v}_w ~ ly_w - 1', data=zz).fit(cov_type='cluster', cov_kwds={'groups': zz['m']})
            print(f'{v} per 10 launch years, within module, {name}: {r.params.iloc[0]:+.2f} (SE {r.bse.iloc[0]:.2f}), n = {len(zz):,}')


if __name__ == '__main__':
    main()
