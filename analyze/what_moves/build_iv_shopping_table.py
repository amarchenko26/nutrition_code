"""The income IV on shopping (Anya 2026-11-15): tabs/what_moves/iv_shopping.tex, one row per outcome (estimate with its standard
error below, the placebo on purchases from t-3 to t-2, the mean of the outcome on the estimation sample, the first-stage F and the
household-years). Reads interim/what_moves/iv_shopping.csv (build_iv_shopping.do); outcomes built by
build/expenditure/build_shopping_panel.py.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from utils.stars import star_num

import pandas as pd

PANELS = [
    (r'A. Spending and trips',
     [('spend_month', r'Food spending per month (\$), households of the same size', 2, 0),
      ('spend_month_all', r'Food spending per month (\$), all households', 2, 0),
      ('trips_week', 'Shopping trips per week, households of the same size', 3, 2),
      ('trips_week_all', 'Shopping trips per week, all households', 3, 2)]),
    (r'B. Share of food spending (\%)',
     [('sh_grocery', 'Grocery stores', 2, 1), ('sh_discount', 'Discount and dollar stores', 2, 1), ('sh_club', 'Warehouse clubs', 2, 1),
      ('sh_drug', 'Drug stores', 2, 1), ('sh_conv', 'Convenience stores', 2, 1)]),
    ('C. Main store (the RMS store with the most food spending)',
     [('switched', 'Switched main store (\\%)', 2, 1), ('farther', 'Switched to a store farther from home (\\%)', 2, 1),
      ('samezip', 'Main store in the home ZIP (\\%)', 2, 1), ('dist_km', 'Distance to the main store (km)', 2, 1),
      ('store_hi', 'Health Index of the food it sells (per 1,000 kcal)', 3, 2), ('store_zipinc', "Median income of its shoppers' ZIPs (\\$1,000s)", 2, 1)]),
]


def main():
    d = pd.read_csv(idir / 'what_moves' / 'iv_shopping.csv').set_index(['outcome', 'window'])
    tex = [r'\begin{tabular}{lccccc}', r'\toprule',
           r' & Estimate & Placebo & Mean of & First- & Household- \\',
           r' & ($t$ to $t+1$) & ($t-3$ to $t-2$) & the outcome & stage F & years \\', r'\midrule']
    for k, (title, rows) in enumerate(PANELS):
        if k:
            tex.append(r'\midrule')
        tex.append(r'\multicolumn{6}{l}{\textit{' + title + r'}} \\')
        for o, lab, dec, mdec in rows:
            m, b = d.loc[(o, 'main')], d.loc[(o, 'before')]
            tex.append(f"{lab} & {star_num(m['b'], m['se'], dec)} & {star_num(b['b'], b['se'], dec)} & {m['mean']:,.{mdec}f} & {m['f']:.0f} & {int(m['n']):,} \\\\")
            tex.append(f" & ({m['se']:.{dec}f}) & ({b['se']:.{dec}f}) &  &  &  \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_shopping.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    r = d.loc[('switched_retailer', 'main')]
    print(f"switched main retailer (not tabulated): {r['b']:.3f} ({r['se']:.3f}), mean {r['mean']:.1f}%")
    print(f'saved {out}')


if __name__ == '__main__':
    main()
