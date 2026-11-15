"""The income IV on the parts of the diet: column 3 of tabs/labor_supply/hi_clean.tex with other outcomes, in standard deviations
(the ones analyze/labor_supply/12_ standardizes with). Moved from build_summary_table.py on 2026-11-15, when Anya deleted the summary
table (Table 3 of 0_main.tex) and its code.

Reads interim/what_moves/iv_suboutcomes.csv and iv_suboutcomes_same_size.csv (build_iv_suboutcomes.do).
Output: tabs/what_moves/iv_suboutcomes.tex, printed as well.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd
from scipy.stats import norm

WM = idir / 'what_moves'


def two_sided(b, se):
    return 2 * (1 - norm.cdf(abs(b / se)))


def iv_table():
    """Column 3 of tabs/labor_supply/hi_clean.tex for the parts of the diet: tabs/what_moves/iv_suboutcomes.tex. Standard deviations are the ones 12_ standardizes with."""
    a = pd.read_csv(WM / 'iv_suboutcomes.csv')
    b = pd.read_csv(WM / 'iv_suboutcomes_same_size.csv')
    a, b = a[a['window'] == 'main'].set_index('outcome'), b[b['window'] == 'main'].set_index('outcome')
    x = a.loc['hei_usda']
    print(f"HEI, all households: {x['b'] / x['sd_z']:.4f} ({x['se'] / x['sd_z']:.4f}) SD per $10k, {x['b']:.3f} ({x['se']:.3f}) points, F {x['f']:.0f}, N {int(x['n']):,}, SD {x['sd_z']:.3f}")
    x = a.loc['lhei_usda']
    print(f"Linear HEI, all households: {x['b'] / x['sd_z']:.4f} ({x['se'] / x['sd_z']:.4f}) SD per $10k, {x['b']:.3f} ({x['se']:.3f}) points, F {x['f']:.0f}, N {int(x['n']):,}, SD {x['sd_z']:.3f}")
    cols = [(a, 'sugar_1000', 'Sugar', r'(g per 1,000 kcal)', 1), (a, 'sodium_1000', 'Sodium', r'(mg per 1,000 kcal)', 0), (a, 'upf_share', 'Ultra-processed', r'(\% of calories)', 1),
            (a, 'produce_share', 'Produce', r'(\% of calories)', 1), (a, 'produce_spend_share', 'Produce', r'(\% of spending)', 1), (a, 'soda_spend_share', 'Soda', r'(\% of spending)', 1),
            (a, 'whole_share', 'Whole-grain', r'(\% of bread)', 1), (a, 'cals_ae_day', 'Calories', r'(per adult eq.)', 0), (b, 'cals_ae_day', 'Calories, same size', r'(per adult eq.)', 0)]
    rows = [df.loc[o] for df, o, *_ in cols]
    def star(r):
        pv = two_sided(r['b'], r['se'])
        return '***' if pv < 0.01 else '**' if pv < 0.05 else '*' if pv < 0.10 else ''
    tex = [r'\begin{tabular}{l' + 'c' * len(cols) + '}', r'\toprule',
           ' & ' + ' & '.join(f'({i + 1})' for i in range(len(cols))) + r' \\',
           ' & ' + ' & '.join(c[2] for c in cols) + r' \\',
           ' & ' + ' & '.join(c[3] for c in cols) + r' \\', r'\midrule',
           r'$\Delta$ Income (\$10k) & ' + ' & '.join(f"{r['b'] / r['sd_z']:.3f}" + (f'$^{{{star(r)}}}$' if star(r) else '') for r in rows) + r' \\',
           ' & ' + ' & '.join(f"({r['se'] / r['sd_z']:.3f})" for r in rows) + r' \\', r'\midrule',
           'First-stage F & ' + ' & '.join(f"{r['f']:.0f}" for r in rows) + r' \\',
           'N & ' + ' & '.join(f"{int(r['n']):,}" for r in rows) + r' \\',
           'Mean of the outcome & ' + ' & '.join(f"{r['mean']:,.{c[4]}f}" for r, c in zip(rows, cols)) + r' \\',
           'SD of the outcome & ' + ' & '.join(f"{r['sd_z']:,.{c[4]}f}" for r, c in zip(rows, cols)) + r' \\', r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_suboutcomes.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(f'saved {out}')


if __name__ == '__main__':
    iv_table()
