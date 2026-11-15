"""The income IV on the time intensity of the groceries (Anya 2026-11-15): tabs/what_moves/iv_time.tex, one row per outcome (estimate
with its standard error below, the placebo on purchases from t-3 to t-2, the mean of the outcome on the estimation sample, the
first-stage F and the household-years). Reads interim/what_moves/iv_time.csv (build_iv_time.do); outcomes built by
build/prep_time/build_prep_time_panel.py.
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

ROWS = [('prep_time_raw', 'Preparation score of the groceries (1--9)', 3, 2),
        ('sh_open', r'Open and eat, \% of spending', 2, 1),
        ('sh_rte', r'Ready-to-eat meals, \% of spending', 2, 1),
        ('sh_open_rte', r'Open and eat or ready-to-eat, \% of spending', 2, 1),
        ('sh_little', r'Little preparation, \% of spending', 2, 1),
        ('sh_cook', r'Cooking, \% of spending', 2, 1),
        ('sh_scratch', r'Scratch ingredients, \% of spending', 2, 1),
        ('prep_min_ae_day', 'Minutes of preparation per adult equivalent per day', 2, 1)]


def main():
    d = pd.read_csv(idir / 'what_moves' / 'iv_time.csv').set_index(['outcome', 'window'])
    tex = [r'\begin{tabular}{lccccc}', r'\toprule',
           r' & Estimate & Placebo & Mean of & First- & Household- \\',
           r' & ($t$ to $t+1$) & ($t-3$ to $t-2$) & the outcome & stage F & years \\', r'\midrule']
    for o, lab, dec, mdec in ROWS:
        m, b = d.loc[(o, 'main')], d.loc[(o, 'before')]
        tex.append(f"{lab} & {star_num(m['b'], m['se'], dec)} & {star_num(b['b'], b['se'], dec)} & {m['mean']:,.{mdec}f} & {m['f']:.0f} & {int(m['n']):,} \\\\")
        tex.append(f" & ({m['se']:.{dec}f}) & ({b['se']:.{dec}f}) &  &  &  \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_time.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(f'saved {out}')


if __name__ == '__main__':
    main()
