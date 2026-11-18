"""The income IV on the heads' labor supply (Anya 2026-11-18): tabs/what_moves/iv_labor.tex, one row per outcome: the estimate
for the change over the same years as the income change (t-1 to t), for the next year (t to t+1, the window of the nutrition
outcomes), the placebo (t-3 to t-2), the mean of the outcome, and the first-stage F and household-years of the t to t+1 column.
Reads interim/what_moves/iv_labor.csv (build_iv_labor.do).
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

ROWS = [('hh_avg_workhours', 'Hours worked per week, all heads (0 if not working)', 2, 1),
        ('emp100', r'Heads who work, \% of heads', 2, 1),
        ('hh_avg_workhours_if_employed', 'Hours worked per week, heads who work', 2, 1)]


def main():
    d = pd.read_csv(idir / 'what_moves' / 'iv_labor.csv').set_index(['outcome', 'window'])
    tex = [r'\begin{tabular}{lcccccc}', r'\toprule',
           r' & Same years & Next year & Placebo & Mean of & First- & Household- \\',
           r' & ($t-1$ to $t$) & ($t$ to $t+1$) & ($t-3$ to $t-2$) & the outcome & stage F & years \\', r'\midrule']
    for o, lab, dec, mdec in ROWS:
        s, m, b = d.loc[(o, 'same')], d.loc[(o, 'main')], d.loc[(o, 'before')]
        tex.append(f"{lab} & {star_num(s['b'], s['se'], dec)} & {star_num(m['b'], m['se'], dec)} & {star_num(b['b'], b['se'], dec)} & {m['mean']:,.{mdec}f} & {m['f']:.0f} & {int(m['n']):,} \\\\")
        tex.append(f" & ({s['se']:.{dec}f}) & ({m['se']:.{dec}f}) & ({b['se']:.{dec}f}) &  &  &  \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_labor.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    for o, lab, _, _ in ROWS:
        s = d.loc[(o, 'same')]
        print(f"{lab}, same years: F {s['f']:.0f}, household-years {int(s['n']):,}, mean at t-1 {s['mean']:.1f}")
    print(f'saved {out}')


if __name__ == '__main__':
    main()
