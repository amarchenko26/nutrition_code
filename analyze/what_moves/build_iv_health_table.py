"""The income IV on reported health conditions (Anya's note in 0_main.tex, 2026-11-18): tabs/what_moves/iv_health.tex, one row per
condition (estimate with its standard error below, the placebo on the change from t-3 to t-2, the mean of the outcome, the
first-stage F and the household-years). Reads interim/what_moves/iv_health.csv (build_iv_health.do).
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

ROWS = [('obesity', r'Obesity or overweight reported (\% of households)', 2, 1),
        ('diabetes_type2', r'Type 2 diabetes reported (\% of households)', 2, 1),
        ('hypertension', r'Hypertension reported (\% of households)', 2, 1)]


def main():
    d = pd.read_csv(idir / 'what_moves' / 'iv_health.csv').set_index(['outcome', 'window'])
    tex = [r'\begin{tabular}{lccccc}', r'\toprule',
           r' & Estimate & Placebo & Mean of & First- & Household- \\',
           r' & ($t$ to $t+1$) & ($t-3$ to $t-2$) & the outcome & stage F & years \\', r'\midrule']
    for o, lab, dec, mdec in ROWS:
        m, b = d.loc[(o, 'main')], d.loc[(o, 'before')]
        tex.append(f"{lab} & {star_num(m['b'], m['se'], dec)} & {star_num(b['b'], b['se'], dec)} & {m['mean']:,.{mdec}f} & {m['f']:.0f} & {int(m['n']):,} \\\\")
        tex.append(f" & ({m['se']:.{dec}f}) & ({b['se']:.{dec}f}) &  &  &  \\\\")
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'iv_health.tex'
    out.write_text('\n'.join(tex) + '\n')
    print('\n'.join(tex))
    print(f'saved {out}')


if __name__ == '__main__':
    main()
