"""Sugar and calories from store-bought food in NHANES, a benchmark that uses no package labels (2026-11-05).

Day-1 dietary recalls, 2003-04 to 2017-March 2020 cycles (raw/nhanes, DR1TOT, DR1IFF, DEMO), people aged 2 and over
with a complete recall. Store-sourced items are DR1FS in {1, 27, 28}. Per cycle, weighted by the day-1 dietary weight:
sugar per 1,000 kcal of store-bought food (all sugar over all kcal), and store-bought kcal per person per day.
Output: interim/what_moves/nhanes_sugar.parquet (cycle, year_mid, store_sugar_1000, store_kcal, n)
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

R = rdir / 'nhanes'
CYCLES = [('2003-04', 2003.5, '_C', 'WTDRD1'), ('2005-06', 2005.5, '_D', 'WTDRD1'), ('2007-08', 2007.5, '_E', 'WTDRD1'), ('2009-10', 2009.5, '_F', 'WTDRD1'),
          ('2011-12', 2011.5, '_G', 'WTDRD1'), ('2013-14', 2013.5, '_H', 'WTDRD1'), ('2015-16', 2015.5, '_I', 'WTDRD1'), ('2017-Mar20', 2018.5, 'P', 'WTDRD1PP')]
STORE = [1, 27, 28]


def main():
    rows = []
    for lab, mid, s, wv in CYCLES:
        f = (lambda n: R / f'P_{n}.xpt') if s == 'P' else (lambda n: R / f'{n}{s}.xpt')
        T = pd.read_sas(f('DR1TOT'), format='xport')
        I = pd.read_sas(f('DR1IFF'), format='xport', encoding=None)
        D = pd.read_sas(f('DEMO'), format='xport')
        T = T[(T['DR1DRSTZ'] == 1) & (T[wv] > 0)].merge(D[['SEQN', 'RIDAGEYR']], on='SEQN')
        T = T[T['RIDAGEYR'] >= 2]
        st = I[I['DR1FS'].isin(STORE)].groupby('SEQN')[['DR1IKCAL', 'DR1ISUGR']].sum()
        T = T.merge(st, left_on='SEQN', right_index=True, how='left').fillna({'DR1IKCAL': 0, 'DR1ISUGR': 0})
        w = T[wv]
        rows.append({'cycle': lab, 'year_mid': mid, 'store_sugar_1000': 1000 * (w * T['DR1ISUGR']).sum() / (w * T['DR1IKCAL']).sum(),
                     'store_kcal': np.average(T['DR1IKCAL'], weights=w), 'n': len(T)})
        print(rows[-1], flush=True)
    (idir / 'what_moves').mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(idir / 'what_moves' / 'nhanes_sugar.parquet', index=False)


if __name__ == '__main__':
    main()
