"""When was each condition diagnosed, and how is it managed? Nielsen Health Care Survey, 2011-2015.

In 2011-2015 each household member aged 13+ answers for themselves. For every condition selected in
Q1 the questionnaire then asks (format files, raw/ailments/<year>/): Q2 "About how long ago was this
household member diagnosed with <condition>?" (1 less than a year ago, 2 1-2 years ago, 3 3-4 years
ago, 4 5 or more years ago); Q3 sought medical attention in the past 6 months; Q4 how the member
treated or managed it in the past 6 months (Q4_2 prescription medication, Q4_3 diet, Q4_4 exercise,
Q4_7 did not treat or manage); Q6 severity. clean_ailments_members.py keeps only the Q1 flags.

Fielding dates: 14 Jan 2011, 25 Jan 2012, 7 Feb 2013 (2014-2015 also early in the year), so "less than a
year ago" in the year-t survey means a diagnosis in about calendar year t-1.

Usage: python clean_ailments_diagnosis_timing.py <year>   writes interim/ailments/by_year/dxtiming_<year>.parquet
       python clean_ailments_diagnosis_timing.py combine  writes interim/ailments/ailments_diagnosis_timing.parquet
Output: one row per member x survey year x condition selected in Q1 (household_code, panel_year, member,
condition, recency, seek, rx, diet, exercise, no_treat, severity).
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import glob, re, time
import numpy as np
import pandas as pd
from openpyxl import load_workbook

CONDS = {15: 'diabetes', 22: 'hypertension', 11: 'cholesterol', 21: 'heart', 30: 'obesity'}
CACHE = idir / 'ailments' / 'by_year'


def extract(year):
    t0 = time.time()
    f = glob.glob(str(rdir / 'ailments' / str(year) / '*ParsedData*.xlsx'))[0]
    wb = load_workbook(f, read_only=True)
    ws = wb.worksheets[0]
    rows = ws.iter_rows(values_only=True)
    h = [str(x) for x in next(rows)]
    ih = next(i for i, c in enumerate(h) if any(k in c.lower() for k in ('household id', 'househiold')))
    pos = {}
    for j, name in CONDS.items():
        q1 = next(i for i, c in enumerate(h) if re.match(rf'^Q1_Ailment{j}\b', c))
        pos[name] = dict(q1=q1, **{k: h.index(f'Ailment{j}_{q}') for k, q in
                                   dict(recency='Q2', seek='Q3', rx='Q4_2', diet='Q4_3', exercise='Q4_4', no_treat='Q4_7', severity='Q6').items()})
    out, seen, n = [], {}, 0
    for row in rows:
        hh = row[ih]
        if hh is None:
            continue
        n += 1
        seen[hh] = seen.get(hh, 0) + 1
        for name, p in pos.items():
            if row[p['q1']] == 1:
                out.append((hh, year, seen[hh], name, row[p['recency']], row[p['seek']], row[p['rx']], row[p['diet']],
                            row[p['exercise']], row[p['no_treat']], row[p['severity']]))
    d = pd.DataFrame(out, columns=['household_code', 'panel_year', 'member', 'condition', 'recency', 'seek', 'rx', 'diet', 'exercise', 'no_treat', 'severity'])
    for c in ['household_code', 'recency', 'seek', 'rx', 'diet', 'exercise', 'no_treat', 'severity']:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    CACHE.mkdir(parents=True, exist_ok=True)
    d.to_parquet(CACHE / f'dxtiming_{year}.parquet', index=False)
    print(f'{year}: {n:,} member rows, {len(seen):,} households, {len(d):,} condition selections ({time.time() - t0:.0f}s); '
          f'recency answered {d.recency.notna().mean():.3f}', flush=True)
    print(d.groupby('condition').recency.value_counts(normalize=True).unstack().round(3).to_string(), flush=True)


def combine():
    d = pd.concat([pd.read_parquet(CACHE / f'dxtiming_{y}.parquet') for y in range(2011, 2016)], ignore_index=True)
    d['household_code'] = d['household_code'].astype('int64')
    d.to_parquet(idir / 'ailments' / 'ailments_diagnosis_timing.parquet', index=False)
    print(f'saved {len(d):,} rows to {idir / "ailments" / "ailments_diagnosis_timing.parquet"}')


if __name__ == '__main__':
    combine() if sys.argv[1] == 'combine' else extract(int(sys.argv[1]))
