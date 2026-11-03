"""Parse the published ATUS Table A-1 PDFs (one per year) into a tidy table.

Input: raw/atus/tables_a1/a1_<year>.pdf, the BLS "Table A-1. Time spent in detailed primary
activities and percent of the civilian population engaging in each activity, averages per day by
sex" (https://www.bls.gov/tus/tables.htm; 2003-2019 and 2021-2024; no 2020 table because data
collection stopped from mid-March to mid-May 2020). The PDFs were fetched on 2026-11-03 (the
BLS site refuses scripted downloads; the fetch tool saved them). Each activity row carries nine
numbers: average hours per day (total, men, women), percent engaging per day (total, men,
women), average hours per day of those who engaged (total, men, women); civilian population
aged 15 and over.
Output: interim/atus/table_a1.parquet with one row per year x activity for the activities below.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import re
import fitz
import pandas as pd

RAW = rdir / 'atus' / 'tables_a1'
OUT = idir / 'atus' / 'table_a1.parquet'
# label as printed -> activity key, and which occurrence to take (the first "Eating and drinking"
# row is the category including travel, the second excludes travel)
ROWS = {'Household activities': ('household_activities', 1), 'Food preparation and cleanup': ('food_prep_cleanup', 1),
        'Food and drink preparation': ('food_drink_prep', 1), 'Kitchen and food cleanup': ('kitchen_cleanup', 1),
        'Eating and drinking': ('eating_drinking', 2), 'Grocery shopping': ('grocery_shopping', 1),
        'Purchasing goods and services': ('purchasing', 1)}
COLS = ['hours_total', 'hours_men', 'hours_women', 'pct_total', 'pct_men', 'pct_women', 'hours_part_total', 'hours_part_men', 'hours_part_women']
NUM = re.compile(r'^\(?-?\.?\d*\.?\d+\)?$')


def parse(pdf):
    doc = fitz.open(pdf)
    txt = '\n'.join(p.get_text() for p in doc)
    m = re.search(r'(20\d\d) annual averages', txt)
    year = int(m.group(1)) if m else None
    if year is None:
        m = re.search(r'D:(20\d\d)', doc.metadata.get('creationDate', ''))
        year = int(m.group(1)) - 1 if m else None     # a table published in year y+1 holds year y
    lines = [l.strip() for l in txt.split('\n')]
    seen = {}
    rows = []
    for i, l in enumerate(lines):
        lab = re.sub(r'\s*\.{3,}.*$', '', l).strip()
        if lab in ROWS:
            key, occ = ROWS[lab]
            seen[lab] = seen.get(lab, 0) + 1
            if seen[lab] != occ:
                continue
            nums = []
            for v in lines[i + 1:i + 14]:
                if NUM.match(v.replace(',', '')):
                    nums.append(float(v.strip('()')))
                if len(nums) == 9:
                    break
            if len(nums) == 9:
                rows.append({'year': year, 'activity': key, **dict(zip(COLS, nums))})
    return rows


def main():
    rows = []
    for pdf in sorted(RAW.glob('a1_*.pdf')):
        r = parse(pdf)
        print(f'{pdf.name}: year {r[0]["year"] if r else None}, {len(r)} rows')
        rows += r
    df = pd.DataFrame(rows).sort_values(['activity', 'year']).reset_index(drop=True)
    assert df['year'].notna().all() and not df.duplicated(['year', 'activity']).any(), df[df.duplicated(['year', 'activity'], keep=False)]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(df[df.activity == 'food_prep_cleanup'][['year'] + COLS].to_string(index=False))
    print(f'saved {OUT}')


if __name__ == '__main__':
    main()
