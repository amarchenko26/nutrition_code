"""Product modules by preparation score (Anya 2026-11-15: a table before the time-intensity table showing which foods get which
score): tabs/what_moves/prep_score_examples.tex, one row per score 1-9 and one for modules without a score, with the group (open and
eat 1, little preparation 2-3, cooking 4-9), the minutes per preparation used in the full-price figure (5 per point above 1), the
number of modules, their share of food spending and the four largest modules by that spending.

Reads interim/rms_variety/prep_time_scores.parquet (build/prep_time/build_prep_time_index.py: a score and a written reason per
module) and interim/what_moves/module_time_price.parquet (build_module_time_price.py: Homescan food spending by module, 2004-2020,
modules kept by the food filters). Module names are written out by hand below for the modules shown.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import pandas as pd

NAMES = {3611: 'butter', 1177: 'salad dressing', 3608: 'margarine and spreads', 1403: 'granulated sugar',
         3625: 'refrigerated milk', 1484: 'carbonated soft drinks', 1344: 'ready-to-eat cereal', 1553: 'diet soft drinks',
         1290: 'canned soup', 4225: 'other fresh fruit', 2631: 'frozen pizza', 2621: 'frozen poultry entrees',
         2634: 'frozen potatoes', 3576: 'hot dogs', 1348: 'hot cereal', 1329: 'unpopped popcorn',
         1340: 'boxed pasta dinners', 4275: 'fresh lettuce', 1331: 'macaroni', 1334: 'spaghetti',
         4100: 'fresh eggs', 3577: 'bacon', 3572: 'dinner sausage', 3578: 'breakfast sausage',
         4280: 'other fresh vegetables', 2681: 'frozen poultry', 4050: 'fresh carrots', 2683: 'frozen unbreaded fish',
         3561: 'fresh meat', 4400: 'fresh potatoes', 4350: 'fresh onions', 1437: 'chocolate chips',
         1393: 'all-purpose white flour', 1377: 'other all-purpose flour', 1367: 'corn meal', 1391: 'single-purpose flour',
         1731: 'fresh yeast'}
GROUP = {1: 'Open and eat', 2: 'Little preparation', 3: 'Little preparation'}


def name(code, descr):
    return NAMES.get(int(code), str(descr).lower().replace(' - ', ', ').replace('-', ' '))


def main():
    s = pd.read_parquet(idir / 'rms_variety' / 'prep_time_scores.parquet', columns=['product_module_code', 'product_module_descr', 'prep_time'])
    m = pd.read_parquet(idir / 'what_moves' / 'module_time_price.parquet', columns=['product_module_code', 'spend'])
    x = s.merge(m, on='product_module_code', how='left')
    tot = x['spend'].sum()
    tex = [r'\begin{tabular}{llcccp{7.2cm}}', r'\toprule',
           r'Preparation &  & Minutes per &  & \% of food & \\',
           r'score & Group & preparation & Modules & spending & Largest modules by spending \\', r'\midrule']
    rows = [(k, x[x['prep_time'] == k]) for k in range(1, 10)] + [(None, x[x['prep_time'].isna()])]
    for k, g in rows:
        g = g.sort_values('spend', ascending=False, na_position='last')
        ex = ', '.join(name(r.product_module_code, r.product_module_descr) for r in g.head(4).itertuples())
        sh = 100 * g['spend'].sum() / tot
        if k is None:
            tex.append(r'\midrule')
            tex.append(f"None & No score & -- & {len(g)} & {sh:.1f} & {ex} \\\\")
        else:
            tex.append(f"{k} & {GROUP.get(k, 'Cooking')} & {5 * (k - 1)} & {len(g)} & {sh:.1f} & {ex} \\\\")
        print(k, len(g), round(sh, 1), ex)
    tex += [r'\bottomrule', r'\end{tabular}']
    out = tdir / 'what_moves' / 'prep_score_examples.tex'
    out.write_text('\n'.join(tex) + '\n')
    print(f'saved {out}')


if __name__ == '__main__':
    main()
