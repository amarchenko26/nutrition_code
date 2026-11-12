"""Added sugar per 100 g from Syndigo labels (2026-11-12, for HEI from Syndigo labels, Anya).

Syndigo reports added sugar only on labels in the post-2016 format, from its 2017 files on, under two IDs: 32813 'Includes
Added Sugars' and 19417 'Added Sugars' (37143 'Incl. Added Sugars' on a few dozen records). clean_syndigo.py does not keep it:
it keeps one label year per UPC, the year with the most nutrient lines, and a line that only new labels carry would change
that choice and with it every Syndigo measure. So this script reads the line from the raw files and writes it to its own file.

Per UPC, the most recent as-packaged record with an added-sugar quantity (a percent of the daily value counts at 50 g when the
grams are missing): added sugar over total sugar on that record, both grams per serving, capped at 1, times the UPC's sugar
per 100 g in syndigo_wide.parquet (merge_nielsen_syndigo.py), so the serving grams of the label year that file keeps apply.
A record with zero total sugar has zero added sugar, or none when it reports more than zero.
Output: interim/syndigo_nielsen_merged/syndigo_added_sugar.parquet (upc, addsugar_per_100g, addsugar_share, syndigo_year),
read by build/health/build_upc_dictionary.py syndigo.
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
from clean_syndigo import standardize_nutrient_to_grams, harmonize_syndigo_upc

import numpy as np
import pandas as pd

ADDSUG = [32813, 19417, 37143]     # in order of preference when a record has more than one
SUGAR = [32705, 15]                # total sugars, new-format ID first
DV_ADDSUG_G = 50.0                 # daily value of added sugar on the post-2016 label
YEARS = range(2017, 2025)


def read_year(y):
    n = pd.read_csv(rdir / 'syndigo' / str(y) / 'Nutrient.csv', usecols=['UPC', 'ValuePreparedType', 'NutrientMasterID', 'Quantity', 'UOM', 'Pct'],
                    dtype={'UPC': str, 'UOM': str}, encoding='latin-1', low_memory=False)
    n = n[n['NutrientMasterID'].isin(ADDSUG + SUGAR)].copy()
    vpt = pd.to_numeric(n['ValuePreparedType'], errors='coerce')
    n = n[vpt == vpt.min()]                                  # as packaged, as in clean_syndigo.py
    n['g'] = standardize_nutrient_to_grams(n['Quantity'], n['UOM'], 15).abs()   # negative = 'less than', as in clean_syndigo.py
    n['pct'] = pd.to_numeric(n['Pct'], errors='coerce').abs()
    n['upc'] = harmonize_syndigo_upc(n['UPC'])
    n['rank'] = n['NutrientMasterID'].map({k: i for i, k in enumerate(ADDSUG + SUGAR)})
    a = n[n['NutrientMasterID'].isin(ADDSUG)]
    s = n[n['NutrientMasterID'].isin(SUGAR)].sort_values('rank').drop_duplicates('upc')
    # the two added-sugar IDs on the same record
    both = a[a['g'].notna()].pivot_table(index='upc', columns='NutrientMasterID', values='g', aggfunc='first')
    if {32813, 19417} <= set(both.columns):
        bb = both[[32813, 19417]].dropna()
        print(f'  {y}: {len(bb):,} records with both IDs; within 0.5 g on {(bb[32813] - bb[19417]).abs().le(0.5).mean():.3f}')
    n_pct = int((a['g'].isna() & a['pct'].notna()).sum())
    a = a.assign(g=a['g'].fillna(a['pct'] / 100 * DV_ADDSUG_G)).dropna(subset=['g']).sort_values('rank').drop_duplicates('upc')
    r = a[['upc', 'g']].rename(columns={'g': 'addsug_g'}).merge(s[['upc', 'g']].rename(columns={'g': 'sugar_g'}), on='upc', how='left')
    r['addsugar_share'] = np.where(r['sugar_g'] > 0, np.minimum(r['addsug_g'] / r['sugar_g'], 1.0),
                                   np.where((r['sugar_g'] == 0) & (r['addsug_g'] == 0), 0.0, np.nan))
    r['syndigo_year'] = y
    print(f'  {y}: {len(r):,} UPCs with added sugar ({n_pct:,} from the percent of the daily value); share known on {r.addsugar_share.notna().mean():.3f}; '
          f'share above 1 before the cap {(r.addsug_g > r.sugar_g + 0.5).mean():.3f}', flush=True)
    return r


def main():
    r = pd.concat([read_year(y) for y in YEARS], ignore_index=True).dropna(subset=['addsugar_share'])
    r = r.sort_values('syndigo_year').drop_duplicates('upc', keep='last')
    w = pd.read_parquet(idir / 'syndigo_nielsen_merged' / 'syndigo_wide.parquet', columns=['upc', 'sugar_per_100g']).drop_duplicates('upc')
    r = r.merge(w, on='upc', how='inner')
    r['addsugar_per_100g'] = r['addsugar_share'] * r['sugar_per_100g']
    out = r[['upc', 'addsugar_per_100g', 'addsugar_share', 'syndigo_year']].dropna(subset=['addsugar_per_100g'])
    print(f'{len(out):,} UPCs with added sugar per 100 g; by label year {out.syndigo_year.value_counts().sort_index().to_dict()}')
    print(f'mean share of sugar that is added {out.addsugar_share.mean():.3f}; zero added sugar on {(out.addsugar_share == 0).mean():.3f}')
    p = idir / 'syndigo_nielsen_merged' / 'syndigo_added_sugar.parquet'
    out.to_parquet(p, index=False)
    print(f'saved {p}')


if __name__ == '__main__':
    main()
