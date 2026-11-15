"""
build_shopping_panel.py  --  run LOCALLY

Household-year shopping measures for the income IV on shopping (Anya 2026-11-15, note under the health table of 0_main.tex: grocery
spending, trips, the types of stores, whether the household changes its main store, whether the new one is farther away, and how
healthy it is).

Reads interim/purchases_food/panel_year=<y>/ (2004-2020; household_code, trip_code_uc, retailer_code, store_code_uc,
total_price_paid_real_2013), retailers.tsv inside raw/consumer/Master_Files2004-2020.tgz (retailer channel), interim/panelists/
panelists_all_years.parquet (panelist_zip_code), raw/acs/2020_Gaz_zcta_national.zip (ZIP centroids), raw/acs/acs5_2015_zcta_
median_hh_income.csv (ZIP median household income) and interim/what_moves/store_choice_sets.parquet (what each RMS store sells).

Per household and year:
  spend_month   food spending per month, 2013 dollars (the year's food purchases / 12)
  trips_week    shopping trips with food per week (distinct trips / 52)
  sh_grocery, sh_discount, sh_club, sh_drug, sh_conv, sh_other   % of food spending by store type (retailer channel): grocery;
                discount and dollar stores (Walmart, Target, dollar stores, hypermarkets); warehouse clubs (Costco, Sam's Club, BJ's);
                drug stores; convenience stores and gas station marts; all other
  main_store    the RMS store (store_code_uc) with the most food spending in the year (82% of food spending is at RMS stores);
                main_share = its share of the year's food spending
  main_retailer the retailer with the most food spending, any store (as analyze/what_moves/build_retailer_year.py)
Main-store measures built from other households' trips to the store (the household's own trips left out), over all years, so a
store's value does not change over time; stores with 50+ such trips from 10+ households (the floor of figure_store_access.py):
  dist_km       distance from the household's ZIP centroid to the trip-weighted mean centroid of the other shoppers' ZIPs
  same_zip      1 if the ZIP with the most of the store's other shoppers' trips is the household's ZIP
  store_zipinc  trip-weighted mean ACS 2011-2015 median household income of the other shoppers' ZIPs, $1,000s (Figure 4's x-axis)
  store_hi      Health Index per 1,000 kcal of the food the store sells, mean over its store-years (Figure 4, last panel; no shoppers
                involved, so nothing to leave out)
Output: interim/panel_dataset/shopping_hh_year.parquet / .dta
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import glob
import tarfile
import zipfile
import numpy as np
import pandas as pd

YEARS = range(2004, 2021)
OUT = idir / 'panel_dataset' / 'shopping_hh_year.parquet'
MIN_TRIPS, MIN_HH = 50, 10
GROUPS = {'Grocery': 'grocery', 'Discount Store': 'discount', 'Dollar Store': 'discount', 'Hypermarket': 'discount',
          'Warehouse Club': 'club', 'Drug Store': 'drug', 'Convenience Store': 'conv', 'Gas Mini Mart': 'conv', 'Service Station': 'conv'}
SHARES = ['grocery', 'discount', 'club', 'drug', 'conv', 'other']


def retailers():
    with tarfile.open(rdir / 'consumer' / 'Master_Files2004-2020.tgz') as tf:
        r = pd.read_csv(tf.extractfile('nielsen_extracts/HMS/Master_Files/Latest/retailers.tsv'), sep='\t')
    r['group'] = r['channel_type'].astype(str).str.strip().map(GROUPS).fillna('other')
    return r[['retailer_code', 'group']]


def one_year(y, ret):
    fs = glob.glob(str(idir / 'purchases_food' / f'panel_year={y}' / '*.parquet'))
    p = pd.concat([pd.read_parquet(f, columns=['household_code', 'trip_code_uc', 'retailer_code', 'store_code_uc',
                                               'total_price_paid_real_2013']) for f in fs], ignore_index=True)
    t = (p.groupby('trip_code_uc', sort=False)
          .agg(household_code=('household_code', 'first'), retailer_code=('retailer_code', 'first'),
               store_code_uc=('store_code_uc', 'first'), spend=('total_price_paid_real_2013', 'sum')).reset_index())
    del p
    t = t.merge(ret, on='retailer_code', how='left')
    t['group'] = t['group'].fillna('other')
    hh = t.groupby('household_code').agg(spend=('spend', 'sum'), trips=('trip_code_uc', 'size'))
    ch = t.pivot_table(index='household_code', columns='group', values='spend', aggfunc='sum', fill_value=0).reindex(hh.index, fill_value=0)
    for g in SHARES:
        hh['sh_' + g] = 100 * (ch[g] if g in ch else 0) / hh['spend'].where(hh['spend'] > 0)
    rt = t.groupby(['household_code', 'retailer_code'])['spend'].sum().reset_index()
    rt = rt.sort_values(['household_code', 'spend'], ascending=[True, False]).drop_duplicates('household_code')
    hh['main_retailer'] = rt.set_index('household_code')['retailer_code']
    s = (t[t['store_code_uc'] > 0].groupby(['household_code', 'store_code_uc'])
          .agg(spend=('spend', 'sum'), trips=('trip_code_uc', 'size')).reset_index())
    ms = s.sort_values(['household_code', 'spend'], ascending=[True, False]).drop_duplicates('household_code').set_index('household_code')
    hh['main_store'] = ms['store_code_uc']
    hh['main_share'] = 100 * ms['spend'] / hh['spend'].where(hh['spend'] > 0)
    hh = hh.reset_index().assign(panel_year=y)
    hh['spend_month'] = hh['spend'] / 12
    hh['trips_week'] = hh['trips'] / 52
    print(f"  {y}: {len(hh):,} households; ${hh.spend_month.mean():,.0f} a month; {hh.trips_week.mean():.2f} trips a week; "
          + ', '.join(f"{g} {hh['sh_' + g].mean():.1f}%" for g in SHARES) + f"; main RMS store {hh.main_store.notna().mean():.1%} of households, "
          f"{hh.main_share.mean():.0f}% of their food spending", flush=True)
    return hh, s[['household_code', 'store_code_uc', 'trips']].assign(panel_year=y)


def zip_tables():
    with zipfile.ZipFile(rdir / 'acs' / '2020_Gaz_zcta_national.zip') as z:
        g = pd.read_csv(z.open('2020_Gaz_zcta_national.txt'), sep='\t', dtype={'GEOID': str})
    g.columns = [c.strip() for c in g.columns]
    g = pd.DataFrame({'zip': g['GEOID'].astype(int), 'lat': g['INTPTLAT'].astype(float), 'lon': g['INTPTLONG'].astype(float)})
    a = pd.read_csv(rdir / 'acs' / 'acs5_2015_zcta_median_hh_income.csv', dtype={'zcta': str})
    a = pd.DataFrame({'zip': a['zcta'].astype(int), 'zinc': a['median_hh_income'] / 1000})
    return g.merge(a, on='zip', how='left')


def haversine(lat1, lon1, lat2, lon2):
    p1, p2, dl = np.radians(lat1), np.radians(lat2), np.radians(lon2 - lon1)
    a = np.sin((p2 - p1) / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))


def store_measures(S, pz, zt):
    """Leave-one-household-out store location, shopper ZIP income and modal ZIP, per (store, household), all years pooled."""
    S = S.merge(pz, on=['household_code', 'panel_year'], how='left').merge(zt, on='zip', how='left')
    S['loc'] = S['lat'].notna()
    S['inc'] = S['zinc'].notna()
    for c, f, w in [('slat', 'lat', 'loc'), ('slon', 'lon', 'loc'), ('sinc', 'zinc', 'inc')]:
        S[c] = S['trips'] * S[f].where(S[w], 0)
    S['nloc'] = S['trips'] * S['loc']
    S['ninc'] = S['trips'] * S['inc']
    agg = dict(slat=('slat', 'sum'), slon=('slon', 'sum'), sinc=('sinc', 'sum'), nloc=('nloc', 'sum'), ninc=('ninc', 'sum'))
    hs = S.groupby(['store_code_uc', 'household_code']).agg(**agg).reset_index()
    st = hs.groupby('store_code_uc').agg(T_slat=('slat', 'sum'), T_slon=('slon', 'sum'), T_sinc=('sinc', 'sum'), T_nloc=('nloc', 'sum'),
                                         T_ninc=('ninc', 'sum'), T_hh=('nloc', lambda x: (x > 0).sum())).reset_index()
    hs = hs.merge(st, on='store_code_uc')
    lo_n = hs['T_nloc'] - hs['nloc']
    lo_hh = hs['T_hh'] - (hs['nloc'] > 0)
    ok = (lo_n >= MIN_TRIPS) & (lo_hh >= MIN_HH)
    hs['lo_lat'] = ((hs['T_slat'] - hs['slat']) / lo_n).where(ok)
    hs['lo_lon'] = ((hs['T_slon'] - hs['slon']) / lo_n).where(ok)
    hs['store_zipinc'] = ((hs['T_sinc'] - hs['sinc']) / (hs['T_ninc'] - hs['ninc'])).where(ok & (hs['T_ninc'] - hs['ninc'] > 0))
    # modal ZIP of the other shoppers: the store's two ZIPs with the most trips, less the household's own trips from them
    zc = S[S['zip'].notna()].groupby(['store_code_uc', 'zip'])['trips'].sum().reset_index().sort_values(['store_code_uc', 'trips'], ascending=[True, False])
    zc['rank'] = zc.groupby('store_code_uc').cumcount()
    top = zc[zc['rank'] < 2].pivot(index='store_code_uc', columns='rank', values=['zip', 'trips'])
    top.columns = [f'{a}{b + 1}' for a, b in top.columns]
    top = top.reset_index()
    hz = S[S['zip'].notna()].groupby(['store_code_uc', 'household_code', 'zip'])['trips'].sum().reset_index()
    hs = hs.merge(top, on='store_code_uc', how='left')
    for k in ['1', '2']:
        own = hz.rename(columns={'zip': 'zip' + k, 'trips': 'own' + k})
        hs = hs.merge(own, on=['store_code_uc', 'household_code', 'zip' + k], how='left')
        hs['own' + k] = hs['own' + k].fillna(0)
    c1 = hs['trips1'] - hs['own1']
    c2 = (hs['trips2'] - hs['own2']).fillna(-1)
    hs['mode_zip'] = np.where(c1 >= c2, hs['zip1'], hs['zip2'])
    hs.loc[~ok, 'mode_zip'] = np.nan
    print(f"  store-household pairs {len(hs):,}; with leave-out store measures {ok.mean():.1%}")
    return hs[['store_code_uc', 'household_code', 'lo_lat', 'lo_lon', 'store_zipinc', 'mode_zip']]


def main():
    ret = retailers()
    print('Household-years...')
    parts = [one_year(y, ret) for y in YEARS]
    H = pd.concat([h for h, _ in parts], ignore_index=True)
    S = pd.concat([s for _, s in parts], ignore_index=True)
    del parts
    pz = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet', columns=['household_code', 'panel_year', 'panelist_zip_code'])
    pz = pz.rename(columns={'panelist_zip_code': 'zip'})
    zt = zip_tables()
    print('Store measures from other shoppers...')
    L = store_measures(S, pz, zt)
    H = H.merge(pz, on=['household_code', 'panel_year'], how='left').merge(zt[['zip', 'lat', 'lon']], on='zip', how='left')
    H = H.merge(L.rename(columns={'store_code_uc': 'main_store'}), on=['main_store', 'household_code'], how='left')
    H['dist_km'] = haversine(H['lat'], H['lon'], H['lo_lat'], H['lo_lon'])
    H['same_zip'] = np.where(H['mode_zip'].notna() & H['zip'].notna(), (H['mode_zip'] == H['zip']).astype(float), np.nan)
    hi = pd.read_parquet(idir / 'what_moves' / 'store_choice_sets.parquet', columns=['store_code_uc', 'hi_1000'])
    hi = hi.groupby('store_code_uc')['hi_1000'].mean().rename('store_hi').reset_index().rename(columns={'store_code_uc': 'main_store'})
    H = H.merge(hi, on='main_store', how='left')
    keep = (['household_code', 'panel_year', 'spend_month', 'trips_week'] + ['sh_' + g for g in SHARES]
            + ['main_store', 'main_share', 'main_retailer', 'dist_km', 'same_zip', 'store_zipinc', 'store_hi'])
    H = H[keep].sort_values(['household_code', 'panel_year'])
    assert not H.duplicated(['household_code', 'panel_year']).any()
    print(f"  {len(H):,} household-years; main RMS store {H.main_store.notna().mean():.1%}; distance {H.dist_km.notna().mean():.1%} "
          f"(median {H.dist_km.median():.1f} km); same ZIP {H.same_zip.mean():.1%} of {H.same_zip.notna().mean():.1%}; shopper ZIP income "
          f"{H.store_zipinc.notna().mean():.1%} (mean ${H.store_zipinc.mean():.1f}k); Health Index {H.store_hi.notna().mean():.1%} (mean {H.store_hi.mean():.2f})")
    H.to_parquet(OUT, index=False)
    H.to_stata(str(OUT.with_suffix('.dta')), write_index=False)
    print(f'Saved {OUT} and .dta')


if __name__ == '__main__':
    main()
