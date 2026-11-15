"""Homescan barcode x year panel 2004-2020 with Jaravel's price deciles (his 1d part I / 4b rule, current year), for E1-E3.

Reads  interim/purchases_food/panel_year=<y>, interim/panelists/panelists_all_years.parquet.
Writes interim/variety/jaravel_food/hms_upc_year.parquet (upc, product_module_code, product_group_code, year, spend, qty,
       price_adj, quality_rank): projection-weighted spending and quantity per barcode; price per size unit
       (size1_amount x multi); size1_amount trimmed at the pooled p1/p99 of the year's barcodes (quality_rank missing for
       trimmed barcodes); deciles within module by rank.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import pyarrow.parquet as pq  # noqa: E402

pan = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet',
                      columns=['household_code', 'panel_year', 'projection_factor'])
out = []
for y in range(2004, 2021):
    fs = sorted((idir / 'purchases_food' / f'panel_year={y}').glob('*.parquet'))
    pu = pd.concat([pq.read_table(f, columns=['upc', 'quantity', 'total_price_paid', 'product_module_code', 'product_group_code',
                                              'multi', 'size1_amount', 'household_code']).to_pandas() for f in fs], ignore_index=True)
    pu = pu[(pu['total_price_paid'] > 0) & (pu['quantity'] > 0)]
    pu['upc'] = pu['upc'].astype('int64')
    pu = pu.merge(pan.loc[pan.panel_year == y, ['household_code', 'projection_factor']], on='household_code', how='inner')
    pu['ws'] = pu['total_price_paid'] * pu['projection_factor']
    pu['wq'] = pu['quantity'] * pu['projection_factor']
    u = (pu.groupby(['upc', 'product_module_code'], as_index=False)
           .agg(product_group_code=('product_group_code', 'first'), spend=('ws', 'sum'), qty=('wq', 'sum'),
                size1_amount=('size1_amount', 'first'), multi=('multi', 'first')))
    del pu
    lo, hi = stata_pctile(u['size1_amount'], 1), stata_pctile(u['size1_amount'], 99)
    keep = (u['size1_amount'] >= lo) & (u['size1_amount'] <= hi)
    u['price_adj'] = u['spend'] / u['qty'] / (u['size1_amount'] * u['multi'])
    dec = assign_deciles(u[keep].copy(), 'price_adj')[['upc', 'quality_rank']]
    u = u.merge(dec, on='upc', how='left')
    u['year'] = y
    out.append(u[['upc', 'product_module_code', 'product_group_code', 'year', 'spend', 'qty', 'price_adj', 'quality_rank']])
    print(y, f"{len(u):,} UPCs, {keep.mean():.3f} kept by size trim, spending {u.spend.sum()/1e9:.1f}bn", flush=True)
pd.concat(out, ignore_index=True).to_parquet(DATA / 'hms_upc_year.parquet', index=False)
