"""Shift-share instrument by food module x price decile, Jaravel (2019) 4b, 4c part 2 and 4d, on Homescan food purchases.

Reads  interim/purchases_food/panel_year=2004..2006, interim/panelists/panelists_all_years.parquet,
       interim/variety/jaravel_food/acs_groups.parquet.
Writes interim/variety/jaravel_food/instrument_cells.parquet (product_module_code, quality_rank, inst_raw, inst_linageres,
       inst_allres, total_spend_HMS0406, n_groups).
Steps (his): (1) each of 2004-2006: barcode price per size unit from projection-weighted spending and quantity; size1_amount
trimmed at the pooled p1/p99; deciles within module.  (2) household types: average age of the heads from birth years (under 20
dropped), 12 bins; maximum education of the heads; race; children; state; spending x projection factor by type x module x
decile, summed over 2004-2006.  (3) ACS type growth restricted to types seen in Homescan 2004-06, residualized on a linear
age trend (linageres) or all type dummies (allres), weights log((P+P_initial)/2), plus the weighted mean.  (4) instrument =
sum over types of the type's share of the cell's spending (types matched to ACS) x growth.
Homescan spending is not net of coupons (purchases_food has no coupon column).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import pyarrow.parquet as pq  # noqa: E402

pan_all = pd.read_parquet(idir / 'panelists' / 'panelists_all_years.parquet', columns=[
    'household_code', 'panel_year', 'projection_factor', 'male_head_birth', 'female_head_birth',
    'male_head_education', 'female_head_education', 'race', 'age_and_presence_of_children', 'fips_state_code'])
keys = ['statefip', 'age_bin', 'education', 'race', 'children']
cell_spend = []
for y in [2004, 2005, 2006]:
    fs = sorted((idir / 'purchases_food' / f'panel_year={y}').glob('*.parquet'))
    pu = pd.concat([pq.read_table(f, columns=['upc', 'quantity', 'total_price_paid', 'product_module_code',
                                              'multi', 'size1_amount', 'household_code']).to_pandas() for f in fs], ignore_index=True)
    pu['upc'] = pu['upc'].astype('int64')
    pu = pu[(pu['total_price_paid'] > 0) & (pu['quantity'] > 0)]
    hu = (pu.groupby(['household_code', 'upc', 'product_module_code'], as_index=False)
            .agg(total_spending=('total_price_paid', 'sum'), total_quantity=('quantity', 'sum')))
    size = pu.groupby('upc', as_index=False).agg(size1_amount=('size1_amount', 'first'), multi=('multi', 'first'))
    del pu
    pan = pan_all[pan_all['panel_year'] == y].drop(columns='panel_year')
    hu = hu.merge(pan[['household_code', 'projection_factor']], on='household_code', how='inner')
    hu['ws'] = hu['total_spending'] * hu['projection_factor']
    hu['wq'] = hu['total_quantity'] * hu['projection_factor']
    u = hu.groupby(['upc', 'product_module_code'], as_index=False)[['ws', 'wq']].sum()
    u['average_unit_price_all'] = u['ws'] / u['wq']
    u = u.merge(size, on='upc', how='inner')
    lo, hi = stata_pctile(u['size1_amount'], 1), stata_pctile(u['size1_amount'], 99)
    u = u[(u['size1_amount'] >= lo) & (u['size1_amount'] <= hi)].copy()
    u['average_unit_price_adj'] = u['average_unit_price_all'] / (u['size1_amount'] * u['multi'])
    u = assign_deciles(u, 'average_unit_price_adj')
    hu = hu.merge(u[['upc', 'quality_rank']], on='upc', how='inner')
    p = pan.copy()
    ma, fa = y - p['male_head_birth'], y - p['female_head_birth']
    p['age'] = ((ma + fa) / 2).where(ma.notna() & fa.notna(), ma.where(ma.notna(), fa))
    p = p.dropna(subset=['age'])
    p = p[p['age'] >= 20]
    p['age_bin'] = np.searchsorted(np.array([26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76], dtype=float), p['age'].to_numpy(), side='right') + 1
    p['education_old'] = p[['male_head_education', 'female_head_education']].max(axis=1)
    p = p[p['education_old'] != 0]
    p['education'] = p['education_old'].map({1: 1, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5})
    p = p.dropna(subset=['race', 'age_and_presence_of_children'])
    p['race'] = np.select([p['race'] == 1, p['race'] == 2], [1, 2], 3)
    p['children'] = (p['age_and_presence_of_children'] < 9).astype(int)
    p['statefip'] = p['fips_state_code']
    hu = hu.merge(p[['household_code'] + keys], on='household_code', how='inner')
    cs = hu.groupby(keys + ['product_module_code', 'quality_rank'], as_index=False)['ws'].sum().rename(columns={'ws': 'total_spending'})
    cell_spend.append(cs)
    print(f"{y}: {len(u):,} barcodes after the size trim; {hu['household_code'].nunique():,} households with a type", flush=True)
    del hu

cs = pd.concat(cell_spend, ignore_index=True).groupby(keys + ['product_module_code', 'quality_rank'], as_index=False)['total_spending'].sum()
used = cs[keys].drop_duplicates()
g = pd.read_parquet(DATA / 'acs_groups.parquet').merge(used, on=keys, how='inner')
w = np.log((g['P'] + g['P_initial']) / 2)
y_ = g['g_n_annual'].to_numpy()
mean_g = np.average(y_, weights=w)


def resid(X):
    X = np.column_stack([np.ones(len(g)), X])
    sw = np.sqrt(w.to_numpy())
    b, *_ = np.linalg.lstsq(X * sw[:, None], y_ * sw, rcond=None)
    return y_ - X @ b + mean_g


g['g_n_annual_linageres'] = resid(g['age_bin'].to_numpy(dtype=float))
g['g_n_annual_allres'] = resid(pd.get_dummies(g[['age_bin', 'education', 'statefip', 'race', 'children']].astype(str), drop_first=True).to_numpy(dtype=float))
for c in ['g_n_annual', 'g_n_annual_linageres', 'g_n_annual_allres']:
    m = np.average(g[c], weights=w)
    sd = np.sqrt(np.average((g[c] - m) ** 2, weights=w) * len(g) / (len(g) - 1))
    print(f"{c:24s} types {len(g):,}  mean {m:.4f}  sd {sd:.4f}   (his Table IV A: 7,326 types; raw 0.021 / 0.041)")

m = cs.merge(g[keys + ['g_n_annual', 'g_n_annual_linageres', 'g_n_annual_allres']], on=keys, how='inner')
m['s_nl'] = m['total_spending'] / m.groupby(['product_module_code', 'quality_rank'])['total_spending'].transform('sum')
for x, c in [('raw', 'g_n_annual'), ('linageres', 'g_n_annual_linageres'), ('allres', 'g_n_annual_allres')]:
    m[f'inst_{x}'] = m['s_nl'] * m[c]
inst = (m.groupby(['product_module_code', 'quality_rank'], as_index=False)
          .agg(inst_raw=('inst_raw', 'sum'), inst_linageres=('inst_linageres', 'sum'), inst_allres=('inst_allres', 'sum'),
               total_spend_HMS0406=('total_spending', 'sum'), n_groups=('s_nl', 'size')))
inst.to_parquet(DATA / 'instrument_cells.parquet', index=False)
print(f"instrument cells: {len(inst):,} in {inst['product_module_code'].nunique():,} modules")
