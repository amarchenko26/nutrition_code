"""Household-type growth in the ACS, Jaravel (2019) 4c_household_shocks_building.do part 1.

Reads  raw/acs/usa_00003.csv (IPUMS ACS 2000-2004 and 2012-2016).
Writes interim/variety/jaravel_food/acs_groups.parquet: one row per household type with households in both windows.
Rules (his): household heads (RELATED == 101); types = 12 age bins (20-25, 26-30, ..., 71-75, 76+; under 20 dropped)
x 5 education (EDUCD <62 / <65 / <101 / <114 / else) x 3 race (white, black, other) x children (NCHILD > 0) x state;
P = sum of HHWT over each five-year window; g = log(P_late) - log(P_early); annual = g / 12.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import pyarrow.csv as pacsv  # noqa: E402

cols = ['YEAR', 'HHWT', 'STATEFIP', 'RELATED', 'AGE', 'RACE', 'EDUCD', 'NCHILD']
acs = pacsv.read_csv(rdir / 'acs' / 'usa_00003.csv', convert_options=pacsv.ConvertOptions(include_columns=cols)).to_pandas()
print(f"ACS rows: {len(acs):,}")
acs = acs[acs['RELATED'] == 101]
acs = acs[acs['YEAR'].between(2000, 2004) | acs['YEAR'].between(2012, 2016)].copy()
acs['period'] = np.where(acs['YEAR'] <= 2004, 1, 2)
acs = acs.dropna(subset=['EDUCD', 'RACE', 'NCHILD', 'AGE'])
e = acs['EDUCD']
acs['education'] = np.select([e < 62, e < 65, e < 101, e < 114], [1, 2, 3, 4], 5)
acs['race'] = np.select([acs['RACE'] == 1, acs['RACE'] == 2], [1, 2], 3)
acs['children'] = (acs['NCHILD'] > 0).astype(int)
acs = acs[acs['AGE'] >= 20]
acs['age_bin'] = np.searchsorted(np.array([26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76]), acs['AGE'].to_numpy(), side='right') + 1
acs['statefip'] = acs['STATEFIP'].astype(int)

keys = ['age_bin', 'education', 'statefip', 'race', 'children']
P = acs.groupby(keys + ['period'])['HHWT'].sum().unstack('period')
P.columns = ['P_initial', 'P']
P = P.dropna()
P = P[(P['P_initial'] > 0) & (P['P'] > 0)]
P['g_n'] = np.log(P['P']) - np.log(P['P_initial'])
P['g_n_annual'] = P['g_n'] / (2016 - 2004)
P = P.reset_index()
print(f"household types with both windows: {len(P):,}")
P.to_parquet(DATA / 'acs_groups.parquet', index=False)
