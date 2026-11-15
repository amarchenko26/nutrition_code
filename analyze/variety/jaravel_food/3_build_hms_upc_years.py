"""Homescan food barcodes bought in each year 2004-2020 (his available_upcs_<y>), for the new and exit flags of 4_.

Reads  interim/purchases_food/panel_year=<y>.
Writes interim/variety/jaravel_food/hms_upc_years.parquet (upc, year).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import *  # noqa: E402,F401,F403
import pyarrow.parquet as pq  # noqa: E402

out = []
for y in range(2004, 2021):
    fs = sorted((idir / 'purchases_food' / f'panel_year={y}').glob('*.parquet'))
    u = pd.concat([pq.read_table(f, columns=['upc']).column('upc').unique().to_pandas() for f in fs]).unique()
    out.append(pd.DataFrame({'upc': pd.Series(u).astype('int64'), 'year': y}))
    print(y, len(u), flush=True)
pd.concat(out, ignore_index=True).drop_duplicates().to_parquet(DATA / 'hms_upc_years.parquet', index=False)
