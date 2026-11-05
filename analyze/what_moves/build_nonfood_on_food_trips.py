"""Non-food spending on the trips that contain food, per household-year.

For the employment-event studies (build_time_events.do): if households scan less carefully when
they work more, recorded non-food items should fall along with recorded food; if they buy less food
for home because they eat more meals elsewhere, non-food items should hold up and the food share of
the trip total should fall. Reads interim/purchases_food/panel_year=<y>/ (one row per trip-UPC,
total_spent = the trip total carried on every row, real versions in 2013 dollars, MAGNET rows
dropped). Trips with no food item are not in that file, so non-food purchased on non-food trips is
not counted.

Per household-year: food_real (sum of real item prices), trip_total_real (sum of real trip totals),
nonfood_real = sum over trips of max(trip total - food on the trip, 0), n_food_trips.
Output: interim/what_moves/nonfood_on_food_trips.parquet
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import gc
import time
import pandas as pd

OUT = idir / 'what_moves' / 'nonfood_on_food_trips.parquet'
out = []
t0 = time.time()
for y in range(2004, 2021):
    p = idir / 'purchases_food' / f'panel_year={y}'
    if not p.exists():
        continue
    d = pd.read_parquet(p, columns=['trip_code_uc', 'household_code', 'total_price_paid_real_2013', 'total_spent_real_2013', 'department_descr'])
    d = d[d['department_descr'] != 'MAGNET DATA']
    t = d.groupby('trip_code_uc', sort=False).agg(hh=('household_code', 'first'), food=('total_price_paid_real_2013', 'sum'), tot=('total_spent_real_2013', 'first'))
    t['nonfood'] = (t['tot'] - t['food']).clip(lower=0)
    h = t.groupby('hh').agg(food_real=('food', 'sum'), trip_total_real=('tot', 'sum'), nonfood_real=('nonfood', 'sum'), n_food_trips=('food', 'size')).reset_index().rename(columns={'hh': 'household_code'})
    h['panel_year'] = y
    out.append(h)
    print(f'{y}: {len(d):,} rows, {len(t):,} trips, {len(h):,} households; mean food share of trip totals {h.food_real.sum() / h.trip_total_real.sum():.3f} ({time.time() - t0:.0f}s)', flush=True)
    del d, t; gc.collect()
res = pd.concat(out, ignore_index=True)
OUT.parent.mkdir(parents=True, exist_ok=True)
res.to_parquet(OUT, index=False)
print(f'saved {OUT}: {len(res):,} household-years')
