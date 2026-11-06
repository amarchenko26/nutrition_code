"""Income-quintile movers against matched non-movers (2026-11-06, Anya's comment on the income movers figure).

The matched non-movers of an event are households that never change quintile (mover_type 0 of income_movers_panel) in the event's
ORIGIN quintile. For each year of the event, the outcome loses the cumulated year-to-year change of the same non-moving households:
their projection-weighted mean change from t-1 to t, in the event's cell at t-1 (origin x year x education bin x size bin x 5-year age
bin, cells with 20+ pairs; fallback origin x year x age bin, then origin x year). Changes of the same households, because the mean
LEVEL of a cell also moves when different households enter it, most of all when the panel grows in 2007.
hei_lev is the mean-level version, the rule of 3_figure_movers_change_vs_delta.py, kept as a comparison.

`do 4_build_movers_event_study.do income_matched` runs the regressions of the income movers on these outcomes, without year fixed effects.
Output: interim/movers/income_matched_movers_events_panel.dta
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

MV = idir / 'movers'
OUTCOMES = ['hei_usda', 'real_income']
ID = ['event_id', 'hhid', 'year', 'r', 'origin', 'up_income', 'projection_factor', 'age5', 'kids', 'hh_comp', 'educ_bin', 'size_bin']
MIN_PAIRS = 20


def mean_by(d, keys, y, w, minn=1):
    d = d.dropna(subset=[y])
    t = d.assign(wy=d[y] * d[w]).groupby(keys).agg(wy=('wy', 'sum'), w=(w, 'sum'), n=('wy', 'size'))
    t = t[t['n'] >= minn]
    return (t['wy'] / t['w']).rename('m').reset_index()


def chained(ev, pairs, y):
    """Cumulated change of the matched non-movers along each event; the first year of the event is 0."""
    d = f'd_{y}'
    full, mid, lo = ['year', 'origin', 'pe', 'ps', 'pa'], ['year', 'origin', 'pa'], ['year', 'origin']
    e = ev.merge(mean_by(pairs, full, d, 'pw', MIN_PAIRS).rename(columns={'m': 'mf'}), on=full, how='left') \
          .merge(mean_by(pairs, mid, d, 'pw', MIN_PAIRS).rename(columns={'m': 'mm'}), on=mid, how='left')
    bylo = mean_by(pairs, lo, d, 'pw')
    e = e.merge(bylo.rename(columns={'m': 'ml'}), on=lo, how='left')
    has_prev = e['py'].notna()
    print(f"{y}: yearly steps on the full cell {e.loc[has_prev, 'mf'].notna().mean():.3f}")
    step = e['mf'].fillna(e['mm']).fillna(e['ml']).fillna(0.0).values
    # years the event skips: the origin x year change of each
    L = bylo.set_index(['origin', 'year'])['m']
    for i in np.where((e['year'] - e['py']).fillna(1).values > 1)[0]:
        step[i] += sum(L.get((e['origin'].iat[i], a), 0.0) for a in range(int(e['py'].iat[i]) + 1, int(e['year'].iat[i])))
    step[~has_prev.values] = 0.0
    return pd.Series(step).groupby(e['event_id'].values).cumsum().values


def main():
    ev = pd.read_parquet(MV / 'income_movers_events_panel.parquet', columns=ID + OUTCOMES + ['delta_hei_usda', 'delta_real_income'])
    ev = ev.sort_values(['event_id', 'year']).reset_index(drop=True)
    df = pd.read_parquet(MV / 'income_movers_panel.parquet', columns=['hhid', 'year', 'occ', 'mover_type', 'analysis', 'educ_bin', 'size_bin', 'age5', 'projection_factor'] + OUTCOMES)
    nm = df[(df['analysis'] == 1) & (df['mover_type'] == 0)].rename(columns={'occ': 'origin'}).sort_values(['hhid', 'year'])
    g = nm.groupby('hhid')
    pairs = nm.assign(py=g['year'].shift(1), pe=g['educ_bin'].shift(1), ps=g['size_bin'].shift(1), pa=g['age5'].shift(1), pw=g['projection_factor'].shift(1),
                      **{f'd_{y}': nm[y] - g[y].shift(1) for y in OUTCOMES})
    pairs = pairs[pairs['year'] - pairs['py'] == 1]
    print(f"never-movers: {nm['hhid'].nunique():,} households, {pairs['hhid'].nunique():,} with consecutive years, {len(pairs):,} pairs; events {ev['event_id'].nunique():,}, event-years {len(ev):,}")

    # the level version, before the outcome is replaced
    m = None
    for keys in [['year', 'origin', 'educ_bin', 'size_bin', 'age5'], ['year', 'origin', 'age5'], ['year', 'origin']]:
        c = ev[keys].merge(mean_by(nm, keys, 'hei_usda', 'projection_factor'), on=keys, how='left')['m'].values
        m = c if m is None else np.where(np.isnan(m), c, m)
    ev['hei_lev'] = ev['hei_usda'] - m

    gg = ev.groupby('event_id')
    ev['py'], ev['pe'], ev['ps'], ev['pa'] = gg['year'].shift(1), gg['educ_bin'].shift(1), gg['size_bin'].shift(1), gg['age5'].shift(1)
    for y in OUTCOMES:
        ev[y] = ev[y] - chained(ev, pairs, y)
    ev = ev.drop(columns=['educ_bin', 'origin', 'py', 'pe', 'ps', 'pa'])
    for c in ev.columns:
        if ev[c].dtype.name in ('object', 'category'):
            ev[c] = pd.to_numeric(ev[c], errors='coerce')
    ev.to_stata(MV / 'income_matched_movers_events_panel.dta', write_index=False, version=118)
    print('saved income_matched_movers_events_panel.dta', ev.shape)


if __name__ == '__main__':
    main()
