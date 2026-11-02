"""
Stacked occupation-move panel: every dated occupation change is its own event,
so households that move more than once contribute all of their moves.
Reads interim/movers/occupation_movers_panel.parquet and occupation_means.csv (1_).

For an event (household i changes code between consecutive file years m-1 and m)
the window is the spell of years in the origin code before the change (back to
the previous change) and the spell in the destination code after it (up to the
next change, exclusive). A year between two changes therefore belongs to two
windows, as the post period of one event and the pre period of the next; the
Stata event study absorbs event fixed effects and clusters on household.
FGW timing as in 1_: r = 0 is file year m-1. Events whose origin or destination
is armed forces (7) or students (10) are dropped; a change across a gap in the
panel is a spell boundary but not an event.

Flags per event: single (the household's only change: the single movers of 1_),
first_event (the household's first change: the "including multiple movers"
variant of 4_), reversed (the next change returns to the origin code),
n_pre / n_post (years in the window), emp2emp, notemp.
delta_<outcome> = ybar_dest - ybar_orig from occupation_means.csv.

Output: interim/movers/stacked_moves_panel.parquet / .dta
"""

import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data') / 'interim' / 'movers'
EMPLOYED = {1, 2, 3, 4, 5, 6, 8, 9, 11}
DROP = {7, 10}
OUTCOMES = ['hei_usda', 'hi_usda', 'hi', 'real_income']


def main():
    df = pd.read_parquet(BASE / 'occupation_movers_panel.parquet')
    means = pd.read_csv(BASE / 'occupation_means.csv').set_index('occ')
    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    g = df.groupby('hhid')
    df['boundary'] = df['occ_prev'].notna() & (df['occ'] != df['occ_prev'])
    df['seg'] = g['boundary'].cumsum()
    n_bound = g['boundary'].transform('sum')

    ev = df[df['boundary'] & (df['gap'] == 1) & ~df['occ'].isin(DROP) & ~df['occ_prev'].isin(DROP)]
    ev = ev[['hhid', 'year', 'seg', 'occ_prev', 'occ']].rename(
        columns={'year': 'm_file', 'occ_prev': 'origin', 'occ': 'dest'}).reset_index(drop=True)
    ev['event_id'] = np.arange(1, len(ev) + 1)
    ev['single'] = (ev['hhid'].map(n_bound.groupby(df['hhid']).first()) == 1).astype(int)
    ev['first_event'] = (ev['seg'] == 1).astype(int)
    # next change of the same household: does it return to the origin code?
    nxt = df[df['boundary']][['hhid', 'seg', 'occ']].rename(columns={'occ': 'next_code'})
    nxt['seg'] -= 1
    ev = ev.merge(nxt, on=['hhid', 'seg'], how='left')
    ev['reversed'] = (ev['next_code'] == ev['origin']).astype(int)
    ev['emp2emp'] = (ev['origin'].isin(EMPLOYED) & ev['dest'].isin(EMPLOYED)).astype(int)
    ev['notemp'] = ((ev['origin'] == 12) | (ev['dest'] == 12)).astype(int)
    for v in OUTCOMES:
        ev[f'delta_{v}'] = ev['dest'].map(means[f'ybar_{v}']) - ev['origin'].map(means[f'ybar_{v}'])
    print(f'events: {len(ev):,} (single-mover events {ev.single.sum():,}, first moves {ev.first_event.sum():,}, '
          f'reversed {ev.reversed.sum():,}, employed to employed {ev.emp2emp.sum():,})')

    cols = ['hhid', 'year', 'seg', 'occ', 'projection_factor', 'age5', 'kids', 'hh_comp', 'avg_age_hh_head',
            'educ_bin', 'size_bin', 'state'] + [v for v in OUTCOMES if v != 'real_income'] + ['real_income']
    evcols = ['event_id', 'hhid', 'seg', 'm_file', 'origin', 'dest', 'single', 'first_event', 'reversed',
              'emp2emp', 'notemp'] + [f'delta_{v}' for v in OUTCOMES]
    pre = df[cols].merge(ev[evcols].assign(seg=ev['seg'] - 1), on=['hhid', 'seg'])
    post = df[cols].merge(ev[evcols], on=['hhid', 'seg'])
    st = pd.concat([pre, post], ignore_index=True)
    st['r'] = st['year'] - (st['m_file'] - 1)
    st = st[~st['occ'].isin(DROP)]
    n_pre = st[st['r'] <= 0].groupby('event_id').size().rename('n_pre')
    n_post = st[st['r'] >= 1].groupby('event_id').size().rename('n_post')
    st = st.merge(n_pre, on='event_id', how='left').merge(n_post, on='event_id', how='left')
    st['n_pre'] = st['n_pre'].fillna(0).astype(int)
    st['n_post'] = st['n_post'].fillna(0).astype(int)
    st = st.sort_values(['event_id', 'year']).reset_index(drop=True)
    print(f'stacked rows: {len(st):,}; relative-year distribution: '
          f'{st["r"].clip(-8, 8).value_counts().sort_index().to_dict()}')
    st.to_parquet(BASE / 'stacked_moves_panel.parquet', index=False)
    st.to_stata(BASE / 'stacked_moves_panel.dta', write_index=False, version=118)
    print(f'saved {BASE / "stacked_moves_panel.dta"}')


if __name__ == '__main__':
    main()
