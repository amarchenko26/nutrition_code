"""
Build the occupation-movers panels for the Finkelstein-Gentzkow-Williams (2016)
design with occupations as "places" and nutrition as the outcome.

Input:  final/labor_supply_reg_data.dta (household x panel year)
Output: interim/movers/occupation_movers_panel.parquet       household-year panel (descriptives)
        interim/movers/movers_events_panel.parquet / .dta    stacked event panel (estimation sample)
        interim/movers/occupation_means.csv                  mean outcome by occupation, y_j
The household-year panel also carries the life-event switches for 8_build_life_event_studies.do:
birth (r_birth/birth_grp), death of a head (r_death/death_grp) and an education increase
(r_educ/educ_grp, delta_educ_hei); first consecutive-year change of the state, panel cut
before the state's next change; never-changers are the comparison group.

Occupation: the household's assigned Nielsen occupation code -- the male head's
unless he is absent (0) or in the armed forces (7), then the female head's (the
paper's cell assignment). Codes: 1 professional, 2 manager/owner, 3 clerical,
4 sales, 5 craftsman, 6 operative/driver, 7 armed forces, 8 service, 9 farmer,
10 student employed <30h, 11 laborer, 12 not employed.

Sample: geographic non-movers (movers_f == 0), as in the paper's regressions.
The descriptive figures use all 12 codes; events drop armed forces (7) and
students (10), which are tiny.

Reversals (2026-11-02): a code that changes in year t and returns to the year
t-1 code in year t+1 (three consecutive observed years) is treated as
measurement error: occ_t is set back to the t-1 code (`occ`; the reported code
is kept as `occ_raw`). A household whose only changes are such blips is a
non-mover.

Events (2026-11-02, main analysis = moves within employment, all movers): every
change of the smoothed code between two consecutive panel years with origin and
destination both employed codes (1-6, 8, 9, 11) is an event (emp2emp = 1);
changes into or out of not employed (12) are built as events too (emp2emp = 0)
but are not the main sample. The event window is the origin spell back to the
previous change (or panel start) and the destination spell up to the next
change (or panel end), so a household that moves twice contributes two events
and the years between its moves sit in both windows; the Stata event study
absorbs event fixed effects and clusters on household. A household with one
move has one event and the design reduces to FGW's single-mover design.

Timing: the year-t panelist file carries the demographics reported in the
NielsenIQ survey of the fall of t-1 (Consumer Panel Dataset Manual 2024,
Panelists file, p. 21; FAQ 9, p. 59), so a code first visible in file year m
reflects a move during calendar year m-1. Following FGW, relative year r = 0
is the (partially treated) move year, i.e. file year m-1; r = -1 is the last
clean pre-move year (file m-2) and r = 1 the first clean post-move year (file m).

delta_<outcome> = y_dest - y_orig (also for real_income, $10k), where y_j is the
projection-weighted mean of the outcome by occupation and year, averaged across
years, on household-years outside any event's r = 0 (as in FGW).

INCOME MOVERS (`--places income`, Anya 2026-11-05): the same build with within-year
income QUINTILES of household income (real_income, projection-weighted cut points
by year) as the places, so a move is a change of quintile between consecutive
panel years (one-year reversals smoothed as above), every dated move an event,
delta = destination minus origin quintile mean (HEI, or income). No employed /
not-employed distinction (emp2emp = 1, notemp = 0 for every event). Outputs
income_movers_events_panel.parquet/.dta, income_quintile_means.csv and
income_movers_panel.parquet/.dta in interim/movers/.

RETAILER MOVERS (`--places retailer`, 2026-11-05): the place is the household's main retailer of the year
(analyze/what_moves/build_retailer_year.py), geographic non-movers, retailers with 100+ household-years, r = 0 is
file year m. Outputs retailer_movers_events_panel.parquet/.dta, retailer_means.csv, retailer_movers_panel.

COUNTY MOVERS (`--places county`, 2026-11-05, place effects for the 'what moves nutrition'
review): the places are COUNTIES (fips), geographic movers are KEPT (they are the events),
every dated county change between consecutive panel years is an event (one-year reversals
smoothed as above), delta = destination minus origin county mean of each outcome on
household-years outside move years; counties with fewer than MIN_COUNTY_HH_YEARS such
household-years get missing means, so events touching them drop out of the Stata run.
County timing differs from the occupation timing above: the county code in file m is the
address at the end of year m, not at the fall m-1 survey. Among county moves whose main
shopping ZIP3 changes (store_zip3 of the household's trips), 5.5% of trips are at the new
ZIP3 in file m-1, 45% in file m and 89% in file m+1. So r = 0 is file year m (the year the
move happens, half its trips on each side), r = -1 the last full year before, r = 1 the
first full year after. Income (fall survey) therefore first shows the move at r = 1.
Outputs county_movers_events_panel.parquet/.dta, county_means.csv, county_movers_panel.
"""

import argparse
import numpy as np
import pandas as pd
import pyreadstat
from pathlib import Path

BASE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
IN = BASE / 'final' / 'labor_supply_reg_data.dta'
OUT_DIR = BASE / 'interim' / 'movers'

# outcomes: nutrition indices, the sub-outcomes added 2026-11-05 at Anya's ask (sugar g and sodium mg per
# 1,000 kcal, whole-grain share %, soda and produce shares of grocery spending %, all from the regression
# data; calories per adult equivalent per day (UPC-dictionary densities, winsorized at p1/p99),
# ultra-processed share of calories % and produce share of calories % from
# interim/health/nova_calorie_panel.parquet, complete rosters only), and real_income in $10k (2013$)
OUTCOMES_DTA = ['hei_usda', 'hi_usda', 'hi', 'sugar_1000', 'sodium_1000', 'whole_share', 'soda_spend_share', 'produce_spend_share']
OUTCOMES_NOVA = ['cals_ae_day', 'upf_share', 'produce_share']
OUTCOMES = OUTCOMES_DTA + OUTCOMES_NOVA + ['real_income']
NOVA = BASE / 'interim' / 'health' / 'nova_calorie_panel.parquet'
OCC_LABELS = {1: 'Professional', 2: 'Manager/owner', 3: 'Clerical', 4: 'Sales',
              5: 'Craftsman', 6: 'Operative/driver', 7: 'Armed forces',
              8: 'Service', 9: 'Farmer', 10: 'Student <30h', 11: 'Laborer',
              12: 'Not employed'}
EMPLOYED = {1, 2, 3, 4, 5, 6, 8, 9, 11}
DROP_FROM_ANALYSIS = {7, 10}
MIN_COUNTY_HH_YEARS = 100   # county means (places = county) need this many household-years outside move years

# cell bins as in build/iv/build_iv.py
EDUC_BINS = [0, 11.9, 12.9, 15.9, np.inf]
SIZE_BINS = [0, 1, 2, 4, np.inf]

COLS = ['hhid', 'year', 'male_head_occupation', 'female_head_occupation',
        'hh_avg_yrsofschool', 'hh_size', 'avg_age_hh_head', 'kids', 'hh_comp',
        'fips_state_code', 'fips', 'zip_code', 'projection_factor', 'real_income',
        'movers_f', 'n_heads', 'marital_status', 'hei_usda', 'hi_usda', 'hi', 'sugar_per_1000cal_usda',
        'sodium_per_1000cal_usda', 'whole_usda', 'spend_soda', 'spend_total', 'spend_share_produce']


def wmean(x, w):
    ok = x.notna() & w.notna()
    return np.average(x[ok], weights=w[ok]) if ok.any() else np.nan


def first_switch_events(df, state, name, is_event):
    """First change of `state` over the household's observed years (8_build_life_event_studies.do).
    The household is a `name` mover if that first change is between consecutive years and
    satisfies is_event (a function of the row, with _prev = previous state, _size_prev =
    previous household size, _nheads_prev = previous number of heads); r_<name> follows the
    FGW timing (r = 0 is file year m-1) and is cut before the state's next change.
    <name>_grp: 1 mover, 0 never-changer, missing otherwise."""
    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    g = df.groupby('hhid')
    df['_prev'] = g[state].shift(1)
    df['_size_prev'] = g['hh_size'].shift(1)
    df['_nheads_prev'] = g['n_heads'].shift(1)
    df['_chg'] = df['_prev'].notna() & (df[state] != df['_prev'])
    df['_chgno'] = g['_chg'].cumsum()
    n_chg = g['_chg'].transform('sum')
    first = df[df['_chg'] & (df['_chgno'] == 1)]
    ok = is_event(first) & (first['gap'] == 1)
    ev = first.loc[ok, ['hhid', 'year', '_prev', state]].rename(
        columns={'year': f'm_{name}', '_prev': f'{name}_orig', state: f'{name}_dest'})
    df = df.merge(ev, on='hhid', how='left')
    df[f'r_{name}'] = df['year'] - (df[f'm_{name}'] - 1)
    df.loc[df['_chgno'] >= 2, f'r_{name}'] = np.nan
    df[f'{name}_grp'] = np.where(df[f'm_{name}'].notna(), 1.0, np.where(n_chg == 0, 0.0, np.nan))
    return df.drop(columns=['_prev', '_size_prev', '_nheads_prev', '_chg', '_chgno'])


def income_quintiles(df):
    """Within-year quintile (1-5) of real_income, projection-weighted cut points; households in the
    same income bracket always share a quintile (the cut point is the smallest income whose
    weighted CDF reaches k/5)."""
    out = pd.Series(np.nan, index=df.index)
    for y, x in df.groupby('year'):
        byinc = x.groupby('real_income')['projection_factor'].sum().sort_index()
        cdf = byinc.cumsum() / byinc.sum()
        cuts = [byinc.index[np.searchsorted(cdf.values, k / 5)] for k in range(1, 5)]
        out.loc[x.index] = 1 + np.searchsorted(np.array(cuts), x['real_income'].values, side='right')
    return out


def smooth_reversals(df):
    """occ_t -> occ_{t-1} when occ_t != occ_{t-1} and occ_{t+1} == occ_{t-1}, with
    t-1, t, t+1 consecutive observed years of the same household. Sequential, so
    a smoothed value feeds the next comparison (A,B,A,B,A -> A,A,A,A,A)."""
    hh = df['hhid'].values
    yr = df['year'].values
    occ = df['occ'].values.copy()
    raw = df['occ'].values
    n_blip = 0
    for i in range(1, len(occ) - 1):
        if hh[i - 1] == hh[i] == hh[i + 1] and yr[i] - yr[i - 1] == 1 and yr[i + 1] - yr[i] == 1 \
                and raw[i] != occ[i - 1] and raw[i + 1] == occ[i - 1]:
            occ[i] = occ[i - 1]
            n_blip += 1
    return occ, n_blip


def main(places='occupation'):
    global OCC_LABELS, EMPLOYED, DROP_FROM_ANALYSIS
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    prefix = {'occupation': '', 'income': 'income_', 'county': 'county_', 'retailer': 'retailer_'}[places]
    if places == 'income':
        OCC_LABELS = {k: f'Q{k}' for k in range(1, 6)}
        EMPLOYED = set(range(1, 6))
        DROP_FROM_ANALYSIS = set()
    if places in ('county', 'retailer'):
        OCC_LABELS = {}
        EMPLOYED = set()
        DROP_FROM_ANALYSIS = set()
    df, _ = pyreadstat.read_dta(str(IN), usecols=COLS)
    df['hhid'] = df['hhid'].astype('int64')
    df['year'] = df['year'].astype(int)
    print(f'loaded {len(df):,} household-years')
    df['sugar_1000'] = df['sugar_per_1000cal_usda']
    df['sodium_1000'] = 1000 * df['sodium_per_1000cal_usda']
    df['whole_share'] = 100 * df['whole_usda']
    df['soda_spend_share'] = 100 * df['spend_soda'] / df['spend_total']
    df['produce_spend_share'] = 100 * df['spend_share_produce']
    nova = pd.read_parquet(NOVA, columns=['household_code', 'panel_year', 'cals_all', 'cals_nova1', 'cals_nova2', 'cals_nova3',
                                          'cals_nova4', 'produce_cals_all', 'ae_total', 'ae_complete'])
    nova = nova[nova['ae_complete'] == 1].copy()
    nova['cals_ae_day'] = nova['cals_all'] / nova['ae_total'] / 365
    lo, hi = nova['cals_ae_day'].quantile([0.01, 0.99])
    nova['cals_ae_day'] = nova['cals_ae_day'].clip(lo, hi)
    nova['upf_share'] = 100 * nova['cals_nova4'] / nova[['cals_nova1', 'cals_nova2', 'cals_nova3', 'cals_nova4']].sum(axis=1)
    nova['produce_share'] = 100 * nova['produce_cals_all'] / nova['cals_all']
    nova = nova.rename(columns={'household_code': 'hhid', 'panel_year': 'year'})[['hhid', 'year'] + OUTCOMES_NOVA]
    nova['hhid'] = nova['hhid'].astype('int64'); nova['year'] = nova['year'].astype(int)
    df = df.merge(nova, on=['hhid', 'year'], how='left')
    print('sub-outcome means:', df[OUTCOMES_DTA[3:] + OUTCOMES_NOVA].mean().round(2).to_dict())

    # --------------------------------------------------- assigned occupation
    if places != 'county':
        df = df[df['movers_f'] == 0].copy()          # county movers ARE the events for places = county
    if places == 'occupation':
        m = df['male_head_occupation'].astype(float)
        f = df['female_head_occupation'].astype(float)
        df['occ'] = m.where(~(m.isna() | m.isin([0, 7])), f)
        df = df[df['occ'].notna() & (df['occ'] > 0)].copy()
    elif places == 'retailer':
        # main retailer of the year (largest share of food spending); geographic non-movers, so a switch is not a house move
        rt = pd.read_parquet(BASE / 'interim' / 'what_moves' / 'hh_year_main_retailer.parquet').rename(columns={'household_code': 'hhid', 'panel_year': 'year'})
        df = df.merge(rt[['hhid', 'year', 'retailer_code']], on=['hhid', 'year'], how='inner')
        df['occ'] = pd.to_numeric(df['retailer_code'], errors='coerce')
        df = df[df['occ'].notna()].copy()
    elif places == 'county':
        df['occ'] = pd.to_numeric(df['fips'], errors='coerce')
        df = df[df['occ'].notna()].copy()
        print(f"households that ever change county: {df[df['movers_f'] == 1]['hhid'].nunique():,} of {df['hhid'].nunique():,}")
    else:
        df = df[df['real_income'].notna() & df['projection_factor'].notna()].copy()
        df['occ'] = income_quintiles(df)
        print('household-years by income quintile:', df['occ'].value_counts().sort_index().to_dict())
    df['occ'] = df['occ'].astype(int)
    print(f'geographic non-movers with an occupation code: {len(df):,} '
          f'household-years, {df.hhid.nunique():,} households')

    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    df['occ_raw'] = df['occ']
    df['occ'], n_blip = smooth_reversals(df)
    print(f'one-year reversals set back to the origin code: {n_blip:,} household-years '
          f'({100 * n_blip / len(df):.2f}% of rows)')

    g = df.groupby('hhid')
    df['n_obs'] = g['year'].transform('size')
    df['gap'] = g['year'].diff()                       # years since previous obs
    df['occ_prev'] = g['occ'].shift(1)
    df['switch'] = (df['occ_prev'].notna()) & (df['occ'] != df['occ_prev'])
    df['switch_consec'] = df['switch'] & (df['gap'] == 1)
    df['n_switch'] = g['switch'].transform('sum').astype(int)
    df['seg'] = g['switch'].cumsum()                   # spell number: 0 before the first change

    # ------------------------------------------------------------ bins
    df['educ_bin'] = pd.cut(df['hh_avg_yrsofschool'], bins=EDUC_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['size_bin'] = pd.cut(df['hh_size'], bins=SIZE_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['age5'] = (np.floor(df['avg_age_hh_head'] / 5) * 5).clip(upper=85)
    df['state'] = pd.to_numeric(df['fips_state_code'], errors='coerce')
    df['county'] = pd.to_numeric(df['fips'], errors='coerce')

    # ------------------------------------------ life events (8_build_life_event_studies.do)
    # birth: no child under 6 -> child under 6 with household size up; death: two heads -> one
    # widowed head; educ_up: the heads' average years of schooling rises by 2+ with the number
    # of heads unchanged (a head's reported education rises: graduation or a correction)
    df['young_child'] = df['kids'].isin([1, 4, 5, 7]).astype(int)
    df['death_state'] = np.where(df['n_heads'] == 2, 0.0,
                                 np.where((df['n_heads'] == 1) & (df['marital_status'] == 2), 1.0, np.nan))
    df['educ_yrs'] = df['hh_avg_yrsofschool'].round(1)
    df = first_switch_events(df, 'young_child', 'birth',
                             lambda d: (d['_prev'] == 0) & (d['young_child'] == 1) & (d['hh_size'] > d['_size_prev']))
    df = first_switch_events(df, 'n_heads', 'death',
                             lambda d: (d['_prev'] == 2) & (d['n_heads'] == 1) & (d['marital_status'] == 2))
    df = first_switch_events(df, 'educ_yrs', 'educ',
                             lambda d: (d['educ_yrs'] - d['_prev'] >= 2) & (d['n_heads'] == d['_nheads_prev']))
    for name in ['birth', 'death', 'educ']:
        print(f"{name}: movers {int((df.groupby('hhid')[f'{name}_grp'].first() == 1).sum()):,}, "
              f"never-changers {int((df.groupby('hhid')[f'{name}_grp'].first() == 0).sum()):,}")
    df = df.sort_values(['hhid', 'year']).reset_index(drop=True)
    g = df.groupby('hhid')

    # ------------------------------------------------------------ events
    ev = df[df['switch_consec'] & ~df['occ'].isin(DROP_FROM_ANALYSIS) & ~df['occ_prev'].isin(DROP_FROM_ANALYSIS)]
    ev = ev[['hhid', 'year', 'seg', 'occ_prev', 'occ']].rename(
        columns={'year': 'm_file', 'occ_prev': 'origin', 'occ': 'dest'}).reset_index(drop=True)
    ev['origin'] = ev['origin'].astype(int)
    ev['event_id'] = np.arange(1, len(ev) + 1)
    ev['emp2emp'] = (ev['origin'].isin(EMPLOYED) & ev['dest'].isin(EMPLOYED)).astype(int) if places not in ('county', 'retailer') else 1
    ev['notemp'] = (((ev['origin'] == 12) | (ev['dest'] == 12)) & (places == 'occupation')).astype(int)
    n_sw = g['switch'].first().rename('n_sw')  # placeholder, replaced below
    n_sw = df.groupby('hhid')['n_switch'].first()
    ev['single'] = (ev['hhid'].map(n_sw) == 1).astype(int)            # the household's only change
    ev['first_event'] = (ev['seg'] == 1).astype(int)                   # the household's first change
    nxt = df[df['switch']][['hhid', 'seg', 'occ']].rename(columns={'occ': 'next_code'})
    nxt['seg'] -= 1
    ev = ev.merge(nxt, on=['hhid', 'seg'], how='left')
    ev['reversed'] = (ev['next_code'] == ev['origin']).astype(int)     # a later change returns to the origin
    ev = ev.drop(columns='next_code')
    # FGW r = 0 is the partially treated year of the move. Occupation and income are reported in the fall of
    # m-1, so it is file year m-1 for them. The COUNTY code in file m is the address at the END of year m
    # (see the docstring), so for places = county r = 0 is file year m.
    # The main RETAILER is measured from the same year's purchases, so r = 0 is also file year m.
    ev['event_year'] = ev['m_file'] - (0 if places in ('county', 'retailer') else 1)

    # household-level mover type (smoothed codes)
    n_ev = ev.groupby('hhid').size()
    n_ee = ev[ev['emp2emp'] == 1].groupby('hhid').size()
    hh = pd.DataFrame({'n_switch': n_sw})
    hh['n_events_ee'] = n_ee.reindex(hh.index).fillna(0).astype(int)
    hh['n_events'] = n_ev.reindex(hh.index).fillna(0).astype(int)
    hh['mover_type'] = np.select(
        [hh['n_switch'] == 0,
         (hh['n_switch'] == 1) & (hh['n_events_ee'] == 1),
         hh['n_events_ee'] >= 1],
        [0, 1, 2], default=3)   # 0 non-mover, 1 one E->E move only, 2 E->E mover with other changes, 3 changes but no E->E event
    df = df.merge(hh[['mover_type', 'n_events', 'n_events_ee']], left_on='hhid', right_index=True, how='left')

    # r = 0 rows of any event (excluded from occupation means)
    r0 = ev[['hhid', 'event_year']].rename(columns={'event_year': 'year'}).drop_duplicates().assign(is_r0=1)
    df = df.merge(r0, on=['hhid', 'year'], how='left')
    df['is_r0'] = df['is_r0'].fillna(0).astype(int)

    # ----------------------------------------------------- analysis flag
    df['analysis'] = (~df['occ'].isin(DROP_FROM_ANALYSIS)).astype(int)

    # ------------------------------------------ occupation means y_j (FGW)
    def occ_means(d, with_se):
        rows = []
        for j, x in d.groupby('occ'):
            rec = {'occ': j, 'label': OCC_LABELS.get(j, str(j)), 'n_hh_years': len(x), 'n_households': x['hhid'].nunique(),
                   'income_w': wmean(x['real_income'], x['projection_factor']),
                   'age_w': wmean(x['avg_age_hh_head'], x['projection_factor'])}
            for v in OUTCOMES:
                by_year = x.groupby('year').apply(lambda z: wmean(z[v], z['projection_factor']))
                rec[f'ybar_{v}'] = by_year.mean()
                if with_se:
                    rec[f'ybar_{v}_se'] = x[v].std() / np.sqrt(x['hhid'].nunique())
            rows.append(rec)
        return pd.DataFrame(rows).set_index('occ')

    base = df[(df['analysis'] == 1) & (df['is_r0'] == 0)]
    means = occ_means(base, with_se=False)
    if places in ('county', 'retailer'):
        small = means['n_hh_years'] < MIN_COUNTY_HH_YEARS
        means.loc[small, [c for c in means.columns if c.startswith('ybar_')]] = np.nan
        print(f'counties with fewer than {MIN_COUNTY_HH_YEARS} household-years outside move years: {int(small.sum()):,} of {len(means):,}; their means set missing')
    means12 = occ_means(df[df['is_r0'] == 0], with_se=True)
    means12['income_rank'] = means12['income_w'].rank(na_option='bottom').astype(int)     # na_option: counties with no income (places = county)
    means12['hei_rank'] = means12['ybar_hei_usda'].rank(na_option='bottom').astype(int)
    means12.to_csv(OUT_DIR / {'occupation': 'occupation_means.csv', 'income': 'income_quintile_means.csv', 'county': 'county_means.csv', 'retailer': 'retailer_means.csv'}[places])
    print(means12.round(3).to_string() if places not in ('county', 'retailer') else means12.round(3).head(10).to_string())

    for v in OUTCOMES:
        ev[f'delta_{v}'] = ev['dest'].map(means[f'ybar_{v}']) - ev['origin'].map(means[f'ybar_{v}'])
    if places in ('county', 'retailer'):
        print(f"{places} moves: {len(ev):,}; with both county means defined: {int(ev['delta_hei_usda'].notna().sum()):,}; "
              f"mean |county HEI gap| {ev['delta_hei_usda'].abs().mean():.2f} points, SD of the gap {ev['delta_hei_usda'].std():.2f}")
    # education gap for the educ_up events: mean HEI by education bin on never-changers (by year, averaged)
    nc = df[(df['educ_grp'] == 0) & (df['analysis'] == 1)]
    eb = nc.groupby(['educ_bin', 'year']).apply(lambda x: wmean(x['hei_usda'], x['projection_factor'])).groupby(level=0).mean()
    orig_bin = pd.cut(df['educ_orig'], bins=EDUC_BINS, labels=[1, 2, 3, 4]).astype(float)
    dest_bin = pd.cut(df['educ_dest'], bins=EDUC_BINS, labels=[1, 2, 3, 4]).astype(float)
    df['delta_educ_hei'] = dest_bin.map(eb) - orig_bin.map(eb)
    print('HEI by education bin (never-changers):', eb.round(2).to_dict())
    df['income_rank'] = df['occ'].map(means12['income_rank'])
    ev['rank_orig'] = ev['origin'].map(means12['income_rank'])
    ev['rank_dest'] = ev['dest'].map(means12['income_rank'])
    ev['up_income'] = (ev['rank_dest'] > ev['rank_orig']).astype(int)

    # ------------------------------------------------- stacked event panel
    cols = ['hhid', 'year', 'seg', 'occ', 'projection_factor', 'age5', 'kids', 'hh_comp', 'avg_age_hh_head',
            'educ_bin', 'size_bin', 'state', 'county', 'real_income'] + [v for v in OUTCOMES if v != 'real_income']
    evcols = ['event_id', 'hhid', 'seg', 'm_file', 'event_year', 'origin', 'dest', 'emp2emp', 'notemp', 'single',
              'first_event', 'reversed', 'rank_orig', 'rank_dest', 'up_income'] + [f'delta_{v}' for v in OUTCOMES]
    pre = df[cols].merge(ev[evcols].assign(seg=ev['seg'] - 1), on=['hhid', 'seg'])
    post = df[cols].merge(ev[evcols], on=['hhid', 'seg'])
    st = pd.concat([pre, post], ignore_index=True)
    st['r'] = st['year'] - st['event_year']
    n_pre = st[st['r'] <= 0].groupby('event_id').size().rename('n_pre')
    n_post = st[st['r'] >= 1].groupby('event_id').size().rename('n_post')
    st = st.merge(n_pre, on='event_id', how='left').merge(n_post, on='event_id', how='left')
    st['n_pre'] = st['n_pre'].fillna(0).astype(int)
    st['n_post'] = st['n_post'].fillna(0).astype(int)
    st = st.drop(columns='seg').sort_values(['event_id', 'year']).reset_index(drop=True)
    st.to_parquet(OUT_DIR / f'{prefix}movers_events_panel.parquet', index=False)
    st.to_stata(OUT_DIR / f'{prefix}movers_events_panel.dta', write_index=False, version=118)

    # -------------------------------------------- household-year panel
    keep = ['hhid', 'year', 'occ', 'occ_raw', 'mover_type', 'n_events', 'n_events_ee', 'n_obs', 'n_switch',
            'switch', 'switch_consec', 'gap', 'occ_prev', 'seg', 'is_r0', 'analysis', 'educ_bin', 'size_bin',
            'age5', 'state', 'county', 'kids', 'hh_comp', 'hh_size', 'avg_age_hh_head', 'projection_factor',
            'real_income', 'income_rank', 'n_heads', 'marital_status', 'young_child', 'death_state', 'educ_yrs',
            'm_birth', 'r_birth', 'birth_grp', 'm_death', 'r_death', 'death_grp', 'm_educ', 'r_educ', 'educ_grp',
            'educ_orig', 'educ_dest', 'delta_educ_hei'] + [v for v in OUTCOMES if v != 'real_income']
    out = df[keep].copy()
    for c in ['switch', 'switch_consec']:
        out[c] = out[c].astype(int)
    name = {'occupation': 'occupation_movers_panel', 'income': 'income_movers_panel', 'county': 'county_movers_panel', 'retailer': 'retailer_movers_panel'}[places]
    out.to_parquet(OUT_DIR / f'{name}.parquet', index=False)
    out.to_stata(OUT_DIR / f'{name}.dta', write_index=False, version=118)

    # ------------------------------------------------------------ summary
    print('\nhouseholds by mover type (0 never changes, 1 one employed-to-employed move only, '
          '2 employed-to-employed mover with other changes, 3 changes but no employed-to-employed event):')
    print(hh['mover_type'].value_counts().sort_index().to_dict())
    ee = ev[ev['emp2emp'] == 1]
    print(f'\nevents: {len(ev):,} dated changes; employed-to-employed {len(ee):,} in {ee.hhid.nunique():,} households '
          f'(single-change households {int(ee.single.sum()):,}, first changes {int(ee.first_event.sum()):,}, '
          f'later reversed {int(ee.reversed.sum()):,}); into/out of not employed {int(ev.notemp.sum()):,}')
    see = st[st['emp2emp'] == 1]
    print(f'stacked rows, employed-to-employed: {len(see):,}; relative-year distribution: '
          f'{see["r"].clip(-8, 8).value_counts().sort_index().to_dict()}')
    print(f'\nsaved {OUT_DIR / (name + ".parquet")}, {OUT_DIR / (prefix + "movers_events_panel.dta")}')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--places', choices=['occupation', 'income', 'county', 'retailer'], default='occupation')
    main(ap.parse_args().places)
