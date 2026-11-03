"""
clean_ailments_members.py

Member-level rebuild of the Nielsen Health Care Survey (ailments) files, 2011-2023,
for a comparable obesity series. Reads raw/ailments/<year>/ (parsed data xlsx and the
survey format file) and writes

  interim/ailments/ailments_members.parquet     one row per household member per year
  interim/ailments/ailments_households.parquet  one row per household per year

Survey regimes (from the format files):
  2011-2015  one questionnaire per household member aged 13+ ("Please have all household
             members, 13 years or older, answer this survey"); each member answers the
             ailment list for themselves (Q1_Ailment1..35; 35 = none of the above); age in
             brackets (Q7), gender (Q14), birth year (Q13a_yy). Rows = members.
  2016-2018  one respondent per household. Household-level list ("select all the
             condition(s) that anyone in your household, including yourself, has
             experienced during the past 6 months": Q16_/Q10_Ailment<j>) AND a per-member
             grid ("What ailments, if any, does each household member suffer from?":
             Q31_mem<k>_Ailment<j> in 2016-17, Q18_mem<k>_Ailment<j> in 2018), with a
             roster: age in years (Q25_age_mem<k>), gender (Q26), relationship to the
             respondent (1 = Myself), and in 2016-2017 weight and height brackets
             (Q27_weight_mem<k>: 1 "20 lbs or less" ... 10 100-149, 11 150-199, 12 200-249,
             13 250-299, 14 300+, 15 refused; Q28_height_feet_mem<k>: 1 <3 ft, 2 3 ft, 3 4 ft,
             4 5 ft, 5 6+ ft, 6 refused; Q28_height_inches_mem<k>: 1 = 0 in ... 12 = 11 in,
             13 refused). Rows = households.
  2019-2023  same design; household list Q36_<j>, per-member grid "18 (Q9_A_<n>)" with
             n = (block - 1) * 9 + member and blocks in the format file's order; roster
             HHmembersQ17 (Q10_A..D_<k>) = name, relationship (1 = Myself), age, gender.
             2022 carries a "Projection weight" column. Rows = households.

The ailment item is "Obesity / Overweight" throughout. The per-member grid is validated
against the household-level list (any member flagged vs household flag) for each year.
"""

import os, re, glob, sys
import numpy as np
import pandas as pd

BASE = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
RAW = os.path.join(BASE, 'raw', 'ailments')
OUT = os.path.join(BASE, 'interim', 'ailments')

TARGETS = {  # regex on the ailment label -> condition name
    'cholesterol': r'cholesterol', 'prediabetes': r'pre-?\s?diabetes', 'diabetes_type1': r'diabetes.*type\s*i\b(?!i)',
    'diabetes_type2': r'diabetes.*type\s*ii', 'heart_disease': r'heart disease', 'hypertension': r'hypertension',
    'obesity': r'obesity', 'none': r'none of the above|does not suffer',
}
Q1_POS = {11: 'cholesterol', 13: 'prediabetes', 14: 'diabetes_type1', 15: 'diabetes_type2',
          21: 'heart_disease', 22: 'hypertension', 30: 'obesity', 35: 'none'}
WEIGHT_MID = {1: 15, 2: 25, 3: 35, 4: 45, 5: 55, 6: 65, 7: 75, 8: 85, 9: 95, 10: 125, 11: 175, 12: 225, 13: 275, 14: 325}
FEET = {1: 2.5, 2: 3, 3: 4, 4: 5, 5: 6}


def log(m):
    print(m, flush=True)


def main_file(d):
    fs = [f for f in os.listdir(d) if f.endswith('.xlsx') and not f.startswith('~')
          and not any(k in f.lower() for k in ('format', 'layout', 'corrected'))]
    return max(fs, key=lambda f: os.path.getsize(os.path.join(d, f)))


def read_main(path):
    """The data sheet is the widest one (2020 has a two-column summary sheet first; 2022 has a
    'Sheet2' with corrected panelist ids, applied below)."""
    from openpyxl import load_workbook
    wb = load_workbook(path, read_only=True)
    widths = {sh: len(next(wb[sh].iter_rows(values_only=True))) for sh in wb.sheetnames}
    wb.close()
    main = max(widths, key=widths.get)
    df = pd.read_excel(path, sheet_name=main, engine='openpyxl')
    others = [sh for sh in widths if sh != main]
    for sh in others:
        o = pd.read_excel(path, sheet_name=sh, engine='openpyxl')
        if any('corrected' in str(c).lower() for c in o.columns) and len(o) == len(df):
            # 2022: the ids in the data sheet carry an extra trailing digit; the sheet gives the
            # corrected id as a formula column and a values column -- use the last (values) column
            orig, corr = o.columns[0], [c for c in o.columns if 'corrected' in str(c).lower()][-1]
            m = dict(zip(pd.to_numeric(o[orig], errors='coerce'), pd.to_numeric(o[corr], errors='coerce')))
            m = {k: v for k, v in m.items() if pd.notna(v)}
            hc = hh_col(df)
            before = pd.to_numeric(df[hc], errors='coerce')
            after = before.map(m).fillna(before)
            log(f'    applied corrected panelist ids from sheet {sh}: {(before != after).sum():,} ids changed')
            df[hc] = after
    return df


def format_lines(year):
    """All rows of the (non-corrected) format file as ' | '-joined strings."""
    fs = [f for f in glob.glob(f'{RAW}/{year}/*') if any(k in os.path.basename(f).lower() for k in ('format', 'layout'))
          and 'corrected' not in os.path.basename(f).lower() and not os.path.basename(f).startswith('~')]
    fs = [f for f in fs if f.endswith(('.xls', '.xlsx'))]
    assert fs, f'no format file for {year}'
    f = sorted(fs)[0]
    lines = []
    if f.endswith('.xls'):
        import xlrd
        wb = xlrd.open_workbook(f)
        for sh in wb.sheets():
            for i in range(sh.nrows):
                lines.append(' | '.join(str(c.value) for c in sh.row(i) if str(c.value) != ''))
    else:
        for sh, df in pd.read_excel(f, sheet_name=None, header=None).items():
            lines += df.astype(str).apply(lambda r: ' | '.join(v for v in r if v != 'nan'), axis=1).tolist()
    return lines


def grid_block_order(year):
    """Ailment names in the order of the per-member grid blocks ('N (Q9) | ... | <name>')."""
    names = [l.split('|')[-1].strip() for l in format_lines(year) if re.match(r'^\d+ \(Q9\)', l)]
    # the format repeats the block header once per ailment; keep order, drop exact duplicates in sequence
    out = []
    for n in names:
        if not out or out[-1] != n:
            out.append(n)
    return out


def q36_items(year):
    """{Q36 number: item label} from the format/layout file."""
    return {int(re.match(r'^Q36_(\d+)', l).group(1)): l.split('|')[-1].strip()
            for l in format_lines(year) if re.match(r'^Q36_\d+ \|', l)}


def match_targets(names):
    """{condition: 1-based block index} from a list of ailment labels."""
    found = {}
    for cond, pat in TARGETS.items():
        hits = [i + 1 for i, n in enumerate(names) if re.search(pat, n, flags=re.I)]
        if cond == 'diabetes_type1':
            hits = [i + 1 for i, n in enumerate(names) if re.search(r'diabetes', n, re.I) and re.search(r'type\s*i\s*$|type\s*i\b(?!i)', n, re.I)]
        if hits:
            found[cond] = hits[0]
    return found


def hh_col(df):
    for c in df.columns:
        if any(k in str(c).lower() for k in ('household id', 'household_id', 'hhid', 'panelistid', 'househiold')):
            return c
    return df.columns[0]


def to01(s):
    return (pd.to_numeric(s, errors='coerce') == 1).astype(int)


# ----------------------------------------------------------------- 2011-2015
def members_2011_2015(df, year):
    hh = pd.to_numeric(df[hh_col(df)], errors='coerce')
    look = {int(m.group(1)): c for c in df.columns for m in [re.match(r'^Q1_Ailment(\d+)', str(c))] if m}
    out = pd.DataFrame({'household_code': hh, 'panel_year': year})
    out['member'] = out.groupby('household_code').cumcount() + 1
    all_q1 = [look[p] for p in sorted(look)]
    out['answered'] = (df[all_q1].apply(pd.to_numeric, errors='coerce') == 1).any(axis=1).astype(int).values
    for pos, cond in Q1_POS.items():
        out[cond] = to01(df[look[pos]]).values
    out['age_bracket'] = pd.to_numeric(df['Q7 Age Break'], errors='coerce').values
    out['gender'] = pd.to_numeric(df['Q14'], errors='coerce').values if 'Q14' in df.columns else np.nan
    yy = pd.to_numeric(df['Q13a_yy'], errors='coerce') if 'Q13a_yy' in df.columns else pd.Series(np.nan, index=df.index)
    byear = np.where(yy.notna(), np.where(yy <= (year % 100), 2000 + yy, 1900 + yy), np.nan)
    out['age'] = np.where(np.isnan(byear), np.nan, year - byear)
    out['is_respondent'] = 1          # each member answered for themselves (parents for children)
    out['regime'] = 'member_self'
    for c in ['weight_lb', 'height_in', 'bmi', 'relationship']:
        out[c] = np.nan
    return out, None


# ----------------------------------------------------------------- 2016-2023
def members_2016_plus(df, year):
    hh = pd.to_numeric(df[hh_col(df)], errors='coerce')
    cols = {str(c).strip(): c for c in df.columns}
    blocks = grid_block_order(year)
    tgt = match_targets(blocks)
    log(f'    format grid blocks: {len(blocks)}; targets at {tgt}')
    # household-level list
    if year <= 2017:
        hh_name = lambda j: f'Q16_Ailment{j}'
        grid = lambda k, j: f'Q31_mem{k}_Ailment{j}'
    elif year == 2018:
        hh_name = lambda j: f'Q10_Ailment{j}'
        grid = lambda k, j: f'Q18_mem{k}_Ailment{j}'
    else:
        # 2019+: the per-member grid has one block of 9 member columns per item, in the order of
        # the Q36 columns PRESENT in the data file (a subset of the format's numbering), so an
        # item's block is the rank of its Q36 number among the present numbers (validated below)
        if year <= 2021:
            q36 = {int(m.group(1)): c for c in cols for m in [re.match(r'^Q36_(\d+)', c)] if m}
        else:
            q36 = {int(m.group(1)): c for c in cols for m in [re.match(r'^10 \(Q36_(\d+)\)', c)] if m}
        present = sorted(q36)
        rank = {n: i + 1 for i, n in enumerate(present)}
        hh_name = lambda j: q36.get(j, f'Q36_{j}')
        n_blocks = sum('(Q9_A_' in c for c in cols) // 9
        log(f'    Q36 numbers present: {len(present)}; grid blocks in data: {n_blocks}')
        # grid layout: ITEM-FIRST (one block of 9 member columns per item, in the order of the
        # present Q36 numbers; 2019-2022) or MEMBER-FIRST (all items for member 1, then member 2,
        # ...; 2023). Both are tested against the household-level obesity flag and the layout
        # that agrees is used.
        n_items = len(present)

        def grid_cols(layout, j):
            if layout == 'item_first':
                idx = [(rank.get(j, 0) - 1) * 9 + k for k in range(1, 10)]
            else:
                idx = [(k - 1) * n_items + rank.get(j, 0) for k in range(1, 10)]
            return [cols.get(f'18 (Q9_A_{i})') for i in idx if cols.get(f'18 (Q9_A_{i})') is not None]

        def agreement(layout, j):
            c = cols.get(hh_name(j).strip(), hh_name(j))
            if c not in df.columns or not grid_cols(layout, j):
                return 0.0
            flag = to01(df[c])
            anym = (df[grid_cols(layout, j)].apply(pd.to_numeric, errors='coerce') == 1).any(axis=1).astype(int)
            return (anym == flag).mean()

        j_ob = next((n for n, nm in q36_items(year).items() if re.search('obesity', nm, re.I)), None)
        scores = {lay: agreement(lay, j_ob) for lay in ('item_first', 'member_first')} if j_ob else {'item_first': 1.0}
        layout = max(scores, key=scores.get)
        log(f'    grid layout: {layout} (agreement with the household obesity flag: '
            + ', '.join(f'{k} {v:.3f}' for k, v in scores.items()) + ')')
        grid = lambda k, j: (grid_cols(layout, j) + [None] * 9)[k - 1] or f'__missing_{j}_{k}'
    # household-level positions: 2016-2018 and 2022-23 use the format's Q36 numbering; 2019-21 match by label
    hh_lines = [l for l in format_lines(year) if re.match(r'^Q36_\d+ \|', l)]
    q36_names = {int(re.match(r'^Q36_(\d+)', l).group(1)): l.split('|')[-1].strip() for l in hh_lines}
    hh_tgt = match_targets([q36_names.get(i, '') for i in range(1, max(q36_names) + 1)])
    log(f'    household list: {len(q36_names)} items; targets at {hh_tgt}')
    if year >= 2019:
        tgt = {c: j for c, j in hh_tgt.items() if c != 'none'}   # grid uses the Q36 numbering via rank
    house = pd.DataFrame({'household_code': hh, 'panel_year': year})
    for cond, j in hh_tgt.items():
        c = hh_name(j)
        c = cols.get(c.strip(), c)
        house[f'hh_{cond}'] = to01(df[c]).values if c in df.columns else np.nan
    if 'Projection weight' in cols:
        house['survey_weight'] = pd.to_numeric(df[cols['Projection weight']], errors='coerce').values
    resp_age_col = next((cols[c] for c in cols if c.startswith('QS')), None)
    house['respondent_age'] = pd.to_numeric(df[resp_age_col], errors='coerce').values if resp_age_col is not None else np.nan

    rows = []
    for k in range(1, 10):
        if year <= 2018:
            age = df.get(f'Q25_age_mem{k}'); gen = df.get(f'Q26_gender_mem{k}')
            rel = next((df[c] for c in df.columns if re.match(rf'^Q24.*mem{k}$', str(c).strip()) or re.match(rf'^Q24_rel\w*_mem{k}', str(c).strip())), None)
            wt = df.get(f'Q27_weight_mem{k}') if year <= 2017 else None
            hf = df.get(f'Q28_height_feet_mem{k}') if year <= 2017 else None
            hi = df.get(f'Q28_height_inches_mem{k}') if year <= 2017 else None
            name = None
        else:
            name = df.get(cols.get(f'HHmembersQ17 (Q10_A_{k})')); rel = df.get(cols.get(f'HHmembersQ17 (Q10_B_{k})'))
            age = df.get(cols.get(f'HHmembersQ17 (Q10_C_{k})')); gen = df.get(cols.get(f'HHmembersQ17 (Q10_D_{k})'))
            wt = hf = hi = None
        if age is None:
            continue
        m = pd.DataFrame({'household_code': hh, 'panel_year': year, 'member': k})
        m['age'] = pd.to_numeric(age, errors='coerce').values
        m['gender'] = pd.to_numeric(gen, errors='coerce').values if gen is not None else np.nan
        m['relationship'] = pd.to_numeric(rel, errors='coerce').values if rel is not None else np.nan
        # empty roster slots carry age 0, gender 0 and relationship 0 (2016-2018) or blank fields (2019+);
        # a member is present if any of gender, relationship, age or name is filled
        present = m['gender'].isin([1, 2]) | (m['relationship'] > 0) | (m['age'] > 0)
        if name is not None:
            present |= name.notna().values & (name.astype(str).str.strip().isin(['', 'None', 'nan']) == False).values
        for cond, j in tgt.items():
            c = cols.get(grid(k, j).strip(), grid(k, j))
            m[cond] = to01(df[c]).values if c in df.columns else np.nan
        if wt is not None:
            w = pd.to_numeric(wt, errors='coerce'); f = pd.to_numeric(hf, errors='coerce'); i = pd.to_numeric(hi, errors='coerce')
            m['weight_lb'] = w.map(WEIGHT_MID).values
            m['height_in'] = (f.map(FEET) * 12 + (i - 1).where(i.between(1, 12))).values
            m['bmi'] = 703 * m['weight_lb'] / m['height_in'] ** 2
        else:
            m['weight_lb'] = m['height_in'] = m['bmi'] = np.nan
        m = m[present.values]
        rows.append(m)
    mem = pd.concat(rows, ignore_index=True)
    mem['is_respondent'] = (mem['relationship'] == 1).astype(int)
    mem['age_bracket'] = np.nan
    mem['answered'] = 1
    mem['regime'] = 'member_proxy'
    if 'none' in tgt:
        mem = mem.rename(columns={'none': 'none'})
    # validation: any-member grid vs household list
    for cond in ['obesity', 'hypertension', 'cholesterol', 'diabetes_type2']:
        if cond in mem.columns and f'hh_{cond}' in house.columns:
            anym = mem.groupby('household_code')[cond].max()
            v = house.set_index('household_code')[f'hh_{cond}'].to_frame('hh').join(anym.rename('mem'))
            v = v.dropna()
            agree = (v.hh == v.mem).mean()
            log(f'    validate {cond:14s}: household flag {v.hh.mean():.3f}, any-member grid {v.mem.mean():.3f}, '
                f'agreement {agree:.3f}, P(member|hh) {v.loc[v.hh == 1, "mem"].mean():.3f}')
            assert agree > 0.95, f'{year}: grid mapping for {cond} disagrees with the household list'
    return mem, house


def main():
    os.makedirs(OUT, exist_ok=True)
    years = [int(a) for a in sys.argv[1:] if a.isdigit()] or list(range(2011, 2024))
    force = '--force' in sys.argv
    cache = os.path.join(OUT, 'by_year'); os.makedirs(cache, exist_ok=True)
    mems, houses = [], []
    for y in years:
        d = f'{RAW}/{y}'
        if not force and os.path.exists(f'{cache}/members_{y}.parquet'):
            log(f'\n{y}: cached'); mems.append(pd.read_parquet(f'{cache}/members_{y}.parquet'))
            houses.append(pd.read_parquet(f'{cache}/households_{y}.parquet')); continue
        f = main_file(d)
        log(f'\n{y}: {f}')
        df = read_main(f'{d}/{f}')
        log(f'    {len(df):,} rows x {len(df.columns)} cols')
        mem, house = (members_2011_2015 if y <= 2015 else members_2016_plus)(df, y)
        if house is None:
            house = mem.groupby(['household_code', 'panel_year'], as_index=False).agg(
                **{f'hh_{c}': (c, 'max') for c in ['cholesterol', 'prediabetes', 'diabetes_type1', 'diabetes_type2', 'heart_disease', 'hypertension', 'obesity']},
                n_members_answering=('answered', 'sum'))
        log(f'    members: {len(mem):,} rows, {mem.household_code.nunique():,} households; '
            f'obesity (member rows) {mem.obesity.mean():.3f}; adults 18+ {mem.loc[mem.age >= 18, "obesity"].mean() if mem.age.notna().any() else float("nan"):.3f}; '
            f'any-member {mem.groupby("household_code").obesity.max().mean():.3f}')
        mem.to_parquet(f'{cache}/members_{y}.parquet', index=False); house.to_parquet(f'{cache}/households_{y}.parquet', index=False)
        mems.append(mem); houses.append(house)
    mem = pd.concat(mems, ignore_index=True)
    house = pd.concat(houses, ignore_index=True)
    for c in ['household_code', 'panel_year', 'member']:
        mem[c] = mem[c].astype('int64'); 
    house['household_code'] = house['household_code'].astype('int64')
    mem.to_parquet(f'{OUT}/ailments_members.parquet', index=False)
    house.to_parquet(f'{OUT}/ailments_households.parquet', index=False)
    log(f'\nsaved {len(mem):,} member rows and {len(house):,} household rows to {OUT}')


if __name__ == '__main__':
    main()
