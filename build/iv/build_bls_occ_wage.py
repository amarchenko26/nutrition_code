"""
build_bls_occ_wage.py  --  run LOCALLY

National BLS occupation wages mapped to Nielsen occupation codes, for the
external-instrument robustness check: instead of instrumenting a household's
income change with other Nielsen households' cell income, instrument it with
the change in the national BLS (OEWS) mean annual wage of the household's
occupation group. Identification: differential national wage growth across
occupations (year FE absorb the common trend). Built entirely outside
Nielsen, so it is immune to Nielsen-internal reporting comovement.

Source: OEWS (formerly OES) national May files, 2004-2020
  https://www.bls.gov/oes/special-requests/oesm{yy}nat.zip
Major-group (2-digit SOC) mean annual wages (A_MEAN), employment-weighted
(TOT_EMP) when a Nielsen code spans several SOC groups.

Nielsen occupation code -> SOC major groups, per the Kilts codebook list
(Anya 2026-09-10). Code 12 = not employed, 0 = no head (unmapped):
  1 Professional (scientists, accountants, engineers, artists, programmers,
    doctors, educators, lawyers, med techs, clergy) -> 15,17,19,21,23,25,27,29
  2 Manager/Admin/Owner/Banker/Public official      -> 11,13
  3 Clerical (tellers, clerks, secretaries, postal) -> 43
  4 Sales (industrial, insurance/real estate, retail)-> 41
  5 Craftsman/Foreman (carpenters, mechanics, techs) -> 47,49
  6 Operative (factory machine operators, drivers)   -> 51,53
  7 Armed forces (no OEWS coverage)                  -> 00 (all occupations)
  8 Service (barbers, food, childcare, police, janitors) -> 31,33,35,37,39
  9 Farmer                                           -> 45
  10 Students employed <30 hours                     -> 00 (all occupations)
  11 Laborer (construction worker, shipping, fisherman,
     gardener, lumberman)                            -> 37,45,53
Note: the 2010 SOC revision creates minor series breaks at the group level;
they are common within occupation and largely absorbed by differencing.

Raw downloads -> raw/bls/oews/oesm{yy}nat.zip (cached)
Output        -> interim/panel_dataset/bls_occ_wage.parquet / .dta
                 columns: occ_src (Nielsen code), year, bls_occ_wage ($/yr)
"""

import io
import re
import zipfile
from pathlib import Path

import pandas as pd
import requests

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
BASE = data
RAW = BASE / 'raw' / 'bls' / 'oews'
OUT = BASE / 'interim' / 'panel_dataset' / 'bls_occ_wage.parquet'
YEARS = range(2004, 2021)
UA = {'User-Agent': 'Mozilla/5.0 (Macintosh) research data download'}

NIELSEN_SOC = {
    1:  ['15', '17', '19', '21', '23', '25', '27', '29'],
    2:  ['11', '13'],
    3:  ['43'],
    4:  ['41'],
    5:  ['47', '49'],
    6:  ['51', '53'],
    7:  ['00'],
    8:  ['31', '33', '35', '37', '39'],
    9:  ['45'],
    10: ['00'],
    11: ['37', '45', '53'],
}


def fetch(year):
    yy = f"{year % 100:02d}"
    dest = RAW / f"oesm{yy}nat.zip"
    if not dest.exists():
        url = f"https://www.bls.gov/oes/special-requests/oesm{yy}nat.zip"
        print(f"  downloading {url}")
        r = requests.get(url, headers=UA, timeout=120)
        r.raise_for_status()
        dest.write_bytes(r.content)
    return dest


def read_national(zpath):
    """Return the national data sheet as a DataFrame with lowercase columns.

    Legacy .xls members are parsed with xlrd 1.2 directly (pandas 2.x refuses
    xlrd < 2, and xlrd >= 2 dropped .xls support); .xlsx via pandas/openpyxl.
    """
    with zipfile.ZipFile(zpath) as z:
        cands = [n for n in z.namelist()
                 if re.search(r'nat.*dl.*\.(xls|xlsx)$', n, re.I)
                 or re.search(r'national.*\.(xls|xlsx)$', n, re.I)]
        if not cands:
            raise RuntimeError(f"no national sheet in {zpath.name}: {z.namelist()}")
        name = sorted(cands, key=len)[0]
        with z.open(name) as f:
            raw = f.read()
    if name.lower().endswith('.xlsx'):
        df = pd.read_excel(io.BytesIO(raw), engine='openpyxl')
    else:
        import xlrd
        book = xlrd.open_workbook(file_contents=raw)
        sh = book.sheet_by_index(0)
        rows = [sh.row_values(i) for i in range(sh.nrows)]
        df = pd.DataFrame(rows[1:], columns=rows[0])
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def group_wages(df, year):
    """Major-group (and all-occupations) mean wage + employment."""
    occ = df['occ_code'].astype(str).str.strip()
    if 'o_group' in df.columns or 'group' in df.columns:
        grp_col = 'o_group' if 'o_group' in df.columns else 'group'
        grp = df[grp_col].astype(str).str.strip().str.lower()
        is_major = grp.eq('major') | (grp.isin(['', 'nan']) & occ.str.endswith('-0000') & ~occ.eq('00-0000'))
    else:
        # some vintages (e.g. 2012) carry no group column
        is_major = occ.str.endswith('-0000') & ~occ.eq('00-0000')
    is_total = occ.eq('00-0000')

    out = df[is_major | is_total].copy()
    out['soc2'] = occ[is_major | is_total].str[:2]
    for c in ('a_mean', 'tot_emp'):
        out[c] = pd.to_numeric(out[c], errors='coerce')
    out = out.dropna(subset=['a_mean'])
    out['year'] = year
    return out[['year', 'soc2', 'a_mean', 'tot_emp']]


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    frames = []
    for y in YEARS:
        df = group_wages(read_national(fetch(y)), y)
        print(f"  {y}: {len(df)} SOC groups, all-occ mean "
              f"${df.loc[df.soc2 == '00', 'a_mean'].iloc[0]:,.0f}")
        frames.append(df)
    soc = pd.concat(frames, ignore_index=True)

    rows = []
    for code, groups in NIELSEN_SOC.items():
        sub = soc[soc['soc2'].isin(groups)].copy()
        w = sub.groupby('year').apply(
            lambda d: (d['a_mean'] * d['tot_emp']).sum() / d['tot_emp'].sum())
        for year, wage in w.items():
            rows.append({'occ_src': code, 'year': year, 'bls_occ_wage': wage})
    out = pd.DataFrame(rows).sort_values(['occ_src', 'year'])

    print("\nMean annual wage by Nielsen code, 2004 vs 2020:")
    piv = out.pivot(index='occ_src', columns='year', values='bls_occ_wage')
    for code in piv.index:
        print(f"  {code:>2}: ${piv.loc[code, 2004]:>8,.0f} -> ${piv.loc[code, 2020]:>8,.0f}"
              f"   ({100 * (piv.loc[code, 2020] / piv.loc[code, 2004] - 1):.0f}%)")

    out.to_parquet(OUT, index=False)
    out.to_stata(str(OUT.with_suffix('.dta')), write_index=False)
    print(f"\nSaved: {OUT} ({len(out)} rows)")


if __name__ == '__main__':
    main()
