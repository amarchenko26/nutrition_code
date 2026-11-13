"""
download_ipums_acs.py  --  run LOCALLY

Submits the IPUMS USA extract behind the ACS group incomes (build_acs_cell_iv.py), waits for IPUMS to finish it,
and saves the compressed data (usa_<number>.csv.gz, read as is by pandas) and its codebooks in raw/acs/.

Extract (Anya 2026-11-13): annual ACS 2004-2020, one row per person, csv. The variables of extract usa_00004
(ACS 2005-2020, made by raw/acs/download_ipums_acs_occ_cells.py) plus
  EMPSTAT  employment status in the survey week (IPUMS adds the detailed EMPSTATD), so that a household whose
           occupation person does not work leaves the working groups, as in build_iv.bin_occupation
  ADJUST   the Census factor that puts income for the 12 months before the interview in survey-year dollars
and the 2004 survey, so that the group-income change exists for t = 2005, the first year of the main IV.
usa_00004 stays in raw/acs for the earlier version of the ACS row.

The API key is read from the environment variable IPUMS_API_KEY and is never written into the repo.
Run:  IPUMS_API_KEY=... python3 build/iv/download_ipums_acs.py                 submit, wait, download
      IPUMS_API_KEY=... python3 build/iv/download_ipums_acs.py --submit-only   print the extract number and stop
      IPUMS_API_KEY=... python3 build/iv/download_ipums_acs.py --number N      wait for extract N and download it
"""
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import argparse
import os
import shutil
import time

import requests

BASE_URL = 'https://api.ipums.org'
OUT_DIR = rdir / 'acs'
SAMPLES = [f'us{y}a' for y in range(2004, 2021)]
VARIABLES = ['YEAR', 'SERIAL', 'PERNUM', 'HHWT', 'PERWT', 'GQ', 'RELATE', 'SEX', 'AGE', 'MARST', 'NCHILD',
             'NUMPREC', 'OCC2010', 'EDUC', 'EDUCD', 'HHINCOME', 'STATEFIP', 'EMPSTAT', 'ADJUST']


def headers():
    key = os.environ.get('IPUMS_API_KEY')
    if not key:
        sys.exit('Set IPUMS_API_KEY to the IPUMS API key of the account.')
    return {'Authorization': key, 'Content-Type': 'application/json'}


def submit(h):
    body = {'description': 'ACS group incomes for the Nielsen income instrument, with employment status '
                           '(annual ACS 2004-2020)',
            'dataStructure': {'rectangular': {'on': 'P'}},
            'dataFormat': 'csv',
            'samples': {s: {} for s in SAMPLES},
            'variables': {v: {} for v in VARIABLES}}
    r = requests.post(f'{BASE_URL}/extracts?collection=usa&version=2', headers=h, json=body, timeout=60)
    if not r.ok:
        sys.exit(f'IPUMS refused the extract ({r.status_code}): {r.text[:2000]}')
    number = r.json()['number']
    print(f'submitted extract usa:{number}', flush=True)
    return number


def wait_and_download(h, number, poll=60):
    while True:
        try:
            r = requests.get(f'{BASE_URL}/extracts/{number}?collection=usa&version=2', headers=h, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f'  transient error, retrying: {e}', flush=True)
            time.sleep(poll)
            continue
        info = r.json()
        status = info.get('status')
        print(f'  {time.strftime("%H:%M")} extract {number}: {status}', flush=True)
        if status == 'completed':
            break
        if status in ('failed', 'canceled'):
            sys.exit(f'extract {number} {status}')
        time.sleep(poll)
    links = info.get('downloadLinks', {})
    for k in ('data', 'ddiCodebook', 'basicCodebook'):
        if k not in links:
            continue
        url = links[k]['url']
        fname = OUT_DIR / os.path.basename(url)
        part = fname.with_name(fname.name + '.part')
        print(f'  downloading {fname.name}', flush=True)
        with requests.get(url, headers=h, stream=True, timeout=600) as resp:
            resp.raise_for_status()
            with open(part, 'wb') as f:
                shutil.copyfileobj(resp.raw, f)
        part.rename(fname)
        print(f'  saved {fname} ({fname.stat().st_size / 1e6:,.0f} MB)', flush=True)
    print('done', flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--submit-only', action='store_true')
    ap.add_argument('--number', type=int)
    a = ap.parse_args()
    h = headers()
    number = a.number if a.number else submit(h)
    if not a.submit_only:
        wait_and_download(h, number)


if __name__ == '__main__':
    main()
