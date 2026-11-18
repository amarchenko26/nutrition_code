"""Minutes of preparation per barcode: applies the rules of prep_minutes_rules.py to every food barcode.

Reads  interim/prep_time/upc_base.parquet (build_prep_upc_base.py)
       build/prep_time/prep_minutes_rules.py (RULES: per module, minutes per cooking session and ordered rules on Nielsen's
       attributes and the abbreviated description; the first rule that matches a barcode gives its minutes)
Writes interim/prep_time/upc_prep_minutes.parquet: upc, product_module_code, min_session (minutes per cooking session),
       basis ('serv4' = one session cooks four label servings; 'pkg' = the package is cooked in one session, e.g. a whole
       chicken, a cake mix, a frozen pizza), rule (0 = the module's minutes, k = the module's k-th rule, -1 = module without
       rules: zero minutes), why.
       A rule's conditions may also use the package size: grams_min (at least) and grams_max (fewer than), in grams or ml of
       the whole package, to tell a single-serve tray from a family-size one.

Check mode, for writing rules:
    python build_prep_minutes_upc.py check <rules file.py> [module codes ...]
prints, for each module of that file (or the listed ones), every rule with the barcodes and the share of the module's spending
it matched, the three largest barcodes it matched, the share left to the module's minutes, and the package-weighted mean
minutes; it writes nothing.
"""
import importlib.util
import re
import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *

import numpy as np
import pandas as pd

from utils.food_measures import UNIT_TO_GRAMS

BASE = idir / 'prep_time' / 'upc_base.parquet'
OUT = idir / 'prep_time' / 'upc_prep_minutes.parquet'
FIELDS = ['type', 'style', 'form', 'use', 'product', 'variety', 'formula', 'flavor', 'container', 'common_consumer_name', 'descr', 'brand']
BASES = {'serv4', 'pkg'}
SIZE_KEYS = {'grams_min', 'grams_max'}     # package size in grams or ml (size x multi x unit; count-sized packages have none)


def load_rules(path):
    spec = importlib.util.spec_from_file_location('rules_file', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.RULES


def validate(rules):
    """Stop on a malformed entry, so a typing error never becomes a silent zero."""
    for code, m in rules.items():
        assert isinstance(code, int), f'{code}: module code must be an integer'
        assert set(m) <= {'name', 'minutes', 'basis', 'why', 'rules'}, f'{code}: unknown keys {set(m)}'
        assert isinstance(m['minutes'], (int, float)) and 0 <= m['minutes'] <= 480, f'{code}: minutes {m["minutes"]}'
        assert m.get('basis', 'serv4') in BASES, f'{code}: basis {m.get("basis")}'
        for k, r in enumerate(m.get('rules', []), 1):
            assert len(r) in (3, 4), f'{code} rule {k}: (conditions, minutes, why[, basis])'
            cond, mins, why = r[0], r[1], r[2]
            assert isinstance(cond, dict) and cond, f'{code} rule {k}: conditions must be a non-empty dict'
            for key, pat in cond.items():
                if key in SIZE_KEYS:
                    assert isinstance(pat, (int, float)) and pat > 0, f'{code} rule {k}: {key} must be a number of grams'
                    continue
                f = key[:-4] if key.endswith('_not') else key
                assert f in FIELDS, f'{code} rule {k}: unknown field {key}'
                re.compile(pat)
            assert isinstance(mins, (int, float)) and 0 <= mins <= 480, f'{code} rule {k}: minutes {mins}'
            assert isinstance(why, str) and why, f'{code} rule {k}: reason missing'
            if len(r) == 4:
                assert r[3] in BASES, f'{code} rule {k}: basis {r[3]}'


def apply_module(g, m):
    """g: the module's barcodes. Returns min_session, basis, rule number, why (arrays aligned with g)."""
    n = len(g)
    true_name = g['module'].iloc[0]
    assert m.get('name') == true_name, f'{g["product_module_code"].iloc[0]}: rules written for "{m.get("name")}" but the module is "{true_name}"'
    mins = np.full(n, float(m['minutes']))
    basis = np.full(n, m.get('basis', 'serv4'), dtype=object)
    rule = np.zeros(n, dtype=np.int16)
    why = np.full(n, m.get('why', ''), dtype=object)
    free = np.ones(n, dtype=bool)
    size = (g['size1_amount'] * g['multi'] * g['size1_units'].astype(str).str.strip().map(UNIT_TO_GRAMS)).values
    for k, r in enumerate(m.get('rules', []), 1):
        hit = free.copy()
        for key, pat in r[0].items():
            if key == 'grams_min':             # at least this many grams; a package without a size in grams never matches
                hit &= size >= pat
                continue
            if key == 'grams_max':             # fewer than this many grams
                hit &= size < pat
                continue
            neg = key.endswith('_not')
            f = key[:-4] if neg else key
            found = g[f].fillna('').str.contains(pat, regex=True).values
            hit &= ~found if neg else found
        mins[hit] = r[1]
        why[hit] = r[2]
        rule[hit] = k
        if len(r) == 4:
            basis[hit] = r[3]
        free &= ~hit
    return mins, basis, rule, why


def check(path, codes):
    rules = load_rules(path)
    validate(rules)
    d = pd.read_parquet(BASE)
    for code in (codes or sorted(rules)):
        m = rules[code]
        g = d[d['product_module_code'] == code]
        if g.empty:
            print(f'\n{code} {m.get("name", "")}: no barcodes bought')
            continue
        try:
            mins, basis, rule, why = apply_module(g, m)
        except AssertionError as e:
            print(f'\nERROR {e}')
            continue
        sp, q = g['spending'].values, g['quantity'].values
        print(f'\n{code} {g["module"].iloc[0]} | old score {g["old_score"].iloc[0]} | {len(g):,} barcodes')
        for k in range(0, len(m.get('rules', [])) + 1):
            sel = rule == k
            label = f'module minutes {m["minutes"]} ({m.get("basis", "serv4")})' if k == 0 else f'rule {k}: {m["rules"][k - 1][0]} -> {m["rules"][k - 1][1]} min'
            top = g[sel].sort_values('spending', ascending=False).head(3)
            ex = '; '.join(f'{a} [{b}]' for a, b in zip(top['descr'].fillna('no description'), top['brand'].fillna('')))
            print(f'  {label}: {sel.sum():,} barcodes, {100 * sp[sel].sum() / sp.sum():.1f}% of spending | {ex}')
        print(f'  mean minutes per session, weighted by packages: {np.average(mins, weights=np.maximum(q, 1e-9)):.1f}; '
              f'basis pkg on {100 * sp[basis == "pkg"].sum() / sp.sum():.0f}% of spending')


def main():
    rules = load_rules(Path(__file__).resolve().parent / 'prep_minutes_rules.py')
    validate(rules)
    d = pd.read_parquet(BASE)
    out = []
    for code, g in d.groupby('product_module_code'):
        if code in rules:
            mins, basis, rule, why = apply_module(g, rules[code])
        else:
            mins, basis, rule, why = np.zeros(len(g)), np.full(len(g), 'serv4', dtype=object), np.full(len(g), -1, dtype=np.int16), np.full(len(g), 'module without rules', dtype=object)
        out.append(pd.DataFrame({'upc': g['upc'].values, 'product_module_code': code, 'min_session': mins, 'basis': basis, 'rule': rule, 'why': why}))
    out = pd.concat(out, ignore_index=True)
    out.to_parquet(OUT, index=False)
    w = d.set_index('upc')['spending'].reindex(out['upc']).values
    print(f'{len(out):,} barcodes; modules with rules {out.loc[out.rule >= 0, "product_module_code"].nunique()}, without {out.loc[out.rule < 0, "product_module_code"].nunique()} '
          f'({100 * w[out.rule.values < 0].sum() / w.sum():.1f}% of spending)')
    print(f'share of spending: zero minutes {100 * w[out.min_session.values == 0].sum() / w.sum():.1f}%, matched by a rule {100 * w[out.rule.values > 0].sum() / w.sum():.1f}%, '
          f'basis pkg {100 * w[(out.basis == "pkg").values].sum() / w.sum():.1f}%')
    print(f'saved {OUT}')


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'check':
        check(sys.argv[2], [int(c) for c in sys.argv[3:]])
    else:
        main()
