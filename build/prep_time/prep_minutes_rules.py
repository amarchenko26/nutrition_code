"""Minutes of preparation: the rules, per Nielsen product module (written 2026-11-18; replaces the 1-9 module score of
build_prep_time_index.py for the household outcome).

RULES[module code] = dict(name, minutes, basis, why, rules). The rules themselves are typed in build/prep_time/rules/, one file
per cluster of product groups; this file joins them and states the definitions they follow.

MINUTES = elapsed minutes of ONE cooking session that makes about four servings, from the moment the cook starts until the food
is ready to eat, by the package directions or the usual home method, with steps done in a sensible order (the oven heats while
the batter is mixed).
  Counts: washing, peeling, chopping, mixing, shaping; heating the water, pan or oven; cooking, baking, simmering, microwaving;
          standing times under 10 minutes that the directions require.
  Does not count: thawing, rising, marinating, soaking, chilling or setting, cooling; shopping; cleaning up; eating.
  0 = eaten or drunk as sold (opening, pouring, slicing bread, spreading, pouring milk over cereal).
  Condiments, sauces, gravies, dressings, spreads, dips, jams, sweeteners, syrups, butter, margarine, toppings, pickles and baking
  additions are 0: the main food of the dish carries the time.
  Raw main ingredients (flour, corn meal, dry beans, rice, dry pasta, raw meat, poultry and fish, eggs, vegetables that are
  cooked) carry the time of the typical simple dish made with them. Foods with package directions get the time of the directions,
  by the method most households use (single-serve microwave products: the microwave; pizzas, pies, lasagna, breaded frozen
  items, baked goods: the oven, including about 10 minutes for the oven to heat when nothing else is done meanwhile).

BASIS = how many sessions one package makes (build_prep_minutes_package.py):
  'serv4' (default): the package is portioned over several meals; one session cooks four label servings.
  'pkg': the package is cooked or prepared all at once (whole birds, roasts, hams; cake, brownie, muffin, cookie and bread
         mixes; refrigerated dough; frozen pizzas, pies and multi-serve entrees; boxed dinners and kits; gelatin and pudding
         mixes; juice concentrate); minutes = the time to cook the whole package once.

A module's `minutes`, `basis`, `why` apply to barcodes that none of its rules match. Each rule is (conditions, minutes, why) or
(conditions, minutes, why, basis); the FIRST rule that matches a barcode gives its minutes. conditions: a dict, all entries must
hold; key = type, style, form, use, product, variety, formula, flavor, container, common_consumer_name (Nielsen's attributes,
interim/products_extra), descr (the abbreviated description) or brand; value = a regular expression searched in the upper-case
text (a missing value is the empty string); a key ending in _not requires that the pattern is not found. Two numeric keys test
the package size in grams or ml (size x multi): grams_min (at least) and grams_max (fewer than); a package sized by count
matches neither.
Check a rules file against the data:  python build_prep_minutes_upc.py check rules/<file>.py [module codes]
"""
import importlib.util
from pathlib import Path

RULES = {}
for _f in sorted((Path(__file__).resolve().parent / 'rules').glob('rules_*.py')):
    _spec = importlib.util.spec_from_file_location(_f.stem, _f)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    _dup = set(RULES) & set(_mod.RULES)
    assert not _dup, f'{_f.name}: modules already defined in another file: {sorted(_dup)}'
    RULES.update(_mod.RULES)
