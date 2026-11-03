"""Flag ultra-processed products from USDA Branded Food ingredient lists (NOVA group 4 markers).

Reads raw/usda/Branded Food/FoodData_Central_branded_food_csv_2024-04-18/branded_food.csv
(gtin_upc, ingredients, branded_food_category) and writes
interim/usda_nielsen_merged/usda_upf.parquet keyed by the canonical 13-digit UPC (GTIN with
the check digit dropped, as in build_usda_wide.py).

Rule (NOVA, Monteiro et al. 2019, operationalized on ingredient lists as in the US
literature on the NHANES and FoodData Central files): a product is ultra-processed if its
ingredient list contains at least one substance of either kind below.
  (a) food substances of no or rare culinary use: high-fructose corn syrup and other corn
      syrups, invert sugar, maltodextrin, dextrose, hydrogenated or interesterified oils,
      hydrolyzed proteins, protein isolates and concentrates, modified starch, mechanically
      separated meat;
  (b) cosmetic additives: flavors and flavor enhancers (natural or artificial flavor, MSG,
      yeast extract, disodium inosinate/guanylate), colors (FD&C dyes, caramel color,
      annatto, titanium dioxide), emulsifiers and emulsifying salts (lecithin, mono- and
      diglycerides, polysorbate, DATEM, sodium stearoyl lactylate, sodium/potassium/calcium
      phosphates), non-sugar sweeteners (aspartame, sucralose, acesulfame, saccharin,
      stevia/rebaudioside, sugar alcohols), thickeners, gelling, bulking and glazing agents
      (carrageenan, xanthan, guar, locust bean and cellulose gums, carboxymethylcellulose,
      microcrystalline cellulose, polydextrose), humectants (glycerin, propylene glycol),
      anti-caking and anti-foaming agents (silicon dioxide, dimethylpolysiloxane).
Each marker is a regular expression; the output keeps the count of markers hit, the list of
hit marker names and the number of ingredients, so the rule can be tightened later (for
example, requiring two markers, or dropping the flavor and lecithin markers).
Products with an empty ingredient list are left unclassified (upf missing).
"""
import re, os, time
import pandas as pd

USDA = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/usda/Branded Food/FoodData_Central_branded_food_csv_2024-04-18/branded_food.csv'
OUT = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda_nielsen_merged/usda_upf.parquet'

MARKERS = {
    # (a) substances of no or rare culinary use
    'corn_syrup': r'corn syrup|hfcs|glucose syrup|glucose-fructose|invert (sugar|syrup)|rice syrup|tapioca syrup',
    'maltodextrin_dextrose': r'maltodextrin|dextrose|dextrin\b',
    'hydrogenated_oil': r'hydrogenated|interesterified',
    'protein_isolate': r'protein isolate|protein concentrate|hydrolyzed (soy|corn|wheat|vegetable|protein)|hydrolysed|whey protein|soy protein|textured (vegetable|soy) protein|caseinate',
    'modified_starch': r'modified (corn |food |tapioca |potato |wheat )?starch',
    'mech_separated_meat': r'mechanically separated',
    # (b) cosmetic additives
    'flavor': r'\bflavou?r(s|ing|ings)?\b',
    'flavor_enhancer': r'monosodium glutamate|\bmsg\b|yeast extract|disodium (inosinate|guanylate)|autolyzed yeast',
    'color': r'\bcolou?r(s|ed|ing)?\b|fd&c|fd and c|yellow [56]|red 40|blue [12]|caramel colou?r|annatto|titanium dioxide|carmine|beta.?carotene \(colou?r|turmeric \(colou?r',
    'emulsifier': r'lecithin|mono.?and.?diglycerides|monoglycerides|diglycerides|polysorbate|datem|sodium stearoyl|sorbitan|polyglycerol|sucrose esters',
    'emulsifying_salt': r'(di|tri|mono)?sodium phosphate|sodium (hexameta|tripoly|acid pyro)phosphate|potassium phosphate|calcium phosphate|sodium citrate',
    'sweetener': r'aspartame|sucralose|acesulfame|saccharin|stevia|rebaudioside|neotame|advantame|sorbitol|xylitol|erythritol|maltitol|mannitol|isomalt',
    'thickener_gum': r'carrageenan|xanthan|guar gum|locust bean|cellulose gum|carboxymethyl|methylcellulose|microcrystalline cellulose|polydextrose|gellan|agar|alginate|pectin|gum arabic|gum acacia|tara gum|konjac',
    'humectant': r'glycerin|glycerol|propylene glycol|sorbitol syrup',
    'anticaking_antifoam': r'silicon dioxide|silica|dimethylpolysiloxane|calcium silicate|sodium aluminosilicate|magnesium stearate',
}
RX = {k: re.compile(v, re.I) for k, v in MARKERS.items()}


def canon_upc(g):
    g = str(g).strip()
    if not g.isdigit():
        return None
    return g.zfill(14)[:-1]


def classify(ing):
    if not isinstance(ing, str) or not ing.strip():
        return None, None, None
    hits = [k for k, rx in RX.items() if rx.search(ing)]
    n_ing = ing.count(',') + 1
    return len(hits), ';'.join(hits), n_ing


def main():
    t0 = time.time()
    parts = []
    for i, ch in enumerate(pd.read_csv(USDA, usecols=['fdc_id', 'gtin_upc', 'ingredients', 'branded_food_category', 'available_date', 'modified_date', 'discontinued_date'],
                                       dtype=str, chunksize=200_000)):
        ch['upc13'] = ch['gtin_upc'].map(canon_upc)
        ch = ch.dropna(subset=['upc13'])
        res = ch['ingredients'].map(classify)
        ch['n_markers'] = [r[0] for r in res]
        ch['markers'] = [r[1] for r in res]
        ch['n_ingredients'] = [r[2] for r in res]
        parts.append(ch[['fdc_id', 'upc13', 'branded_food_category', 'n_markers', 'markers', 'n_ingredients', 'available_date', 'modified_date', 'discontinued_date']])
        print(f'chunk {i}: {len(ch):,} rows ({time.time() - t0:.0f}s)', flush=True)
    df = pd.concat(parts, ignore_index=True)
    # one row per UPC: keep the most recently modified record
    df = df.sort_values(['upc13', 'modified_date']).drop_duplicates('upc13', keep='last')
    df['upf'] = (df['n_markers'] >= 1).astype('float')
    df.loc[df['n_markers'].isna(), 'upf'] = None
    df['upf_strict'] = (df['n_markers'] >= 2).astype('float')
    df.loc[df['n_markers'].isna(), 'upf_strict'] = None
    df.to_parquet(OUT, index=False)
    print(f'saved {len(df):,} UPCs; classified {df.upf.notna().sum():,}; ultra-processed (>=1 marker) '
          f'{df.upf.mean():.3f}; >=2 markers {df.upf_strict.mean():.3f} ({time.time() - t0:.0f}s)')
    print(df['markers'].str.split(';').explode().value_counts().head(20).to_string())


if __name__ == '__main__':
    main()
