"""Classify USDA Branded Food products into NOVA groups from their ingredient lists.

Supersedes build_usda_upf.py (2026-11-03): the group-4 marker rule is unchanged; groups 1-3
are added so that products can be compared as unprocessed (1) against ultra-processed (4).

Reads raw/usda/Branded Food/FoodData_Central_branded_food_csv_2024-04-18/branded_food.csv
(gtin_upc, ingredients, branded_food_category) and writes
interim/usda_nielsen_merged/usda_nova.parquet keyed by the canonical 13-digit UPC (GTIN with
the check digit dropped, as in build_usda_wide.py). The ingredient text is kept in the
output so the rules can be changed without re-reading the 3 GB csv.

NOVA (Monteiro et al. 2019), operationalized on the ingredient list:
  group 4, ultra-processed: at least one marker of either kind below.
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
  group 2, processed culinary ingredients: no marker, and every ingredient is an oil, fat,
    sugar, syrup, honey, salt or vinegar (oils, butter, lard, sugar, honey, maple syrup,
    table salt, vinegar).
  group 3, processed: no marker, not group 2, and the list contains an added culinary
    ingredient (sugar, salt, oil, fat, honey, syrup, vinegar) or a preservative or curing
    agent (benzoate, sorbate, nitrite, nitrate, sulfite, BHA, BHT, TBHQ, propionate,
    smoke). Cheese, bread, canned vegetables with salt, salted nuts and cured meat fall here.
  group 1, unprocessed or minimally processed: no marker and none of the group-3 terms
    (fresh, frozen and dried produce, plain milk and yogurt, eggs, meat, fish, grains,
    flour, pasta, unsalted nuts, legumes, spices, coffee, tea, water, 100% juice).
Vitamins and minerals (enrichment), cultures, enzymes, water, citric and ascorbic acid and
calcium chloride are treated as neutral. Products with an empty ingredient list are left
unclassified (nova missing). Columns upf (= group 4) and upf_strict (two or more markers)
are kept for the scripts that used usda_upf.parquet.
"""
import re, time
import numpy as np
import pandas as pd

USDA = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/raw/usda/Branded Food/FoodData_Central_branded_food_csv_2024-04-18/branded_food.csv'
OUT = '/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/usda_nielsen_merged/usda_nova.parquet'

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

# group-3 terms: added culinary ingredients and preservatives
ADDED = {
    'sugar': r'\bsugar\b|\bsugars\b|\bhoney\b|\bsyrup\b|molasses|\bnectar\b|evaporated cane|cane juice|agave|brown rice syrup|\bsucrose\b|\bfructose\b',
    'salt': r'\bsalt\b|\bsalted\b|sodium chloride|\bbrine\b',
    'fat': r'\boils?\b|\bbutter\b|\blard\b|shortening|\btallow\b|\bghee\b|\bmargarine\b|milk ?fat|cocoa butter|cream cheese',
    'vinegar': r'vinegar|acetic acid',
    'preservative': r'benzoate|sorbate|(?<![a-z])nitr(ite|ate)s?\b|sulfite|sulphite|bisulfite|\bbha\b|\bbht\b|tbhq|propionate|\bsmoke\b|\bsmoked\b|\bcured\b|\bcuring\b|edta|natamycin|nisin|\bpreservative',
}
RX_ADDED = {k: re.compile(v, re.I) for k, v in ADDED.items()}
# group-2 products: every ingredient is one of these
CULINARY = re.compile(r'^(organic |pure |extra virgin |virgin |refined |unrefined |expeller pressed |cold pressed |100% |natural |raw |filtered |clover |sea |kosher |iodized |table |brown |cane |granulated |powdered |light |dark |salted |unsalted |sweet |clarified |rendered |)*'
                      r'([a-z ]*oil|[a-z ]*oils|butter|lard|tallow|ghee|shortening|sugar|honey|maple syrup|cane syrup|molasses|corn starch|salt|vinegar|[a-z ]*vinegar|cream of tartar|baking soda|baking powder|sodium bicarbonate|water|iodide|potassium iodide|dextrose|calcium silicate|lactic acid|citric acid|cultures|annatto)\.?$', re.I)
NOISE = re.compile(r'\b(ingredients?|contains? (2|two)\s?(%|percent) or less( of)?( each of)?( the following)?|made (from|with)|and|or|may contain)\b[: ]*', re.I)


def canon_upc(g):
    g = str(g).strip()
    if not g.isdigit():
        return None
    return g.zfill(14)[:-1]


def tokens(ing):
    """Top-level ingredient names with parentheticals removed."""
    s = re.sub(r'\([^()]*\)|\[[^\[\]]*\]', '', ing)
    s = NOISE.sub(' ', s)
    toks = [t.strip(' .;:*') for t in re.split(r'[,;.]', s)]
    return [t for t in toks if t]


def classify(ing):
    if not isinstance(ing, str) or not ing.strip():
        return None
    hits = [k for k, rx in RX.items() if rx.search(ing)]
    n_ing = ing.count(',') + 1
    added = [k for k, rx in RX_ADDED.items() if rx.search(ing)]
    if hits:
        nova = 4
    else:
        toks = tokens(ing)
        if toks and all(CULINARY.match(t) for t in toks) and any(k in added for k in ('sugar', 'salt', 'fat', 'vinegar')):
            nova = 2
        elif added:
            nova = 3
        else:
            nova = 1
    return len(hits), ';'.join(hits), n_ing, ';'.join(added), nova


def main():
    t0 = time.time()
    parts = []
    cols = ['fdc_id', 'gtin_upc', 'ingredients', 'branded_food_category', 'available_date', 'modified_date', 'discontinued_date']
    for i, ch in enumerate(pd.read_csv(USDA, usecols=cols, dtype=str, chunksize=200_000)):
        ch['upc13'] = ch['gtin_upc'].map(canon_upc)
        ch = ch.dropna(subset=['upc13']).copy()
        res = ch['ingredients'].map(classify)
        ok = res.notna()
        ch['n_markers'] = np.where(ok, [r[0] if r else None for r in res], None)
        ch['markers'] = [r[1] if r else None for r in res]
        ch['n_ingredients'] = [r[2] if r else None for r in res]
        ch['added'] = [r[3] if r else None for r in res]
        ch['nova'] = [r[4] if r else None for r in res]
        parts.append(ch[['fdc_id', 'upc13', 'branded_food_category', 'ingredients', 'n_markers', 'markers', 'n_ingredients', 'added', 'nova',
                         'available_date', 'modified_date', 'discontinued_date']])
        print(f'chunk {i}: {len(ch):,} rows ({time.time() - t0:.0f}s)', flush=True)
    df = pd.concat(parts, ignore_index=True)
    # one row per UPC: keep the most recently modified record
    df = df.sort_values(['upc13', 'modified_date']).drop_duplicates('upc13', keep='last')
    for c in ('n_markers', 'n_ingredients', 'nova'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    df['upf'] = (df['n_markers'] >= 1).astype('float')
    df.loc[df['n_markers'].isna(), 'upf'] = np.nan
    df['upf_strict'] = (df['n_markers'] >= 2).astype('float')
    df.loc[df['n_markers'].isna(), 'upf_strict'] = np.nan
    df.to_parquet(OUT, index=False)
    cl = df['nova'].notna()
    print(f'saved {len(df):,} UPCs; classified {cl.sum():,}; NOVA shares: '
          + ', '.join(f'{g}: {(df.loc[cl, "nova"] == g).mean():.3f}' for g in (1, 2, 3, 4)) + f' ({time.time() - t0:.0f}s)')
    print(df['markers'].str.split(';').explode().value_counts().head(20).to_string())
    print('\nNOVA group by USDA category (top 30 categories):')
    top = df['branded_food_category'].value_counts().head(30).index
    print(pd.crosstab(df.loc[cl & df.branded_food_category.isin(top), 'branded_food_category'], df.loc[cl & df.branded_food_category.isin(top), 'nova'], normalize='index').round(2).to_string())


if __name__ == '__main__':
    main()
