"""
build_hei_crosswalk.py

Assigns each Nielsen product module to a USDA HEI-2020 food-pattern group with a
grams-per-equivalent conversion factor, so household cup/oz food-pattern
equivalents can be computed from purchase grams (labels do not carry
food-pattern equivalents). Same authoring pattern as build_claude_hi.py:
explicit per-module judgments, versioned in code, exported as a reviewable
artifact.

Schema: module_code -> (tag, grams_per_equiv, whole_share, note)
  tag ∈ {fruit_whole, fruit_juice, veg, greens_beans, legume, grain, dairy,
         protein, seafood_plant}  (modules not listed contribute to NO
         food-pattern group -- they still contribute calories and the four
         label-based HEI components)
  grams_per_equiv: grams of AS-PURCHASED product per ONE cup-eq (fruit/veg/
         dairy) or oz-eq (grains/protein). Factors follow FPED/MyPlate
         conventions with stated approximations (e.g., canned fruit/veg
         include packing liquid; dried fruit counts double; nuts 14 g/oz-eq;
         raw meat 37 g/oz-eq for a 28 g cooked oz-eq).
  whole_share: for tag='grain' only -- fixed share of the module's grain
         equivalents scored as WHOLE grains (remainder refined). Fixed,
         auditable module-level shares instead of per-UPC text regexes.

Group nesting handled downstream (build_hei_panel.py):
  total fruits = fruit_whole + fruit_juice;  whole fruits = fruit_whole
  total veg    = veg + greens_beans + legume;  greens&beans = greens_beans + legume
  total protein= protein + seafood_plant + legume;  seafood/plant = seafood_plant + legume
  whole grains = grain*ws;  refined grains = grain*(1-ws);  dairy = dairy

V1 limitations (stated): mixed dishes (frozen/canned entrees, pizza, soup,
sandwiches, dry dinners with sauces) get NO food-pattern group -- their
ingredients span groups and Nielsen grams cannot be apportioned. They still
enter calories and the label-based components. This understates food-group
equivalents roughly proportionally across households.

Output: interim/hei/module_hei_crosswalk.parquet (+ .csv for review)
"""
import pandas as pd
from pathlib import Path

OUT_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/hei')
OUT_DIR.mkdir(parents=True, exist_ok=True)
UNIVERSE = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data'
                ) / 'interim' / 'usda_comparison' / 'upc_universe.parquet'

X = {
# ============ FRESH PRODUCE ============
4225: ('fruit_whole', 150, 0, "fresh fruit remaining: 1 cup-eq ~150g"),
4470: ('fruit_whole', 150, 0, "strawberries"),
4010: ('fruit_whole', 150, 0, "apples"),
4355: ('fruit_whole', 150, 0, "oranges"),
4180: ('fruit_whole', 150, 0, "grapefruit"),
4085: ('fruit_whole', 100, 0, "cranberries (tart, used in smaller cup weights)"),
4400: ('veg', 150, 0, "potatoes: starchy veg counts in total veg"),
4280: ('veg', 150, 0, "fresh veg remaining"),
4475: ('veg', 150, 0, "tomatoes"),
4050: ('veg', 130, 0, "carrots (~128g/cup chopped)"),
4275: ('veg', 100, 0, "lettuce: 2 cups raw leafy = 1 cup-eq"),
4023: ('veg', 150, 0, "mushrooms"),
4350: ('veg', 160, 0, "onions"),
4060: ('veg', 120, 0, "celery"),
4460: ('greens_beans', 60, 0, "spinach: dark green; 2 cups raw (~60g) = 1 cup-eq"),
4055: ('veg', 130, 0, "cauliflower"),
4230: ('fruit_whole', 150, 0, "kiwi (miscoded V in Nielsen produce flags; fruit)"),
4415: ('veg', 120, 0, "radishes"),
4015: ('veg', 100, 0, "sprouts"),
# herbs 4020, garlic 4140: seasoning quantities -> no group
# ============ FRUIT - CANNED / DRIED ============
1021: ('fruit_whole', 200, 0, "canned pineapple incl juice/syrup weight"),
1017: ('fruit_whole', 200, 0, "canned peaches"),
1003: ('fruit_whole', 245, 0, "applesauce: 1 cup = 1 cup-eq"),
1014: ('fruit_whole', 200, 0, "canned oranges"),
1009: ('fruit_whole', 250, 0, "cranberry sauce: heavy syrup share"),
1090: ('veg', 245, 0, "canned pumpkin: red/orange veg"),
1020: ('fruit_whole', 200, 0, "canned pears"),
1011: ('fruit_whole', 200, 0, "fruit cocktail"),
1012: ('fruit_whole', 200, 0, "fruit mixes"),
1024: ('fruit_whole', 200, 0, "canned cherries"),
1007: ('fruit_whole', 200, 0, "canned apricots"),
1028: ('fruit_whole', 200, 0, "canned fruit remaining"),
1002: ('fruit_whole', 300, 0, "pie filling: fruit share diluted by sugar syrup"),
1425: ('fruit_whole', 75, 0, "dried fruit: 1/2 cup dried = 1 cup-eq (~75g)"),
1424: ('fruit_whole', 75, 0, "raisins"),
1423: ('fruit_whole', 75, 0, "prunes"),
1426: ('fruit_whole', 75, 0, "dates"),
2664: ('fruit_whole', 150, 0, "frozen fruit"),
3560: ('fruit_whole', 150, 0, "refrigerated fruit (cut fruit)"),
3563: ('fruit_whole', 175, 0, "refrigerated fruit salads (some dressing weight)"),
# ============ JUICE ============
1040: ('fruit_juice', 240, 0, "orange juice: 1 cup = 1 cup-eq total fruit"),
1044: ('fruit_juice', 240, 0, "fruit juice remaining (100% juice)"),
1033: ('fruit_juice', 240, 0, "apple juice"),
1034: ('fruit_juice', 240, 0, "grape juice"),
1032: ('fruit_juice', 240, 0, "grapefruit juice"),
1038: ('fruit_juice', 240, 0, "pineapple juice"),
1039: ('fruit_juice', 240, 0, "prune juice"),
1036: ('fruit_juice', 480, 0, "lemon/lime juice: used in small quantities, ~50% credit"),
1045: ('fruit_juice', 300, 0, "nectars: partial juice content"),
1042: ('fruit_juice', 600, 0, "fruit drinks: ~40% juice content assumed"),
1041: ('fruit_juice', 600, 0, "fruit drinks canned: ~40% juice"),
1030: ('fruit_juice', 550, 0, "cranberry juice/drink blends: ~45% juice"),
2667: ('fruit_juice', 70, 0, "frozen OJ concentrate: reconstitutes ~3.5x"),
2668: ('fruit_juice', 70, 0, "frozen grape juice concentrate"),
2666: ('fruit_juice', 70, 0, "frozen apple juice concentrate"),
2674: ('fruit_juice', 70, 0, "frozen juice remaining, concentrate"),
2670: ('fruit_juice', 170, 0, "frozen fruit drink mixes: partial juice, concentrate"),
1054: ('veg', 240, 0, "tomato juice: 1 cup = 1 cup-eq veg"),
1055: ('veg', 240, 0, "vegetable juice (V8 etc.)"),
# ============ VEGETABLES - CANNED ============
1105: ('veg', 220, 0, "canned tomatoes remaining, incl liquid"),
1060: ('veg', 220, 0, "canned green beans"),
1072: ('veg', 220, 0, "canned corn: starchy veg"),
1109: ('veg', 125, 0, "tomato sauce: ~2x concentrated"),
1144: ('veg', 220, 0, "canned mushrooms"),
1080: ('veg', 220, 0, "canned peas"),
1076: ('legume', 250, 0, "kidney/red beans: legume (veg+G&B+protein)"),
1065: ('legume', 250, 0, "canned beans remaining"),
1077: ('veg', 220, 0, "canned onions"),
1108: ('veg', 65, 0, "tomato paste: ~4x concentrated"),
1088: ('legume', 250, 0, "chili beans"),
1082: ('veg', 220, 0, "mixed vegetables"),
1057: ('veg', 220, 0, "artichokes"),
1068: ('veg', 220, 0, "beets"),
1071: ('veg', 220, 0, "cream-style corn"),
1107: ('veg', 220, 0, "whole canned tomatoes"),
1093: ('veg', 220, 0, "sauerkraut"),
1086: ('veg', 220, 0, "sweet potatoes/yams: red/orange veg"),
1110: ('veg', 220, 0, "stewed tomatoes"),
1087: ('legume', 250, 0, "pinto beans"),
1058: ('veg', 220, 0, "asparagus"),
1085: ('veg', 220, 0, "canned potatoes"),
1066: ('legume', 250, 0, "garbanzo beans"),
1070: ('veg', 220, 0, "canned carrots"),
1062: ('legume', 250, 0, "white/northern/navy beans"),
1263: ('veg', 220, 0, "oriental canned vegetables"),
1079: ('veg', 220, 0, "peas remaining"),
1094: ('greens_beans', 220, 0, "canned spinach: dark green"),
1064: ('legume', 250, 0, "vegetarian beans"),
1067: ('legume', 250, 0, "lima beans"),
1102: ('greens_beans', 220, 0, "canned greens (collard etc.): dark green"),
1097: ('veg', 220, 0, "canned veg remaining"),
1106: ('veg', 90, 0, "tomato puree: ~2.7x concentrated"),
1074: ('veg', 220, 0, "hominy"),
1081: ('veg', 220, 0, "peas & carrots"),
1160: ('veg', 220, 0, "pickled vegetables & fruit"),
# ============ VEGETABLES - FROZEN ============
2634: ('veg', 100, 0, "frozen potatoes/fries: partially dehydrated by frying prep"),
2640: ('veg', 150, 0, "frozen mixed veg"),
2636: ('veg', 150, 0, "frozen veg remaining"),
2639: ('veg', 170, 0, "veg in sauce: sauce weight"),
2641: ('greens_beans', 150, 0, "frozen broccoli: dark green"),
2630: ('veg', 150, 0, "frozen corn"),
2620: ('veg', 150, 0, "frozen peas"),
2633: ('veg', 150, 0, "frozen green beans"),
2608: ('veg', 150, 0, "corn on the cob"),
2638: ('veg', 170, 0, "breaded onions"),
2644: ('legume', 150, 0, "frozen lima beans"),
2642: ('veg', 150, 0, "frozen carrots"),
2635: ('veg', 170, 0, "breaded veg"),
# ============ VEG/GRAINS DRIED ============
1319: ('grain', 28, 0.10, "rice packaged/bulk: mostly white; ~10% brown/wild"),
1321: ('grain', 28, 0.05, "instant rice: predominantly white"),
1315: ('legume', 95, 0, "dry beans: 1 cup-eq cooked ~ 95g dry"),
1316: ('legume', 95, 0, "dry peas/lentils"),
# ============ SALAD / DELI PRODUCE ============
3544: ('veg', 100, 0, "precut salad mix: leafy, 2 cups = 1 cup-eq"),
3565: ('veg', 220, 0, "refrigerated sauerkraut"),
# ============ BREAD AND BAKED GOODS ============
4000: ('grain', 28, 0.15, "fresh bread: 1 slice (28g) = 1 oz-eq; ~15% whole-grain share"),
4001: ('grain', 32, 0.10, "buns"),
4002: ('grain', 32, 0.10, "rolls"),
4008: ('grain', 32, 0.10, "bagels"),
4003: ('grain', 55, 0.05, "muffins: ~half flour by weight"),
4004: ('grain', 65, 0.02, "fresh cakes: grain share diluted by fat/sugar"),
4005: ('grain', 60, 0.02, "breakfast cakes/sweet rolls"),
4006: ('grain', 60, 0.02, "doughnuts"),
4007: ('grain', 70, 0.02, "fresh pies"),
4011: ('grain', 55, 0.05, "bakery remaining"),
2654: ('grain', 28, 0.15, "frozen bread"),
2647: ('grain', 32, 0.10, "frozen bagels"),
2651: ('grain', 40, 0.05, "frozen biscuits/rolls/muffins"),
2646: ('grain', 30, 0.10, "frozen bread dough"),
2660: ('grain', 70, 0.02, "frozen pies"),
2661: ('grain', 55, 0.02, "frozen bakery remaining"),
2656: ('grain', 65, 0.02, "frozen dessert cakes"),
2652: ('grain', 60, 0.02, "frozen breakfast cakes"),
2653: ('grain', 65, 0.02, "cobbler/strudel"),
# ============ CEREAL / BREAKFAST ============
1344: ('grain', 28, 0.35, "RTE cereal: 28g = 1 oz-eq; ~35% whole-grain (spend-wtd guess)"),
1348: ('grain', 28, 0.85, "hot cereal: predominantly oatmeal"),
1346: ('grain', 33, 0.90, "granola & natural cereals"),
1366: ('grain', 28, 0.05, "grits: degermed corn, refined"),
1349: ('grain', 28, 1.00, "wheat germ"),
1441: ('grain', 38, 0.45, "granola/yogurt bars: ~oat-based, sugar-diluted"),
1445: ('grain', 38, 0.40, "breakfast bars"),
1351: ('grain', 50, 0.05, "toaster pastries"),
2659: ('grain', 35, 0.10, "frozen waffles/pancakes"),
1395: ('grain', 30, 0.10, "pancake mix (dry)"),
# ============ PASTA / FLOUR / GRAIN-BASED ============
1331: ('grain', 28, 0.03, "dry macaroni: 28g dry = 1 oz-eq"),
1334: ('grain', 28, 0.03, "dry spaghetti"),
1336: ('grain', 28, 0.03, "noodles & dumplings"),
1266: ('grain', 28, 0.02, "oriental noodles"),
3360: ('grain', 40, 0.03, "fresh refrigerated pasta (hydrated)"),
2692: ('grain', 60, 0.03, "frozen plain pasta (cooked weight)"),
1393: ('grain', 28, 0.00, "white flour"),
1377: ('grain', 28, 0.30, "flour remaining (incl whole wheat)"),
1391: ('grain', 28, 0.10, "single purpose flour"),
1367: ('grain', 28, 0.20, "corn meal"),
1257: ('grain', 30, 0.50, "tortillas: corn (whole, masa) vs flour (refined) mix"),
1255: ('grain', 30, 0.60, "taco shells: mostly corn masa"),
1363: ('grain', 28, 0.95, "rice cakes: brown rice"),
1320: ('grain', 30, 0.05, "rice mixes: white rice + seasoning"),
1340: ('grain', 40, 0.03, "dry pasta dinners (mac&cheese: pasta + cheese powder)"),
1364: ('grain', 28, 0.05, "stuffing: dried bread"),
1343: ('grain', 28, 0.05, "breading"),
1350: ('grain', 28, 0.05, "croutons"),
1408: ('grain', 28, 0.05, "graham/dessert crumbs"),
# ============ CRACKERS / COOKIES / SNACK GRAINS ============
1360: ('grain', 28, 0.10, "flavored snack crackers"),
1356: ('grain', 28, 0.05, "cheese crackers"),
1355: ('grain', 28, 0.05, "butter crackers"),
1353: ('grain', 28, 0.05, "soda crackers"),
1354: ('grain', 28, 0.30, "graham crackers (part whole wheat)"),
1352: ('grain', 28, 0.15, "flake crackers"),
1359: ('grain', 28, 0.15, "wafers/breadsticks"),
1357: ('grain', 28, 0.20, "crackers remaining (incl whole-grain types)"),
1361: ('grain', 28, 0.05, "oyster crackers"),
1247: ('grain', 28, 0.10, "matzo"),
1341: ('grain', 30, 0.05, "sandwich cracker packs"),
1362: ('grain', 55, 0.03, "cookies: ~half flour"),
1365: ('grain', 30, 0.02, "ice cream cones"),
1330: ('grain', 28, 0.10, "pretzels"),
1326: ('grain', 28, 0.60, "tortilla chips: corn masa"),
1325: ('grain', 28, 0.60, "corn chips"),
1318: ('grain', 28, 0.05, "puffed cheese snacks: cornmeal base"),
1329: ('grain', 8, 1.00, "unpopped popcorn: whole grain; 8g kernels = 1 oz-eq (3c popped)"),
1328: ('grain', 28, 1.00, "popped popcorn"),
1332: ('grain', 35, 0.90, "caramel corn (popcorn base, sugar-diluted)"),
1452: ('grain', 40, 0.45, "health bars: oat/grain base"),
1333: ('grain', 28, 0.15, "snack variety packs"),
# dough products (refrigerated)
3613: ('grain', 40, 0.03, "refrigerated biscuits"),
3614: ('grain', 40, 0.03, "refrigerated dinner rolls"),
3595: ('grain', 55, 0.02, "refrigerated cookie/brownie dough"),
3596: ('grain', 50, 0.02, "refrigerated sweet rolls"),
3615: ('grain', 40, 0.03, "refrigerated dough remaining"),
# baking mixes (dry, grain-based)
1380: ('grain', 45, 0.02, "brownie mix"),
1375: ('grain', 45, 0.02, "layer cake mix"),
1381: ('grain', 40, 0.05, "muffin mix"),
1386: ('grain', 35, 0.03, "rolls & biscuit mix"),
1383: ('grain', 33, 0.15, "bread mix"),
1387: ('grain', 45, 0.02, "cookie mix"),
1373: ('grain', 45, 0.02, "specialty cake mix"),
1384: ('grain', 45, 0.02, "dessert mixes misc"),
1376: ('grain', 45, 0.02, "small layer cake mix"),
1374: ('grain', 45, 0.02, "small specialty cake mix"),
1358: ('grain', 45, 0.02, "prepared pie/pastry shells"),
# ============ DAIRY ============
3625: ('dairy', 245, 0, "milk: 1 cup = 1 cup-eq"),
3592: ('dairy', 245, 0, "flavored milk"),
3626: ('dairy', 245, 0, "buttermilk"),
3650: ('dairy', 245, 0, "eggnog"),
3628: ('dairy', 300, 0, "refrigerated shakes/drinks: partial dairy"),
1297: ('dairy', 125, 0, "canned evaporated milk: 2x concentrated"),
1296: ('dairy', 245, 0, "shelf-stable milk"),
1298: ('dairy', 30, 0, "powdered milk: ~1/8 reconstituted weight"),
3603: ('dairy', 245, 0, "yogurt: 1 cup = 1 cup-eq"),
3612: ('dairy', 245, 0, "yogurt drinks"),
3590: ('dairy', 42, 0, "shredded cheese: 1.5 oz natural = 1 cup-eq"),
3550: ('dairy', 42, 0, "cheddar"),
3546: ('dairy', 42, 0, "mozzarella"),
3548: ('dairy', 42, 0, "natural cheese remaining"),
3588: ('dairy', 42, 0, "specialty/imported cheese"),
3587: ('dairy', 42, 0, "swiss"),
3549: ('dairy', 42, 0, "colby"),
3545: ('dairy', 42, 0, "muenster"),
3589: ('dairy', 42, 0, "variety pack cheese"),
3553: ('dairy', 42, 0, "grated cheese"),
3586: ('dairy', 56, 0, "processed slices: 2 oz processed = 1 cup-eq"),
3554: ('dairy', 56, 0, "processed slices remaining"),
3555: ('dairy', 56, 0, "processed loaves"),
3556: ('dairy', 56, 0, "processed snack cheese"),
3605: ('dairy', 450, 0, "cottage cheese: 2 cups = 1 cup-eq"),
3552: ('dairy', 250, 0, "ricotta"),
2672: ('dairy', 400, 0, "bulk ice cream: 1 cup ~ 1/3 cup-eq dairy"),
2675: ('dairy', 400, 0, "frozen novelties"),
2671: ('dairy', 300, 0, "frozen yogurt"),
3566: ('dairy', 400, 0, "refrigerated pudding: ~half milk"),
# cream cheese 3606, cream 3627, sour cream 3604, whipping 3591, sherbet 2673,
# creamers, butter/margarine: FPED counts these as fats, not dairy -> no group
# ============ PROTEIN ============
4100: ('protein', 50, 0, "eggs: 1 large egg (50g) = 1 oz-eq"),
3561: ('protein', 37, 0, "fresh meat: raw; 37g raw ~ 28g cooked = 1 oz-eq"),
2681: ('protein', 37, 0, "frozen poultry (raw)"),
2688: ('protein', 37, 0, "frozen ground beef"),
2687: ('protein', 37, 0, "frozen beef steak"),
2690: ('protein', 37, 0, "frozen sandwich steak"),
2680: ('protein', 37, 0, "frozen meat remaining"),
3577: ('protein', 28, 0, "bacon (renders to much less; keep 28g as-purchased)"),
3574: ('protein', 28, 0, "sliced lunchmeat (RTE)"),
3618: ('protein', 28, 0, "deli pouch lunchmeat"),
3572: ('protein', 32, 0, "dinner sausage"),
3576: ('protein', 32, 0, "frankfurters"),
3578: ('protein', 32, 0, "breakfast sausage"),
3617: ('protein', 28, 0, "nonsliced lunchmeat"),
3567: ('protein', 32, 0, "bratwurst/knockwurst"),
3582: ('protein', 32, 0, "cocktail franks"),
3581: ('protein', 28, 0, "canned ham"),
1243: ('protein', 32, 0, "shelf-stable chicken"),
1234: ('protein', 30, 0, "spiced lunch meat canned (SPAM)"),
1233: ('protein', 30, 0, "vienna sausage"),
1213: ('protein', 35, 0, "corned beef hash (potato mix)"),
1212: ('protein', 28, 0, "canned corned beef"),
1215: ('protein', 28, 0, "canned roast beef"),
1246: ('protein', 28, 0, "canned turkey"),
1218: ('protein', 28, 0, "dried beef"),
1271: ('protein', 28, 0, "meat snacks (jerky)"),
1270: ('protein', 28, 0, "pork rinds"),
# ============ SEAFOOD & PLANT PROTEINS ============
1209: ('seafood_plant', 32, 0, "canned tuna"),
1205: ('seafood_plant', 32, 0, "canned salmon"),
1207: ('seafood_plant', 32, 0, "sardines"),
1202: ('seafood_plant', 32, 0, "canned seafood remaining"),
1200: ('seafood_plant', 32, 0, "canned clams"),
1204: ('seafood_plant', 32, 0, "canned oysters"),
1203: ('seafood_plant', 32, 0, "canned crab"),
1208: ('seafood_plant', 32, 0, "canned shrimp"),
1199: ('seafood_plant', 32, 0, "anchovies"),
2682: ('seafood_plant', 37, 0, "frozen shrimp (raw)"),
2683: ('seafood_plant', 37, 0, "frozen fish unbreaded"),
2607: ('seafood_plant', 45, 0, "breaded frozen fish (breading weight)"),
2643: ('seafood_plant', 45, 0, "breaded frozen shrimp"),
2686: ('seafood_plant', 37, 0, "frozen seafood remaining"),
2645: ('seafood_plant', 37, 0, "frozen crab"),
2679: ('seafood_plant', 45, 0, "breaded seafood remaining"),
3579: ('seafood_plant', 32, 0, "refrigerated seafood"),
1508: ('seafood_plant', 14, 0, "nuts bags: 1/2 oz nuts = 1 oz-eq"),
1506: ('seafood_plant', 14, 0, "nuts cans"),
1507: ('seafood_plant', 14, 0, "nuts jars"),
1509: ('seafood_plant', 28, 0, "unshelled nuts (shell weight ~half)"),
1421: ('seafood_plant', 16, 0, "peanut butter: 1 Tbsp = 1 oz-eq"),
1422: ('seafood_plant', 20, 0, "trail mix (nuts + dried fruit)"),
1225: ('seafood_plant', 32, 0, "imitation meat (soy)"),
# ============ LEGUME-BASED PREPARED ============
1063: ('legume', 250, 0, "beans with meat (pork & beans)"),
1254: ('legume', 250, 0, "shelf-stable chili"),
1253: ('legume', 250, 0, "refried beans"),
# ============ TOMATO-BASED SAUCES (vegetable credit) ============
1120: ('veg', 125, 0, "spaghetti/marinara sauce: ~2x tomato concentration"),
1114: ('veg', 130, 0, "Mexican sauce/salsa"),
1100: ('veg', 250, 0, "ketchup: concentrated but sugar-diluted; small credit"),
1118: ('veg', 125, 0, "pizza sauce"),
1115: ('veg', 140, 0, "meat sauce (tomato base)"),
1101: ('veg', 250, 0, "chili sauce"),
# ============ PICKLES / PEPPERS ============
1168: ('veg', 200, 0, "dill pickles (brine weight)"),
1166: ('veg', 220, 0, "sweet pickles"),
1156: ('veg', 150, 0, "peppers"),
1142: ('veg', 150, 0, "chilies"),
1143: ('veg', 150, 0, "pimentos"),
# olives (1147, 1149): FPED treats as fats -> no group
# ============ DEHYDRATED POTATOES ============
1084: ('veg', 30, 0, "instant mashed potatoes: rehydrates ~5x"),
1083: ('veg', 30, 0, "dehydrated specialty potatoes"),
1323: ('veg', 55, 0, "potato chips: ~150g fresh potato per 55g chips"),
1324: ('veg', 55, 0, "potato sticks"),
}

# ============================================================
# ASSEMBLE + AUDIT
# ============================================================
rows = [(code, tag, gpe, ws, note) for code, (tag, gpe, ws, note) in X.items()]
cw = pd.DataFrame(rows, columns=['product_module_code', 'hei_group',
                                 'grams_per_equiv', 'whole_share', 'note'])
assert cw['product_module_code'].is_unique

u = pd.read_parquet(UNIVERSE, columns=['product_module_code', 'product_module',
                                       'product_group', 'spend'])
mod = u.groupby(['product_module_code', 'product_module', 'product_group'],
                as_index=False)['spend'].sum()
cw = cw.merge(mod[['product_module_code', 'product_module', 'product_group']],
              on='product_module_code', how='left')

cw.to_parquet(OUT_DIR / 'module_hei_crosswalk.parquet', index=False)
cw.to_csv(OUT_DIR / 'module_hei_crosswalk.csv', index=False)

# audit: spend coverage by HEI group + biggest unassigned modules
mod = mod.merge(cw[['product_module_code', 'hei_group']], on='product_module_code', how='left')
tot = mod['spend'].sum()
print(f"Crosswalk: {len(cw)} modules assigned "
      f"({mod.loc[mod['hei_group'].notna(), 'spend'].sum()/tot:.1%} of food spend)")
print("\nSpend share by HEI group:")
print((mod.groupby('hei_group')['spend'].sum() / tot).sort_values(ascending=False)
      .map('{:.1%}'.format).to_string())
un = mod[mod['hei_group'].isna()].nlargest(20, 'spend')
print("\nLargest UNASSIGNED modules (no food-pattern group -- candy/soda/fats/mixed dishes expected):")
for _, r in un.iterrows():
    print(f"  {int(r['product_module_code']):5d} {r['product_module'][:45]:<45s} ${r['spend']/1e6:5.1f}M  ({r['product_group']})")
print(f"\nSaved {OUT_DIR/'module_hei_crosswalk.parquet'} (+ .csv for review)")
