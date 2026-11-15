"""
build_prep_time_index.py

Assigns a preparation time score (1-9) to each Nielsen product module.

Scale: 1 = open and consume (no prep); 9 = intensive from-scratch cooking.

None = explicitly excluded (condiments, sauces, toppings, sweeteners).
These are additions to a meal, not meals themselves, and would
mechanically bias the spending-weighted average toward low scores.

Output: prep_time_scores.parquet and prep_time_scores.csv
"""

import pandas as pd
from pathlib import Path

import sys
from pathlib import Path
_r = Path(__file__).resolve().parent
while not (_r / "paths.py").exists():
    _r = _r.parent
sys.path.insert(0, str(_r))
from paths import *
OUT_DIR = idir / 'rms_variety'

# ============================================================
# SCORES: module_code -> (prep_time, rationale)
# None = excluded (condiment / sauce / topping / sweetener)
# ============================================================
SCORES = {

    # === BAKED GOODS-FROZEN ===
    # All pre-baked; reheat only.
    2651: (2,    "Frozen biscuits/rolls/muffins: heat 10 min"),
    2653: (2,    "Frozen cobbler/strudel: heat"),
    2656: (2,    "Frozen dessert cakes: thaw"),
    2648: (2,    "Frozen doughnuts: thaw"),
    2660: (2,    "Frozen pies: heat 15 min"),
    2661: (2,    "Frozen remaining baked goods: heat"),
    2647: (2,    "Frozen bagels: toast"),
    2654: (2,    "Frozen bread: thaw, slice"),
    2652: (2,    "Frozen breakfast cakes/sweet rolls: heat"),
    2649: (2,    "Frozen cheesecake: thaw"),
    2655: (3,    "Frozen cookie dough: slice + bake 10-12 min"),
    2646: (2,    "Frozen dough products-bread: heat"),

    # === BAKING MIXES ===
    # Mix + bake; excludes frosting (topping).
    1380: (5,    "Brownie mix: mix + bake 25 min"),
    1376: (5,    "Cake mix ≤10oz: mix + bake 30 min"),
    1375: (5,    "Cake mix >10oz: mix + bake 30 min"),
    1374: (5,    "Specialty cake mix ≤10oz: mix + bake"),
    1373: (5,    "Specialty cake mix >10oz: mix + bake"),
    1388: (5,    "Hushpuppy mix: mix + fry"),
    1395: (4,    "Pancake mix: mix + griddle 15-20 min"),
    1383: (6,    "Bread mix: mix + rise + bake 1+ hr"),
    1382: (5,    "Coffee cake mix: mix + bake"),
    1387: (5,    "Cookie mix: mix + bake 10 min"),
    1384: (5,    "Dessert misc mix: mix + bake"),
    1394: (4,    "Dumpling/kugel mix: mix + cook"),
    1390: (None, "Frosting mix: topping — excluded"),
    1385: (5,    "Gingerbread mix: mix + bake"),
    1381: (5,    "Muffin mix: mix + bake 20 min"),
    1392: (6,    "Pie crust mix: mix + roll + fill + bake"),
    1386: (4,    "Rolls/biscuits mix: mix + bake 10-15 min"),
    1695: (5,    "Perishable bread mix: mix + bake"),
    1734: (5,    "Perishable dessert mix: mix + bake"),
    1719: (4,    "Perishable pancake mix: mix + griddle"),

    # === BAKING SUPPLIES ===
    # Purchasing these implies from-scratch baking/cooking.
    1418: (7,    "Milk chocolate baking chips: from-scratch baking"),
    1438: (7,    "Other baking chips: from-scratch baking"),
    1436: (7,    "Baking chocolate: from-scratch baking"),
    1343: (5,    "Breading products: coat + fry 20-30 min"),
    1389: (None, "Cake decorations/icing: topping — excluded"),
    1153: (None, "Capers: condiment — excluded"),
    1437: (7,    "Chocolate chips: from-scratch baking"),
    1440: (7,    "Cocoa: from-scratch baking"),
    1435: (6,    "Coconut: baking ingredient"),
    1471: (6,    "Corn/potato starch: cooking thickener"),
    1350: (None, "Croutons: topping — excluded"),
    1371: (None, "Frosting refrigerated: topping — excluded"),
    1372: (None, "Frosting ready-to-spread: topping — excluded"),
    1448: (None, "Fruit protectors: non-food additive — excluded"),
    1429: (6,    "Glazed fruit: baking ingredient"),
    1408: (6,    "Graham cracker crumbs: crust-making ingredient"),
    1241: (7,    "Matzo meal: baking/coating ingredient"),
    1702: (7,    "Perishable baking chocolate/chip/cocoa"),
    1725: (6,    "Perishable baking supply: misc"),
    1612: (5,    "Perishable breading/stuffing: coat + bake"),
    1677: (None, "Perishable cake decoration/icing: topping — excluded"),
    1720: (None, "Perishable capers: condiment — excluded"),
    1639: (5,    "Perishable specialty dried bread: baking ingredient"),
    1731: (9,    "Yeast: implies from-scratch bread baking"),
    1358: (6,    "Pie/pastry shells: fill + bake"),
    6056: (None, "RBC cake decoration/icing: topping — excluded"),
    1364: (3,    "Stuffing products: add liquid + heat 10 min"),

    # === BREAD AND BAKED GOODS ===
    # Fresh bakery: already baked, slice and serve.
    4000: (1,    "Fresh bread: slice and serve"),
    4011: (1,    "Remaining fresh bakery: ready to eat"),
    4008: (2,    "Fresh bagels: toast"),
    4009: (1,    "Fresh biscuits: ready to eat"),
    4005: (1,    "Breakfast cakes/sweet rolls: ready to eat"),
    4001: (1,    "Fresh buns: ready to eat"),
    4004: (1,    "Fresh cakes: slice and serve"),
    4012: (1,    "Fresh cheesecake: slice and serve"),
    4006: (1,    "Fresh doughnuts: ready to eat"),
    4003: (1,    "Fresh muffins: ready to eat"),
    4007: (1,    "Fresh pies: slice and serve"),
    4002: (1,    "Fresh rolls: ready to eat"),
    1600: (2,    "Perishable bagel: toast"),
    1601: (1,    "Perishable bread: slice"),
    1610: (1,    "Perishable cake: slice"),
    1611: (1,    "Perishable cookie: ready to eat"),
    1675: (1,    "Perishable crumpet/scone: ready to eat"),
    1692: (1,    "Perishable crust/shell: used as base"),
    1604: (1,    "Perishable doughnut: ready to eat"),
    1674: (2,    "Perishable English muffin: toast"),
    1605: (1,    "Perishable muffin: ready to eat"),
    1606: (1,    "Perishable pie/pastry: slice"),
    1607: (1,    "Perishable roll/bun/biscuit: ready to eat"),
    1609: (1,    "Perishable sweet roll: ready to eat"),
    6039: (1,    "RBC bread: slice"),
    6040: (1,    "RBC cake: slice"),

    # === BREAKFAST FOOD ===
    1445: (1,    "Breakfast bars: open and eat"),
    1441: (1,    "Granola & yogurt bars: open and eat"),
    1443: (2,    "Instant breakfast powdered: mix with milk"),
    1351: (2,    "Toaster pastries: toast 2 min"),

    # === BREAKFAST FOODS-FROZEN ===
    2659: (2,    "Frozen waffles/pancakes/french toast: toast or microwave"),
    2629: (2,    "Frozen/refrig breakfast items: microwave 2-3 min"),
    1640: (2,    "Perishable breakfast entree: microwave"),
    1641: (2,    "Perishable breakfast pancake/waffle: heat"),

    # === BUTTER AND MARGARINE === → condiment/cooking fat, excluded
    3611: (None, "Butter: condiment/cooking fat — excluded"),
    3608: (None, "Margarine/spreads: condiment — excluded"),
    1625: (None, "Perishable butter: condiment — excluded"),
    1732: (None, "Perishable margarine/spread: condiment — excluded"),

    # === CANDY ===
    1493: (1,    "Chocolate candy: open and eat"),
    1492: (1,    "Chocolate miniatures: open and eat"),
    1494: (1,    "Specialty chocolate: open and eat"),
    1536: (1,    "Dietetic chocolate candy: open and eat"),
    1533: (1,    "Dietetic non-chocolate: open and eat"),
    1491: (1,    "Hard/rolled candy: open and eat"),
    1511: (1,    "Candy kits: open and eat"),
    1501: (1,    "Lollipops: open and eat"),
    1498: (1,    "Non-chocolate candy: open and eat"),
    1497: (1,    "Non-chocolate miniatures: open and eat"),
    1503: (1,    "Marshmallows: open and eat"),
    1617: (1,    "Perishable chocolate candy: open and eat"),
    1709: (1,    "Perishable marshmallow: open and eat"),
    1618: (1,    "Perishable non-chocolate candy: open and eat"),

    # === CARBONATED BEVERAGES ===
    7743: (1,    "Fountain beverage syrup: pour over ice"),
    1484: (1,    "Regular soda: open and pour"),
    1553: (1,    "Diet soda: open and pour"),

    # === CEREAL ===
    1346: (1,    "Granola/natural cereal: pour and eat"),
    1348: (3,    "Hot cereal (oatmeal): boil water or microwave 5 min"),
    1344: (1,    "RTE cereal: pour and eat"),
    1366: (3,    "Hominy grits: cook 5-20 min"),
    1679: (1,    "Perishable cereal: pour and eat"),
    1619: (3,    "Perishable grain: cook"),
    6075: (1,    "RBC cereal: pour and eat"),
    1349: (1,    "Wheat germ: sprinkle on food"),

    # === CHEESE ===
    # All ready to eat; slicing/shredding is minimal.
    3553: (1,    "Grated cheese: open and sprinkle"),
    3550: (1,    "Cheddar: slice"),
    3549: (1,    "Colby: slice"),
    3547: (1,    "Brick cheese: slice"),
    3546: (1,    "Mozzarella: slice"),
    3548: (1,    "Remaining natural cheese: slice"),
    3589: (1,    "Natural cheese variety pack: ready to eat"),
    3606: (1,    "Cream cheese: spread"),
    3555: (1,    "Processed cheese loaves: slice"),
    3556: (1,    "Processed cheese snacks: open and eat"),
    3554: (1,    "Processed slices remaining: ready to eat"),
    3590: (1,    "Shredded cheese: open and use"),
    3588: (1,    "Specialty/imported: slice"),
    3545: (1,    "Muenster: slice"),
    3587: (1,    "Swiss: slice"),
    3586: (1,    "Processed American slices: ready to eat"),
    1631: (1,    "Perishable cheese: slice"),
    1681: (1,    "Perishable cheese spread: spread"),
    1632: (1,    "Perishable cream cheese: spread"),
    6076: (1,    "RBC cheese: slice"),

    # === CONDIMENTS, GRAVIES, AND SAUCES ===
    # Most excluded. Marinara/tomato sauce kept because it implies cooking pasta.
    1111: (None, "BBQ sauce: condiment — excluded"),
    1100: (None, "Ketchup: condiment — excluded"),
    1101: (None, "Chili sauce: condiment — excluded"),
    1123: (None, "Cooking sauce: sauce — excluded"),
    1342: (3,    "Egg mixes dry: mix + scramble 5-10 min"),
    1112: (None, "Fish/seafood/cocktail sauce: condiment — excluded"),
    1140: (3,    "Fondue sauce: heat + dip 10 min"),
    1005: (None, "Fruit glazes: condiment — excluded"),
    1116: (None, "Meat glazes: condiment — excluded"),
    1127: (None, "Canned gravy: sauce — excluded"),
    1126: (None, "Gravy aids/beef extract: condiment — excluded"),
    1130: (None, "Gravy mixes: sauce — excluded"),
    1163: (None, "Hot dog sauce: condiment — excluded"),
    1113: (None, "Hot sauce: condiment — excluded"),
    1115: (None, "Meat sauce: sauce — excluded"),
    1114: (None, "Mexican sauce (salsa): condiment — excluded"),
    1117: (None, "Mushroom sauce: sauce — excluded"),
    1186: (None, "Mustard: condiment — excluded"),
    1268: (None, "Oriental sauces: condiment — excluded"),
    1658: (None, "Perishable sauce/gravy/seasoning: excluded"),
    1714: (None, "Perishable sauce/gravy mix: excluded"),
    1704: (None, "Perishable vinegar/cooking wine: condiment — excluded"),
    1118: (None, "Pizza sauce: condiment — excluded"),
    1138: (None, "Sauce/seasoning mix remaining: excluded"),
    1139: (None, "Mexican sauce/seasoning mix: excluded"),
    1132: (None, "Cheese sauce mix: sauce — excluded"),
    1135: (None, "Meat loaf mix: sauce — excluded"),
    1134: (None, "Sour cream sauce mix: sauce — excluded"),
    1131: (2,    "Spaghetti/marinara sauce jar: heat + pour over pasta"),
    1136: (None, "Taco seasoning: condiment — excluded"),
    1137: (None, "Sloppy joe mix: sauce — excluded"),
    1128: (None, "Dipping sauces: condiment — excluded"),
    1124: (None, "Misc sauces shelf stable: excluded"),
    1133: (None, "Chili seasoning mix: condiment — excluded"),
    1120: (2,    "Spaghetti/marinara sauce: heat + pour over pasta"),
    1119: (None, "Tabasco/pepper sauce: condiment — excluded"),
    1122: (None, "Worcestershire sauce: condiment — excluded"),

    # === COOKIES ===
    1362: (1,    "Cookies: open and eat"),
    1365: (1,    "Ice cream cones/cups: open and fill"),

    # === COTTAGE CHEESE, SOUR CREAM, TOPPINGS ===
    3605: (1,    "Cottage cheese: open and eat"),
    3551: (1,    "Farmers cheese: scoop"),
    3552: (1,    "Ricotta: scoop"),
    3557: (None, "Potato topping refrig: condiment — excluded"),
    3604: (None, "Sour cream: condiment — excluded"),
    1682: (1,    "Perishable cottage cheese/ricotta: scoop"),
    1710: (None, "Perishable sour cream: condiment — excluded"),
    1703: (None, "Perishable topping whipped: condiment — excluded"),
    1700: (None, "Perishable whipping cream/topping: condiment — excluded"),
    3594: (None, "Toppings refrigerated: condiment — excluded"),
    3591: (None, "Whipping cream: condiment — excluded"),

    # === CRACKERS ===
    1356: (1,    "Cheese crackers: open and eat"),
    1353: (1,    "Flaked soda crackers: open and eat"),
    1360: (1,    "Flavored snack crackers: open and eat"),
    1354: (1,    "Graham crackers: open and eat"),
    1361: (1,    "Oyster crackers: open and eat"),
    1357: (1,    "Remaining crackers: open and eat"),
    1355: (1,    "Butter-sprayed crackers: open and eat"),
    1352: (1,    "Flake crackers sprayed: open and eat"),
    1247: (1,    "Matzo: open and eat"),
    1602: (1,    "Perishable cracker: open and eat"),
    1359: (1,    "Wafers/toast/breadsticks: open and eat"),

    # === DESSERTS, GELATINS, SYRUP ===
    1455: (1,    "RTS dessert canned: open and serve"),
    1446: (3,    "Gelatin sweetened: mix with hot water, refrigerate 2+ hr"),
    1453: (4,    "Ice cream mix: mix + freeze"),
    1454: (3,    "Diet pudding mix: mix + refrigerate 30 min"),
    1434: (2,    "Plum pudding: heat"),
    1450: (3,    "Sweetened pudding mix: mix + set"),
    1430: (1,    "Pudding/pie filling canned: open and serve"),
    1431: (None, "Chocolate syrup: topping — excluded"),
    1411: (None, "Specialty syrup: topping — excluded"),
    1433: (None, "Toppings liquid/dry: topping — excluded"),
    1432: (None, "Toppings mixes: topping — excluded"),

    # === DESSERTS/FRUITS/TOPPINGS-FROZEN ===
    2677: (1,    "Frozen cream substitutes: thaw"),
    2665: (1,    "Frozen desserts (ice cream, sorbet): scoop"),
    2664: (2,    "Frozen fruits: thaw or use frozen"),
    2678: (None, "Whipped toppings frozen: topping — excluded"),

    # === DOUGH PRODUCTS ===
    3595: (4,    "Refrig cookie/brownie dough: slice + bake 10-12 min"),
    3613: (3,    "Refrig biscuit dough: pop can + bake 10 min"),
    3614: (3,    "Refrig dinner roll dough: shape + bake 12 min"),
    3615: (3,    "Remaining refrig dough: bake"),
    3596: (4,    "Refrig sweet roll dough: proof + bake"),

    # === DRESSINGS/SALADS/PREP FOODS-DELI ===
    3573: (2,    "Chili refrigerated: heat 5 min"),
    3598: (1,    "Combination lunches (Lunchables): open and eat"),
    3601: (1,    "Cracklins refrig: open and eat"),
    3580: (2,    "Entrees refrigerated: heat"),
    3563: (1,    "Fruit salads refrig: open and eat"),
    3560: (2,    "Refrigerated fruit: wash and eat"),
    3562: (1,    "Gelatin salads: open and eat"),
    3571: (None, "Horseradish: condiment — excluded"),
    3569: (None, "Meat/sandwich spreads refrig: condiment — excluded"),
    3360: (3,    "Pasta refrigerated: boil 2-3 min"),
    1626: (1,    "Perishable cracklins: open and eat"),
    1680: (2,    "Perishable frozen/refrig fruit: wash and eat"),
    1644: (1,    "Perishable precut fresh salad mix: open bag, dress"),
    1650: (1,    "Perishable prepared salad: open and serve"),
    1651: (1,    "Perishable prepared sandwich: open and eat"),
    3616: (3,    "Pizza refrigerated: heat 15 min"),
    3544: (1,    "Precut fresh salad mix: open bag, dress"),
    6084: (1,    "RBC prepared salad: open and serve"),
    3564: (1,    "Remaining ready-made salads: open and serve"),
    3568: (None, "Salad dressing refrig: condiment — excluded"),
    3597: (2,    "Sandwiches refrig/frozen: heat or eat cold"),
    3565: (1,    "Sauerkraut refrig: open and serve"),

    # === EGGS ===
    4100: (5,    "Fresh eggs: cook 5-10 min (scramble/fry/boil)"),
    1673: (5,    "Perishable egg: cook"),

    # === FLOUR ===
    # Buying flour signals from-scratch baking/cooking.
    1367: (8,    "Corn meal: from-scratch baking/cooking"),
    1377: (8,    "All-purpose flour: from-scratch baking"),
    1391: (8,    "Single purpose flour: from-scratch baking"),
    1393: (8,    "White wheat flour: from-scratch baking"),
    1727: (8,    "Perishable flour/meal: from-scratch baking"),

    # === FRESH MEAT ===
    3561: (7,    "Fresh meat: season + cook 20-45 min"),
    1628: (7,    "Perishable fresh/frozen meat: cook"),

    # === FRESH PRODUCE ===
    # Fresh fruit: wash and eat (user-specified 2).
    # Fresh vegetables: vary by amount of prep required.
    4010: (2,    "Fresh apples: wash and eat"),
    4050: (6,    "Fresh carrots: peel, chop, cook or eat raw"),
    4055: (6,    "Fresh cauliflower: trim, chop, cook 20-25 min"),
    4060: (5,    "Fresh celery: wash, trim, cut"),
    4085: (2,    "Fresh cranberries: wash and eat/use"),
    4225: (2,    "Fresh fruit remaining: wash and eat"),
    4140: (5,    "Fresh garlic: peel, mince, cook"),
    4180: (2,    "Fresh grapefruit: cut and eat"),
    4020: (4,    "Fresh herbs: wash, chop"),
    4230: (2,    "Fresh kiwi: peel and eat"),
    4275: (4,    "Fresh lettuce: wash, tear, assemble salad"),
    4023: (5,    "Fresh mushrooms: clean, slice, sauté 10-15 min"),
    4350: (7,    "Fresh onions: peel, chop, cook"),
    4355: (2,    "Fresh oranges: peel and eat"),
    4400: (7,    "Fresh potatoes: peel, boil/roast 30-45 min"),
    4415: (3,    "Fresh radishes: wash, slice"),
    4460: (5,    "Fresh spinach: wash, cook or use raw in salad"),
    4015: (2,    "Fresh sprouts: rinse and use"),
    4470: (2,    "Fresh strawberries: wash and eat"),
    4475: (5,    "Fresh tomatoes: wash, slice/dice — often cooked"),
    4280: (6,    "Fresh vegetables remaining: wash, chop, cook"),
    1706: (3,    "Perishable coconut: shred or use"),
    1620: (2,    "Perishable fresh fruit: wash and eat"),
    1670: (6,    "Perishable fresh vegetable/herb: wash, chop, cook"),
    1621: (1,    "Perishable precut fresh/refrig fruit: open and eat"),
    1688: (3,    "Perishable precut fresh/refrig vegetable: open, cook briefly"),
    1701: (1,    "Perishable shelf-stable fruit: open and eat"),
    6049: (2,    "RBC fresh fruit: wash and eat"),
    6064: (6,    "RBC fresh vegetable/herb: wash, chop, cook"),
    6050: (1,    "RBC precut fresh/refrig fruit: open and eat"),
    6070: (3,    "RBC precut fresh/refrig vegetable: open, cook briefly"),

    # === FRUIT - CANNED ===
    1003: (1,    "Applesauce: open and eat"),
    1006: (1,    "Canned apples: open and eat"),
    1008: (1,    "Canned berries: open and eat"),
    1010: (1,    "Canned figs: open and eat"),
    1012: (1,    "Fruit cocktail: open and eat"),
    1023: (1,    "Canned grapes: open and eat"),
    1014: (1,    "Canned oranges: open and eat"),
    1016: (1,    "Canned peaches freestone: open and eat"),
    1021: (1,    "Canned pineapple: open and eat"),
    1027: (1,    "Canned prunes: open and eat"),
    1028: (1,    "Remaining canned fruit: open and eat"),
    1007: (1,    "Canned apricots: open and eat"),
    1024: (1,    "Canned cherries: open and eat"),
    1011: (1,    "Fruit cocktail remaining: open and eat"),
    1013: (1,    "Canned grapefruit: open and eat"),
    1017: (1,    "Canned peaches cling: open and eat"),
    1020: (1,    "Canned pears: open and eat"),
    1026: (1,    "Canned plums: open and eat"),
    1428: (1,    "Maraschino cherries: open and use"),
    1009: (1,    "Cranberries shelf stable: open and use"),
    1427: (1,    "Mincemeat canned: open and use"),
    1678: (4,    "Perishable pie filling: fill + bake"),
    1002: (4,    "Pie/pastry filling: fill + bake"),
    1090: (2,    "Pumpkin canned: open, use in baking/cooking"),

    # === FRUIT - DRIED ===
    1426: (1,    "Dates: open and eat"),
    1425: (1,    "Dried fruit/snacks: open and eat"),
    1622: (1,    "Perishable dried fruit: open and eat"),
    1423: (1,    "Prunes dried: open and eat"),
    1424: (1,    "Raisins: open and eat"),

    # === ICE CREAM, NOVELTIES ===
    2675: (1,    "Frozen novelties: unwrap and eat"),
    2672: (1,    "Ice cream bulk: scoop"),
    2673: (1,    "Ice milk/sherbet: scoop"),
    1721: (1,    "Perishable ice cream bulk: scoop"),
    1691: (1,    "Perishable novelty frozen dessert: unwrap"),
    2671: (1,    "Frozen yogurt: scoop"),

    # === JAMS, JELLIES, SPREADS === → condiments/toppings
    1419: (None, "Fruit butter/honey: condiment — excluded"),
    1545: (None, "Fruit spreads: condiment — excluded"),
    1182: (None, "Garlic spreads: condiment — excluded"),
    1420: (None, "Honey: condiment — excluded"),
    1410: (None, "Jams: condiment — excluded"),
    1412: (None, "Jelly: condiment — excluded"),
    1415: (None, "Marmalade: condiment — excluded"),
    1421: (1,    "Peanut butter: open and spread"),
    1656: (None, "Perishable honey: condiment — excluded"),
    1684: (None, "Perishable jam/jelly/preserves: condiment — excluded"),
    1685: (1,    "Perishable peanut butter/nut butter: open and spread"),
    1417: (None, "Preserves: condiment — excluded"),

    # === JUICE, DRINKS ===
    1051: (1,    "Clam juice: open and pour"),
    1030: (1,    "Cranberry juice: open and pour"),
    1041: (1,    "Fruit drinks canned: open and pour"),
    1042: (1,    "Fruit drinks other container: open and pour"),
    1033: (1,    "Apple juice: open and pour"),
    1034: (1,    "Grape juice: open and pour"),
    1032: (1,    "Grapefruit juice: open and pour"),
    1036: (1,    "Lemon/lime juice: open and pour"),
    1040: (1,    "OJ other container: open and pour"),
    1038: (1,    "Pineapple juice: open and pour"),
    1035: (1,    "Grapefruit juice canned: open and pour"),
    1045: (1,    "Fruit juice nectars: open and pour"),
    1037: (1,    "OJ canned: open and pour"),
    1039: (1,    "Prune juice: open and pour"),
    1044: (1,    "Fruit juice remaining: open and pour"),
    1635: (1,    "Perishable juice/drink: open and pour"),
    6079: (1,    "RBC juice/drink: open and pour"),
    1054: (1,    "Tomato/vegetable juice: open and pour"),
    1055: (1,    "Vegetable juice remaining: open and pour"),

    # === JUICES, DRINKS-FROZEN ===
    2670: (2,    "Frozen fruit drinks/mixes: thaw or add water"),
    2669: (2,    "Frozen OJ drinks: thaw"),
    2666: (2,    "Frozen apple juice: thaw"),
    2668: (2,    "Frozen grape juice: thaw"),
    2663: (2,    "Frozen grapefruit juice: thaw"),
    2667: (2,    "Frozen OJ: thaw"),
    2674: (2,    "Frozen fruit juice remaining: thaw"),
    2662: (2,    "Frozen unconcentrated juice: thaw"),

    # === MILK ===
    3626: (1,    "Buttermilk: open and pour"),
    3627: (1,    "Cream: open and pour"),
    3592: (1,    "Flavored milk: open and pour"),
    3625: (1,    "Plain milk: open and pour"),
    3650: (1,    "Eggnog: open and pour"),
    1716: (1,    "Perishable cream/creamer: pour"),
    1633: (1,    "Perishable milk/milk product drink: pour"),
    6041: (1,    "RBC milk: pour"),
    3628: (1,    "Remaining drinks/shakes refrig: open and drink"),

    # === NUTS ===
    1508: (1,    "Nuts bags: open and eat"),
    1506: (1,    "Nuts cans: open and eat"),
    1507: (1,    "Nuts jars: open and eat"),
    1509: (1,    "Nuts unshelled: crack and eat"),

    # === PACKAGED MEATS-DELI ===
    3584: (4,    "Beef bacon/canned bacon: pan cook 5-10 min"),
    3577: (5,    "Bacon refrigerated: pan cook, tending required 10-15 min"),
    3567: (5,    "Bratwurst/knockwurst: grill/pan cook 15 min"),
    3576: (3,    "Frankfurters: boil or grill 5 min"),
    3582: (3,    "Cocktail franks: heat 5 min"),
    3585: (2,    "Ham patties canned: heat"),
    3581: (2,    "Canned/refrig ham: slice, heat optional"),
    3618: (1,    "Deli pouch lunchmeat: open and eat"),
    3617: (1,    "Non-sliced lunchmeat: slice"),
    3574: (1,    "Sliced lunchmeat refrig: open and eat"),
    1624: (5,    "Perishable bacon: pan cook"),
    1623: (2,    "Perishable canned ham: slice"),
    1697: (2,    "Perishable canned meat product: heat"),
    1627: (3,    "Perishable frankfurter: boil/grill"),
    1629: (1,    "Perishable lunchmeat: open and eat"),
    1689: (1,    "Perishable lunchmeat variety pack: open and eat"),
    1630: (5,    "Perishable sausage: pan cook 15 min"),
    3578: (5,    "Breakfast sausage: pan cook"),
    3572: (5,    "Dinner sausage: grill/pan cook"),

    # === PACKAGED MILK AND MODIFIERS ===
    1468: (1,    "Powdered creamers: spoon into coffee"),
    3629: (1,    "Liquid creamers: pour into coffee"),
    1297: (1,    "Canned milk: open and use"),
    1298: (2,    "Powdered milk: mix with water"),
    1296: (1,    "Shelf-stable milk: open and pour"),
    1442: (2,    "Milk/water additives sweetened: mix with milk"),
    1729: (2,    "Perishable powdered drink: mix with water"),

    # === PASTA ===
    1266: (4,    "Oriental noodles: boil water, cook 3-5 min"),
    1331: (4,    "Macaroni: boil water, cook 8-10 min"),
    1336: (4,    "Noodles/dumplings: boil 6-10 min"),
    1334: (4,    "Spaghetti: boil water, cook 8-10 min"),
    1707: (3,    "Perishable fresh pasta: cook 2-3 min"),

    # === PICKLES, OLIVES, AND RELISH === → condiments
    1142: (None, "Chilies: condiment — excluded"),
    1149: (None, "Black olives: condiment — excluded"),
    1147: (None, "Green olives: condiment — excluded"),
    1156: (None, "Peppers: condiment — excluded"),
    1668: (None, "Perishable olives: condiment — excluded"),
    1672: (None, "Perishable pickle/relish: condiment — excluded"),
    1168: (None, "Dill pickles: condiment — excluded"),
    1166: (None, "Sweet pickles: condiment — excluded"),
    1143: (None, "Pimentos: condiment — excluded"),
    1164: (None, "Relishes: condiment — excluded"),

    # === PIZZA/SNACKS/HORS D'OEUVRES-FROZEN ===
    2628: (2,    "Frozen hors d'oeuvres/snacks: heat in oven 15 min"),
    1643: (3,    "Perishable pizza: heat 15 min"),
    1645: (2,    "Perishable prepared appetizer: heat or serve cold"),
    2632: (5,    "Pizza crust frozen: top + bake 20 min"),
    2631: (2,    "Pizza frozen: bake 12-15 min"),
    6081: (2,    "RBC pizza: heat"),
    6082: (2,    "RBC prepared appetizer: heat or serve"),

    # === PREPARED FOOD-DRY MIXES ===
    1340: (4,    "Dry pasta dinners (Hamburger Helper): brown meat + cook 20-25 min"),
    1337: (4,    "Dry dinners remaining: cook 20 min"),
    1339: (3,    "Dry rice dinners: add water, cook 15-20 min"),
    7792: (5,    "Meal kit: cook from semi-fresh ingredients 30-45 min"),
    1251: (4,    "Mexican dinners dry: cook 20 min"),
    1255: (2,    "Mexican shells: heat"),
    1257: (2,    "Mexican tortillas: heat on pan"),
    1370: (4,    "Ethnic specialty mixes: cook 20-30 min"),
    1265: (2,    "Ramen noodles: add boiling water, wait 3 min"),
    1724: (5,    "Perishable meal kit: cook 30-45 min"),
    1676: (2,    "Perishable tortilla/Mexican shell: heat"),
    1261: (4,    "Pizza crust/pie mixes: mix + roll + top + bake"),
    1320: (3,    "Rice mixes: add water, cook 15-20 min"),
    1084: (2,    "Mashed potato dehydrated: add hot water, stir"),
    1083: (2,    "Specialty dehydrated potato: add hot water"),

    # === PREPARED FOOD-READY-TO-SERVE ===
    # Shelf-stable cooked food: open, heat briefly (or eat cold).
    1219: (2,    "Barbecued beef/pork canned: heat"),
    1368: (1,    "Specialty canned bread: open and eat"),
    1243: (1,    "Shelf-stable chicken: open, drain, use"),
    1254: (2,    "Shelf-stable chili: heat 5 min"),
    1212: (2,    "Corned beef canned: heat or slice cold"),
    1213: (2,    "Corned beef hash: heat 10 min"),
    1228: (1,    "Deviled ham canned: spread"),
    1218: (1,    "Dried beef: open and use"),
    1335: (2,    "Canned dumplings: heat"),
    1125: (2,    "Shelf-stable entrees/sides: heat"),
    1262: (2,    "Lasagna canned: heat"),
    1258: (2,    "Macaroni shelf stable (SpaghettiOs): heat"),
    1225: (2,    "Imitation meat products: heat"),
    1223: (2,    "Misc canned meat: heat"),
    1250: (2,    "Mexican dinners canned: heat"),
    1249: (2,    "Mexican specialties remaining: heat"),
    1253: (2,    "Refried beans: heat"),
    1264: (2,    "Chow mein canned: heat"),
    1269: (2,    "Oriental foods misc: heat"),
    1647: (2,    "Perishable prepared meal multi-course: heat or serve"),
    1648: (2,    "Perishable prepared meal single course: heat or serve"),
    1723: (1,    "Perishable service bar: serve"),
    1240: (1,    "Pickled pork products: open and use"),
    1160: (1,    "Pickled vegetables/fruit: open and serve"),
    1092: (1,    "Potato salad canned: open and serve"),
    1229: (1,    "Potted meat canned: spread"),
    1242: (1,    "Prepared sandwich shelf stable: open and eat"),
    1256: (2,    "Ravioli canned: heat"),
    1322: (2,    "Rice canned: heat"),
    1363: (1,    "Rice cakes: open and eat"),
    1215: (2,    "Canned roast beef: heat"),
    1216: (2,    "Roast beef hash: heat 10 min"),
    1180: (None, "Sandwich spreads meat: condiment — excluded"),
    1235: (2,    "Sausage canned: heat"),
    1260: (2,    "Spaghetti canned: heat"),
    1234: (1,    "Spiced lunch meat canned: open"),
    1230: (1,    "Spreads hors d'oeuvres: open and serve"),
    1221: (2,    "Beef stew: heat 5-10 min"),
    1245: (2,    "Chicken stew: heat"),
    1244: (2,    "Remaining stew: heat"),
    1246: (1,    "Turkey canned: open and use"),
    1063: (2,    "Vegetables/beans with meat: heat"),
    1233: (1,    "Vienna sausage: open and eat"),

    # === PREPARED FOODS-FROZEN ===
    # All microwaveable/oven entrees: score 2.
    2693: (2,    "Corn dogs: heat in oven 15 min"),
    2615: (2,    "Frozen dinners: microwave 5-8 min"),
    2623: (2,    "Italian frozen entrees 1-food: microwave"),
    2614: (2,    "Italian frozen entrees 2-food: microwave"),
    2619: (2,    "Meat frozen entrees 1-food: microwave"),
    2611: (2,    "Meat frozen entrees 2-food: microwave"),
    2624: (2,    "Mexican frozen entrees 1-food: microwave"),
    2616: (2,    "Mexican frozen entrees 2-food: microwave"),
    2613: (2,    "Multi-pack frozen entrees: microwave"),
    2622: (2,    "Oriental frozen entrees 1-food: microwave"),
    2627: (2,    "Oriental frozen entrees 2-food: microwave"),
    2621: (2,    "Poultry frozen entrees 1-food: microwave"),
    2612: (2,    "Poultry frozen entrees 2-food: microwave"),
    2625: (2,    "Remaining frozen entrees 1-food: microwave"),
    2606: (2,    "Remaining frozen entrees 2-food: microwave"),
    2626: (2,    "Seafood frozen entrees 1-food: microwave"),
    2609: (2,    "Seafood frozen entrees 2-food: microwave"),
    2603: (3,    "Meal starters frozen: heat + assemble 15 min"),
    2692: (3,    "Plain frozen pasta: cook + add sauce"),
    1690: (3,    "Perishable meal starter: heat + assemble"),
    1653: (2,    "Perishable soup/chowder: heat"),
    1654: (2,    "Perishable stew/chili: heat"),
    2617: (3,    "Pot pies frozen: bake 30 min"),
    6083: (2,    "RBC prepared meal single course: heat"),
    2695: (None, "Sauces/gravies frozen: sauce — excluded"),
    2694: (2,    "Frozen soup: heat 5 min"),
    2696: (3,    "Taco filling frozen: heat + assemble"),

    # === PUDDING, DESSERTS-DAIRY ===
    1646: (1,    "Perishable prepared gelatin: open and serve"),
    1649: (1,    "Perishable prepared pudding: open and serve"),
    3566: (1,    "Pudding refrigerated: open and serve"),

    # === SALAD DRESSINGS, MAYO, TOPPINGS === → condiments
    1175: (None, "Mayonnaise: condiment — excluded"),
    1616: (None, "Salad/potato topping dry: condiment — excluded"),
    1657: (None, "Perishable salad dressing: condiment — excluded"),
    1737: (None, "Perishable salad dressing mix: condiment — excluded"),
    1170: (None, "Salad/potato toppings dry: condiment — excluded"),
    1173: (None, "Miracle Whip type: condiment — excluded"),
    1177: (None, "Salad dressing liquid: condiment — excluded"),
    1179: (None, "Salad dressing mixes dry: condiment — excluded"),
    1539: (None, "Reduced-calorie salad dressing: condiment — excluded"),
    1181: (None, "Sandwich spreads relish type: condiment — excluded"),

    # === SEAFOOD - CANNED ===
    # Open, drain, use — minimal prep.
    1198: (None, "Anchovy paste: condiment — excluded"),
    1715: (1,    "Perishable shelf-stable seafood: open and use"),
    6071: (1,    "RBC shelf-stable seafood: open and use"),
    1199: (1,    "Anchovies canned: open and use"),
    1204: (1,    "Oysters canned: open and use"),
    1202: (1,    "Remaining canned seafood: open and use"),
    1205: (2,    "Salmon canned: drain, flake, use"),
    1207: (1,    "Sardines: open and eat"),
    1208: (1,    "Shrimp canned: drain and use"),
    1200: (1,    "Clams canned: drain and use"),
    1203: (1,    "Crab canned: drain and use"),
    1209: (1,    "Tuna shelf-stable: drain and mix"),

    # === SNACKS ===
    1341: (1,    "Cracker sandwich packs: open and eat"),
    1185: (1,    "Dip canned: open and serve"),
    1184: (2,    "Dip mixes: mix + refrigerate"),
    1708: (1,    "Perishable combo snack: open and eat"),
    1696: (1,    "Perishable lunch combo: open and eat"),
    1660: (1,    "Perishable meat/seafood snack: open and eat"),
    1665: (1,    "Perishable nuts: open and eat"),
    1661: (1,    "Perishable popcorn: open and eat or microwave"),
    1687: (1,    "Perishable pork rind/skin: open and eat"),
    1662: (1,    "Perishable potato chips: open and eat"),
    1663: (1,    "Perishable pretzel: open and eat"),
    1608: (1,    "Perishable snack bar: open and eat"),
    1666: (1,    "Perishable snack remaining: open and eat"),
    1664: (1,    "Perishable tortilla chips: open and eat"),
    1667: (1,    "Perishable trail mix: open and eat"),
    1328: (1,    "Popcorn popped: open and eat"),
    1329: (3,    "Popcorn unpopped: microwave 3 min or stove-pop"),
    1332: (1,    "Caramel corn: open and eat"),
    1325: (1,    "Corn chips: open and eat"),
    1452: (1,    "Health bars & sticks: open and eat"),
    1271: (1,    "Meat snacks (jerky): open and eat"),
    1270: (1,    "Pork rinds: open and eat"),
    1323: (1,    "Potato chips: open and eat"),
    1324: (1,    "Potato sticks: open and eat"),
    1330: (1,    "Pretzels: open and eat"),
    1318: (1,    "Puffed cheese snacks: open and eat"),
    1327: (1,    "Remaining snacks: open and eat"),
    1326: (1,    "Tortilla chips: open and eat"),
    1333: (1,    "Variety snack packs: open and eat"),
    1422: (1,    "Trail mixes: open and eat"),

    # === SNACKS, SPREADS, DIPS-DAIRY ===
    3602: (1,    "Dairy dip refrig: open and serve"),
    3570: (None, "Garlic spreads refrig: condiment — excluded"),
    1655: (1,    "Perishable dip/spread: open and serve"),
    1652: (1,    "Perishable prepared spread: open and serve"),
    6077: (1,    "RBC dip/spread: open and serve"),
    3579: (1,    "Seafood refrigerated (smoked salmon): open and serve"),
    3583: (1,    "Remaining spreads: open and spread"),

    # === SOFT DRINKS-NON-CARBONATED ===
    1048: (2,    "Breakfast drinks powdered: mix with water"),
    1046: (None, "Fruit punch bases/syrups: condiment — excluded"),
    1052: (1,    "Ice pops unfrozen: open and eat"),
    1713: (1,    "Perishable cocktail mix: open and mix"),
    1638: (1,    "Perishable soft drink/water: open and drink"),
    6086: (1,    "RBC soft drink/water: open and drink"),
    1049: (1,    "Remaining non-refrig drinks: open and drink"),
    1050: (2,    "Powdered soft drinks (Kool-Aid): mix with water"),

    # === SOUP ===
    1295: (2,    "Bouillon: dissolve in hot water"),
    1338: (2,    "Instant meals (cup noodles): add hot water, wait 3 min"),
    1683: (2,    "Perishable broth/bouillon: heat"),
    6087: (3,    "RBC soup/chowder: heat"),
    1293: (3,    "Soup mixes dry: add water, heat 10 min"),
    1290: (2,    "Canned soup: open, heat 5 min"),
    1292: (3,    "Stew mixes dry: add water, simmer"),

    # === SUGAR, SWEETENERS === → excluded
    1705: (None, "Sugar: sweetener — excluded"),
    1718: (None, "Perishable sugar substitute: excluded"),
    1400: (None, "Brown sugar: excluded"),
    1402: (None, "Remaining sugar: excluded"),
    1535: (None, "Sugar substitutes: excluded"),
    1403: (None, "Granulated sugar: excluded"),
    1401: (None, "Powdered sugar: excluded"),

    # === TABLE SYRUPS, MOLASSES === → condiments/toppings
    1407: (None, "Molasses: condiment/baking ingredient — excluded"),
    1686: (None, "Perishable syrup/molasses: excluded"),
    1406: (None, "Berry/fruit type syrup: topping — excluded"),
    1404: (None, "Sorghum/sugar syrup: excluded"),
    1405: (None, "Table syrup (pancake syrup): topping — excluded"),

    # === UNPREPPED MEAT/POULTRY/SEAFOOD-FROZEN ===
    # Raw proteins: thaw + cook; significant prep time.
    2688: (6,    "Frozen ground beef: thaw, brown 10-15 min"),
    2689: (6,    "Frozen pork: thaw, cook 20-40 min"),
    2680: (6,    "Frozen remaining meat: thaw, cook"),
    2690: (6,    "Frozen sandwich steak: thaw, grill/pan 10 min"),
    2691: (7,    "Frozen veal: thaw, cook carefully"),
    2687: (7,    "Frozen beef steak: thaw, season, grill 15-25 min"),
    2681: (6,    "Frozen poultry (chicken/turkey): thaw + cook 30-60 min"),
    1659: (5,    "Perishable frozen/refrig seafood: season, cook 10-15 min"),
    6078: (6,    "RBC fresh/frozen meat: cook"),
    6065: (5,    "RBC frozen/refrig seafood: cook"),
    2607: (4,    "Fish breaded frozen: bake/fry 15-20 min"),
    2643: (4,    "Shrimp breaded frozen: bake/fry 15 min"),
    2682: (5,    "Shrimp unbreaded frozen: thaw, cook 5 min"),
    2645: (5,    "Crab unbreaded frozen: thaw, cook"),
    2683: (6,    "Fish unbreaded frozen: thaw, season, cook 15-20 min"),
    2679: (4,    "Remaining seafood breaded: bake 15-20 min"),
    2686: (5,    "Remaining seafood unbreaded: cook 15 min"),

    # === VEGETABLES - CANNED ===
    # Open, drain, heat if desired. All score 1-2.
    1267: (1,    "Bean sprouts canned: drain and use"),
    1155: (None, "Cocktail onions: condiment — excluded"),
    1103: (1,    "Grape leaves canned: drain and use"),
    1144: (1,    "Mushrooms shelf stable: drain and use"),
    1263: (1,    "Oriental canned vegetables: drain and heat"),
    1091: (1,    "Jelled aspic salad: open and serve"),
    1108: (1,    "Tomato paste: open and spoon"),
    1106: (1,    "Tomato puree: open and use"),
    1109: (1,    "Tomato sauce: open and heat briefly"),
    1105: (1,    "Remaining canned tomatoes: open and use"),
    1110: (2,    "Stewed tomatoes: heat"),
    1107: (1,    "Whole canned tomatoes: open and crush/use"),
    1069: (1,    "Red cabbage canned: drain and heat"),
    1057: (2,    "Artichokes canned: drain, heat"),
    1058: (1,    "Asparagus canned: drain and serve"),
    1088: (2,    "Chili beans canned: drain, heat"),
    1066: (1,    "Garbanzo/chickpeas canned: drain and use"),
    1060: (1,    "Green beans canned: drain and heat"),
    1076: (1,    "Kidney beans canned: drain, heat"),
    1067: (1,    "Lima beans canned: drain and heat"),
    1087: (1,    "Pinto beans canned: drain and heat"),
    1065: (1,    "Remaining beans canned: drain and heat"),
    1064: (2,    "Baked beans: heat"),
    1061: (1,    "Waxed beans canned: drain and serve"),
    1062: (1,    "White/navy beans canned: drain and use"),
    1068: (1,    "Beets canned: drain and slice"),
    1070: (1,    "Carrots canned: drain and heat"),
    1073: (1,    "Corn on cob canned: drain and heat"),
    1071: (1,    "Cream-style corn: heat"),
    1072: (1,    "Whole kernel corn canned: drain and heat"),
    1102: (2,    "Greens canned: drain, season, heat"),
    1074: (2,    "Hominy canned: drain and heat"),
    1082: (1,    "Mixed vegetables canned: drain and heat"),
    1075: (1,    "Okra canned: drain and heat"),
    1077: (1,    "Onions canned: drain and use"),
    1081: (1,    "Peas and carrots canned: drain and heat"),
    1080: (1,    "Peas canned: drain and heat"),
    1079: (1,    "Remaining peas canned: drain and heat"),
    1085: (2,    "Potatoes canned: drain, heat"),
    1097: (1,    "Remaining canned vegetables: drain and heat"),
    1093: (1,    "Sauerkraut: drain and serve or heat"),
    1094: (1,    "Spinach canned: drain and heat"),
    1095: (1,    "Squash/rutabagas canned: drain and heat"),
    1096: (1,    "Succotash canned: drain and heat"),
    1086: (2,    "Sweet potatoes/yams canned: drain and heat"),

    # === VEGETABLES AND GRAINS - DRIED ===
    1317: (5,    "Barley dry: rinse, simmer 45 min"),
    1315: (6,    "Dried beans: soak overnight + simmer 1-2 hr"),
    7803: (5,    "Remaining grains: cook 30-45 min"),
    1316: (5,    "Peas/lentils/corn dry: simmer 20-30 min"),
    1669: (4,    "Perishable dried vegetable: rehydrate, cook"),
    1321: (2,    "Instant rice: add boiling water, wait 5 min"),
    1319: (4,    "Packaged/bulk rice: rinse, cook 20 min"),
    1439: (3,    "Tapioca pure: cook, stir"),

    # === VEGETABLES-FROZEN ===
    # Microwave or oven from frozen; low-moderate prep.
    1671: (3,    "Perishable frozen/refrig vegetable: microwave 3-5 min"),
    1642: (3,    "Perishable frozen/refrig specialty potato: heat 20 min"),
    2635: (3,    "Breaded frozen vegetables: bake 15-20 min"),
    2641: (3,    "Frozen broccoli: microwave 4 min"),
    2642: (3,    "Frozen carrots: microwave 4 min"),
    2630: (3,    "Frozen corn: microwave 3 min"),
    2608: (3,    "Frozen corn on cob: microwave 4 min"),
    2644: (3,    "Frozen lima beans: microwave 5 min"),
    2640: (3,    "Frozen mixed vegetables: microwave 4 min"),
    2637: (3,    "Frozen breaded mushrooms: bake 15 min"),
    2638: (4,    "Frozen breaded onion rings: bake 15-20 min"),
    2620: (3,    "Frozen peas: microwave 3 min"),
    2634: (3,    "Frozen potatoes (french fries): bake 20 min"),
    2636: (3,    "Remaining frozen vegetables: microwave"),
    2618: (3,    "Vegetables in pastry frozen: bake 20 min"),
    2633: (3,    "Frozen green beans: microwave 4 min"),
    2639: (3,    "Vegetables in sauce frozen: microwave 4 min"),

    # === YOGURT ===
    1738: (1,    "Perishable yogurt shakes/drinks: open and drink"),
    1634: (1,    "Perishable yogurt spoonable: open and eat"),
    3603: (1,    "Yogurt refrigerated: open and eat"),
    3612: (1,    "Yogurt drinks: open and drink"),
}


def main():
    base = data
    health = pd.read_parquet(OUT_DIR / 'module_healthiness.parquet')

    rows = []
    missing = []
    for _, row in health.iterrows():
        code = int(row['product_module_code'])
        if code in SCORES:
            val = SCORES[code]
            score     = val[0]   # None if excluded
            rationale = val[1]
        else:
            missing.append((code, row['product_module_descr'], row['product_group_descr']))
            score, rationale = (None, 'NOT SCORED')
        rows.append({
            'product_module_code': code,
            'product_module_descr': row['product_module_descr'],
            'product_group_descr': row['product_group_descr'],
            'prep_time': score,
            'rationale': rationale,
        })

    df = pd.DataFrame(rows)

    if missing:
        print(f"\nWARNING: {len(missing)} modules not explicitly scored:")
        for code, name, grp in missing:
            print(f"  {code:5d}  {grp:<40s}  {name}")

    out_parquet = OUT_DIR / 'prep_time_scores.parquet'
    out_csv     = OUT_DIR / 'prep_time_scores.csv'
    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)

    scored    = df['prep_time'].notna()
    excluded  = df['rationale'].str.contains('excluded', na=False)
    print(f"\nSaved: {out_parquet}")
    print(f"Modules scored:   {scored.sum()} / {len(df)}")
    print(f"Modules excluded: {excluded.sum()} / {len(df)}")
    print(f"\nScore distribution (non-excluded):")
    print(df.loc[scored, 'prep_time'].value_counts().sort_index().to_string())
    print(f"\nTop 15 highest prep-time modules:")
    print(df.loc[scored].nlargest(15, 'prep_time')[
        ['product_module_descr', 'product_group_descr', 'prep_time', 'rationale']
    ].to_string(index=False))
    print(f"\nBottom 15 lowest prep-time modules:")
    print(df.loc[scored].nsmallest(15, 'prep_time')[
        ['product_module_descr', 'product_group_descr', 'prep_time', 'rationale']
    ].to_string(index=False))


if __name__ == '__main__':
    main()
