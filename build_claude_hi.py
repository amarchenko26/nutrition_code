"""
build_claude_hi.py

Assigns a "Claude HI" nutritional score (0-10) to each Nielsen product module,
using module name, product group, and nutrition facts as inputs, plus common-sense
nutritional judgment where the formula-based HI misleads.

Key corrections vs. the formula-based HI:
  - Fruit juice treated as similar to soda (removes fiber, concentrates sugar),
    NOT equivalent to whole fruit. E.g. pineapple juice → 3, not ~5.
  - Canned fruit in heavy syrup scored lower than fresh/frozen fruit.
  - Processed deli meats (bacon, hot dogs, lunchmeat) scored low (2-3)
    due to high sodium, saturated fat, and processing.
  - Dried beans/lentils scored very high (9) — one of the most nutritious
    food categories (fiber, protein, low fat).
  - Fresh/frozen vegetables scored highest (7-10).
  - Candy, soda, pure sugar scored 0-1.
  - Nuts scored 7 (healthy fats + protein despite calorie density).
  - Plain yogurt 7, flavored yogurt 5 (added sugar).

Scale: 0 = not nutritious at all; 10 = extremely nutritious.

Output: claude_hi_scores.parquet and claude_hi_scores.csv
"""

import pandas as pd
from pathlib import Path

OUT_DIR = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data/interim/rms_variety')

# ============================================================
# SCORES: module_code -> (claude_hi, rationale)
# ============================================================
# Organized by product group. Score then one-line rationale.
SCORES = {

    # === BAKED GOODS-FROZEN ===
    # Refined flour baked goods; desserts lowest, plain bread highest.
    2651: (3,  "Biscuits/rolls/muffins: refined flour, some fat/salt"),
    2653: (2,  "Cobbler/strudel: added sugar, fat"),
    2656: (1,  "Dessert cakes: high sugar, sat fat"),
    2648: (1,  "Doughnuts: fried, high sugar and fat"),
    2660: (1,  "Pies: high sugar, fat, refined crust"),
    2661: (2,  "Remaining baked goods: mixed, mostly indulgent"),
    2647: (3,  "Bagels: low fat, but refined carbs, low fiber"),
    2654: (4,  "Bread frozen: better than desserts; some fiber"),
    2652: (1,  "Breakfast cakes/sweet rolls: high sugar"),
    2649: (1,  "Cheesecake: very high fat and sugar"),
    2655: (1,  "Cookies/cookie dough: high sugar and fat"),
    2646: (4,  "Dough products-bread: similar to bread"),

    # === BAKING MIXES ===
    1380: (1,  "Brownie mix: mostly sugar and fat"),
    1376: (1,  "Cake mix ≤10oz: high sugar"),
    1375: (1,  "Cake mix >10oz: high sugar"),
    1374: (1,  "Specialty cake mix ≤10oz: high sugar"),
    1373: (1,  "Specialty cake mix >10oz: high sugar"),
    1388: (2,  "Hushpuppy mix: fried, refined corn"),
    1395: (3,  "Pancake mix: refined flour; moderate if not overdone"),
    1383: (4,  "Bread mix: lower sugar than cake"),
    1382: (1,  "Coffee cake mix: high sugar"),
    1387: (1,  "Cookie mix: high sugar"),
    1384: (1,  "Dessert misc mix: dessert = low"),
    1394: (3,  "Dumpling/kugel mix: starchy but not sugary"),
    1390: (0,  "Frosting mix: almost pure sugar"),
    1385: (2,  "Gingerbread mix: high sugar, some spice benefit"),
    1381: (2,  "Muffin mix: refined, often high sugar"),
    1392: (1,  "Pie crust mix: fat and refined flour"),
    1386: (3,  "Rolls/biscuits mix: lower sugar"),
    1695: (4,  "Perishable bread mix: similar to bread"),
    1734: (1,  "Perishable dessert mix: dessert"),
    1719: (3,  "Perishable pancake mix: similar to pancake"),

    # === BAKING SUPPLIES ===
    1418: (2,  "Milk chocolate baking chips: mostly sugar + fat"),
    1438: (2,  "Other baking chips: sugar + fat"),
    1436: (4,  "Baking chocolate (dark): flavanols, low sugar when pure"),
    1343: (2,  "Breading products: refined flour coating"),
    1389: (0,  "Cake decorations/icing: pure sugar, food dye"),
    1153: (6,  "Capers: very low cal, high flavor; some antioxidants"),
    1437: (2,  "Chocolate chips: sugar + fat"),
    1440: (4,  "Cocoa: flavanols; healthy when unsweetened"),
    1435: (4,  "Coconut: high sat fat but natural; some fiber"),
    1471: (2,  "Corn/potato starch: refined, low nutrient"),
    1350: (2,  "Croutons: refined bread + oil"),
    1371: (0,  "Frosting refrigerated: pure sugar + fat"),
    1372: (0,  "Frosting ready-to-spread: pure sugar + fat"),
    1448: (5,  "Fruit protectors (ascorbic acid etc): vitamin C"),
    1429: (2,  "Glazed fruit: high sugar"),
    1408: (2,  "Graham cracker crumbs: refined, some fiber"),
    1241: (3,  "Matzo meal: unleavened wheat, low sugar"),
    1702: (4,  "Perishable baking chocolate/chip/cocoa"),
    1725: (3,  "Perishable baking supply: misc"),
    1612: (2,  "Perishable breading/stuffing/crouton: refined"),
    1677: (0,  "Perishable cake decoration/icing: sugar"),
    1720: (6,  "Perishable capers"),
    1639: (2,  "Perishable specialty dried bread"),
    1731: (4,  "Yeast: B vitamins; negligible caloric contribution"),
    1358: (1,  "Pie/pastry shells: fat + refined flour"),
    6056: (0,  "RBC cake decoration/icing: sugar"),
    1364: (2,  "Stuffing products: refined bread + sodium"),

    # === BREAD AND BAKED GOODS ===
    4000: (4,  "Fresh bread: refined carbs; better than pastries"),
    4011: (2,  "Remaining fresh bakery: mixed, often sweet"),
    4008: (3,  "Fresh bagels: low fat, low fiber refined carb"),
    4009: (2,  "Fresh biscuits: fat + refined flour"),
    4005: (1,  "Breakfast cakes/sweet rolls: high sugar"),
    4001: (3,  "Fresh buns: similar to bread"),
    4004: (1,  "Fresh cakes: high sugar + fat"),
    4012: (1,  "Fresh cheesecake: very high fat + sugar"),
    4006: (1,  "Fresh doughnuts: fried + high sugar"),
    4003: (2,  "Fresh muffins: often high sugar"),
    4007: (1,  "Fresh pies: high sugar + fat crust"),
    4002: (3,  "Fresh rolls: similar to bread"),
    1600: (3,  "Perishable bagel"),
    1601: (4,  "Perishable bread"),
    1610: (1,  "Perishable cake"),
    1611: (1,  "Perishable cookie"),
    1675: (3,  "Perishable crumpet/scone: moderate"),
    1692: (1,  "Perishable crust/shell: refined fat pastry"),
    1604: (1,  "Perishable doughnut"),
    1674: (3,  "Perishable English muffin"),
    1605: (2,  "Perishable muffin"),
    1606: (1,  "Perishable pie/pastry"),
    1607: (3,  "Perishable roll/bun/biscuit"),
    1609: (1,  "Perishable sweet roll: high sugar"),
    6039: (4,  "RBC bread"),
    6040: (1,  "RBC cake"),

    # === BREAKFAST FOOD ===
    1445: (3,  "Breakfast bars: often high sugar, some protein/fiber"),
    1441: (4,  "Granola & yogurt bars: fiber + protein but often high sugar"),
    1443: (4,  "Instant breakfast powdered: fortified, protein"),
    1351: (1,  "Toaster pastries: high sugar + refined flour"),

    # === BREAKFAST FOODS-FROZEN ===
    2659: (2,  "Waffles/pancakes/french toast: refined flour, syrup expected"),
    2629: (3,  "Frozen/refrig breakfast items: varied; sandwiches, etc."),
    1640: (3,  "Perishable breakfast entree"),
    1641: (2,  "Perishable breakfast pancake/waffle"),

    # === BUTTER AND MARGARINE ===
    3611: (4,  "Butter: natural, high sat fat; better than margarine"),
    3608: (3,  "Margarine/spreads: processed, historically trans fats"),
    1625: (4,  "Perishable butter"),
    1732: (3,  "Perishable margarine/spread"),

    # === CANDY ===
    1493: (1,  "Chocolate candy: high sugar; dark chocolate has some benefit"),
    1492: (1,  "Chocolate miniatures: high sugar"),
    1494: (1,  "Specialty chocolate: high sugar"),
    1536: (2,  "Dietetic chocolate candy: reduced sugar"),
    1533: (2,  "Dietetic non-chocolate: reduced sugar"),
    1491: (1,  "Hard/rolled candy: almost pure sugar"),
    1511: (1,  "Candy kits: sugar"),
    1501: (1,  "Lollipops: pure sugar"),
    1498: (1,  "Non-chocolate candy: pure sugar"),
    1497: (1,  "Non-chocolate miniatures: pure sugar"),
    1503: (1,  "Marshmallows: sugar + gelatin, essentially nothing"),
    1617: (1,  "Perishable chocolate candy"),
    1709: (1,  "Perishable marshmallow"),
    1618: (1,  "Perishable non-chocolate candy"),

    # === CARBONATED BEVERAGES ===
    # Soda: empty calories / acid, no nutrition
    7743: (0,  "Fountain beverage syrup: concentrated sugar for soda"),
    1484: (0,  "Regular soda: sugar + acid, zero nutrition"),
    1553: (1,  "Diet soda: no calories but artificial sweeteners; not beneficial"),

    # === CEREAL ===
    1346: (5,  "Granola/natural cereal: fiber + oats, but often high sugar"),
    1348: (7,  "Hot cereal (oatmeal): high fiber, complex carbs, very healthy"),
    1344: (4,  "RTE cereal: highly variable; many are fortified sugar"),
    1366: (5,  "Hominy grits: whole corn, moderate nutrition"),
    1679: (4,  "Perishable cereal"),
    1619: (6,  "Perishable grain: whole grains"),
    6075: (4,  "RBC cereal"),
    1349: (8,  "Wheat germ: very nutrient dense; vitamin E, zinc, folate"),

    # === CHEESE ===
    3553: (5,  "Grated cheese: protein + calcium, moderate sat fat"),
    3550: (5,  "Cheddar: protein + calcium, moderate sat fat"),
    3549: (5,  "Colby: similar to cheddar"),
    3547: (5,  "Brick cheese: similar"),
    3546: (6,  "Mozzarella: lower fat than many cheeses"),
    3548: (5,  "Remaining natural cheese"),
    3589: (5,  "Natural cheese variety pack"),
    3606: (3,  "Cream cheese: high fat, low protein"),
    3555: (3,  "Processed cheese loaves: lower quality, more sodium"),
    3556: (3,  "Processed cheese snacks"),
    3554: (3,  "Processed slices remaining: sodium + additives"),
    3590: (5,  "Shredded cheese: similar to natural"),
    3588: (5,  "Specialty/imported: often higher quality"),
    3545: (5,  "Muenster: similar to natural cheese"),
    3587: (5,  "Swiss: slightly lower sodium"),
    3586: (3,  "Processed American slices: highly processed"),
    1631: (5,  "Perishable cheese"),
    1681: (3,  "Perishable cheese spread: processed"),
    1632: (3,  "Perishable cream cheese/neufchatel: high fat"),
    6076: (5,  "RBC cheese"),

    # === CONDIMENTS, GRAVIES, AND SAUCES ===
    1111: (2,  "BBQ sauce: high sugar + sodium"),
    1100: (2,  "Ketchup: high sugar"),
    1101: (3,  "Chili sauce: tomato-based, some nutrition"),
    1123: (3,  "Cooking sauce: varies"),
    1342: (4,  "Egg mixes dry: eggs are healthy"),
    1112: (3,  "Fish/seafood/cocktail sauce: tomato-based, some sodium"),
    1140: (3,  "Fondue sauce: varies"),
    1005: (2,  "Fruit glazes: sugar"),
    1116: (2,  "Meat glazes: sugar + sodium"),
    1127: (2,  "Canned gravy: high sodium, fat"),
    1126: (2,  "Gravy aids/beef extract: high sodium"),
    1130: (2,  "Gravy mixes: high sodium"),
    1163: (2,  "Hot dog sauce: processed meat + sugar"),
    1113: (5,  "Hot sauce: capsaicin, very low cal, antioxidants"),
    1115: (3,  "Meat sauce: tomato-based, some sodium/sugar"),
    1114: (4,  "Mexican sauce (salsa): tomato + veggies, low cal"),
    1117: (4,  "Mushroom sauce: some nutrition from mushrooms"),
    1186: (5,  "Mustard: very low cal, some turmeric benefit"),
    1268: (3,  "Oriental sauces: soy sauce sodium"),
    1658: (3,  "Perishable sauce/gravy/seasoning"),
    1714: (3,  "Perishable sauce/gravy mix"),
    1704: (5,  "Perishable vinegar/cooking wine: minimal calories"),
    1118: (4,  "Pizza sauce: tomato-based, lycopene"),
    1138: (3,  "Sauce/seasoning mix remaining"),
    1139: (3,  "Mexican sauce/seasoning mix"),
    1132: (2,  "Cheese sauce mix: fat + sodium"),
    1135: (2,  "Meat loaf mix: processed"),
    1134: (2,  "Sour cream sauce mix"),
    1131: (4,  "Spaghetti/marinara sauce: tomato lycopene"),
    1136: (3,  "Taco seasoning: spices, sodium"),
    1137: (2,  "Sloppy joe mix: sugar + sodium"),
    1128: (3,  "Dipping sauces: varies"),
    1124: (3,  "Misc sauces shelf stable"),
    1133: (3,  "Chili seasoning mix"),
    1120: (5,  "Spaghetti/marinara sauce: tomatoes, lycopene"),
    1119: (5,  "Tabasco/pepper sauce: capsaicin, low cal"),
    1122: (4,  "Worcestershire sauce: some umami, very low cal"),

    # === COOKIES ===
    1362: (1,  "Cookies: high sugar + fat, low fiber"),
    1365: (2,  "Ice cream cones/cups: refined flour + sugar"),

    # === COT CHEESE, SOUR CREAM, TOPPINGS ===
    3605: (7,  "Cottage cheese: high protein, lower fat — excellent"),
    3551: (6,  "Farmers cheese: higher protein than cream cheese"),
    3552: (6,  "Ricotta: protein + calcium"),
    3557: (3,  "Potato topping refrig: fat-based"),
    3604: (3,  "Sour cream: high fat, minimal protein"),
    1682: (6,  "Perishable cottage cheese/ricotta"),
    1710: (3,  "Perishable sour cream"),
    1703: (2,  "Perishable topping: whipped, sugar"),
    1700: (2,  "Perishable whipping cream/topping"),
    3594: (2,  "Toppings refrigerated: whipped cream / sugar"),
    3591: (2,  "Whipping cream: high fat, used in small amounts"),

    # === CRACKERS ===
    1356: (2,  "Cheese crackers: refined + fat + sodium"),
    1353: (3,  "Flaked soda crackers: refined flour, low sugar"),
    1360: (2,  "Flavored snack crackers: refined + sodium"),
    1354: (2,  "Graham crackers: some fiber, high sugar"),
    1361: (3,  "Oyster crackers: small refined flour"),
    1357: (3,  "Remaining crackers"),
    1355: (2,  "Butter-sprayed crackers: fat + refined"),
    1352: (3,  "Flake crackers sprayed"),
    1247: (3,  "Matzo: unleavened wheat, no fat/sugar"),
    1602: (3,  "Perishable cracker"),
    1359: (3,  "Wafers/toast/breadsticks: low fat crackers"),

    # === DESSERTS, GELATINS, SYRUP ===
    1455: (2,  "RTS dessert canned: high sugar"),
    1446: (1,  "Gelatin sweetened: mostly sugar + gelatin"),
    1453: (1,  "Ice cream mix: sugar + fat"),
    1454: (2,  "Diet pudding mix: reduced sugar"),
    1434: (2,  "Plum pudding: high sugar"),
    1450: (1,  "Sweetened pudding mix: high sugar"),
    1430: (2,  "Pudding/pie filling canned: high sugar"),
    1431: (1,  "Chocolate syrup: pure sugar"),
    1411: (1,  "Specialty syrup: sugar"),
    1433: (1,  "Toppings liquid/dry: sugar"),
    1432: (1,  "Toppings mixes: sugar"),

    # === DESSERTS/FRUITS/TOPPINGS-FROZEN ===
    2677: (2,  "Frozen cream substitutes: fat + additives"),
    2665: (1,  "Frozen desserts (sorbet, ice cream etc): sugar"),
    2664: (7,  "Frozen fruits: no added sugar — equivalent to fresh"),
    2678: (1,  "Whipped toppings frozen: fat + sugar"),

    # === DOUGH PRODUCTS ===
    3595: (1,  "Refrig cookie/brownie dough: high sugar + fat"),
    3613: (2,  "Refrig biscuit dough: refined + fat"),
    3614: (3,  "Refrig dinner roll dough: similar to bread"),
    3615: (3,  "Remaining refrig dough"),
    3596: (1,  "Refrig sweet roll dough: high sugar"),

    # === DRESSINGS/SALADS/PREP FOODS-DELI ===
    3573: (4,  "Chili refrigerated: protein + fiber from beans"),
    3598: (3,  "Combination lunches (Lunchables): processed, sodium"),
    3601: (2,  "Cracklins refrig: pork rinds, high fat"),
    3580: (4,  "Entrees refrigerated: varied, moderate"),
    3563: (6,  "Fruit salads refrig: whole fruit, some sugar"),
    3560: (7,  "Refrigerated fruit: whole fruit"),
    3562: (2,  "Gelatin salads: mostly sugar + gelatin"),
    3571: (5,  "Horseradish: very low cal, some antimicrobial compounds"),
    3569: (2,  "Meat/sandwich spreads refrig: processed + fat"),
    3360: (4,  "Pasta refrigerated: moderate carbs"),
    1626: (2,  "Perishable cracklins: pork rinds"),
    1680: (7,  "Perishable frozen/refrig fruit"),
    1644: (9,  "Perishable precut fresh salad mix: leafy greens, excellent"),
    1650: (5,  "Perishable prepared salad: depends on ingredients"),
    1651: (3,  "Perishable prepared sandwich: varies"),
    3616: (3,  "Pizza refrigerated: refined + fat + sodium"),
    3544: (9,  "Precut fresh salad mix: leafy greens"),
    6084: (5,  "RBC prepared salad"),
    3564: (4,  "Remaining ready-made salads: varies"),
    3568: (3,  "Salad dressing refrig: fat + sodium"),
    3597: (3,  "Sandwiches refrig/frozen: varies"),
    3565: (6,  "Sauerkraut refrig: probiotic, very low cal"),

    # === EGGS ===
    4100: (7,  "Fresh eggs: complete protein, vitamins D/B12/choline"),
    1673: (7,  "Perishable egg"),

    # === FLOUR ===
    1367: (4,  "Corn meal: whole grain, some fiber"),
    1377: (3,  "All-purpose flour: refined"),
    1391: (4,  "Single purpose flour: varies (almond, etc.)"),
    1393: (4,  "White wheat flour: slightly better than white"),
    1727: (4,  "Perishable flour/meal"),

    # === FRESH MEAT ===
    3561: (5,  "Fresh meat: protein; score varies by cut/fat content"),
    1628: (5,  "Perishable fresh/frozen meat"),

    # === FRESH PRODUCE ===
    # Highest scoring category overall.
    4010: (8,  "Fresh apples: fiber, vitamin C, antioxidants"),
    4050: (9,  "Fresh carrots: beta-carotene, fiber, very nutritious"),
    4055: (9,  "Fresh cauliflower: glucosinolates, fiber, vitamin C"),
    4060: (9,  "Fresh celery: very low cal, vitamin K"),
    4085: (8,  "Fresh cranberries: antioxidants, vitamin C"),
    4225: (8,  "Fresh fruit remaining: whole fruit, fiber"),
    4140: (9,  "Fresh garlic: allicin, antioxidants, proven benefits"),
    4180: (8,  "Fresh grapefruit: vitamin C, lycopene, low sugar"),
    4020: (9,  "Fresh herbs: extremely nutrient-dense per gram"),
    4230: (8,  "Fresh kiwi: vitamin C, fiber, potassium"),
    4275: (9,  "Fresh lettuce: low cal, vitamins K/A, folate"),
    4023: (8,  "Fresh mushrooms: vitamin D, B vitamins, low cal"),
    4350: (8,  "Fresh onions: quercetin, prebiotics"),
    4355: (8,  "Fresh oranges: vitamin C, folate, fiber"),
    4400: (6,  "Fresh potatoes: starchy, but potassium/vitamin C"),
    4415: (9,  "Fresh radishes: low cal, vitamin C"),
    4460: (10, "Fresh spinach: highest nutrient density; iron, folate, K"),
    4015: (9,  "Fresh sprouts: enzymes, vitamins, very low cal"),
    4470: (8,  "Fresh strawberries: vitamin C, fiber, low sugar"),
    4475: (8,  "Fresh tomatoes: lycopene, vitamin C, low cal"),
    4280: (8,  "Fresh vegetables remaining: assumed similar to above"),
    1706: (4,  "Perishable coconut: high sat fat, some fiber"),
    1620: (8,  "Perishable fresh fruit"),
    1670: (8,  "Perishable fresh vegetable/herb"),
    1621: (7,  "Perishable precut fresh/refrig fruit"),
    1688: (8,  "Perishable precut fresh/refrig vegetable"),
    1701: (5,  "Perishable shelf-stable fruit: some processing"),
    6049: (8,  "RBC fresh fruit"),
    6064: (8,  "RBC fresh vegetable/herb"),
    6050: (7,  "RBC precut fresh/refrig fruit"),
    6070: (8,  "RBC precut fresh/refrig vegetable"),

    # === FRUIT - CANNED ===
    # Key correction: canned fruit often in syrup (added sugar).
    # Better than juice (has fiber), worse than fresh.
    1003: (5,  "Applesauce: fiber intact; unsweetened is fine"),
    1006: (5,  "Canned apples: fiber but processed"),
    1008: (6,  "Canned berries: high antioxidants, some fiber"),
    1010: (4,  "Canned figs: high sugar, some fiber"),
    1012: (4,  "Fruit cocktail: mixed, usually in syrup"),
    1023: (4,  "Canned grapes: high sugar"),
    1014: (5,  "Canned oranges: some vitamin C"),
    1016: (4,  "Canned peaches freestone: often in syrup"),
    1021: (5,  "Canned pineapple: vitamin C; better in juice not syrup"),
    1027: (6,  "Canned prunes: high fiber, sorbitol; digestive benefit"),
    1028: (4,  "Remaining canned fruit"),
    1007: (5,  "Canned apricots: beta-carotene, some fiber"),
    1024: (4,  "Canned cherries: often in syrup"),
    1011: (4,  "Fruit cocktail: usually in syrup, mixed quality"),
    1013: (5,  "Canned grapefruit: tart, less sugar than other canned"),
    1017: (4,  "Canned peaches cling: often in syrup"),
    1020: (4,  "Canned pears: soft, low fiber vs fresh"),
    1026: (4,  "Canned plums: some fiber"),
    1428: (1,  "Maraschino cherries: mostly sugar dye, negligible fruit"),
    1009: (3,  "Cranberries shelf stable: usually heavily sweetened"),
    1427: (2,  "Mincemeat canned: very high sugar"),
    1678: (2,  "Perishable pie filling: high sugar + starch"),
    1002: (2,  "Pie/pastry filling: high sugar + starch"),
    1090: (7,  "Pumpkin canned: very high vitamin A, fiber, low cal"),

    # === FRUIT - DRIED ===
    1426: (4,  "Dates: very high sugar but natural; some fiber/potassium"),
    1425: (4,  "Dried fruit/snacks: concentrated sugar, some fiber"),
    1622: (4,  "Perishable dried fruit"),
    1423: (5,  "Prunes dried: high fiber, sorbitol, potassium"),
    1424: (5,  "Raisins: iron, antioxidants; high natural sugar"),

    # === ICE CREAM, NOVELTIES ===
    2675: (2,  "Frozen novelties: high sugar"),
    2672: (2,  "Ice cream bulk: high sugar + fat"),
    2673: (2,  "Ice milk/sherbet: lower fat, still high sugar"),
    1721: (2,  "Perishable ice cream bulk"),
    1691: (2,  "Perishable novelty: frozen dessert"),
    2671: (3,  "Frozen yogurt: lower fat, still high sugar"),

    # === JAMS, JELLIES, SPREADS ===
    1419: (4,  "Fruit butter/honey: natural sugars, some nutrients"),
    1545: (3,  "Fruit spreads: mostly sugar, some fruit"),
    1182: (4,  "Garlic spreads: garlic benefits, but often high fat"),
    1420: (5,  "Honey: natural, some antimicrobial/antioxidant properties"),
    1410: (3,  "Jams: mostly sugar, some fruit"),
    1412: (2,  "Jelly: mostly sugar, minimal fruit"),
    1415: (3,  "Marmalade: some peel benefits, mostly sugar"),
    1421: (6,  "Peanut butter: protein + healthy fats + fiber"),
    1656: (5,  "Perishable honey"),
    1684: (3,  "Perishable jam/jelly/preserves"),
    1685: (6,  "Perishable peanut butter/nut butter: protein + fats"),
    1417: (3,  "Preserves: mostly sugar"),

    # === JUICE, DRINKS - CANNED, BOTTLED ===
    # Key correction: juice ≠ whole fruit. Removes fiber, concentrates sugar.
    # People consume more volume. Scores capped at 5 even for 'healthier' juices.
    1051: (5,  "Clam juice: very low cal, some minerals"),
    1030: (4,  "Cranberry juice: some antioxidants; usually very sweetened"),
    1041: (2,  "Fruit drinks canned: low juice content, high sugar"),
    1042: (2,  "Fruit drinks other container: same as above"),
    1033: (3,  "Apple juice: no fiber, high sugar — essentially sugar water"),
    1034: (3,  "Grape juice: high natural sugar, some resveratrol"),
    1032: (4,  "Grapefruit juice other: more tart, less sugar than OJ"),
    1036: (5,  "Lemon/lime juice: used in small amounts, vitamin C"),
    1040: (4,  "OJ other container: vitamin C, folate; but high sugar"),
    1038: (3,  "Pineapple juice: high sugar, no fiber — user example"),
    1035: (4,  "Grapefruit juice canned"),
    1045: (3,  "Fruit juice nectars: often diluted with sweetener"),
    1037: (4,  "OJ canned: vitamin C but high sugar"),
    1039: (5,  "Prune juice: fiber benefits, digestive health"),
    1044: (3,  "Fruit juice remaining: assume high sugar"),
    1635: (4,  "Perishable juice/drink: includes fresh-pressed"),
    6079: (4,  "RBC juice/drink"),
    1054: (7,  "Tomato/vegetable juice: lycopene, vitamins, low sugar"),
    1055: (6,  "Vegetable juice remaining: better than fruit juice"),

    # === JUICES, DRINKS-FROZEN ===
    # Concentrated; when reconstituted = juice. Same logic applies.
    2670: (2,  "Frozen fruit drinks/mixes: mostly sugar"),
    2669: (3,  "Frozen OJ drinks: sugar + additives"),
    2666: (4,  "Frozen apple juice: concentrated, no fiber"),
    2668: (3,  "Frozen grape juice: high natural sugar"),
    2663: (4,  "Frozen grapefruit juice: more tart, some nutrition"),
    2667: (4,  "Frozen OJ: vitamin C, but concentrated sugar"),
    2674: (3,  "Frozen fruit juice remaining"),
    2662: (4,  "Frozen unconcentrated juice: less processed"),

    # === MILK ===
    3626: (6,  "Buttermilk: probiotic, protein, calcium"),
    3627: (3,  "Cream: high fat, used in small amounts"),
    3592: (4,  "Flavored milk (chocolate etc): protein + calcium; added sugar"),
    3625: (7,  "Plain milk: protein, calcium, B12, vitamin D — excellent"),
    3650: (2,  "Eggnog: very high sugar + fat"),
    1716: (3,  "Perishable cream/creamer"),
    1633: (6,  "Perishable milk/milk product drink"),
    6041: (6,  "RBC milk"),
    3628: (3,  "Remaining drinks/shakes refrig: usually high sugar"),

    # === NUTS ===
    1508: (7,  "Nuts bags: healthy fats, protein, fiber, magnesium"),
    1506: (7,  "Nuts cans: same"),
    1507: (7,  "Nuts jars: same"),
    1509: (7,  "Nuts unshelled: same — minimally processed"),

    # === PACKAGED MEATS-DELI ===
    # High sodium, high processed — these score low. WHO classifies
    # processed meats as Group 1 carcinogens.
    3584: (2,  "Beef bacon/canned bacon: high sat fat + sodium"),
    3577: (2,  "Bacon refrigerated: very high sodium + sat fat, processed"),
    3567: (2,  "Bratwurst/knockwurst: processed, high fat + sodium"),
    3576: (2,  "Frankfurters: high sodium, processed meat"),
    3582: (2,  "Cocktail franks: same as frankfurters"),
    3585: (3,  "Ham patties canned: moderate protein, high sodium"),
    3581: (3,  "Canned/refrig ham: moderate protein, high sodium"),
    3618: (3,  "Deli pouch lunchmeat: protein but high sodium + nitrates"),
    3617: (3,  "Non-sliced lunchmeat: same"),
    3574: (3,  "Sliced lunchmeat refrig: processed, sodium"),
    1624: (2,  "Perishable bacon"),
    1623: (3,  "Perishable canned ham"),
    1697: (2,  "Perishable canned meat product: highly processed"),
    1627: (2,  "Perishable frankfurter"),
    1629: (3,  "Perishable lunchmeat"),
    1689: (3,  "Perishable lunchmeat variety pack"),
    1630: (2,  "Perishable sausage: high fat + sodium"),
    3578: (2,  "Breakfast sausage: high fat + sodium + processed"),
    3572: (3,  "Dinner sausage: somewhat more varied"),

    # === PACKAGED MILK AND MODIFIERS ===
    1468: (2,  "Powdered creamers: often hydrogenated oil + sugar"),
    3629: (3,  "Liquid creamers: fat + added sugar"),
    1297: (6,  "Canned milk (evaporated/condensed): protein; condensed = high sugar"),
    1298: (6,  "Powdered milk: protein + calcium, useful"),
    1296: (6,  "Shelf-stable milk: UHT, same nutrients as regular"),
    1442: (3,  "Milk/water additives sweetened: chocolate powder etc"),
    1729: (2,  "Perishable powdered drink: usually high sugar"),

    # === PASTA ===
    1266: (4,  "Oriental noodles: refined wheat"),
    1331: (4,  "Macaroni: refined carbs, moderate"),
    1336: (4,  "Noodles/dumplings: refined"),
    1334: (4,  "Spaghetti: low GI if al dente, moderate"),
    1707: (4,  "Perishable pasta: fresh pasta, similar"),

    # === PICKLES, OLIVES, AND RELISH ===
    1142: (5,  "Chilies: vitamin C, capsaicin, low cal"),
    1149: (6,  "Black olives: healthy fats, vitamin E"),
    1147: (6,  "Green olives: healthy fats; high sodium"),
    1156: (5,  "Peppers: vitamin C, low cal"),
    1668: (6,  "Perishable olives"),
    1672: (4,  "Perishable pickle/relish"),
    1168: (4,  "Dill pickles: low cal, but high sodium"),
    1166: (3,  "Sweet pickles: added sugar + sodium"),
    1143: (5,  "Pimentos: vitamin C, low cal"),
    1164: (3,  "Relishes: often sweetened"),

    # === PIZZA/SNACKS/HORS DOEURVES-FRZN ===
    2628: (3,  "Frozen hors d'oeuvres/snacks: processed, varied"),
    1643: (3,  "Perishable pizza: refined + fat + sodium"),
    1645: (3,  "Perishable prepared appetizer"),
    2632: (2,  "Pizza crust frozen: refined flour"),
    2631: (3,  "Pizza frozen: refined + fat + sodium"),
    6081: (3,  "RBC pizza"),
    6082: (3,  "RBC prepared appetizer"),

    # === PREPARED FOOD-DRY MIXES ===
    1340: (3,  "Dry pasta dinners (Hamburger Helper etc): sodium + refined"),
    1337: (3,  "Dry dinners remaining: highly processed"),
    1339: (3,  "Dry rice dinners: sodium + refined"),
    7792: (4,  "Meal kit: fresher ingredients, but varies"),
    1251: (3,  "Mexican dinners dry: sodium + refined"),
    1255: (3,  "Mexican shells: refined flour"),
    1257: (5,  "Mexican tortillas: corn tortillas are whole grain"),
    1370: (4,  "Ethnic specialty mixes"),
    1265: (2,  "Ramen noodles: very high sodium, refined, low nutrition"),
    1724: (4,  "Perishable meal kit: fresher"),
    1676: (4,  "Perishable tortilla/Mexican shell"),
    1261: (3,  "Pizza crust/pie mixes: refined"),
    1320: (4,  "Rice mixes: sodium added"),
    1084: (3,  "Mashed potato dehydrated: processing removes nutrients"),
    1083: (3,  "Specialty dehydrated potato: similar"),

    # === PREPARED FOOD-READY-TO-SERVE ===
    1219: (3,  "Barbecued beef/pork canned: protein, high sodium/sugar"),
    1368: (3,  "Specialty canned bread"),
    1243: (5,  "Shelf-stable chicken: lean protein, high sodium"),
    1254: (5,  "Shelf-stable chili: protein + fiber from beans"),
    1212: (3,  "Corned beef canned: protein, very high sodium"),
    1213: (3,  "Corned beef hash: protein, high sodium + fat"),
    1228: (2,  "Deviled ham canned: processed, high fat"),
    1218: (3,  "Dried beef: protein, very high sodium"),
    1335: (3,  "Canned dumplings: refined + sodium"),
    1125: (3,  "Shelf-stable entrees/sides: processed, sodium"),
    1262: (3,  "Lasagna canned: refined + fat + sodium"),
    1258: (3,  "Macaroni shelf stable (SpaghettiOs etc)"),
    1225: (3,  "Imitation meat products"),
    1223: (3,  "Misc canned meat: processed"),
    1250: (3,  "Mexican dinners canned: sodium + refined"),
    1249: (3,  "Mexican specialties remaining"),
    1253: (5,  "Refried beans: high fiber + protein, some sodium"),
    1264: (3,  "Chow mein canned: sodium"),
    1269: (3,  "Oriental foods misc"),
    1647: (4,  "Perishable prepared meal multi-course: fresher"),
    1648: (4,  "Perishable prepared meal single course"),
    1723: (3,  "Perishable service bar"),
    1240: (2,  "Pickled pork products: very high sodium, processed"),
    1160: (4,  "Pickled vegetables/fruit: probiotics in some"),
    1092: (3,  "Potato salad canned: refined + mayo"),
    1229: (2,  "Potted meat canned: highly processed"),
    1242: (3,  "Prepared sandwich shelf stable"),
    1256: (3,  "Ravioli canned: refined + sodium"),
    1322: (4,  "Rice canned: moderate"),
    1363: (4,  "Rice cakes: low cal, whole grain"),
    1215: (4,  "Canned roast beef: lean protein, sodium"),
    1216: (3,  "Roast beef hash: fat + sodium"),
    1180: (2,  "Sandwich spreads meat: processed + fat"),
    1235: (2,  "Sausage canned: processed + fat + sodium"),
    1260: (3,  "Spaghetti canned: refined + sodium"),
    1234: (2,  "Spiced lunch meat canned: highly processed"),
    1230: (3,  "Spreads hors d'oeuvres"),
    1221: (4,  "Beef stew: protein + some veg"),
    1245: (4,  "Chicken stew: lean protein + veg"),
    1244: (3,  "Remaining stew: varies"),
    1246: (5,  "Turkey canned: lean protein"),
    1063: (5,  "Vegetables/beans with meat: fiber + protein"),
    1233: (2,  "Vienna sausage: highly processed, high sodium"),

    # === PREPARED FOODS-FROZEN ===
    2693: (2,  "Corn dogs: processed meat + fried breading"),
    2615: (4,  "Frozen dinners: varies; TV dinners average moderate"),
    2623: (4,  "Italian frozen entrees 1-food: pasta-based"),
    2614: (4,  "Italian frozen entrees 2-food"),
    2619: (4,  "Meat frozen entrees 1-food: protein-focused"),
    2611: (4,  "Meat frozen entrees 2-food"),
    2624: (4,  "Mexican frozen entrees 1-food"),
    2616: (4,  "Mexican frozen entrees 2-food"),
    2613: (4,  "Multi-pack frozen entrees"),
    2622: (5,  "Oriental frozen entrees 1-food: tend to be lower cal"),
    2627: (5,  "Oriental frozen entrees 2-food"),
    2621: (5,  "Poultry frozen entrees 1-food: chicken-based"),
    2612: (5,  "Poultry frozen entrees 2-food"),
    2625: (4,  "Remaining frozen entrees 1-food"),
    2606: (4,  "Remaining frozen entrees 2-food"),
    2626: (5,  "Seafood frozen entrees 1-food: fish-based"),
    2609: (5,  "Seafood frozen entrees 2-food"),
    2603: (4,  "Meal starters frozen"),
    2692: (4,  "Plain frozen pasta"),
    1690: (4,  "Perishable meal starter"),
    1653: (5,  "Perishable soup/chowder: vegetable/broth based"),
    1654: (4,  "Perishable stew/chili"),
    2617: (3,  "Pot pies frozen: high fat crust + sodium"),
    6083: (4,  "RBC prepared meal single course"),
    2695: (3,  "Sauces/gravies frozen: sodium + fat"),
    2694: (5,  "Frozen soup: broth-based, better than pot pies"),
    2696: (4,  "Taco filling frozen"),

    # === PUDDING, DESSERTS-DAIRY ===
    1646: (2,  "Perishable prepared gelatin: mostly sugar"),
    1649: (2,  "Perishable prepared pudding: high sugar"),
    3566: (2,  "Pudding refrigerated: high sugar"),

    # === SALAD DRESSINGS, MAYO, TOPPINGS ===
    1175: (3,  "Mayonnaise: fat-dense; some healthy fats in good mayo"),
    1616: (2,  "Salad/potato topping dry: sodium + sugar"),
    1657: (3,  "Perishable salad dressing: fat + sodium"),
    1737: (3,  "Perishable salad dressing mix"),
    1170: (2,  "Salad/potato toppings dry: sodium"),
    1173: (3,  "Miracle Whip type: fat + sugar"),
    1177: (3,  "Salad dressing liquid: fat + sodium"),
    1179: (3,  "Salad dressing mixes dry"),
    1539: (4,  "Reduced-calorie salad dressing: better than full-fat"),
    1181: (3,  "Sandwich spreads relish type: sugar + sodium"),

    # === SEAFOOD - CANNED ===
    # Fish is healthy: omega-3s, protein, lean
    1198: (5,  "Anchovy paste: high sodium but omega-3s, concentrated"),
    1715: (6,  "Perishable shelf-stable seafood"),
    6071: (6,  "RBC shelf-stable seafood"),
    1199: (6,  "Anchovies canned: omega-3s, calcium from bones"),
    1204: (6,  "Oysters canned: high zinc, protein, omega-3s"),
    1202: (6,  "Remaining canned seafood"),
    1205: (8,  "Salmon canned: excellent omega-3s + calcium from bones"),
    1207: (8,  "Sardines: excellent omega-3s + calcium; low mercury"),
    1208: (7,  "Shrimp canned: high protein, very low fat"),
    1200: (6,  "Clams canned: protein, iron, B12"),
    1203: (6,  "Crab canned: lean protein"),
    1209: (7,  "Tuna shelf-stable: protein + omega-3s; low mercury in light"),

    # === SNACKS ===
    1341: (2,  "Cracker sandwich packs: refined + fat + sodium"),
    1185: (2,  "Dip canned: fat + sodium"),
    1184: (2,  "Dip mixes: sodium"),
    1708: (2,  "Perishable combo snack: processed"),
    1696: (2,  "Perishable lunch combo: processed"),
    1660: (4,  "Perishable meat/seafood snack: protein, but high sodium"),
    1665: (7,  "Perishable nuts: healthy fats + protein"),
    1661: (4,  "Perishable popcorn: whole grain, depends on preparation"),
    1687: (2,  "Perishable pork rind/skin: high fat + sodium"),
    1662: (2,  "Perishable potato chips: fried, fat + sodium"),
    1663: (3,  "Perishable pretzel: lower fat, still refined + sodium"),
    1608: (4,  "Perishable snack bar: varies widely"),
    1666: (3,  "Perishable snack remaining"),
    1664: (3,  "Perishable tortilla chips: moderate"),
    1667: (6,  "Perishable trail mix: nuts + dried fruit"),
    1328: (4,  "Popcorn popped: whole grain, fiber; depends on butter/salt"),
    1329: (5,  "Popcorn unpopped: whole grain, high fiber when plain"),
    1332: (1,  "Caramel corn: popcorn + sugar coating"),
    1325: (2,  "Corn chips: fried, high fat + sodium"),
    1452: (4,  "Health bars & sticks: fiber/protein; often high sugar"),
    1271: (4,  "Meat snacks (jerky): protein, but high sodium + nitrates"),
    1270: (2,  "Pork rinds: high fat, no fiber"),
    1323: (2,  "Potato chips: fried, high fat + sodium"),
    1324: (2,  "Potato sticks: same"),
    1330: (3,  "Pretzels: low fat but refined + sodium"),
    1318: (2,  "Puffed cheese snacks: refined + fat + sodium"),
    1327: (2,  "Remaining snacks: assume chip-like"),
    1326: (3,  "Tortilla chips: moderate — whole corn"),
    1333: (2,  "Variety snack packs: mixed junk food"),
    1422: (6,  "Trail mixes: nuts + dried fruit — nutritious"),

    # === SNACKS, SPREADS, DIPS-DAIRY ===
    3602: (3,  "Dairy dip refrig: sour cream based, fat + sodium"),
    3570: (4,  "Garlic spreads refrig: garlic benefit, some fat"),
    1655: (3,  "Perishable dip/spread"),
    1652: (3,  "Perishable prepared spread"),
    6077: (3,  "RBC dip/spread"),
    3579: (6,  "Seafood refrigerated: smoked salmon, etc."),
    3583: (3,  "Remaining spreads"),

    # === SOFT DRINKS-NON-CARBONATED ===
    1048: (3,  "Breakfast drinks powdered (Tang etc): fortified but sugary"),
    1046: (1,  "Fruit punch bases/syrups: mostly sugar"),
    1052: (1,  "Ice pops unfrozen: mostly sugar + water"),
    1713: (1,  "Perishable cocktail mix: sugar + artificial"),
    1638: (2,  "Perishable soft drink/water: includes water (good) + sugary drinks"),
    6086: (2,  "RBC soft drink/water"),
    1049: (2,  "Remaining non-refrig drinks: usually sugary"),
    1050: (1,  "Powdered soft drinks (Kool-Aid): sugar + artificial color"),

    # === SOUP ===
    1295: (2,  "Bouillon: mostly sodium, minimal nutrition"),
    1338: (2,  "Instant meals (cup noodles): very high sodium, refined"),
    1683: (3,  "Perishable broth/bouillon: somewhat better than cube"),
    6087: (4,  "RBC soup/chowder"),
    1293: (4,  "Soup mixes dry: varies"),
    1290: (4,  "Canned soup: vegetable/tomato soups have nutrition; cream soups less"),
    1292: (3,  "Stew mixes dry: sodium + refined"),

    # === SUGAR, SWEETENERS ===
    1705: (0,  "Sugar: pure empty calories"),
    1718: (1,  "Perishable sugar substitute: no calories, but zero nutrition"),
    1400: (0,  "Brown sugar: slightly more minerals than white, but essentially same"),
    1402: (0,  "Remaining sugar: same"),
    1535: (1,  "Sugar substitutes: no calories, no nutrition"),
    1403: (0,  "Granulated sugar: empty calories, benchmark lowest"),
    1401: (0,  "Powdered sugar: same"),

    # === TABLE SYRUPS, MOLASSES ===
    1407: (4,  "Molasses: iron, calcium, potassium; has actual minerals"),
    1686: (2,  "Perishable syrup/molasses"),
    1406: (2,  "Berry/fruit type syrup: mostly sugar with fruit flavoring"),
    1404: (2,  "Sorghum/sugar syrup"),
    1405: (1,  "Table syrup (pancake syrup): mostly HFCS or sugar"),

    # === UNPREP MEAT/POULTRY/SEAFOOD-FRZN ===
    2688: (4,  "Frozen ground beef: protein; fat varies by grade"),
    2689: (4,  "Frozen pork: protein; fat varies by cut"),
    2680: (4,  "Frozen remaining meat: varies"),
    2690: (4,  "Frozen sandwich steak: moderate protein"),
    2691: (5,  "Frozen veal: lean protein"),
    2687: (5,  "Frozen beef steak: leaner cuts have good protein profile"),
    2681: (6,  "Frozen poultry (chicken/turkey): lean protein, low sat fat"),
    1659: (6,  "Perishable frozen/refrig seafood: lean protein + omega-3s"),
    6078: (5,  "RBC fresh/frozen meat"),
    6065: (6,  "RBC frozen/refrig seafood"),
    2607: (4,  "Fish breaded frozen: fish benefits reduced by frying + breading"),
    2643: (4,  "Shrimp breaded frozen: same"),
    2682: (7,  "Shrimp unbreaded frozen: high protein, very low fat"),
    2645: (6,  "Crab unbreaded frozen: lean protein"),
    2683: (7,  "Fish unbreaded frozen: omega-3s, protein"),
    2679: (4,  "Remaining seafood breaded: breaded reduces score"),
    2686: (6,  "Remaining seafood unbreaded: lean protein"),

    # === VEGETABLES - CANNED ===
    # Vegetables retain most nutrients when canned; sodium is the main downside.
    1267: (6,  "Bean sprouts canned: low cal, some fiber"),
    1155: (4,  "Cocktail onions: low cal, some sugar added"),
    1103: (6,  "Grape leaves canned: fiber, low cal"),
    1144: (7,  "Mushrooms shelf stable: B vitamins, low cal"),
    1263: (6,  "Oriental canned vegetables: low cal, fiber"),
    1091: (3,  "Jelled aspic salad: gelatin + sugar"),
    1108: (7,  "Tomato paste: concentrated lycopene, fiber — very nutritious"),
    1106: (7,  "Tomato puree: similar to paste"),
    1109: (6,  "Tomato sauce: lycopene, moderate sodium"),
    1105: (6,  "Remaining canned tomatoes: lycopene, low cal"),
    1110: (6,  "Stewed tomatoes: some added sodium/sugar"),
    1107: (7,  "Whole canned tomatoes: lycopene, low cal"),
    1069: (5,  "Red cabbage canned: anthocyanins, some sugar added"),
    1057: (7,  "Artichokes canned: high fiber, prebiotic"),
    1058: (7,  "Asparagus canned: folate, low cal"),
    1088: (8,  "Chili beans canned: very high fiber + protein"),
    1066: (8,  "Garbanzo/chickpeas canned: fiber + protein"),
    1060: (7,  "Green beans canned: fiber, vitamins"),
    1076: (8,  "Kidney beans canned: very high fiber + protein"),
    1067: (7,  "Lima beans canned: fiber + protein"),
    1087: (8,  "Pinto beans canned: fiber + protein"),
    1065: (8,  "Remaining beans canned: high fiber + protein"),
    1064: (7,  "Vegetarian beans (baked beans): fiber, some added sugar"),
    1061: (7,  "Waxed beans canned: fiber, very low cal"),
    1062: (8,  "White/navy/northern beans: excellent fiber + protein"),
    1068: (5,  "Beets canned: folate, some sugar added"),
    1070: (6,  "Carrots canned: beta-carotene, some sodium"),
    1073: (5,  "Corn on the cob canned: starchy but whole grain"),
    1071: (5,  "Cream-style corn: added sugar + starch"),
    1072: (5,  "Whole kernel corn canned: starchy vegetable"),
    1102: (7,  "Greens canned (collards/kale): high nutrient density"),
    1074: (5,  "Hominy canned: starchy corn"),
    1082: (6,  "Mixed vegetables canned: fiber + vitamins"),
    1075: (6,  "Okra canned: fiber, vitamin C"),
    1077: (4,  "Onions canned: very high cal listed — check data"),
    1081: (6,  "Peas and carrots canned: fiber + beta-carotene"),
    1080: (7,  "Peas canned: fiber + protein"),
    1079: (7,  "Remaining peas canned: fiber + protein"),
    1085: (5,  "Potatoes canned: starchy, some potassium"),
    1097: (6,  "Remaining canned vegetables"),
    1093: (6,  "Sauerkraut: probiotic, very low cal"),
    1094: (8,  "Spinach canned: iron, folate, vitamin K"),
    1095: (6,  "Squash/rutabagas canned: fiber, low cal"),
    1096: (6,  "Succotash canned: corn + beans = fiber + protein"),
    1086: (5,  "Sweet potatoes/yams canned: nutritious but often in syrup"),

    # === VEGETABLES AND GRAINS - DRIED ===
    1317: (8,  "Barley dry: very high fiber, beta-glucan, lowers cholesterol"),
    1315: (9,  "Dried beans: highest fiber + protein; one of healthiest foods"),
    7803: (7,  "Remaining grains: whole grains"),
    1316: (9,  "Peas/lentils/corn dry: fiber + protein"),
    1669: (7,  "Perishable dried vegetable"),
    1321: (4,  "Instant rice: processed, lower fiber than brown"),
    1319: (5,  "Packaged/bulk rice: white rice moderate, brown higher"),
    1439: (2,  "Tapioca pure: starchy, low nutrient"),

    # === VEGETABLES-FROZEN ===
    # Frozen vegetables retain nearly all nutrients of fresh.
    1671: (8,  "Perishable frozen/refrig vegetable"),
    1642: (7,  "Perishable frozen/refrig specialty potato"),
    2635: (4,  "Breaded frozen vegetables: breading + frying reduces score"),
    2641: (8,  "Frozen broccoli: excellent — fiber, vitamin C, glucosinolates"),
    2642: (8,  "Frozen carrots: beta-carotene, fiber"),
    2630: (6,  "Frozen corn: starchy but whole vegetable"),
    2608: (5,  "Frozen corn on cob: starchy"),
    2644: (7,  "Frozen lima beans: fiber + protein"),
    2640: (8,  "Frozen mixed vegetables: varied nutrients"),
    2637: (4,  "Frozen breaded mushrooms: breaded reduces score"),
    2638: (3,  "Frozen breaded onion rings: battered + fried"),
    2620: (8,  "Frozen peas: fiber + protein + vitamins"),
    2634: (4,  "Frozen potatoes (french fries): fried, starchy"),
    2636: (7,  "Remaining frozen vegetables: assumed like other veg"),
    2618: (4,  "Vegetables in pastry frozen: added fat + refined flour"),
    2633: (8,  "Frozen green beans: fiber, vitamins, low cal"),
    2639: (6,  "Vegetables in sauce frozen: sauce adds sodium/fat"),

    # === YOGURT ===
    1738: (4,  "Perishable yogurt shakes/drinks: high sugar"),
    1634: (6,  "Perishable yogurt spoonable: protein + probiotic; varies by sugar"),
    3603: (5,  "Yogurt refrigerated: high sugar (19g/100g); probiotic benefit"),
    3612: (4,  "Yogurt drinks: similar sugar level as regular yogurt"),
}

# ============================================================
# BUILD OUTPUT
# ============================================================
def main():
    base = Path('/Users/anyamarchenko/CEGA Dropbox/Anya Marchenko/nielsen_data')
    health = pd.read_parquet(OUT_DIR / 'module_healthiness.parquet')

    rows = []
    missing = []
    for _, row in health.iterrows():
        code = int(row['product_module_code'])
        if code in SCORES:
            score, rationale = SCORES[code]
        else:
            missing.append((code, row['product_module_descr'], row['product_group_descr']))
            score, rationale = (None, 'NOT SCORED')
        rows.append({
            'product_module_code': code,
            'product_module_descr': row['product_module_descr'],
            'product_group_descr': row['product_group_descr'],
            'claude_hi': score,
            'rationale': rationale,
        })

    df = pd.DataFrame(rows)

    if missing:
        print(f"\nWARNING: {len(missing)} modules not explicitly scored:")
        for code, name, grp in missing:
            print(f"  {code:5d}  {grp:<40s}  {name}")

    # Normalize to 0-1 so it's on the same scale as other health indices
    df['claude_hi_norm'] = df['claude_hi'] / 10.0

    out_parquet = OUT_DIR / 'claude_hi_scores.parquet'
    out_csv     = OUT_DIR / 'claude_hi_scores.csv'
    df.to_parquet(out_parquet, index=False)
    df.to_csv(out_csv, index=False)

    print(f"\nSaved: {out_parquet}")
    print(f"Saved: {out_csv}")
    print(f"Modules scored: {df['claude_hi'].notna().sum()} / {len(df)}")
    print(f"\nTop 15 highest-scoring modules:")
    print(df.nlargest(15, 'claude_hi')[['product_module_descr', 'product_group_descr', 'claude_hi', 'rationale']].to_string(index=False))
    print(f"\nBottom 15 lowest-scoring modules:")
    print(df.nsmallest(15, 'claude_hi')[['product_module_descr', 'product_group_descr', 'claude_hi', 'rationale']].to_string(index=False))


if __name__ == '__main__':
    main()
