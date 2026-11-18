# Cluster D_canned: canned fruit; pickles, olives and relish; soup; canned vegetables (81 modules).
# Line drawn for canned vegetables: a vegetable heated on its own as a side dish or as the vegetable part of a dish = 4 minutes;
# a product eaten cold as sold, or a small addition stirred into another dish (tomatoes, mushrooms, water chestnuts, chilies) = 0.

HEAT = 'canned vegetable: drained and heated in a pan or the microwave'
BEANS = 'canned beans: heated in a pan or the microwave'
FRUIT = 'canned fruit: eaten as sold'

RULES = {
    # ---------------- canned fruit ----------------
    1021: dict(name='CANNED FRUIT - PINEAPPLE', minutes=0, why=FRUIT),
    1017: dict(name='CANNED FRUIT-PEACHES-CLING', minutes=0, why=FRUIT),
    1003: dict(name='CANNED FRUIT - APPLE SAUCE', minutes=0, why='applesauce: eaten as sold'),
    1002: dict(name='PIE & PASTRY FILLING - CANNED', minutes=0, why='pie filling: a baking addition, the crust carries the time'),
    1090: dict(name='PUMPKIN-CANNED', minutes=0, why='pumpkin puree or pie mix: a baking addition, the crust carries the time'),
    1014: dict(name='CANNED FRUIT - ORANGES', minutes=0, why=FRUIT),
    1009: dict(name='CRANBERRIES - SHELF STABLE', minutes=0, why='cranberry sauce: served cold as sold'),
    1020: dict(name='CANNED FRUIT-PEARS', minutes=0, why=FRUIT),
    1011: dict(name='CANNED FRUIT-FRUIT COCKTAIL', minutes=0, why=FRUIT),
    1012: dict(name='CANNED FRUIT - FRUIT MIXES & SALAD FRUITS', minutes=0, why=FRUIT),
    1428: dict(name='CHERRIES-MARASCHINO', minutes=0, why='maraschino cherries: a topping, used as sold'),
    1024: dict(name='CANNED FRUIT-CHERRIES', minutes=0, why=FRUIT),
    1007: dict(name='CANNED FRUIT-APRICOTS', minutes=0, why=FRUIT),
    1028: dict(name='CANNED FRUIT - REMAINING', minutes=0, why=FRUIT, rules=[
        (dict(product='ACKEE'), 4, 'canned ackee: drained and heated in a pan'),
    ]),
    1016: dict(name='CANNED FRUIT - PEACHES - FREESTONE', minutes=0, why=FRUIT),
    1006: dict(name='CANNED FRUIT - APPLES', minutes=0, why='canned apple slices or spiced rings: eaten cold or used as pie filling', rules=[
        (dict(descr=r'\bFRD\b|FRD/|ESCALLOPED'), 4, 'canned fried apples: heated as a side dish'),
        (dict(brand=r"LUCK'S|GLORY"), 4, 'canned fried apples: heated as a side dish'),
    ]),
    1008: dict(name='CANNED FRUIT - BERRIES', minutes=0, why=FRUIT),
    1013: dict(name='CANNED FRUIT-GRAPEFRUIT', minutes=0, why=FRUIT),
    1027: dict(name='CANNED FRUIT - PRUNES', minutes=0, why='cooked prunes in a jar: eaten as sold'),
    1427: dict(name='MINCEMEAT-CANNED', minutes=0, why='mincemeat: a pie filling, the crust carries the time'),
    1026: dict(name='CANNED FRUIT-PLUMS', minutes=0, why=FRUIT),
    1010: dict(name='CANNED FRUIT - FIGS', minutes=0, why=FRUIT),
    1023: dict(name='CANNED FRUIT - GRAPES', minutes=0, why=FRUIT),

    # ---------------- pickles, olives, relish ----------------
    1168: dict(name='PICKLES - DILL', minutes=0, why='pickles: eaten as sold'),
    1149: dict(name='OLIVES - BLACK', minutes=0, why='olives: eaten as sold'),
    1147: dict(name='OLIVES - GREEN', minutes=0, why='olives: eaten as sold'),
    1166: dict(name='PICKLES - SWEET', minutes=0, why='pickles: eaten as sold'),
    1156: dict(name='PEPPERS', minutes=0, why='pickled or roasted peppers in a jar: a condiment, used as sold'),
    1164: dict(name='RELISHES', minutes=0, why='relish or chutney: a condiment'),
    1142: dict(name='CHILIES', minutes=0, why='canned green chilies: a flavoring added to another dish'),
    1143: dict(name='PIMENTOS - CANNED', minutes=0, why='pimentos: a flavoring added cold to another dish'),

    # ---------------- soup ----------------
    1290: dict(name='SOUP-CANNED', minutes=4, basis='serv4', why='ready-to-serve canned soup: poured into a pan and heated', rules=[
        (dict(flavor=r'(?:BROTH|STOCK|CONSOMME|BONE)$',
              flavor_not=r'NDL|PASTA|PST|MTBL|SPNH|SHELLS|FRENCH ON|VG.?BEEF|VG/BF|BARLEY|SCOTCH|RICE| IN |MINESTRONE|ESCAROLE|MATZO|& VG|PN/|HSRWMC'),
         0, 'broth or stock: a cooking liquid, the dish made with it carries the time'),
        (dict(type='CONCENTRATE'), 0, 'broth concentrate: a cooking ingredient'),
        (dict(type='CONDENSED', flavor=r'^CREAM OF (?:MR|CH|CELERY)|^CREAM CH MR|^GOLDEN MR|^CCH$'),
         0, 'condensed cream soup for cooking: the sauce of a casserole, which carries the time'),
        (dict(style=r'^MICRO'), 3, 'microwave soup bowl or cup: 2 minutes in the microwave, 1 minute standing'),
        (dict(type='CONDENSED'), 5, 'condensed soup: stirred with a can of water or milk and heated'),
    ]),
    1293: dict(name='SOUP MIXES - DRY & BASES', minutes=12, basis='serv4', why='dry soup mix: stirred into boiling water and simmered 5-10 minutes', rules=[
        (dict(form='BASE', flavor=r'GUMBO|JAMBALAYA|ETOUFFEE'), 30, 'gumbo base mix: water and meat added, simmered 25 minutes', 'pkg'),
        (dict(form=r'BROTH|BASE|POWDER|GRANUL|TABLET|CUBE|PASTE|LIQUID'), 0, 'broth base or bouillon: a cooking ingredient'),
        (dict(form=r'SOUP & DIP'), 0, 'recipe soup mix: a seasoning for dips, roasts and meat loaf'),
        (dict(flavor=r'^(?:(?:BEEFY|GOLDEN|FRENCH) )?ON(?: MR)?$'), 0, 'onion soup mix: a seasoning for dips, roasts and meat loaf'),
        (dict(brand='^KNORR$', flavor=r'^VG$'), 0, 'vegetable recipe mix: stirred cold into spinach dip'),
        (dict(flavor=r'(?:TEN|ELEVEN|THIRTEEN|FIFTEEN|SIXTEEN|FOUR|1\d) BEAN|^(?:CJ |CJ FLV )?BEAN$|MIXED BEAN|^RED BEAN$|GREAT NORTHERN BEAN|BLACKEYE PEA|WHOLE GRAIN & BEAN'),
         90, 'dry bean soup mix: beans simmered until soft, soaking not counted', 'pkg'),
        (dict(descr=r'\b1\d ?BN\b'), 90, 'dry bean soup mix: beans simmered until soft, soaking not counted', 'pkg'),
        (dict(flavor=r'MATZO BALL'), 30, 'matzo ball soup mix: balls mixed, shaped and simmered 20 minutes', 'pkg'),
        (dict(brand=r"MANISCHEWITZ|STREIT|KEDEM", flavor=r'PEA|LTL|BARLEY|MINESTRONE|^VG$|^HY BB$|VARIETY'),
         90, 'dry pea, lentil or barley soup tube: simmered about 90 minutes', 'pkg'),
        (dict(brand=r"BOB'S RED MILL"), 60, 'dry soup mix of split peas, lentils and barley: simmered 60 minutes', 'pkg'),
        (dict(brand=r"BEAR CREEK|SHORE LUNCH|CUGINO|FRONTIER"), 20, 'family-size dry soup mix: whisked into 8 cups boiling water, simmered 15 minutes', 'pkg'),
        (dict(brand=r'CUP NOODLES|BOWL NOODLES|SOUPER MEAL|INSTANT LUNCH|MISO-CUP'), 4, 'cup or bowl noodles: boiling water added, 3 minutes standing'),
        (dict(style=r'^ORI', type=r'INSTANT|MICROWAVE|READY', brand_not=r'LIPTON|WYLER|SHORE'), 4, 'cup or bowl noodles: boiling water added, 3 minutes standing'),
        (dict(style=r'^ORI', brand_not=r'LIPTON|WYLER|SHORE'), 5, 'ramen block: boiled 3 minutes after the water boils'),
        (dict(type=r'INSTANT|MICROWAVE', brand_not=r'ALESSI|GIA RUSSA|OSEM'), 3, 'instant cup soup: boiling water stirred in'),
    ]),
    1338: dict(name='INSTANT MEALS', minutes=4, basis='serv4', why='cup noodles or microwave noodle tray: water added, 3-4 minutes', rules=[
        (dict(brand=r'MOUNTAIN HOUSE|ALPINE AIRE|AUGASON|BACKPACKER'), 12, 'freeze-dried pouch meal: boiling water poured in, 9 minutes standing', 'pkg'),
    ]),
    1295: dict(name='BOUILLON', minutes=0, why='bouillon cubes or powder: a cooking ingredient'),
    1292: dict(name='STEW MIXES - DRY', minutes=30, basis='pkg', why='dry stew mix: water and meat added, simmered 25 minutes', rules=[
        (dict(brand=r'MOUNTAIN HOUSE'), 12, 'freeze-dried pouch meal: boiling water poured in, 9 minutes standing', 'pkg'),
    ]),

    # ---------------- canned vegetables ----------------
    1105: dict(name='TOMATOES - REMAINING - CANNED', minutes=0, why='diced or crushed tomatoes: an ingredient of a sauce or dish that carries the time'),
    1060: dict(name='VEGETABLES-BEANS-GREEN-CANNED', minutes=4, why=HEAT),
    1072: dict(name='VEGETABLES-CORN-WHOLE KERNEL-CANNED', minutes=4, why=HEAT),
    1109: dict(name='TOMATO SAUCE', minutes=0, why='tomato sauce: a sauce, the main food carries the time'),
    1144: dict(name='MUSHROOMS - SHELF STABLE', minutes=0, why='canned mushrooms: drained and added to a sauce, pizza or casserole'),
    1076: dict(name='VEGETABLES-BEANS-KIDNEY/RED-CANNED', minutes=4, why=BEANS),
    1080: dict(name='VEGETABLES-PEAS-CANNED', minutes=4, why=HEAT, rules=[
        (dict(container='POLYBAG'), 7, 'frozen peas in a bag: boiled on the stove'),
    ]),
    1065: dict(name='VEGETABLES-BEANS-REMAINING-CANNED', minutes=4, why=BEANS),
    1077: dict(name='VEGETABLES-ONIONS-CANNED', minutes=0, why='french fried onions and crispy onion toppings: a topping, used as sold', rules=[
        (dict(descr=r'\bWH\b', container='BAG'), 3, 'fresh whole onions in a bag: peeled and chopped'),
        (dict(descr=r'\bWH\b', type_not=r'COCKTAIL|PICKLED|SOUR|VINEGAR|SILVER|MEDIUM'), 4, 'whole boiled onions in a jar or can: heated'),
    ]),
    1108: dict(name='TOMATO PASTE', minutes=0, why='tomato paste: an ingredient of a sauce or dish that carries the time'),
    1088: dict(name='VEGETABLES-BEANS-CHILI-CANNED', minutes=4, why='canned chili beans in sauce: heated in a pan or the microwave'),
    1082: dict(name='VEGETABLES-MIXED-CANNED', minutes=4, why=HEAT, rules=[
        (dict(product=r'SALAD|MUFF|GIARDINIERA|GUACAMOLE'), 0, 'bean salad, olive salad or pickled vegetable mix: eaten cold as sold'),
        (dict(product=r'CHILI?E? & TOMATO'), 0, 'diced tomatoes with green chilies: an ingredient of a dip or dish'),
        (dict(container='GLASS|JAR'), 0, 'pickled or marinated vegetable mix in a jar: eaten cold as sold'),
        (dict(container='POLYBAG', product=r'VEGETABLE MIX|MIXED VEGETABLES'), 7, 'frozen vegetable mix in a bag: boiled on the stove'),
    ]),
    1107: dict(name='TOMATOES-WHOLE-CANNED', minutes=0, why='whole canned tomatoes: an ingredient of a sauce; grape tomatoes in a tub: eaten raw'),
    1057: dict(name='VEGETABLES-ARTICHOKES-CANNED', minutes=4, why='artichoke hearts in water: drained and heated in a dish', rules=[
        (dict(descr=r'MRN|IM-MR|MARIN|GRLD'), 0, 'marinated artichoke hearts: eaten cold as sold'),
    ]),
    1068: dict(name='VEGETABLES-BEETS-SHELF STABLE', minutes=4, why='plain or Harvard beets: heated in a pan or the microwave', rules=[
        (dict(style=r'PICKL|BALSAMIC|MARINATED|HORSERADISH|SALAD|TANGY'), 0, 'pickled beets: eaten cold as sold'),
        (dict(descr=r'\bPKL\b|PCKL'), 0, 'pickled beets: eaten cold as sold'),
    ]),
    1071: dict(name='VEGETABLES-CORN-CREAM STYLE-CANNED', minutes=4, why=HEAT),
    1093: dict(name='VEGETABLES-SAUERKRAUT-SHELF STABLE', minutes=4, why='sauerkraut: heated in a pan as a side dish'),
    1086: dict(name='VEGETABLES-SWEET POTATOES & YAMS-CANNED', minutes=4, why='canned yams in syrup: heated in a pan or the microwave', rules=[
        (dict(container=r'^BAG$|POLYBAG|WRAP', form='WHOLE'), 30, 'fresh sweet potatoes: peeled, cut and boiled or baked'),
    ]),
    1087: dict(name='VEGETABLES-BEANS-PINTO-CANNED', minutes=4, why=BEANS),
    1110: dict(name='TOMATOES-STEWED', minutes=0, why='stewed tomatoes: an ingredient of a soup, stew or casserole that carries the time'),
    1058: dict(name='VEGETABLES-ASPARAGUS-SHELF STABLE', minutes=4, why=HEAT, rules=[
        (dict(type=r'MARINATED'), 0, 'marinated asparagus in a jar: eaten cold as sold'),
    ]),
    1085: dict(name='VEGETABLES-POTATOES-CANNED', minutes=4, why='canned potatoes: drained and heated or browned in a pan', rules=[
        (dict(container=r'BAG|PLASTIC|TRAY'), 25, 'fresh potatoes in a bag: washed and boiled'),
        (dict(container_not=r'CAN|GLASS', descr=r'RSST|\bF$|POTATO WHL$'), 25, 'fresh potatoes: washed and boiled'),
    ]),
    1066: dict(name='VEGETABLES-BEANS-GARBANZO - CANNED', minutes=4, why='canned chickpeas: heated in a dish'),
    1070: dict(name='VEGETABLES-CARROTS-SHELF STABLE', minutes=4, why=HEAT),
    1062: dict(name='VEGETABLES-BEANS-WHITE/NORTHERN/NAVY-CANNED', minutes=4, why=BEANS),
    1064: dict(name='VEGETABLES-BEANS-VEGETARIAN-SHELF STABLE', minutes=4, why='canned baked beans: heated in a pan or the microwave'),
    1263: dict(name='ORIENTAL CANNED VEGETABLES', minutes=4, why='canned chop suey or stir-fry vegetables: drained and heated', rules=[
        (dict(product=r'WATER CHESTNUT|BAMBOO'), 0, 'water chestnuts or bamboo shoots: a small addition to a stir-fry or dip'),
        (dict(product=r'ZUKE'), 0, 'Japanese pickled vegetables: eaten cold as sold'),
    ]),
    1079: dict(name='VEGETABLES-PEAS-REMAINING-CANNED', minutes=4, why='canned blackeye, field or pigeon peas: heated in a pan'),
    1094: dict(name='VEGETABLES-SPINACH-CANNED', minutes=4, why=HEAT),
    1067: dict(name='VEGETABLES-BEANS-LIMA-CANNED', minutes=4, why=BEANS),
    1102: dict(name='VEGETABLES-GREENS-CANNED', minutes=4, why='canned collard, turnip or mustard greens: heated in a pan'),
    1097: dict(name='VEGETABLES-REMAINING-CANNED', minutes=4, why='canned cabbage, corn, callaloo and other vegetables: heated in a pan', rules=[
        (dict(product=r'PALM|OLIVE|GARLIC|JALAPENO|PAPRIKA|GINGER|HORSERADISH|PIPARRA|CUCUMBER|BRUSCHETTA|RADISH|SHALLOT'),
         0, 'hearts of palm, olives, jarred garlic: eaten cold or used as a flavoring'),
        (dict(product='BROCCOLI', container='POLYBAG|PLASTIC'), 12, 'fresh broccoli: cut and steamed or boiled'),
        (dict(container=r'GLASS|JAR|TUB|TRAY'), 0, 'pickled or marinated vegetables in a jar: eaten cold as sold'),
    ]),
    1106: dict(name='TOMATO PUREE', minutes=0, why='tomato puree: an ingredient of a sauce or dish that carries the time'),
    1074: dict(name='VEGETABLES-HOMINY-CANNED', minutes=4, why='canned hominy: drained and heated'),
    1081: dict(name='VEGETABLES-PEAS & CARROTS-CANNED', minutes=4, why=HEAT),
    1061: dict(name='VEGETABLES-BEANS-WAXED-CANNED', minutes=4, why=HEAT),
    1075: dict(name='VEGETABLES-OKRA-CANNED', minutes=4, why='canned cut okra: heated in a pan', rules=[
        (dict(type=r'PICKLED|DILL'), 0, 'pickled okra: eaten cold as sold'),
        (dict(descr=r'PKLD'), 0, 'pickled okra: eaten cold as sold'),
    ]),
    1267: dict(name='BEAN SPROUTS-CANNED', minutes=4, why='canned bean sprouts: drained and heated in a stir-fry'),
    1073: dict(name='VEGETABLES-CORN ON THE COB-CANNED', minutes=0, why='canned baby corn: a small addition to a stir-fry or salad'),
    1069: dict(name='VEGETABLES - RED CABBAGE - CANNED', minutes=4, why='sweet and sour red cabbage in a jar: heated as a side dish'),
    1095: dict(name='VEGETABLES-SQUASH & RUTABAGAS-CANNED', minutes=4, why=HEAT),
    1155: dict(name='COCKTAIL ONIONS', minutes=0, why='cocktail onions: a garnish, used as sold'),
    1096: dict(name='VEGETABLES-SUCCOTASH-CANNED', minutes=4, why=HEAT),
    1103: dict(name='GRAPE LEAVES-CANNED', minutes=75, basis='pkg', why='grape leaves in brine: filled with rice, rolled and simmered 45 minutes'),
    1091: dict(name='SALAD-JELLED ASPIC', minutes=0, why='jellied tomato aspic: served cold as sold'),
}
