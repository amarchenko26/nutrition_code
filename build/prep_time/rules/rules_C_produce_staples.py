# Cluster C_produce_staples: fresh produce, dried fruit, nuts, pasta, rice, dry beans and grains.
# Minutes = elapsed minutes of one cooking session of about four servings (see prep_rules_brief.md).
# Abbreviations used in descr (learned from the module profiles):
#   produce: F fresh, WH whole, CT cut, C&W cut and washed, FLRT florets, B bunch, BBY/BY baby, PLD peeled, SHRD shredded,
#            DCD diced, CKD cooked, MW microwave steam bag, NDL noodles (spiralized), BTRNT butternut, SSP sugar snap peas,
#            G-BN green beans, B-S brussels sprouts, CMB cucumber, PPR pepper, BRC broccoli, ASP asparagus, SQSH squash
#   rice:    RC rice, CM commodity (regular milled), PB parboiled, INS instant, BB boil-in-bag, BRWN brown, WGBR whole grain
#            brown rice, WLD wild, ARB arborio
#   pasta:   MCR macaroni products, SPG spaghetti, E-NDL egg noodles, THN thin, LASGN lasagna, GNCH gnocchi, PTO potato

RULES = {
    # ------------------------------------------------------------------ fresh fruit
    4225: dict(name='FRESH FRUIT-REMAINING', minutes=0, basis='serv4', why='berries, grapes, bananas, stone fruit, pears: eaten as sold', rules=[
        (dict(product=r'WATERMELON|CANTALOUPE|HONEYDEW|MELON|^CANTA$|DEWLICIOUS|^CASPER$'), 5, 'whole melon: halve, seed, cut off the rind, slice'),
        (dict(product=r'PINEAPPLE|PINA MIEL'), 8, 'whole pineapple: cut off the skin, core, slice'),
        (dict(product=r'MANGO|PAPAYA'), 4, 'mango or papaya: peel, cut the flesh off the seed'),
        (dict(product=r'POMEGRANATE'), 5, 'pomegranate: cut open and pick out the seeds'),
        (dict(product=r'COCONUT'), 8, 'whole coconut: crack open and pry out the meat'),
        (dict(product=r'PLANTAIN'), 12, 'plantain: peel, slice, fry about 8 min'),
        (dict(product=r'PUMPKIN|GOURD'), 30, 'pumpkin: cut, seed, bake or boil like winter squash'),
        (dict(product=r'AVOCADO'), 2, 'avocado: halve, pit, scoop or slice'),
        (dict(product=r'KIWI'), 2, 'kiwi: peel and slice'),
        (dict(product=r'GRAPEFRUIT|PUMMELO|POMELO'), 2, 'grapefruit or pummelo: halve or peel and section'),
        (dict(product=r'MANDARIN|TANGERINE|TANGELO|SATSUMA|CLEMENTINE|MINNEOLA|MURCOTT|ORANGE|UGLI|^CITRUS$|^CITRINE$'), 1, 'mandarins, tangerines, oranges: peel'),
    ]),
    4470: dict(name='FRESH STRAWBERRIES', minutes=0, basis='serv4', why='strawberries: eaten as sold'),
    4010: dict(name='FRESH APPLES', minutes=0, basis='serv4', why='apples, caramel apples: eaten as sold'),
    4355: dict(name='FRESH ORANGES', minutes=1, basis='serv4', why='oranges: peel or cut into wedges'),
    4180: dict(name='FRESH GRAPEFRUIT', minutes=2, basis='serv4', why='grapefruit: halve and loosen the sections, or peel'),
    4230: dict(name='FRESH KIWI', minutes=2, basis='serv4', why='kiwi: peel and slice, or halve and scoop'),
    4085: dict(name='FRESH CRANBERRIES', minutes=15, basis='serv4', why='raw cranberries: boil with sugar and water into sauce, 10-15 min'),
    6049: dict(name='RBC FRESH FRUIT', minutes=0, basis='serv4', why='single fresh fruit, mostly grapes and cherries: eaten as sold'),
    1620: dict(name='PERISHABLE FRESH FRUIT', minutes=0, basis='serv4', why='fresh fruit: eaten as sold', rules=[
        (dict(descr=r'ORANGE|MDRN|TNGRN'), 1, 'oranges, mandarins: peel'),
        (dict(descr=r'AVOCADO'), 2, 'avocado: halve, pit, scoop or slice'),
    ]),
    6050: dict(name='RBC PRECUT FRESH AND REFRIGERATED FRUIT', minutes=0, basis='serv4', why='precut fruit: eaten as sold'),

    # ------------------------------------------------------------------ fresh vegetables
    4400: dict(name='FRESH POTATOES', minutes=25, basis='serv4', why='potatoes: peel, cut, boil 15-20 min', rules=[
        (dict(descr=r'MICRO|MINUTE BAKER|FAST BAKER|EXPRESS BAKE|E-B-P|STEAMABLE|\bSSS\b|\bMW\b'), 8, 'potato wrapped or bagged for the microwave: 7-8 min'),
        (dict(descr=r'\bSWT\b|SWEET|\bYAM'), 30, 'sweet potatoes: peel, cut, boil or bake about 25-30 min'),
    ]),
    4280: dict(name='FRESH VEGETABLES-REMAINING', minutes=8, basis='serv4', why='other fresh vegetable, kind not recognized: wash, cut, cook briefly', rules=[
        # ready to eat
        (dict(product=r'DIP|/DRS|/DR\b|/DEL\b|TRAY|TRAX|VEGGIE/|/CHEESE'), 0, 'cut vegetable tray with dip: eaten as sold'),
        (dict(descr=r'/DIP|&DIP|DIP&|/DR\b|VEG TRY'), 0, 'cut vegetable tray with dip: eaten as sold'),
        (dict(descr=r'\bCKD\b'), 0, 'cooked beets or edamame: eaten as sold'),
        (dict(product=r'PICO DE GALLO|KIM ?CHI|KRAUT|BRUSCHETTA|SALSA|GUACAMOLE|ANTIPASTI'), 0, 'pico de gallo, kimchi, sauerkraut: eaten as sold'),
        # microwave steam bags
        (dict(descr=r'\bMW\b'), 5, 'cut vegetables in a microwave steam bag: 3-5 min'),
        # brussels sprouts before the sprout and salad rules
        (dict(product=r'BRUSSEL'), 20, 'brussels sprouts: trim, halve, roast or steam 15-20 min'),
        # salad greens
        (dict(product=r'BEAN SPR'), 5, 'bean sprouts: rinse, stir-fry or blanch 2-3 min'),
        (dict(product=r'^SPINACH$'), 3, 'bagged spinach leaves: eaten raw or wilted in a pan'),
        (dict(product=r'SALAD|SLAW|ARUGULA|ROMAINE|CRESS|ENDIVE|RADICCHIO|LETTUCE|POWER|SUPER GREENS|SUPER SPINACH|JUICING|WHEAT GRASS|SPROUT|SPRS|PEA SHOOT|SPRING|SPINACH|^GREENS VEG MIX|MIXED GREENS|ZEN BLEND|KALE MIX|MIX KALE|KALE MEDLEY'), 0, 'washed salad greens and baby greens: eaten as sold'),
        # cooking greens
        (dict(product=r'COLLARD|TURNIP GREENS|MUSTARD|SOUTHERN|COUNTRY GREENS|SOUP GREENS|DANDELION|^GREENS'), 45, 'collard, turnip or mustard greens: wash, simmer 40-45 min'),
        (dict(product=r'KALE GREENS'), 25, 'cut kale greens: simmer 20-25 min'),
        (dict(product=r'KALE|CHARD|BOK ?CHOY|RABE|RAPINI|ESCAROLE'), 10, 'kale, chard, bok choy: wash, chop, saute 5-8 min'),
        # broccoli family, asparagus
        (dict(product=r'ASPARAGUS|ASPARATION|BROCCO|^BRC|\bCFL\b|CAULIFLOWER|FIESTAFLOWER'), 12, 'broccoli, cauliflower, asparagus: trim, steam or boil 8-10 min'),
        # peas and beans
        (dict(product=r'SNAP|SNOW|\bSNO\b|\bSSP\b'), 3, 'sugar snap or snow peas: eaten raw or sauteed 3-5 min'),
        (dict(product=r'BLACK-?EYED|BLACKEYED|CROWDER|PURPLE HULL|FIELD PEA|LIMA|BUTTER ?BEAN|PINTO|BLACK BEAN|LENTIL'), 30, 'fresh shelled field peas or beans: simmer 25-30 min'),
        (dict(product=r'EDAMAME|SOYBEAN'), 8, 'edamame: boil 5 min'),
        (dict(product=r'BEAN'), 15, 'green beans: trim, boil or saute 10-12 min'),
        (dict(product=r'\bPEAS?\b'), 8, 'shelled green peas: boil 5 min'),
        (dict(product=r'\bCORN'), 15, 'corn on the cob: husk, boil 8-10 min'),
        (dict(product=r'CUCUMBER'), 2, 'cucumbers: wash, slice'),
        # cut mixes with a use
        (dict(product=r'MIREPOIX|TRINITY|STUFFING|CLY ?& ?ON|CLY & WHITE ON'), 0, 'diced onion, celery, pepper for flavoring: no preparation'),
        (dict(product=r'FAJITA|STIR FRY|CHOP SUEY|ASIAN|SAUTE'), 7, 'cut vegetables for stir fry or fajitas: saute 5-7 min'),
        (dict(product=r'SOUP|STEW|ROAST|KABOB|GRILL'), 25, 'cut vegetables for soup, stew or roasting: cook 20-30 min'),
        (dict(product=r'PEPPER|JALAPENO|CHILE|CHILI|POBLANO|S&PRR'), 3, 'peppers: wash, seed, slice'),
        # squash
        (dict(product=r'SQUASH|ZUCCHINI', descr=r'\bNDL\b|SPR-NDL|VGCNE|SPIRAL'), 5, 'squash noodles: saute 3-5 min'),
        (dict(product=r'SQUASH|ZUCCHINI|BUTTERNUT', descr=r'BTRNT.*(?:\bCT\b|\bCB\b|\bDCD\b|\bDC\b|CHNK|C&W)'), 20, 'cut butternut squash: roast or boil 15-20 min'),
        (dict(descr=r'BTRNT|SPAGHETTI SQ|ACORN'), 30, 'whole winter squash: halve, seed, bake'),
        (dict(product=r'BUTTERNUT|SPAGHETTI SQUASH|ACORN|PUMPKIN|KABOCHA|DELICATA|WINTER'), 30, 'whole winter squash or pumpkin: halve, seed, bake'),
        (dict(product=r'SQUASH|ZUCCHINI|\bY-S\b'), 10, 'zucchini or yellow squash: slice, saute 6-8 min'),
        # roots and other cooked vegetables
        (dict(product=r'YAM|SWEET POTATO'), 30, 'sweet potatoes: peel, cut, boil or bake about 25-30 min'),
        (dict(product=r'POTATO'), 25, 'potatoes: peel, cut, boil 15-20 min'),
        (dict(product=r'ARTICHOKE'), 40, 'artichokes: trim, steam 30-40 min'),
        (dict(product=r'BEET'), 45, 'raw beets: boil 35-45 min, peel'),
        (dict(product=r'PARSNIP|TURNIP|RUTABAGA|KOHLRABI|CELERIAC|CELERY ROOT'), 25, 'parsnips, turnips, rutabaga: peel, cut, boil or roast 20 min'),
        (dict(product=r'CABBAGE', descr=r'SHRD'), 0, 'shredded cabbage for slaw: eaten as sold'),
        (dict(product=r'CABBAGE'), 15, 'cabbage: core, cut, boil or saute 10 min'),
        (dict(product=r'OKRA'), 15, 'okra: slice, fry or stew 10-12 min'),
        (dict(product=r'EGGPLANT'), 25, 'eggplant: slice, fry or bake 20 min'),
        (dict(product=r'LEEK|ANISE|FENNEL'), 5, 'leeks or fennel: wash, slice; cooked in the dish'),
        (dict(product=r'CACTUS|NOPAL'), 10, 'cactus pads: trim, dice, boil or grill'),
        (dict(product=r'JICAMA'), 4, 'jicama: peel, cut into sticks'),
        (dict(product=r'GINGER|TURMERIC|GARLIC|SHALLOT|HORSERADISH|LEMON ?GRASS'), 1, 'ginger, garlic, shallots: peel, mince'),
        (dict(product=r'CARROT|\bCLY\b|CELERY|^TM\b|TOMATO|RADISH'), 0, 'carrot and celery sticks, small tomatoes: eaten as sold'),
        (dict(product=r'VEG|MEDLEY|MIX|BLEND'), 10, 'cut mixed vegetables: steam or boil 8-10 min'),
    ]),
    4475: dict(name='FRESH TOMATOES', minutes=2, basis='serv4', why='slicing tomatoes: wash, core, slice', rules=[
        (dict(type=r'GRAPE|CHERRY|CHERUB|SALAD|BABY|PEARL|SUGAR PLUM|GLORYS|ANGEL SWEET|SANTA|BITE|MEDLEY|SWEET|MINI|TEARDROP|SNACK'), 0, 'grape or cherry tomatoes: eaten as sold'),
        (dict(brand=r'NATURESWEET|SANTA SWEETS|ANGEL SWEET|ZIMA|SUGAR BOMB'), 0, 'small snacking tomatoes: eaten as sold'),
    ]),
    4050: dict(name='FRESH CARROTS', minutes=10, basis='serv4', why='whole carrots: peel, cut; eaten raw or boiled about 10 min', rules=[
        (dict(descr=r'BBY|BABY|B-P|\bMN\b|\bPT\b|PLD|PL-B|MINI|PETITE'), 0, 'baby or peeled mini carrots: eaten as sold'),
        (dict(descr=r'MATCHSTICK|SHREDDED|SHRD|\bSTK|STIX|CHIP|JULIENNE|COIN|CL-CT|\bC-C\b|SLCD|/DIP|/R-D|/F-F|DIPPIN|F-CT|\bCT\b'), 0, 'shredded carrots, sticks or chips: eaten as sold'),
    ]),
    4275: dict(name='FRESH LETTUCE', minutes=5, basis='serv4', why='head lettuce or romaine hearts: core, wash, tear or chop', rules=[
        (dict(product=r'SALAD MIX|BLEND'), 0, 'bagged shredded lettuce or salad mix: eaten as sold'),
        (dict(type=r'BABY LEAF|LEAVES'), 0, 'washed lettuce leaves: eaten as sold'),
    ]),
    4023: dict(name='FRESH MUSHROOMS', minutes=8, basis='serv4', why='mushrooms: wipe, slice, saute 5-6 min'),
    4350: dict(name='FRESH ONIONS', minutes=3, basis='serv4', why='onions: peel, chop; cooked in the dish', rules=[
        (dict(descr=r'\bDCD\b|\bCHP\b|CHPD|\bSL\b|SLCD|DICED|SLICED'), 0, 'diced or sliced onions: no preparation'),
        (dict(descr=r'GREEN|\bGRN\b|SCALLION|BUNCH'), 1, 'green onions: wash, trim, slice'),
    ]),
    4060: dict(name='FRESH CELERY', minutes=2, basis='serv4', why='celery stalk or hearts: wash, trim, cut', rules=[
        (dict(descr=r'\bSTK\b|STICK|SLICED|\bDCD\b|CHOPPED|SNAX|\bCUT\b|DIP'), 0, 'celery sticks or diced celery: no preparation'),
    ]),
    4460: dict(name='FRESH SPINACH', minutes=3, basis='serv4', why='bagged spinach leaves: eaten raw or wilted in a pan about 5 min', rules=[
        (dict(product=r'SALAD'), 0, 'spinach salad mix: eaten as sold'),
        (dict(descr=r'\bBY\b|BABY|\bBBY\b|SLD MX'), 0, 'washed baby spinach: eaten as sold'),
        (dict(descr=r'BUNCH|\bBCH\b|\bB F$'), 8, 'bunch spinach: wash well, trim, saute 3-5 min'),
    ]),
    4055: dict(name='FRESH CAULIFLOWER', minutes=15, basis='serv4', why='cauliflower head: trim, cut into florets, boil or steam 10 min', rules=[
        (dict(descr=r'RICE|\bRCD\b|CRUMBLE|PEARL'), 6, 'riced cauliflower: saute or microwave 5 min'),
        (dict(descr=r'\bMW\b'), 5, 'cauliflower florets in a microwave steam bag: 3-5 min'),
        (dict(descr=r'FLRT|FLORET|\bCUT\b|CHOPPED'), 12, 'cauliflower florets: boil or steam 8-10 min'),
    ]),
    4020: dict(name='FRESH HERBS', minutes=0, basis='serv4', why='fresh herbs: flavoring, the dish carries the time'),
    4140: dict(name='FRESH GARLIC', minutes=1, basis='serv4', why='garlic: peel and mince a few cloves'),
    4415: dict(name='FRESH RADISHES', minutes=2, basis='serv4', why='radishes: wash, trim, slice'),
    4015: dict(name='FRESH SPROUTS', minutes=0, basis='serv4', why='alfalfa, broccoli or clover sprouts: eaten as sold', rules=[
        (dict(descr=r'BEAN|MUNG|SOYBEAN'), 5, 'bean sprouts: rinse, stir-fry or blanch 2-3 min'),
    ]),
    6064: dict(name='RBC FRESH VEGETABLE AND HERB', minutes=8, basis='serv4', why='single fresh vegetable, kind not stated: wash, cut, cook briefly', rules=[
        (dict(descr=r'POTATO|IDHPTT'), 25, 'potatoes: peel, cut, boil 15-20 min'),
        (dict(descr=r'ONION'), 3, 'onions: peel, chop; cooked in the dish'),
        (dict(descr=r'\bMSH'), 8, 'mushrooms: saute 5-6 min'),
        (dict(type=r'BEEFSTEAK|ROMA|BIBB'), 2, 'tomatoes or lettuce: wash, slice'),
    ]),
    1670: dict(name='PERISHABLE FRESH VEGETABLE AND HERB', minutes=2, basis='serv4', why='fresh vegetable, kind not stated: wash, slice', rules=[
        (dict(descr=r'MSHRM'), 8, 'mushrooms: wipe, slice, saute 5-6 min'),
        (dict(descr=r'BLPR|PPR'), 3, 'peppers: wash, seed, slice'),
        (dict(descr=r'\bGRP\b|\bTM\b'), 0, 'grape or snacking tomatoes: eaten as sold'),
    ]),
    6070: dict(name='RBC PRECUT FRESH AND RFRGR VEGETABLE', minutes=5, basis='serv4', why='precut vegetable, kind not stated: eaten raw or cooked briefly'),

    # ------------------------------------------------------------------ dried fruit and nuts
    1425: dict(name='FRUIT-DRIED AND SNACKS', minutes=0, basis='serv4', why='dried fruit and fruit snacks: eaten as sold'),
    1424: dict(name='RAISINS', minutes=0, basis='serv4', why='raisins: eaten as sold'),
    1423: dict(name='PRUNES-DRIED', minutes=0, basis='serv4', why='prunes: eaten as sold'),
    1426: dict(name='DATES', minutes=0, basis='serv4', why='dates: eaten as sold'),
    1508: dict(name='NUTS - BAGS', minutes=0, basis='serv4', why='nuts and seeds: eaten as sold or added to baking', rules=[
        (dict(descr=r'CHSTNT.*\bRW\b|\bRW\b.*CHSTNT'), 25, 'raw chestnuts: score, roast 20-25 min'),
    ]),
    1506: dict(name='NUTS - CANS', minutes=0, basis='serv4', why='nuts: eaten as sold'),
    1507: dict(name='NUTS - JARS', minutes=0, basis='serv4', why='nuts: eaten as sold'),
    1509: dict(name='NUTS - UNSHELLED', minutes=0, basis='serv4', why='nuts in the shell, roasted, raw or boiled: crack and eat'),

    # ------------------------------------------------------------------ pasta
    1331: dict(name='PASTA - MACARONI', minutes=18, basis='serv4', why='dry pasta shapes: water to a boil 8 min, cook 10 min', rules=[
        (dict(variety=r'LASAGN'), 60, 'lasagna noodles: assemble the dish and bake about 60 min'),
        (dict(variety=r'MANICOTTI|CANNELLONI|CANNELONI'), 60, 'manicotti tubes: boil, fill, bake about 60 min'),
        (dict(variety=r'SHELL|CONCHIGLI', form=r'JUMBO|GIANT'), 60, 'jumbo shells: boil, fill, bake about 60 min'),
        (dict(variety=r'^GNOCCHI$|GNOCCHI DUMPLING'), 11, 'shelf-stable potato gnocchi: boil 2-3 min after the water boils'),
        (dict(variety=r'GNOCCHI', type=r'POTATO'), 11, 'shelf-stable potato gnocchi: boil 2-3 min after the water boils'),
        (dict(variety=r'RAMEN'), 5, 'ramen noodle block: boil 3 min'),
    ]),
    1334: dict(name='PASTA-SPAGHETTI', minutes=18, basis='serv4', why='spaghetti, linguine: water to a boil 8 min, cook 10 min', rules=[
        (dict(variety=r'ANGEL|CAPELLINI|CAPPELLINI|CAPELLI|FIDELINI|FEDELINI'), 13, 'angel hair: water to a boil 8 min, cook 4-5 min'),
        (dict(form=r'THIN|FINE'), 15, 'thin spaghetti: water to a boil 8 min, cook 6-7 min'),
        (dict(variety=r'VERMICELLI|SPAGHETTINI|FIDEO'), 15, 'vermicelli, spaghettini, fideo: water to a boil 8 min, cook 6-7 min'),
        (dict(descr=r'\bTHN\b'), 15, 'thin spaghetti: water to a boil 8 min, cook 6-7 min'),
    ]),
    1336: dict(name='PASTA - NOODLES & DUMPLINGS', minutes=15, basis='serv4', why='egg noodles: water to a boil 8 min, cook 7 min', rules=[
        (dict(variety=r'LASAGN'), 60, 'egg lasagna sheets: assemble the dish and bake about 60 min'),
        (dict(variety=r'ANGEL|CAPELLINI'), 13, 'angel hair: water to a boil 8 min, cook 4-5 min'),
        (dict(type=r'F/|FILLED'), 18, 'dry filled tortellini or ravioli: boil 10-11 min after the water boils'),
        (dict(variety=r'TORTELL|RAVIOL|MEZZALUNA'), 18, 'dry filled tortellini or ravioli: boil 10-11 min after the water boils'),
        (dict(variety=r'KLUSKI|POT PIE|BOTT BOI'), 22, 'kluski or pot pie noodles: boil 12-15 min after the water boils'),
        (dict(type=r'HOMEMADE|AMISH|HOMESTYLE'), 22, 'thick homestyle egg noodles: boil 12-15 min after the water boils'),
        (dict(variety=r'SPAETZLE|SPATZLE'), 20, 'dry spaetzle: boil 12 min after the water boils'),
        (dict(variety=r'SPAGHETTI|PENNE|ELBOW|ROTINI|RIGATONI|FARFALLE|SPIRAL|FETTUC|LINGUIN|ZITI|SHELL|ROTELLE|MACARONI|FUSILLI|PAPPARDELLE|PAPARDELLE|TAGLIATELLE'), 18, 'dry pasta shapes: water to a boil 8 min, cook 10 min'),
        (dict(form=r'FINE|THIN'), 12, 'fine egg noodles: water to a boil 8 min, cook 4 min'),
    ]),
    1266: dict(name='ORIENTAL NOODLES', minutes=12, basis='serv4', why='dry Asian wheat, rice or buckwheat noodles: boil 3-5 min after the water boils', rules=[
        (dict(brand=r'LA CHOY'), 0, 'crisp fried chow mein or rice noodles in a can: eaten as sold'),
        (dict(product=r'^C-M|CHOW MEIN', brand=r'WEL-PAC|SUN LUCK|KA-ME|NANKA|ANNIE CHUN|WAN HUA|TAKAMORI|LAM\'S|TYTON|NEW HONG KONG'), 11, 'chow mein noodles to cook: boil 3 min after the water boils'),
        (dict(product=r'^C-M (?:STICK |BWF )?NOODLE$|^CHOW MEIN NOODLE$'), 0, 'crisp fried chow mein noodles: eaten as sold as a topping'),
        (dict(product=r'SHIRATAKI|SHRTK|SHRK|KONJAC|KELP'), 4, 'shirataki noodles in water: drain, rinse, heat 2-3 min'),
        (dict(brand=r'MIRACLE NOODLE|PASTA ZERO'), 4, 'shirataki noodles in water: drain, rinse, heat 2-3 min'),
    ]),

    # ------------------------------------------------------------------ rice, dry beans, grains
    1319: dict(name='RICE - PACKAGED AND BULK', minutes=22, basis='serv4', why='white rice: bring to a boil, simmer 15-18 min', rules=[
        (dict(type=r'PARBOILED', product=r'BROWN|WGB'), 35, 'parboiled brown rice: simmer 30 min'),
        (dict(descr=r'\bPB\b', product=r'BROWN|WGB'), 35, 'parboiled brown rice: simmer 30 min'),
        (dict(product=r'^TXMT WHITE BROWN WILD'), 22, 'quick-cooking blend of white, brown and wild rice: simmer 15 min'),
        (dict(product=r'WILD'), 50, 'wild rice or a blend with wild rice: simmer 45-50 min'),
        (dict(product=r'BROWN|WGB|BLACK|\bRED\b|FORBIDDEN|WEHANI|PURPLE|CBWSBHR|IBBR|WHOLE GRAIN|GEN-JI'), 45, 'brown, black or red rice: simmer 40-45 min'),
        (dict(product=r'ARBORIO|RISOTTO|CARNAROLI'), 30, 'risotto rice: stir with broth 25-30 min'),
        (dict(type=r'PARBOILED'), 25, 'parboiled white rice: simmer 20-25 min'),
        (dict(descr=r'\bPB\b'), 25, 'parboiled white rice: simmer 20-25 min'),
    ]),
    1321: dict(name='RICE - INSTANT', minutes=7, basis='serv4', why='instant white rice: boil water, stir in, stand 5 min', rules=[
        (dict(type=r'BOIL IN BAG'), 12, 'boil-in-bag rice: boil 10 min'),
        (dict(descr=r'\bBB\b'), 12, 'boil-in-bag rice: boil 10 min'),
        (dict(product=r'BROWN|WG-BR|WILD'), 12, 'instant brown rice: simmer 5-10 min, stand 5 min'),
    ]),
    1315: dict(name='BEANS-DRY', minutes=90, basis='serv4', why='dry beans: simmer about 90 min, soaking not counted', rules=[
        (dict(descr=r'LENTIL|SPLIT|\bDAL\b|MASOOR|MOONG|MUNG|TOOR|URAD'), 35, 'lentils, split dal, mung beans: simmer 30-35 min'),
        (dict(descr=r'LM-B|LIMA|BLKEYE|BLACKEYE'), 60, 'dry lima beans or black-eyed peas: simmer 45-60 min'),
    ]),
    1316: dict(name='PEAS & LENTILS & CORN - DRY', minutes=35, basis='serv4', why='lentils and split peas: simmer 30-35 min', rules=[
        (dict(descr=r'CORN|HOMINY|POSOLE'), 90, 'dry corn or hominy: simmer 90 min or more'),
        (dict(descr=r'CHICK|CHANA|GARBANZO|\bBNS\b|YLWEYE'), 90, 'dry chickpeas or beans: simmer about 90 min'),
        (dict(descr=r'BLKEYE|BLACK-?EYE|\bCOW\b|FIELD|PIGEON|CROWDER|PURPLE HULL|\bLADY\b|CREAM|RED PEAS'), 60, 'dry black-eyed, field or pigeon peas: simmer 45-60 min'),
        (dict(descr=r'PEAS(?: SMALL)? WHL', descr_not=r'SPLT|SPLIT'), 60, 'whole dry green or yellow peas: simmer 45-60 min'),
    ]),
    1439: dict(name='TAPIOCA - PURE', minutes=12, basis='serv4', why='quick-cooking tapioca: stand 5 min, cook into pudding 6-7 min', rules=[
        (dict(descr=r'\bPRL\b|PEARL|BEAD'), 30, 'pearl tapioca: simmer 20-30 min, soaking not counted'),
    ]),
    1317: dict(name='BARLEY-DRY', minutes=45, basis='serv4', why='pearl barley: simmer 40-45 min', rules=[
        (dict(descr=r'QUICK|\bQK\b|\bINS\b'), 15, 'quick barley: simmer 10-12 min'),
    ]),
    7803: dict(name='GRAIN - REMAINING', minutes=25, basis='serv4', why='other whole grain, kind not recognized: simmer about 20-25 min', rules=[
        (dict(descr=r'PA-CS|PEARL COUSCOUS|ISRAELI'), 12, 'pearl couscous: simmer 8-10 min'),
        (dict(descr=r'COUSCOUS'), 7, 'couscous: boil water, stir in, stand 5 min'),
        (dict(descr=r'BRAN'), 0, 'wheat bran: added to baking or cereal'),
        (dict(descr=r'BUCKWHEAT|BKWT|KASHA|BULGUR|BULGAR|CRACKED'), 15, 'buckwheat groats or bulgur: simmer 10-12 min'),
        (dict(descr=r'BERR|KAMUT|SPELT|SORGHUM|EINKORN|KHORASAN|\bRYE\b|EMMER|\bHWS\b|\bHRS\b|\bWB\b|WWB|WHL|WHT HARD|WHT SPRING'), 60, 'wheat berries, kamut, spelt, sorghum: simmer about 60 min'),
    ]),
}
