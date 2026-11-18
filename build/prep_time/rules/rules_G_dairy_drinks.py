RULES = {
    # ---- butter and margarine
    3611: dict(name='BUTTER', minutes=0, basis='serv4', why='butter: spread or cooking fat, the dish carries the time'),
    3608: dict(name='MARGARINE AND SPREADS', minutes=0, basis='serv4', why='margarine and spreads: spread or cooking fat'),

    # ---- carbonated beverages
    1484: dict(name='SOFT DRINKS - CARBONATED', minutes=0, basis='serv4', why='soda and sparkling water: drunk as sold'),
    1553: dict(name='SOFT DRINKS - LOW CALORIE', minutes=0, basis='serv4', why='diet soda and sparkling water: drunk as sold'),
    7743: dict(name='FOUNTAIN BEVERAGE SYRUP', minutes=0, basis='serv4', why='soda syrup for a fountain machine: the machine mixes it'),

    # ---- cheese
    3590: dict(name='CHEESE - SHREDDED', minutes=0, basis='serv4', why='shredded cheese: sprinkled on a dish'),
    3550: dict(name='CHEESE - NATURAL - AMERICAN CHEDDAR', minutes=0, basis='serv4', why='cheddar in chunks, slices or sticks: eaten as sold'),
    3586: dict(name='CHEESE-PROCESSED SLICES-AMERICAN', minutes=0, basis='serv4', why='American cheese slices: eaten as sold'),
    3606: dict(name='CHEESE - PROCESSED - CREAM CHEESE', minutes=0, basis='serv4', why='cream cheese: spread'),
    3588: dict(name='CHEESE - SPECIALTY/IMPORTED', minutes=0, basis='serv4', why='feta, provolone, blue, brie, gouda and similar: eaten as sold', rules=[
        (dict(descr=r'FREIR|HALLOUMI|HALOUMI|PANEER|SAGANAKI|\bFONDUE|RACLETTE'), 6,
         'frying cheese, paneer, fondue or raclette: sliced and pan-fried or melted'),
    ]),
    3546: dict(name='CHEESE - NATURAL - MOZZARELLA', minutes=0, basis='serv4', why='string cheese and mozzarella chunks or slices: eaten as sold'),
    3548: dict(name='CHEESE - NATURAL - REMAINING', minutes=0, basis='serv4', why='jack, colby jack and other cheese: eaten as sold'),
    3553: dict(name='CHEESE - GRATED', minutes=0, basis='serv4', why='grated parmesan and romano: sprinkled on a dish'),
    3587: dict(name='CHEESE-NATURAL-SWISS', minutes=0, basis='serv4', why='Swiss cheese slices and chunks: eaten as sold'),
    3556: dict(name='CHEESE - PROCESSED - SNACK', minutes=0, basis='serv4', why='cheese spreads, wedges, spray cheese, cheese balls: eaten as sold'),
    3555: dict(name='CHEESE - PROCESSED - LOAVES', minutes=0, basis='serv4', why='processed cheese loaf: sliced or melted into a dish that carries the time'),
    3554: dict(name='CHEESE - PROCESSED SLICES - REMAINING', minutes=0, basis='serv4', why='processed cheese slices: eaten as sold'),
    3549: dict(name='CHEESE - NATURAL - AMERICAN COLBY', minutes=0, basis='serv4', why='colby cheese: eaten as sold'),
    3545: dict(name='CHEESE-NATURAL-MUENSTER', minutes=0, basis='serv4', why='muenster cheese: eaten as sold'),
    3589: dict(name='CHEESE - NATURAL - VARIETY PACK', minutes=0, basis='serv4', why='packs of assorted cheese slices: eaten as sold'),
    3547: dict(name='CHEESE - NATURAL - BRICK', minutes=0, basis='serv4', why='brick cheese: eaten as sold'),

    # ---- cottage cheese, sour cream, toppings
    3605: dict(name='CHEESE - COTTAGE', minutes=0, basis='serv4', why='cottage cheese: eaten as sold'),
    3604: dict(name='DAIRY - SOUR CREAM - REFRIGERATED & CANNED', minutes=0, basis='serv4', why='sour cream: spooned on a dish'),
    3591: dict(name='WHIPPING CREAM', minutes=0, basis='serv4', why='cream: poured into a recipe or whipped as a topping'),
    3594: dict(name='TOPPINGS - REFRIGERATED', minutes=0, basis='serv4', why='whipped cream and whipped topping: sprayed or spooned on'),
    3552: dict(name='CHEESE - RICOTTA', minutes=0, basis='serv4', why='ricotta: spooned into a dish that carries the time'),
    3551: dict(name='CHEESE - FARMERS', minutes=0, basis='serv4', why='farmers cheese: eaten as sold'),
    3557: dict(name='DAIRY - POTATO TOPPING - REFRIGERATED', minutes=0, basis='serv4', why='sour cream and chive topping: spooned on'),

    # ---- juice and drinks, canned and bottled
    1042: dict(name='FRUIT DRINKS-OTHER CONTAINER', minutes=0, basis='serv4', why='fruit drinks, sports drinks, drink pouches: drunk as sold'),
    1040: dict(name='FRUIT JUICE - ORANGE - OTHER CONTAINER', minutes=0, basis='serv4', why='orange juice: drunk as sold'),
    1044: dict(name='FRUIT JUICE-REMAINING', minutes=0, basis='serv4', why='fruit juice and juice blends: drunk as sold'),
    1030: dict(name='FRUIT DRINKS & JUICES-CRANBERRY', minutes=0, basis='serv4', why='cranberry juice and juice drinks: drunk as sold'),
    1055: dict(name='VEGETABLE JUICE AND DRINK REMAINING', minutes=0, basis='serv4', why='vegetable juice and juice blends: drunk as sold'),
    1033: dict(name='FRUIT JUICE - APPLE', minutes=0, basis='serv4', why='apple juice: drunk as sold'),
    1034: dict(name='FRUIT JUICE - GRAPE', minutes=0, basis='serv4', why='grape juice: drunk as sold'),
    1036: dict(name='FRUIT JUICE - LEMON/LIME', minutes=0, basis='serv4', why='bottled lemon and lime juice: squeezed into a drink or dish'),
    1041: dict(name='FRUIT DRINKS-CANNED', minutes=0, basis='serv4', why='canned fruit drinks: drunk as sold'),
    1054: dict(name='VEGETABLE JUICE - TOMATO', minutes=0, basis='serv4', why='tomato juice: drunk as sold'),
    1032: dict(name='FRUIT JUICE - GRAPEFRUIT - OTHER CONTAINERS', minutes=0, basis='serv4', why='grapefruit juice: drunk as sold'),
    1039: dict(name='FRUIT JUICE-PRUNE', minutes=0, basis='serv4', why='prune juice: drunk as sold'),
    1038: dict(name='FRUIT JUICE - PINEAPPLE', minutes=0, basis='serv4', why='pineapple juice: drunk as sold'),
    1045: dict(name='FRUIT JUICE-NECTARS', minutes=0, basis='serv4', why='fruit nectars: drunk as sold'),
    1051: dict(name='CLAM JUICE', minutes=0, basis='serv4', why='clam juice: poured into a dish that carries the time'),
    1035: dict(name='FRUIT JUICE-GRAPEFRUIT-CANNED', minutes=0, basis='serv4', why='canned grapefruit juice: drunk as sold'),
    1037: dict(name='FRUIT JUICE-ORANGE-CANNED', minutes=0, basis='serv4', why='canned orange juice: drunk as sold'),

    # ---- milk
    3625: dict(name='DAIRY-MILK-REFRIGERATED', minutes=0, basis='serv4', why='milk: drunk as sold'),
    3627: dict(name='DAIRY-CREAM-REFRIGERATED', minutes=0, basis='serv4', why='half and half, cream, creamer: poured into coffee'),
    3628: dict(name='REMAINING DRINKS & SHAKES-REFRIGERATED', minutes=0, basis='serv4', why='soy and almond milk, bottled shakes: drunk as sold'),
    3592: dict(name='DAIRY-FLAVORED MILK-REFRIGERATED', minutes=0, basis='serv4', why='chocolate and other flavored milk: drunk as sold'),
    3650: dict(name='EGGNOG - FRESH & CANNED', minutes=0, basis='serv4', why='eggnog: drunk as sold'),
    3626: dict(name='DAIRY-BUTTERMILK-REFRIGERATED', minutes=0, basis='serv4', why='buttermilk: drunk or poured into a recipe'),
    6041: dict(name='RBC MILK AND MILK PRODUCT DRINK', minutes=0, basis='serv4', why='milk: drunk as sold'),

    # ---- packaged milk and modifiers
    3629: dict(name='CREAMERS-LIQUID', minutes=0, basis='serv4', why='liquid coffee creamer: poured into coffee'),
    1442: dict(name='MILK/WATER ADDITIVES-SWEETENED', minutes=2, basis='serv4',
               why='hot cocoa or hot cider mix: water or milk heated, powder stirred in', rules=[
        (dict(form='BEAD IN STRAW'), 0, 'flavor straw: milk is sipped through it'),
        (dict(form='TABLET'), 8, 'Mexican chocolate tablet: dissolved in milk heated on the stove'),
        (dict(product='SMOOTHIE|SHAKE'), 3, 'smoothie or shake mix: blended with ice and milk or fruit'),
        (dict(product=r'^DRINK|^MILK DRINK|MALTED|BEVERAGE MIX|RICE DRINK|OAT BEVERAGE|EGG NOG', descr_not=r'\bH (?:CC|CH)\b'), 1,
         'chocolate, strawberry or malt powder: stirred into cold milk'),
    ]),
    1468: dict(name='CREAMERS - POWDERED', minutes=0, basis='serv4', why='powdered coffee creamer: spooned into coffee'),
    1297: dict(name='MILK - CANNED', minutes=0, basis='serv4', why='evaporated or condensed milk: poured from the can into coffee or a recipe'),
    1296: dict(name='MILK-SHELF STABLE', minutes=0, basis='serv4', why='shelf-stable milk boxes and half and half cups: drunk as sold'),
    1298: dict(name='MILK - POWDERED', minutes=1, basis='serv4', why='powdered milk: stirred into water', rules=[
        (dict(type='BUTTER ?MILK'), 0, 'buttermilk powder: added dry to a baking recipe'),
    ]),

    # ---- dairy snacks, spreads, dips
    3583: dict(name='SPREADS-REMAINING', minutes=0, basis='serv4', why='hummus, hazelnut spread, guacamole, cheese spreads: eaten as sold'),
    3602: dict(name='DAIRY - DIP - REFRIGERATED & FROZEN', minutes=0, basis='serv4', why='refrigerated dips: eaten as sold', rules=[
        (dict(descr=r'\bF(?: \d+P)?$', flavor='SPINACH|ART|QUESO|CHILI|CHEESE|CHEDDAR|CRAB|BUFFALO|CHICKEN|SHRIMP|POTATO|SSBB'), 4,
         'frozen hot dip: tray microwaved 3-4 min', 'pkg'),
    ]),
    3579: dict(name='SEAFOOD-REFRIGERATED', minutes=0, basis='serv4',
               why='imitation crab, smoked salmon, pickled herring, cooked crab meat: eaten as sold', rules=[
        (dict(product='OYSTER', type_not='SMOK'), 10, 'raw shucked oysters: breaded and fried, or stewed'),
        (dict(descr=r'\bOYST\b', type_not='SMOK'), 10, 'raw shucked oysters: breaded and fried, or stewed'),
        (dict(product='CLAM|MUSSEL', type_not='SMOK|OREGANATA|PTZ'), 10, 'live clams or mussels: scrubbed and steamed'),
        (dict(product='SCALLOP', type_not='SMOK|IMIT|BREADED'), 8, 'raw scallops: seared in a pan'),
        (dict(product='SHRIMP|PRAWN', type=r'RAW|HEADLESS|SHELL ON|H-R-SO|TIGER|\bP-D\b', type_not=r'COOK|C-P|\bCKD\b'), 8,
         'raw shrimp: boiled or sauteed'),
        (dict(product=r'^COD$|^POLLOCK$|BACALAO', type_not='IMIT|IMITAION|SURIMI|FCD|PTZ|SMOK|COOK'), 20,
         'cod and pollock, nearly all salted here: soaking not counted, simmered for a dish'),
        (dict(product='MACKEREL', type='SALT|BRINE'), 20, 'salt mackerel: soaking not counted, simmered or broiled'),
        (dict(product=r'BLACK COD|TILAPIA|WALLEYE|FLOUNDER|CATFISH|HADDOCK|GROUPER|SALMON|STEELHEAD|WHITING|PERCH|\bSOLE\b|SWAI|SNAPPER|MAHI|HALIBUT|SWORDFISH|\bBASS\b|TROUT|MACKEREL|TUNA',
              form='FILLET|PORTION|LOIN|STEAK', descr_not=r'\bSMK\b|SMOK',
              type_not=r'SMOK|SMK|IMIT|SURIMI|NOVA|LOX|CURED|PICKL|COOK|KIPPER|ROAST|GRAVLAX|GRAVAD|PASTRAMI|FCD|PTZ|WINE|CREAM|JERKY|CANDIED|NC&CS'), 15,
         'raw fish fillets: pan-fried or baked'),
    ]),
    3570: dict(name='GARLIC SPREADS-REFRIGERATED', minutes=0, basis='serv4', why='garlic spread: spread on bread'),

    # ---- soft drinks, non-carbonated
    1050: dict(name='SOFT DRINKS - POWDERED', minutes=1, basis='serv4', why='powdered drink mix: stirred into water'),
    1049: dict(name='REMAINING DRINKS & SHAKES-NON REFRIGERATED', minutes=0, basis='serv4', why='shelf-stable soy, almond and chocolate drinks: drunk as sold'),
    1046: dict(name='FRUIT PUNCH BASES & SYRUPS', minutes=0, basis='serv4',
               why='liquid water flavoring or juice concentrate: squirted or poured into a glass of water', rules=[
        (dict(product='SMOOTHIE|^BASE|^MIX'), 3, 'smoothie mix or base: blended with ice'),
        (dict(container=r'^CAN$|ASEPTIC'), 2, 'canned juice concentrate: stirred with three cans of water in a pitcher', 'pkg'),
    ]),
    1052: dict(name='ICE POPS - UNFROZEN', minutes=0, basis='serv4', why='freezer pops sold liquid: frozen at home, freezing not counted'),
    1048: dict(name='BREAKFAST DRINKS - POWDERED', minutes=1, basis='serv4', why='powdered orange breakfast drink: stirred into water'),

    # ---- yogurt
    3603: dict(name='YOGURT-REFRIGERATED', minutes=0, basis='serv4', why='yogurt: eaten as sold'),
    3612: dict(name='YOGURT-REFRIGERATED-SHAKES & DRINKS', minutes=0, basis='serv4', why='yogurt drinks and kefir: drunk as sold'),
}
