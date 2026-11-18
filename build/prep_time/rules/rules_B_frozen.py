# Cluster B_frozen: frozen prepared foods, pizza and snacks, breakfast foods, baked goods, desserts, juices, vegetables.
# Package size is not a rule field, so single-serve and family-size packages are told apart by brand line, product
# and description words only.

# Brands whose frozen meals are single-serve trays or bowls heated in the microwave.
SS = (r"LEAN CUISINE|HEALTHY CHOICE|SMART ONES|SMART MADE|MICHELINA|WEIGHT WATCHERS|KASHI|AMY'S|SAFFRON ROAD|TANDOOR CHEF"
      r"|ETHNIC GOURMET|EVOL|FIT KITCHEN|ATKINS|BOSTON MARKET|DEVOUR|SIGNATURE CLASSICS|SIMPLE DISHES|CLASSIC DISHES"
      r"|SATISFYING SERVINGS|CORNER BISTRO|STOUFFER'S RESTAURANT CLASSICS|MAC CUPS|KID CUISINE|HUNGRY-MAN|SWANSON"
      r"|BUDGET GOURMET|TAI PEI|LIFE CUISINE|SOUTH BEACH|EATING RIGHT|CEDARLANE|PATIO|ZATARAIN|HAWAIIAN STYLE|STEAMFRESH"
      r"|GREEN GIANT|CLAIM JUMPER|MARIE CALLENDER'S PASTA|MARIE CALLNDR'S FRSH|MARIE CALLENDER'S DELIGHTS")
# Brands of bagged or boxed meals for two to four, cooked in a skillet.
SKILLET = (r"VOILA|SKILLET|SAUTES|P\.F\. CHANG|WANCHAI|CONTESSA|GOURMET DINING|TASTYWOK|MACARONI GRILL|^BERTOLLI$|BUITONI"
           r"|^BIRDS EYE$|STIR FRESH|CREATE A MEAL|NEWMAN'S OWN|PREGO|TIGER THAI")
# Brands of family-size trays baked in the oven.
FAMILY = r"ON-COR|FREEZER QUEEN|COMFORT B|OVEN BAKE|HOMESTYLE SELECTS|PAPA CHARLIE|RESTAURANT CLASSICS BY|STOUFFER'S SIGNATURE$|RED BARON PAN"
# Stouffer's size words in the description: FS family size, LFS large family size, PS party size.
FS = r"(?:^| )L?FS(?: |$)"
PS = r"(?:^| )PS(?: |$)"

STEAM = r"\bSFR\b|\bVFS\b|\bMW\b|\bSTM|\bSTMR\b"


def veg(name, minutes=7, why='frozen vegetables: simmer or microwave 5-7 min', extra=(), after=()):
    """Plain frozen vegetables: 7 min on the stove, 5 min when the bag is made to be steamed in the microwave."""
    steam = 'steam-in-bag vegetables: microwave 4-5 min'
    return dict(name=name, minutes=minutes, basis='serv4', why=why, rules=list(extra) + [
        (dict(descr=STEAM), 5, steam),
        (dict(brand='STEAM'), 5, steam),
        (dict(brand='BIRDS EYE', descr=r'\bSF\b'), 5, steam),
        (dict(brand='GREEN GIANT', descr=r'\bSS\b'), 5, steam),
    ] + list(after))


def juice(name):
    return dict(name=name, minutes=2, basis='pkg', why='frozen juice concentrate: empty the can, add water, stir')


RULES = {
    # ---------------------------------------------------------------- baked goods
    2660: dict(name='BAKERY - PIES - FROZEN', minutes=60, basis='pkg', why='unbaked fruit or pumpkin pie: oven 60-70 min', rules=[
        (dict(brand='EDWARDS'), 0, 'cream or pecan pie sold baked: thawed and served', 'serv4'),
        (dict(flavor=r'CREAM|CREME|KEY LIME|MERINGUE|SATIN|SILK|MOUSSE|TURTLE|CHEESE|COOKIES|PEANUT BUTTER|P-B|CHOCOLATE|\bCH\b|\bCHC\b|FUDGE|HERSHEY|REESE|OREO|ICE BOX|BANANA|COCONUT|LEMON|LIME'), 0,
         'cream, chocolate or citrus pie: thawed and served', 'serv4'),
        (dict(flavor='PECAN'), 0, 'pecan pie sold baked: thawed and served', 'serv4'),
    ]),
    2654: dict(name='BAKERY-BREAD-FROZEN', minutes=15, basis='serv4', why='garlic toast or bread sticks: oven heats, bakes 5-8 min', rules=[
        (dict(type='NAAN|PITA|FLAT|KULCHA|TORTILLA|CHAPATI|ROTI|PARATHA'), 2, 'flat bread: warmed'),
        (dict(type='MONKEY|PULL APART'), 35, 'pull-apart bread: oven heats, bakes 25-30 min', 'pkg'),
        (dict(product='^BREAD$', type_not='TOAST'), 22, 'garlic bread loaf: oven heats, bakes 10-14 min', 'pkg'),
    ]),
    2651: dict(name='BAKERY - BISCUITS/ROLLS/MUFFINS - FROZEN', minutes=18, basis='serv4', why='baked rolls: oven heats, warms 8-10 min', rules=[
        (dict(product='ENGLISH MUFFIN'), 2, 'english muffin: toasted'),
        (dict(product='MUFFIN (?:BATTER|DOUGH)'), 35, 'muffin batter: oven heats, bakes 25 min', 'pkg'),
        (dict(product='MUFFIN'), 1, 'baked muffin: thawed or microwaved 30 seconds'),
        (dict(product='BISCUIT DOUGH'), 30, 'frozen biscuit dough: oven heats, bakes 20-25 min'),
        (dict(product='DOUGH'), 25, 'roll dough: bakes 15-20 min after rising'),
        (dict(product='^BISCUIT'), 18, 'baked biscuits: oven heats, warms 8-10 min'),
        (dict(type='KNOT'), 15, 'garlic knots: oven heats, bakes 6-8 min'),
    ]),
    2661: dict(name='BAKERY - REMAINING - FROZEN', minutes=30, basis='pkg', why='pastry to bake: oven heats, bakes 20-25 min', rules=[
        (dict(product='CREAM PUFF|ECLAIR|PROFITEROLE|^PUFF$|MACARON|BAKLAVA|CANNOLI|TIRAMISU|PETIT'), 0, 'cream puffs and eclairs: thawed and served', 'serv4'),
        (dict(product='FILLO SHELL|PHYLLO SHELL'), 0, 'baked fillo shells: filled and served', 'serv4'),
        (dict(product='PIE CRUST|PIE SHELL|PASTRY SHELL|TART SHELL'), 45, 'pie crust: filled and baked'),
        (dict(product='FILLO|PHYLLO'), 45, 'fillo sheets: layered with a filling and baked'),
        (dict(product='BLINTZ|CREPE'), 12, 'blintzes: pan 10-12 min', 'serv4'),
        (dict(product='CHURRO'), 15, 'churros: oven heats, bakes 6-8 min', 'serv4'),
        (dict(product='^BUN$|PLANTAIN LEAF|BANANA LEAF'), 0, 'buns or wrapping leaves: used as sold', 'serv4'),
    ]),
    2656: dict(name='BAKERY - DESSERT CAKES - FROZEN', minutes=0, why='cake: thawed and served', rules=[
        (dict(type='BATTER'), 35, 'brownie batter in its pan: oven heats, bakes 25-30 min', 'pkg'),
    ]),
    2649: dict(name='BAKERY-CHEESECAKE-FROZEN', minutes=0, why='cheesecake: thawed and served'),
    2646: dict(name='DOUGH PRODUCTS-BREAD-FROZEN', minutes=35, basis='pkg', why='one loaf of bread dough: oven heats, bakes 25 min after rising', rules=[
        (dict(descr=r"\b5'S"), 175, 'five loaves of bread dough, baked one at a time, 35 min each'),
        (dict(descr=r"\b4'S"), 140, 'four loaves of bread dough, baked one at a time, 35 min each'),
        (dict(descr=r"\b3'S"), 105, 'three loaves of bread dough, baked one at a time, 35 min each'),
        (dict(descr=r"\b2'S"), 70, 'two loaves of bread dough, baked one at a time, 35 min each'),
        (dict(type='LEFSE|CHAPATI|TORTILLA'), 2, 'flat bread: warmed', 'serv4'),
    ]),
    2647: dict(name='BAKERY-BAGELS-FROZEN', minutes=2, why='bagel: toasted'),
    2652: dict(name='BAKERY-BREAKFAST CAKES & SWEET ROLLS-FROZEN', minutes=30, basis='pkg', why='sweet roll dough: oven heats, bakes 20 min', rules=[
        (dict(product='COFFEE CAKE|DANISH|BAKLAVA|HONEY BUN|CRUMB'), 0, 'coffee cake or danish: thawed and served', 'serv4'),
        (dict(product='DOUGH'), 30, 'sweet roll dough: oven heats, bakes 20 min'),
        (dict(product='ROLL|BUN|CROISSANT|PULL APART'), 18, 'baked sweet rolls: oven heats, warms 8-10 min'),
    ]),
    2653: dict(name='BAKERY - COBBLER/DUMPLINGS/STRUDEL - FROZEN', minutes=60, basis='pkg', why='fruit cobbler: oven 55-65 min', rules=[
        (dict(product='STRUDEL'), 40, 'strudel: oven heats, bakes 30 min'),
        (dict(product='WRAPPER'), 0, 'dumpling wrappers: the filling carries the time', 'serv4'),
        (dict(product='DUMPLING'), 50, 'fruit dumplings: oven heats, bakes 40 min'),
    ]),
    2655: dict(name='BAKERY-COOKIES RTE/COOKIE DOUGH-FROZEN', minutes=22, basis='pkg', why='cookie dough: oven heats, bakes 12 min', rules=[
        (dict(brand="BEN & JERRY"), 0, 'cookie dough chunks: eaten frozen', 'serv4'),
        (dict(product='RTE|^COOKIE$|SNACK|DELIGHT'), 0, 'baked cookies or dough snacks: eaten as sold', 'serv4'),
    ]),
    2648: dict(name='BAKERY - DOUGHNUTS - FROZEN', minutes=0, why='doughnuts: thawed and served'),

    # ---------------------------------------------------------------- breakfast
    2629: dict(name='FROZEN/REFRIGERATED BREAKFASTS', minutes=3, basis='serv4', why='breakfast sandwich, bowl or burrito: microwave 2-3 min', rules=[
        (dict(product='SUBSTITUTE|ALTERNATIVE|SCR EGG MIX|EGG REPLACER|FRENCH TOAST MIX'), 6, 'liquid eggs: scrambled in a pan'),
        (dict(product='CRUSTLESS'), 3, 'crustless quiche cup: microwave'),
        (dict(product=r'QUICHE|\bQ-L\b|\bQ-F\b'), 30, 'quiche: oven heats, bakes 15-25 min', 'pkg'),
        (dict(descr=r'QUICHE|\bQCH\b'), 30, 'quiche: oven heats, bakes 15-25 min', 'pkg'),
    ]),
    2659: dict(name='FROZEN WAFFLES & PANCAKES & FRENCH TOAST', minutes=2, basis='serv4', why='frozen waffles: toasted', rules=[
        (dict(product='BATTER'), 12, 'pancake batter: poured and cooked on a griddle'),
        (dict(product='PANCAKE|FRENCH TOAST|BLINI|CAKES|CREPE'), 3, 'frozen pancakes or french toast: microwave'),
    ]),

    # ---------------------------------------------------------------- desserts, fruit, toppings, ice cream
    2664: dict(name='FROZEN FRUITS', minutes=0, why='frozen fruit: thawed or used frozen'),
    2665: dict(name='FROZEN DESSERTS', minutes=0, why='ice cream desserts: eaten as sold'),
    2678: dict(name='TOPPINGS-WHIPPED-FROZEN', minutes=0, why='whipped topping'),
    2677: dict(name='FROZEN CREAM SUBSTITUTES', minutes=0, why='coffee creamer'),
    2672: dict(name='ICE CREAM - BULK', minutes=0, why='ice cream: scooped'),
    2675: dict(name='FROZEN NOVELTIES', minutes=0, why='ice cream bars and pops: eaten as sold'),
    2671: dict(name='YOGURT - FROZEN', minutes=0, why='frozen yogurt: eaten as sold'),
    2673: dict(name='ICE MILK AND SHERBET', minutes=0, why='sherbet: scooped'),

    # ---------------------------------------------------------------- juices and drinks
    2667: juice('FRUIT JUICE - ORANGE - FROZEN'),
    2670: dict(name='FRUIT DRINKS & MIXES - FROZEN', minutes=2, basis='pkg', why='frozen drink concentrate or smoothie mix: add liquid, stir or blend'),
    2674: juice('FRUIT JUICE - REMAINING - FROZEN'),
    2668: juice('FRUIT JUICE - GRAPE - FROZEN'),
    2666: juice('FRUIT JUICE - APPLE - FROZEN'),
    2662: dict(name='FRUIT JUICE - UNCONCENTRATED - FROZEN', minutes=0, why='ready juice: thawed and poured'),
    2663: juice('FRUIT JUICE - GRAPEFRUIT - FROZEN'),
    2669: juice('FRUIT DRINKS - ORANGE - FROZEN'),

    # ---------------------------------------------------------------- pizza and snacks
    2631: dict(name='PIZZA-FROZEN', minutes=25, basis='pkg', why='frozen pizza: oven heats, bakes 12-18 min', rules=[
        (dict(style='^MICRO'), 4, 'single-serve pizza: microwave 3-4 min', 'serv4'),
        (dict(descr=r'\bMW\b|\bPFO\b|\bFO\b|FOR ONE'), 4, 'single-serve pizza: microwave 3-4 min', 'serv4'),
        (dict(brand=r"FOR ONE|SINGLES|BY THE SLICE|LEAN CUISINE|SMART ONES|CELESTE|HEALTHY CHOICE|SOUTH BEACH|MICHELINA|WEIGHT WATCHERS|ATKINS|DEEP DISH MINIS|KID CUISINE"), 4,
         'single-serve pizza: microwave 3-4 min', 'serv4'),
        (dict(type='FRN BREAD|FRENCH BREAD|F-B'), 4, 'french bread pizza, single pieces: microwave 3-4 min', 'serv4'),
        (dict(descr=r'\bFB\b|\bCFB\b|\bDS\b|\bDDS\b'), 4, 'french bread or deep dish single pizzas: microwave 3-4 min', 'serv4'),
        (dict(type='TRDNL', brand='DIGIORNO'), 4, 'single-serve pizza: microwave 3-4 min', 'serv4'),
        (dict(type='MINI|INDIVIDUAL|PRS'), 4, 'mini or personal pizzas: microwave 3-4 min', 'serv4'),
        (dict(type='RS-C|RISING|RISE|N-R|STUFFED|RIZER'), 33, 'rising or stuffed crust pizza: oven heats, bakes 22-25 min'),
        (dict(brand='DIGIORNO', descr=r'\bRT\b'), 33, 'rising crust pizza: oven heats, bakes 22-25 min'),
    ]),
    2628: dict(name="FROZEN/REFRIGERATED HORS D' OEUVRES & SNACKS", minutes=20, basis='serv4', why='breaded or filled snacks: oven heats, bakes 10-12 min', rules=[
        (dict(product='HARD BOILED|HARD COOKED|DEVILED|PANINO|PROSCIU|SPIRAL|PINWHEEL|CAVIAR|FRUIT CUP|ROLTINI|ARTISAN WRAP|MEAT APTZR|SALAMI|PLATTER|ROKU'), 0,
         'boiled eggs, meat and cheese rolls: eaten as sold'),
        (dict(product='EGG ROLL|SPRING ROLL|LUMPIA|RANGOON|SAMOSA|EMPANADA|CHIMICHANGA'), 25, 'egg rolls and spring rolls: oven heats, bakes 15 min'),
        (dict(product='PIZZA|BAGEL|PRETZEL|PEP PULL|POCKET|PANCAKE|OMELET|EGG BT'), 3, 'pizza rolls, bagel bites, soft pretzels: microwave 1-3 min'),
        (dict(product='POT ?STICKER|DUMPLING|GYOZA|SHU MAI|WONTON|BAO'), 10, 'potstickers and dumplings: pan-steamed 8-10 min'),
        (dict(product='MEATBALL|SAUSAGE BALL'), 8, 'cooked meatballs: heated'),
        (dict(product="SPANAKOPITA|PASTRY|BLANKET|FILLO|PHYLLO|HORS D'OEUVRE|QUICHE|SCALLOP|STUFFED MUSHROOM|STUFFED CLAM"), 28,
         'pastry appetizers, small quiches, wrapped scallops: oven heats, bakes 15-20 min'),
    ]),
    2632: dict(name='PIZZA CRUST-FROZEN', minutes=22, basis='pkg', why='pizza crust: topped while the oven heats, baked 10-12 min'),

    # ---------------------------------------------------------------- prepared foods
    2621: dict(name='ENTREES - POULTRY - 1 FOOD - FROZEN', minutes=5, basis='serv4', why='chicken entree in a tray or bowl: microwave 5 min', rules=[
        (dict(brand=SS), 5, 'single-serve chicken entree: microwave 5 min'),
        (dict(brand='CROCK POT'), 240, 'slow cooker kit: 4 hours on high', 'pkg'),
        (dict(form='ROAST|LOAF|^WHOLE$'), 120, 'turkey roast or loaf: oven about 2 hours', 'pkg'),
        (dict(descr=r'\bLOAF\b'), 120, 'turkey loaf: oven about 2 hours', 'pkg'),
        (dict(descr=r'\bTK\b', form='^BREAST$', brand='JENNIE-O|BUTTERBALL|HONEYSUCKLE|SHADY BROOK|NORBEST'), 120, 'boneless turkey breast roast: oven about 2 hours', 'pkg'),
        (dict(brand=SKILLET), 12, 'bagged skillet meal: pan 10-13 min', 'pkg'),
        (dict(descr=FS, brand='STOUFFER'), 60, 'family-size chicken bake: oven 60 min', 'pkg'),
        (dict(brand="^STOUFFER'S$", form='TENDER'), 60, 'family-size chicken bake: oven 60 min', 'pkg'),
        (dict(brand=FAMILY), 60, 'family-size tray of chicken or turkey in sauce: oven 45-60 min', 'pkg'),
        (dict(brand='BANQUET', form='SLICED'), 60, 'family-size tray of sliced turkey in gravy: oven 45-60 min', 'pkg'),
        (dict(type=r'C-B|CORDON|KIEV|\bSTF\b|STUFFED', brand_not="STOUFFER|MARIE|BANQUET"), 45, 'raw stuffed breaded chicken breast: oven heats, bakes 35 min'),
        (dict(descr=r'\bC-B\b|\bSTF\b|KIEV', brand="BARBER|MILFORD|KOCH|ANTIOCH|TYSON|CTL BR"), 45, 'raw stuffed breaded chicken breast: oven heats, bakes 35 min'),
        (dict(form='WING|DRUMMETTE', form_not='WYNG|BREAST'), 30, 'chicken wings: oven heats, bakes 20-25 min'),
        (dict(form='THIGH|DRUM|LEG', type='FRIED|BREADED|CRISPY|BATTER'), 50, 'fried bone-in chicken pieces: oven 45-50 min'),
        (dict(form='BURGER'), 10, 'turkey or chicken burgers: pan or grill 10 min'),
        (dict(product='BURGER'), 10, 'turkey or chicken burgers: pan or grill 10 min'),
        (dict(type=r'MEATBALL'), 8, 'cooked meatballs: heated'),
        (dict(type=r'BREAD|CRISPY|BATTER|PANKO|FRIED|COATED|\bS-ST\b|HOMESTYLE|DIPPED|TEMPURA|CRUSTED'), 22, 'breaded chicken: oven heats, bakes 12-15 min'),
        (dict(form='FRITTER|NUGGET|PATT|POPCORN|WYNG|FRIES|NIBBLER|FINGER|DINO|DNSR|FUN|DSBNPF|BNSPDF|PBCF|NG-S'), 22, 'breaded chicken: oven heats, bakes 12-15 min'),
        (dict(brand="ANY'TIZERS"), 22, 'breaded chicken bites: oven heats, bakes 12-15 min'),
        (dict(type='BSY|BUFFALO|HNY|BBQ|HBS', form='STRIP|BITE|CHUNK|TENDER|BREAST'), 22, 'sauced breaded chicken strips: oven heats, bakes 12-15 min'),
        (dict(descr=r'\bBD\b|\bBDB\b|\bNG\b|FRTR|\bPCN\b'), 22, 'breaded chicken: oven heats, bakes 12-15 min'),
        (dict(brand='GRILLED & READY|JOHN SOULES|SHORT CUTS|FAST CLASSICS'), 5, 'cooked grilled chicken strips: heated'),
        (dict(type='GRILLED|FAJITA|ROTISSERIE|PULLED|COOKED|CHAR', form='STRIP|DICED|SLICED|CHUNK|PULLED|SHRED|FILLET|BREAST|BITE'), 5, 'cooked grilled chicken pieces: heated'),
        (dict(form='STRIP|DICED|SLICED|CHUNK|PULLED|SHRED'), 5, 'cooked chicken strips or chunks: heated'),
        (dict(form='THIGH|DRUM|LEG', type_not=r'BNS|B-S|BNLS'), 45, 'raw bone-in chicken parts: baked 40-45 min'),
        (dict(form='TENDERLOIN|TENDER'), 15, 'raw chicken tenderloins: pan 10-15 min'),
        (dict(form='BREAST|THIGH|FILLET|PORTION|CUTLET'), 25, 'raw boneless chicken: pan or oven 20-25 min'),
    ]),
    2623: dict(name='ENTREES - ITALIAN - 1 FOOD - FROZEN', minutes=5, basis='serv4', why='pasta entree in a tray or bowl: microwave 5 min', rules=[
        (dict(brand=SS + '|BANQUET'), 5, 'single-serve pasta entree: microwave 5 min'),
        (dict(descr=PS, brand='STOUFFER'), 90, 'party-size lasagna or pasta bake: oven 90-105 min', 'pkg'),
        (dict(descr=FS, brand='STOUFFER'), 60, 'family-size lasagna or pasta bake: oven 60-70 min', 'pkg'),
        (dict(brand=FAMILY), 60, 'family-size pasta bake: oven 50-65 min', 'pkg'),
        (dict(brand=SKILLET), 12, 'bagged skillet pasta meal: pan 10-12 min', 'pkg'),
        (dict(product='TOASTED|BREADED|FINGER'), 22, 'breaded ravioli or chicken: oven heats, bakes 12 min'),
        (dict(brand='BARBER'), 45, 'raw stuffed breaded chicken breast: oven heats, bakes 35 min'),
        (dict(product='KIT|SKILLET'), 12, 'bagged skillet pasta meal: pan 10-12 min', 'pkg'),
        (dict(product='LASAGNA|ZITI|MANICOTTI|EGGPLT|EGGPLANT|STUFFED SHELL|CANNELLONI|BAKE'), 60, 'multi-serve lasagna or pasta bake: oven 55-65 min', 'pkg'),
    ]),
    2619: dict(name='ENTREES - MEAT - 1 FOOD - FROZEN', minutes=5, basis='serv4', why='meat entree in a tray or bowl: microwave 5 min', rules=[
        (dict(brand=SS + '|ARMOUR CLASSICS'), 5, 'single-serve meat entree: microwave 5 min'),
        (dict(brand='CROCK POT'), 240, 'slow cooker kit: 4 hours on high', 'pkg'),
        (dict(product='MEATBALL', product_not='SPAGHETTI|PENNE|KIT|NOODLE', type_not='PASTA|NOODLE|FETTUC|EGG N'), 8, 'cooked meatballs: heated'),
        (dict(brand=FAMILY), 60, 'family-size tray of meat in gravy: oven 45-60 min', 'pkg'),
        (dict(descr=FS, brand='STOUFFER'), 60, 'family-size meat entree: oven 60 min', 'pkg'),
        (dict(brand="^STOUFFER'S$|^BANQUET$|^CTL BR$|MARIE", product='MEATLOAF|SALISBURY|^POT ROAST|SHEPHERD|CASSEROLE|CHEESEBURGER', type_not='DEEP DISH|MEAL'), 60, 'family-size meat entree: oven 45-60 min', 'pkg'),
        (dict(type='COUNTRY FRIED|BREADED|FRITTER|CHICKEN FRIED'), 22, 'breaded steak patties: oven heats, bakes 12-15 min'),
        (dict(product='FRITTER|SCHNITZEL|FINGER|NUGGET|CHICKEN FRIED|STEAK FG'), 22, 'breaded steak patties: oven heats, bakes 12-15 min'),
        (dict(brand='CARIBBEAN|GOLDEN KRUST|TOWER ISLE'), 3, 'Jamaican meat turnovers: microwave 2-3 min'),
        (dict(product='PATT|BURGER', brand="BALL PARK|QUICK'N EAT|DON LEE|PIERRE|ZAPPERS|WHITE CASTLE"), 3, 'cooked burger patties: microwave 1-2 min'),
        (dict(product='PATT|BURGER'), 10, 'raw burger patties: pan or grill 10 min'),
        (dict(product=r'\bRIBS?\b|RIBLET|SPRB|P-RB|PRB'), 30, 'cooked ribs in sauce: oven heats, warms 20-25 min', 'pkg'),
        (dict(product='STEAK', type='SLICED|SHAPED|CHOPPED|PHLY|THIN'), 8, 'thin sliced steak: pan 5-8 min'),
        (dict(product='STEAK|CHOP|LAMB|VEAL'), 15, 'raw steaks or chops: pan 10-15 min'),
        (dict(product='^PORK$|BRISKET|BBQ|BULGOGI|PULLED'), 10, 'cooked meat in sauce: heated 8-10 min', 'pkg'),
        (dict(product='SAUSAGE'), 8, 'cooked sausage: heated'),
    ]),
    2615: dict(name='DINNERS-FROZEN', minutes=5, basis='serv4', why='frozen dinner tray: microwave 5-6 min', rules=[
        (dict(brand='TOFURKY'), 90, 'vegetarian roast: oven 75-90 min', 'pkg'),
    ]),
    2622: dict(name='ENTREES - ORIENTAL - 1 FOOD - FROZEN', minutes=5, basis='serv4', why='Asian entree in a tray or bowl: microwave 5 min', rules=[
        (dict(brand=SS + "|^MARIE CALLENDER'S$|BANQUET|RICE GOURMET"), 5, 'single-serve Asian entree: microwave 5 min'),
        (dict(brand=SKILLET), 12, 'bagged stir-fry meal: pan 10-13 min', 'pkg'),
        (dict(product='STIR-FRY|STIR FRY|SKILLET'), 12, 'bagged stir-fry meal: pan 10-13 min', 'pkg'),
        (dict(type='STIR-FRY|STIR FRY'), 12, 'bagged stir-fry meal: pan 10-13 min', 'pkg'),
        (dict(brand="INNOVASIAN|CRAZY CUIZINE|KAHIKI|ANY'TIZERS|FOSTER FARMS|PAGODA|LING LING|GARDEIN|SEA PAK"), 25, 'battered chicken with sauce: oven heats, bakes 15-18 min'),
        (dict(type='BATTER|BREADED|TEMPURA|CRISPY', product_not='NOODLE|RICE|MEIN'), 25, 'battered chicken with sauce: oven heats, bakes 15-18 min'),
        (dict(product='TEMPURA|ORANGE CHICKEN|GENERAL TSO|SESAME CHICKEN|M-O CHICKEN|SWEET & SOUR', brand='CTL BR|TYSON$'), 25, 'battered chicken with sauce: oven heats, bakes 15-18 min'),
    ]),
    2624: dict(name='ENTREES - MEXICAN - 1 FOOD - FROZEN', minutes=3, basis='serv4', why='burrito, chimichanga or quesadilla: microwave 2-3 min', rules=[
        (dict(brand=SS), 5, 'single-serve Mexican entree: microwave 5 min'),
        (dict(product='TAQUITO|FLAUTA|^TACO$|ROLLED|MINI TACO'), 22, 'taquitos or rolled tacos: oven heats, bakes 12 min'),
        (dict(brand=FAMILY), 60, 'family-size enchiladas: oven 50-60 min', 'pkg'),
        (dict(brand=SKILLET), 12, 'bagged skillet meal: pan 10-13 min', 'pkg'),
        (dict(product='FAJITA', product_not='BOWL'), 10, 'fajita strips or kit: pan 8-10 min'),
        (dict(brand='STOUFFER', product='ENCHILADA|LASAGNA|CASSEROLE'), 60, 'family-size enchiladas: oven 50-60 min', 'pkg'),
        (dict(product='LASAGNA'), 60, 'family-size Mexican lasagna: oven 60 min', 'pkg'),
        (dict(product='ENCHILADA|CASSEROLE|LASAGNA|TAMALE|RELLENO|PAELLA', product_not='BOWL'), 5, 'enchiladas or tamales: microwave 5 min'),
    ]),
    2626: dict(name='ENTREES - SEAFOOD - 1 FOOD - FROZEN', minutes=22, basis='serv4', why='frozen fish, shrimp or crab cakes: oven heats, bakes 12-18 min', rules=[
        (dict(brand=SS + '|GORTON.S SIMPLY BAKE'), 5, 'single-serve seafood entree: microwave 5 min'),
        (dict(brand=SKILLET, brand_not='GORTON'), 12, 'bagged skillet meal: pan 10-13 min', 'pkg'),
        (dict(product='PASTA|LINGUIN|NOODLE|ALFREDO|PAELLA|JAMBALAYA|GUMBO|ETOUFFEE|CASSEROLE|STIR'), 12, 'seafood and pasta or rice meal: pan 10-12 min', 'pkg'),
        (dict(product='SCAMPI'), 8, 'shrimp in garlic butter: pan 6-8 min'),
        (dict(product='MUSSEL|^CLAM$|CRAWFISH', type_not='BREAD|FRIED'), 8, 'cooked shellfish in sauce: heated 6-8 min'),
        (dict(product='BURGER'), 10, 'fish burgers: pan 8-10 min'),
        (dict(product='GEFILTE'), 0, 'gefilte fish: eaten as sold'),
        (dict(product='^SHRIMP', type_not='BREAD|BTF|BUTTERFLY|BATTER|COCONUT|CNUT|POPCORN|TEMPURA|CRISP|CRUNCH|PANKO|COAT|STUFFED|FRIED|POPPER|CPY|BRDC'), 8, 'unbreaded shrimp: pan 6-8 min'),
    ]),
    2625: dict(name='ENTREES - REMAINING - 1 FOOD - FROZEN', minutes=5, basis='serv4', why='macaroni and cheese or side dish in a tray: microwave 5 min', rules=[
        (dict(brand=SS + '|BANQUET'), 5, 'single-serve entree: microwave 5 min'),
        (dict(descr=PS, brand='STOUFFER'), 90, 'party-size macaroni and cheese: oven 90 min', 'pkg'),
        (dict(descr=FS, brand='STOUFFER'), 60, 'family-size macaroni and cheese or stuffed peppers: oven 60 min', 'pkg'),
        (dict(brand=FAMILY), 60, 'family-size tray: oven 50-60 min', 'pkg'),
        (dict(product='PIEROGI|PEROGI|PIROG'), 12, 'pierogies: boiled or pan-fried'),
        (dict(product='DUMPLING|POTSTICKER|MANDU'), 10, 'dumplings: pan-steamed 8-10 min'),
    ]),
    2617: dict(name='POT PIES-FROZEN', minutes=7, basis='serv4', why='single pot pie: microwave 5-10 min by size', rules=[
        (dict(brand="WILLOW TREE|MRS BUDD|OVERBROOK|SPRING GLEN|CRAVERY"), 75, 'family-size pot pie: oven 65-80 min', 'pkg'),
    ]),
    2611: dict(name='ENTREES - MEAT - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='meat entree with a side in a tray: microwave 5 min'),
    2612: dict(name='ENTREES - POULTRY - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='chicken or turkey entree with a side in a tray: microwave 5 min'),
    2692: dict(name='PASTA-PLAIN-FROZEN', minutes=14, basis='serv4', why='frozen ravioli or tortellini: water to a boil 8, cooks 5-6 min', rules=[
        (dict(product='MANICOTTI|SHELL|CANNELLONI|LASAGNA'), 55, 'stuffed shells or manicotti: baked in sauce 45 min'),
        (dict(product='NOODLE|DUMPLING', type_not='STF'), 28, 'frozen egg noodles or flat dumplings: water to a boil 8, simmer 20 min'),
        (dict(product='DUMPLING|PIEROGI'), 10, 'filled dumplings: pan-steamed 8-10 min'),
    ]),
    2694: dict(name='SOUP-FROZEN-REFRIGERATED', minutes=4, basis='serv4', why='refrigerated soup: heated', rules=[
        (dict(descr=r'\bFZ\b'), 8, 'frozen soup: heated from frozen 7-10 min'),
    ]),
    2693: dict(name='CORN DOGS-FROZEN/REFRIGERATED', minutes=25, basis='serv4', why='corn dogs: oven heats, bakes 15-18 min'),
    2695: dict(name='SAUCES & GRAVIES-FROZEN/REFRIGERATED', minutes=0, why='salsa, pesto, sauces and gravies'),
    2603: dict(name='MEAL STARTERS', minutes=15, basis='pkg', why='skillet or stir-fry starter: meat or eggs added, pan 12-15 min', rules=[
        (dict(brand='JUST CRACK'), 2, 'egg cup: egg added, microwave 2 min', 'serv4'),
        (dict(brand='FRESH TAKE'), 0, 'cheese and crumb coating: the meat carries the time', 'serv4'),
        (dict(brand='SALAD|OKAMI|ET TU'), 0, 'salad kit or salad topping: tossed', 'serv4'),
        (dict(product='SALAD'), 0, 'salad kit: tossed', 'serv4'),
        (dict(brand='SLOW C|CROCK'), 240, 'slow cooker starter: 4 hours on high'),
        (dict(brand='WANCHAI|HAMBURGER HELPER|SIMPLE SUPPERS'), 25, 'dinner kit: meat added, pan 20-25 min'),
    ]),
    2614: dict(name='ENTREES - ITALIAN - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='Italian entree with a side in a tray: microwave 5 min'),
    2627: dict(name='ENTREES - ORIENTAL - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='Asian entree with rice in a tray: microwave 5 min'),
    2616: dict(name='ENTREES - MEXICAN - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='Mexican entree with rice in a tray: microwave 5 min'),
    2609: dict(name='ENTREES - SEAFOOD - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='fish entree with a side in a tray: microwave 5 min'),
    2606: dict(name='ENTREES - REMAINING - 2 FOOD - FROZEN', minutes=5, basis='serv4', why='entree with a side in a tray: microwave 5 min'),
    2613: dict(name='ENTREES - MULTI PACK - FROZEN', minutes=5, basis='serv4', why='pack of single-serve entrees: microwave 5 min each meal'),
    2696: dict(name='TACO FILLING-FROZEN/REFRIGERATED', minutes=6, basis='serv4', why='cooked taco meat: heated and put in shells'),

    # ---------------------------------------------------------------- vegetables
    2634: dict(name='VEGETABLES - POTATOES - FROZEN/REFRIGERATED', minutes=25, basis='serv4', why='frozen fries, tots or potato wedges: oven heats, bakes 15-20 min', rules=[
        (dict(product='MASHED|WHIPPED|MASH'), 5, 'prepared mashed potatoes: microwave 4-5 min'),
        (dict(container='TUB|TRAY', descr=r' R(?: |$)'), 5, 'refrigerated potato side dish: microwave 4-6 min'),
        (dict(brand=SS), 5, 'single-serve potato dish: microwave 5 min'),
        (dict(descr=r'\bMW\b'), 4, 'microwave fries: 3-4 min'),
        (dict(style='^MICRO'), 12, 'steam-and-mash potatoes: microwave 10 min, then mashed'),
        (dict(product='HASH ?BROWN|O.BRIEN|SOUTHERN', type_not='PATT|STICK|ROUND|TRIANGLE|NUGGET', product_not='PATT'), 15, 'shredded or diced hash browns: pan 12-15 min'),
        (dict(product='^POTATO(?:ES)?$', type='^DICED|CUBED|^SHREDDED|SLICED'), 15, 'diced or sliced potatoes: pan 12-15 min'),
    ]),
    2640: veg('VEGETABLES - MIXED - FROZEN'),
    2636: veg('VEGETABLES - REMAINING - FROZEN', extra=[
        (dict(product='COLLARD|TURNIP|MUSTARD|KALE|GREENS'), 25, 'frozen greens: simmer 20-30 min'),
        (dict(product='^ONION|PEPPER|SEASONING|CHIVE|GARLIC|GINGER|HERB|CILANTRO|BASIL'), 0, 'chopped onions, peppers, herbs: added to a dish'),
    ], after=[
        (dict(product='OKRA|BRUSSEL|ARTICHOKE|BLACK-?EYE|FIELD PEA'), 10, 'okra, brussels sprouts: simmer 8-10 min'),
    ]),
    2641: veg('VEGETABLES - BROCCOLI - FROZEN'),
    2639: dict(name='VEGETABLES-IN SAUCE-FROZEN', minutes=7, basis='serv4', why='vegetables in sauce: simmer or microwave 6-8 min', rules=[
        (dict(container='BOX|TRAY'), 5, 'boxed vegetables in sauce pouch: microwave 4-6 min'),
        (dict(descr=STEAM), 5, 'steam-in-bag vegetables in sauce: microwave 5 min'),
        (dict(brand='STEAM'), 5, 'steam-in-bag vegetables in sauce: microwave 5 min'),
    ]),
    2630: veg('VEGETABLES - CORN - FROZEN'),
    2620: veg('VEGETABLES - PEAS - FROZEN', extra=[
        (dict(product='BLACK-?EYE|FIELD|PURPLE|CROWDER|BUTTER PEA|PIGEON|ZIPPER|WHITE ACRE|CREAM PEA|LADY|CHICK'), 35, 'southern field peas: simmer 30-40 min'),
    ]),
    2633: veg('VEGETABLES-GREEN BEANS-FROZEN'),
    2608: dict(name='VEGETABLES - CORN ON THE COB - FROZEN', minutes=15, basis='serv4', why='corn on the cob: water to a boil 8, cooks 5-7 min', rules=[
        (dict(descr=r'\bMW\b|\bSFR\b'), 6, 'microwave corn on the cob: 5-6 min'),
    ]),
    2638: dict(name='VEGETABLES - ONIONS - BREADED - FROZEN', minutes=22, basis='serv4', why='onion rings: oven heats, bakes 12-15 min'),
    2644: veg('VEGETABLES - LIMA BEANS - FROZEN', minutes=18, why='frozen lima beans: water to a boil, simmer 12-15 min', extra=[
        (dict(product='BUTTER BEAN'), 30, 'frozen butter beans: simmer 25-30 min'),
        (dict(type='SPECKLED'), 30, 'frozen speckled butter beans: simmer 25-30 min'),
    ]),
    2642: veg('VEGETABLES - CARROTS - FROZEN'),
    2635: dict(name='VEGETABLES - BREADED - FROZEN', minutes=22, basis='serv4', why='breaded okra or vegetables: oven heats, bakes 12-15 min, or pan-fried'),
    2637: dict(name='VEGETABLES - MUSHROOMS - BREADED - FROZEN', minutes=22, basis='serv4', why='breaded mushrooms: oven heats, bakes 10-12 min'),
    2618: dict(name='VEGETABLES -IN PASTRY - FROZEN', minutes=35, basis='serv4', why='vegetables in pastry: oven heats, bakes 25 min'),
}
