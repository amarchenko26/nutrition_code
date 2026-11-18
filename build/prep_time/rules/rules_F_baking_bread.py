# Cluster F_baking_bread: baking mixes, baking supplies, flour, refrigerated dough, bread and baked goods, desserts,
# puddings, sugar and sweeteners, table syrups, cookies, crackers. Minutes = one cooking session (see prep_rules_brief.md).
MW = r'\bMW\b|\bMUG\b'

RULES = {
    # ------------------------------------------------------------------ BAKING MIXES
    1375: dict(name='MIXES - CAKE/LAYER - OVER 10 OZ.', minutes=40, basis='pkg', why='layer cake mix: mix the batter, bake 28-33 min'),
    1380: dict(name='MIXES - BROWNIES', minutes=35, basis='pkg', why='brownie mix: mix the batter, bake 25-30 min', rules=[
        (dict(variety='^MICROWAVE'), 2, 'single-serve brownie bowl: add water, microwave about 1 min', 'serv4'),
        (dict(descr=MW), 2, 'single-serve brownie bowl: add water, microwave about 1 min', 'serv4'),
    ]),
    1395: dict(name='MIXES - PANCAKE', minutes=15, basis='serv4', why='pancake or waffle mix: stir the batter, cook in batches on a griddle', rules=[
        (dict(descr=r'\bPOT\b|POTATO', descr_not='SWT POT'), 20, 'potato pancake mix: mix, let thicken 5 min, fry in oil'),
    ]),
    1381: dict(name='MIXES-MUFFIN', minutes=25, basis='pkg', why='muffin mix: mix the batter, fill the tin, bake 15-20 min', rules=[
        (dict(style='^MICROWAVE'), 2, 'single-serve muffin cup: add water, microwave about 1 min', 'serv4'),
    ]),
    1386: dict(name='MIXES-ROLLS & BISCUITS', minutes=20, basis='serv4', why='all-purpose baking mix: mix dough, cut or drop biscuits, bake 8-10 min', rules=[
        (dict(product='SCONE'), 25, 'scone mix: mix dough, shape, bake 15-18 min', 'pkg'),
        (dict(product='PRETZEL'), 40, 'soft pretzel kit: knead, shape, dip in soda bath, bake; rising not counted', 'pkg'),
        (dict(product='ROLL|BREAD|BAGEL'), 40, 'yeast roll mix: knead, shape, bake 15-20 min; rising not counted', 'pkg'),
        (dict(product='^BISCUIT MIX|^BISCUIT AND SCONE'), 20, 'biscuit mix pouch, one batch: stir, drop on a sheet, bake 10-15 min', 'pkg'),
    ]),
    1383: dict(name='MIXES-BREAD', minutes=50, basis='pkg', why='quick bread loaf (bake 45 min) or yeast bread mix; rising not counted', rules=[
        (dict(type='CORN'), 30, 'corn bread mix: mix the batter, bake 20-25 min'),
        (dict(descr=r'\bCRN\b|CRNBRD'), 30, 'corn bread mix: mix the batter, bake 20-25 min'),
    ]),
    1387: dict(name='MIXES-COOKIE', minutes=22, basis='pkg', why='cookie mix: mix the dough, drop on sheets, bake 8-11 min', rules=[
        (dict(style='^MICROWAVE', descr_not=r'\bKT\b'), 2, 'single-serve cookie bowl: add water, microwave about 1 min', 'serv4'),
    ]),
    1384: dict(name='MIXES-DESSERT-MISC.', minutes=45, basis='pkg', why='dessert bars or other baked dessert mix: crust and filling, bake 30-35 min', rules=[
        (dict(style='^MICROWAVE'), 3, 'microwave dessert mix: stir, microwave 1-3 min', 'serv4'),
        (dict(descr=r'NO-BK|\bNBK\b'), 15, 'no-bake dessert: press the crumb crust, beat the filling with milk; chilling not counted'),
        (dict(product='CHEESECAKE|PIE MIX', product_not='WHOOPIE|COOKIE PIE', descr=r'PMPKN|PECAN|\bSW-P\b|CHRY'), 60, 'baked pie kit: mix the filling, pour into the crust, bake 45-50 min'),
        (dict(product='CHEESECAKE|PIE MIX|MOUSSE PIE', product_not='WHOOPIE|COOKIE PIE'), 15, 'no-bake cheesecake or cream pie mix: press crust, beat filling; chilling not counted'),
        (dict(product='MOUSSE|PARFAIT|TIRAMISU|TRIFLE|DIRT CUP|FLUFF'), 5, 'mousse mix: whip with cold milk; chilling not counted'),
        (dict(product='MILK SHAKE|BLIZZARD'), 2, 'milk shake mix: blend with milk', 'serv4'),
        (dict(descr=r'BRULEE|\bFLAN\b|CUSTARD'), 10, 'custard dessert mix: heat with milk on the stove; chilling not counted'),
        (dict(product='FUDGE|BON BON'), 15, 'fudge kit: boil milk and sugar 5 min, stir in chocolate, pour; setting not counted'),
        (dict(product='CRISPY RICE|CRISY RICE|CRISPED RICE|KRISPIES|TREAT MIX'), 10, 'crispy rice treat kit: melt marshmallow and butter, stir, press in a pan'),
        (dict(product='COBBLER|CRISP'), 50, 'cobbler or fruit crisp mix: fruit in the dish, topping over it, bake 35-40 min'),
        (dict(descr=r'\bCSP\b|\bCBLR\b'), 50, 'cobbler or fruit crisp mix: fruit in the dish, topping over it, bake 35-40 min'),
        (dict(product='BEIGNET|FUNNEL|DOUGHNUT|DONUT|CHURRO|SOPA|FRITTER|ZEPPOLE|BUNUELO|PUFF PUFF|GULAB'), 25, 'fried dough mix: mix, heat the oil, fry in batches', 'serv4'),
        (dict(product='CREPE|FRENCH TOAST'), 15, 'crepe or french toast batter mix: stir, cook in batches in a pan', 'serv4'),
        (dict(product='YOGURT STARTER'), 10, 'yogurt starter: heat the milk, stir in; incubating not counted'),
    ]),
    1373: dict(name='MIXES - CAKE/SPECIALTY - OVER 10 OZ.', minutes=40, basis='pkg', why='cupcake or specialty cake mix: mix the batter, bake 20-33 min', rules=[
        (dict(style='^MICROWAVE'), 2, 'single-serve mug cake: add water, microwave about 1 min', 'serv4'),
        (dict(descr=MW), 2, 'single-serve mug cake: add water, microwave about 1 min', 'serv4'),
        (dict(descr=r'\bAGL\b|ANGL'), 50, 'angel food cake mix: beat with water, bake 37-47 min in a tube pan'),
        (dict(descr=r'POUND|\bPD-C\b|\bBNT\b|BUNDT'), 60, 'pound or bundt cake mix: mix the batter, bake 45-55 min'),
        (dict(descr=r'CRUMB|\bCR CINN|UPSIDE'), 50, 'crumb cake or upside-down cake mix: layer batter and topping, bake 40 min'),
        (dict(descr='FUNNEL'), 20, 'funnel cake mix: stir the batter, heat the oil, fry one cake at a time', 'serv4'),
    ]),
    1376: dict(name='MIXES - CAKE/LAYER - 10 OZ & UNDER', minutes=35, basis='pkg', why='small cake mix, one layer: mix the batter, bake 23-28 min', rules=[
        (dict(style='^MICROWAVE'), 2, 'single-serve mug cake: add water, microwave about 1 min', 'serv4'),
        (dict(descr=MW + r'|\bWDM?\b|DHPSF1'), 2, 'single-serve bowl or mug cake: add water, microwave about 1 min', 'serv4'),
    ]),
    1374: dict(name='MIXES - CAKE/SPECIALTY - 10 OZ & UNDER', minutes=30, basis='pkg', why='small cupcake mix: mix the batter, fill the cups, bake 18-22 min', rules=[
        (dict(style='^MICROWAVE'), 2, 'single-serve bowl or mug cake: add water, microwave about 1 min', 'serv4'),
        (dict(descr=MW + r'|\bWD\b|DHPSF1|BOWL'), 2, 'single-serve bowl or mug cake: add water, microwave about 1 min', 'serv4'),
        (dict(descr='SWEET CORN'), 60, 'sweet corn cake mix: mix with butter and creamed corn, bake 50-55 min'),
        (dict(descr='FUNNEL'), 20, 'funnel cake mix: stir the batter, heat the oil, fry one cake at a time', 'serv4'),
    ]),
    1392: dict(name='MIXES-PIE CRUST', minutes=55, basis='pkg', why='pie crust mix: mix dough, roll out, fill, bake the pie 40-45 min'),
    1385: dict(name='MIXES-GINGERBREAD', minutes=40, basis='pkg', why='gingerbread cake mix: mix the batter, bake 30-35 min'),
    1390: dict(name='MIXES-FROSTING', minutes=0, why='frosting for a cake: the cake carries the time'),
    1388: dict(name='MIXES - HUSHPUPPY', minutes=20, basis='serv4', why='hushpuppy mix: mix, rest 5 min, heat the oil, fry in batches'),
    1382: dict(name='MIXES-COFFEE CAKE', minutes=45, basis='pkg', why='coffee cake mix: batter and streusel, bake 30-35 min', rules=[
        (dict(style='^MICROWAVE'), 2, 'single-serve coffee cake bowl: add water, microwave about 1 min', 'serv4'),
    ]),
    1394: dict(name='MIXES-DUMPLING & KUGEL', minutes=30, basis='pkg', why='potato dumpling mix: mix with water, shape, simmer 20 min', rules=[
        (dict(product='KUGEL'), 60, 'potato kugel mix: mix with eggs and water, bake 45-50 min'),
    ]),

    # ------------------------------------------------------------------ BAKING SUPPLIES
    1437: dict(name='CHOCOLATE CHIPS & MORSELS', minutes=0, why='added to baking: the baked food carries the time'),
    1364: dict(name='STUFFING PRODUCTS', minutes=10, basis='serv4', why='stove-top stuffing mix: boil water and butter, stir in, stand 5 min', rules=[
        (dict(style='IN BIRD'), 45, 'bread cubes for baked stuffing: cook onion and celery, add broth, bake 30 min'),
        (dict(style='MICROWAVE'), 7, 'microwave stuffing mix: stir with water and butter, microwave 5-6 min'),
    ]),
    1343: dict(name='BREADING PRODUCTS', minutes=0, why='crumbs or coating put on meat or fish: the meat carries the time'),
    1372: dict(name='FROSTING READY-TO-SPREAD', minutes=0, why='spread on a cake as sold'),
    1389: dict(name='CAKE DECORATIONS & ICING', minutes=0, why='decorations put on a cake as sold'),
    1350: dict(name='CROUTONS', minutes=0, why='eaten as sold on salad or soup'),
    1436: dict(name='BAKING CHOCOLATE', minutes=0, why='added to baking: the baked food carries the time'),
    1438: dict(name='BAKING CHIPS OTHER THAN CHOCOLATE', minutes=0, why='added to baking: the baked food carries the time'),
    1418: dict(name='BAKING CHIPS - MILK CHOCOLATE', minutes=0, why='added to baking: the baked food carries the time'),
    1358: dict(name='PIE & PASTRY SHELLS-PREPARED', minutes=10, basis='pkg', why='ready crumb crust: beat a no-bake filling and fill it; chilling not counted', rules=[
        (dict(descr=r'\bFZ\b|PIE SHL|DP DSH|PIE CR R$|PIE CR D-D'), 45, 'unbaked pastry pie shell: fill, bake the pie 40-45 min'),
        (dict(product='PIE CRUST', style='^REGULAR'), 45, 'unbaked pastry pie crust: fill, bake the pie 40-45 min'),
        (dict(brand='WHOLLY WHOLESOME'), 45, 'unbaked frozen pie shell: fill, bake the pie 40-45 min'),
    ]),
    1440: dict(name='COCOA', minutes=0, why='added to baking: the baked food carries the time'),
    1435: dict(name='COCONUT', minutes=0, why='added to baking or eaten as sold'),
    1471: dict(name='CORN/POTATO STARCH', minutes=0, why='thickener added to a sauce or batter: the dish carries the time'),
    1429: dict(name='FRUIT-GLAZED', minutes=0, why='candied fruit added to baking: the baked food carries the time'),
    1153: dict(name='CAPERS', minutes=0, why='condiment, used as sold'),
    1408: dict(name='GRAHAM CRACKER & DESSERT CRUMBS', minutes=12, basis='serv4', why='crumbs for a pie crust: mix with butter, press in a pan, fill'),
    1241: dict(name='MATZO MEAL/MIXES', minutes=30, basis='serv4', why='matzo balls: mix with eggs and oil, shape, simmer 20 min; chilling not counted'),
    1448: dict(name='FRUIT PROTECTORS', minutes=0, why='powder sprinkled on cut fruit'),

    # ------------------------------------------------------------------ BREAD AND BAKED GOODS
    4000: dict(name='BAKERY - BREAD - FRESH', minutes=0, why='bread eaten as sold', rules=[
        (dict(descr=r'\bB/S\b|\bBK/S\b'), 18, 'brown-and-serve bread: oven heats, bake 8-10 min', 'pkg'),
    ]),
    4004: dict(name='BAKERY-CAKES-FRESH', minutes=0, why='baked cakes and snack cakes eaten as sold'),
    4001: dict(name='BAKERY-BUNS-FRESH', minutes=0, why='buns eaten as sold'),
    4003: dict(name='BAKERY-MUFFINS-FRESH', minutes=0, why='muffins and english muffins eaten as sold; toasting not counted'),
    4002: dict(name='BAKERY-ROLLS-FRESH', minutes=0, why='rolls eaten as sold', rules=[
        (dict(type=r'B/S|BAKE & SERVE|HOMEBAKE'), 18, 'brown-and-serve rolls: oven heats, bake 8-10 min', 'pkg'),
    ]),
    4005: dict(name='BAKERY-BREAKFAST CAKES/SWEET ROLLS-FRESH', minutes=0, why='baked pastries eaten as sold'),
    4008: dict(name='BAKERY-BAGELS-FRESH', minutes=0, why='bagels eaten as sold; toasting not counted'),
    4006: dict(name='BAKERY-DOUGHNUTS-FRESH', minutes=0, why='doughnuts eaten as sold'),
    4007: dict(name='BAKERY-PIES-FRESH', minutes=0, why='baked pies eaten as sold'),
    4011: dict(name='BAKERY - REMAINING - FRESH', minutes=0, why='wraps and baked pastries eaten as sold'),
    4012: dict(name='BAKERY-CHEESECAKE-FRESH', minutes=0, why='cheesecake eaten as sold; thawing not counted'),
    4009: dict(name='BAKERY-BISCUITS-FRESH', minutes=0, why='baked biscuits eaten as sold'),
    6039: dict(name='RBC BREAD', minutes=0, why='bread eaten as sold'),
    6040: dict(name='RBC CAKE', minutes=0, why='baked cake eaten as sold'),

    # ------------------------------------------------------------------ COOKIES, CRACKERS
    1362: dict(name='COOKIES', minutes=0, why='eaten as sold'),
    1365: dict(name='ICE CREAM CONES & CUPS', minutes=0, why='filled with ice cream as sold'),
    1360: dict(name='CRACKERS - FLAVORED SNACK', minutes=0, why='eaten as sold'),
    1356: dict(name='CRACKERS - CHEESE', minutes=0, why='eaten as sold'),
    1355: dict(name='CRACKERS - SPRAYED BUTTER', minutes=0, why='eaten as sold'),
    1353: dict(name='CRACKERS - FLAKED SODA', minutes=0, why='eaten as sold'),
    1354: dict(name='CRACKERS - GRAHAM', minutes=0, why='eaten as sold'),
    1352: dict(name='CRACKERS - SPRAYED FLAKE', minutes=0, why='eaten as sold'),
    1359: dict(name='WAFERS & TOAST & BREAD STICKS', minutes=0, why='eaten as sold'),
    1357: dict(name='CRACKERS - REMAINING', minutes=0, why='eaten as sold'),
    1361: dict(name='CRACKERS - OYSTER', minutes=0, why='eaten as sold'),
    1247: dict(name='MATZO', minutes=0, why='eaten as sold'),

    # ------------------------------------------------------------------ DESSERTS, GELATINS, SYRUP
    1455: dict(name='DESSERTS - RTS SINGLE SERVINGS - CANNED', minutes=0, why='fruit, pudding and gelatin cups eaten as sold'),
    1431: dict(name='SYRUP-CHOCOLATE', minutes=0, why='syrup poured as sold'),
    1450: dict(name='PUDDING - SWEETENED - MIX', minutes=3, basis='pkg', why='instant pudding mix: whisk with cold milk 2 min; setting not counted', rules=[
        (dict(formula='^REGULAR'), 10, 'cook-and-serve pudding mix: stir with milk on the stove to a boil'),
    ]),
    1433: dict(name='TOPPINGS-LIQUID & DRY', minutes=0, why='topping used as sold'),
    1446: dict(name='GELATIN-SWEETENED-MIX', minutes=5, basis='pkg', why='gelatin mix: boil water, stir 2 min, add cold water; chilling not counted'),
    1411: dict(name='SYRUP-SPECIALTY', minutes=0, why='syrup poured as sold'),
    1454: dict(name='PUDDING - DIET - MIX', minutes=3, basis='pkg', why='instant pudding mix: whisk with cold milk 2 min; setting not counted', rules=[
        (dict(formula='^REGULAR'), 10, 'cook-and-serve pudding mix: stir with milk on the stove to a boil'),
    ]),
    1432: dict(name='TOPPINGS-MIXES', minutes=0, why='whipped topping for a dessert: the dessert carries the time'),
    1430: dict(name='PUDDING/PIE FILLING - CANNED', minutes=0, why='pudding eaten as sold or filling spooned into a pie'),
    1453: dict(name='MIXES-ICE CREAM', minutes=5, basis='pkg', why='ice cream mix: stir into milk and cream; churning and freezing not counted'),
    1434: dict(name='PUDDING - PLUM - CANNED', minutes=4, basis='serv4', why='canned plum pudding: heat in the microwave or in simmering water'),

    # ------------------------------------------------------------------ DOUGH PRODUCTS (refrigerated)
    3613: dict(name='DOUGH PRODUCTS-BISCUITS-REFRIGERATED', minutes=20, basis='pkg', why='refrigerated biscuits: oven heats, bake 10-15 min'),
    3595: dict(name='DOUGH PRODUCTS - COOKIES & BROWNIES - REFRIGERATED', minutes=20, basis='pkg', why='refrigerated cookie dough: oven heats, place on sheets, bake 10-12 min', rules=[
        (dict(product='BROWNIE'), 35, 'refrigerated brownie batter: oven heats, spread in a pan, bake 25 min'),
    ]),
    3614: dict(name='DOUGH PRODUCTS-DINNER ROLLS-REFRIGERATED', minutes=20, basis='pkg', why='crescent roll dough: oven heats, roll up, bake 9-12 min'),
    3596: dict(name='DOUGH PRODUCTS-SWEET ROLLS-REFRIGERATED', minutes=25, basis='pkg', why='refrigerated cinnamon rolls: oven heats, bake 13-17 min, spread the icing', rules=[
        (dict(descr=r'\bPBG\b'), 35, 'large refrigerated cinnamon rolls: oven heats, bake 23-27 min, spread the icing'),
    ]),
    3615: dict(name='DOUGH PRODUCTS-REMAINING-REFRIGERATED', minutes=45, basis='pkg', why='refrigerated pie crust: unroll, fill, bake the pie 40-45 min', rules=[
        (dict(product='PIZZA'), 28, 'refrigerated pizza crust: unroll, bake 8 min, add toppings, bake 8 min more'),
        (dict(product='BREAD STICK'), 22, 'refrigerated breadstick dough: oven heats, twist, bake 10-13 min'),
        (dict(product='^BREAD'), 38, 'refrigerated bread loaf dough: oven heats, bake 26-30 min'),
        (dict(product='TURNOVER SHELL|PUFF PASTRY|FILLO|PASTRY WRAP|PASTRY POCKET'), 40, 'pastry dough: fill, shape, bake 20-25 min'),
        (dict(product='^TURNOVER'), 25, 'refrigerated turnovers: oven heats, fill and fold, bake 13-17 min'),
        (dict(product='TORTILLA DOUGH'), 8, 'uncooked tortillas: cook about 1 min each in a hot pan', 'serv4'),
        (dict(product='TAMALE? (?:MASA|DOUGH)'), 120, 'tamale dough: spread on husks, fill, wrap, steam 60-90 min', 'serv4'),
        (dict(product='^TAMALE$'), 10, 'ready tamales: steam or microwave to heat', 'serv4'),
        (dict(product='MASA|NIXTAMAL|GORDITA'), 30, 'corn dough: press tortillas or gorditas, cook in batches on a griddle', 'serv4'),
        (dict(product='SCONE|PRETZEL|PASTRY TWIST|MUFFIN|CAKE|CRESCENT'), 25, 'ready dough or batter: oven heats, bake 12-18 min'),
    ]),

    # ------------------------------------------------------------------ FLOUR
    # Flour: a recipe (cookies, muffins, a quick bread: 40 min; a loaf of bread: 50 min; biscuits: 25 min) uses 2 to 3 cups,
    # 8 to 12 label servings of 1/4 cup. Minutes are per four label servings, one cup, so half or a third of the recipe's time;
    # at 40 min per cup a 5-lb bag counted as 19 baking sessions and flour was 22% of all minutes (review 2026-11-18).
    1393: dict(name='FLOUR-ALL PURPOSE-WHITE WHEAT', minutes=20, basis='serv4', why='flour, one cup: half of a 40-min baking recipe (mix, bake 25-30 min)', rules=[
        (dict(style='SELF RISING'), 13, 'self-rising flour, one cup: half of a 25-min batch of biscuits'),
        (dict(type='SELF RISING'), 13, 'self-rising flour, one cup: half of a 25-min batch of biscuits'),
    ]),
    1377: dict(name='FLOUR - ALL PURPOSE - REMAINING', minutes=20, basis='serv4', why='flour, one cup: half of a 40-min baking recipe (mix, bake 25-30 min)', rules=[
        (dict(product='FLAX|XANTHAN|GUM'), 0, 'flaxseed meal or gum stirred into other food: that food carries the time'),
    ]),
    1367: dict(name='CORN MEAL', minutes=30, basis='serv4', why='corn meal: corn bread, mix the batter, bake 20-25 min'),
    1391: dict(name='FLOUR - SINGLE PURPOSE', minutes=20, basis='serv4', why='cake or pastry flour, one cup: half of a 40-min baking recipe', rules=[
        (dict(product='BREAD|PIZZA'), 17, 'bread flour, one cup: a third of a loaf (knead, shape, bake 30-35 min; rising not counted)'),
        (dict(product='SAUCE|SEARING|TEMPURA|CHICKEN FLOUR|FISH FLOUR'), 0, 'flour for thickening gravy or coating meat: that dish carries the time'),
        (dict(product='BISCUIT'), 13, 'self-rising biscuit flour, one cup: half of a 25-min batch of biscuits'),
        (dict(product='TAMAL'), 120, 'corn flour for tamales: make dough, fill, wrap, steam 60-90 min'),
        (dict(product='TORTILLA|CHAP+ATI|MASA'), 30, 'tortilla flour: mix dough, press, cook in batches on a griddle'),
        (dict(product='FUFU|PANCAKE'), 15, 'fufu or pancake flour: stir with water, cook 10 min on the stove'),
    ]),

    # ------------------------------------------------------------------ PUDDING, DESSERTS-DAIRY
    3566: dict(name='PUDDING-REFRIGERATED', minutes=0, why='ready pudding eaten as sold'),

    # ------------------------------------------------------------------ SUGAR, SWEETENERS
    1403: dict(name='SUGAR-GRANULATED', minutes=0, why='sweetener added to other food'),
    1535: dict(name='SUGAR SUBSTITUTES', minutes=0, why='sweetener added to other food'),
    1400: dict(name='SUGAR - BROWN', minutes=0, why='sweetener added to other food'),
    1401: dict(name='SUGAR-POWDERED', minutes=0, why='sweetener added to other food'),
    1402: dict(name='SUGAR - REMAINING', minutes=0, why='sweetener added to other food'),

    # ------------------------------------------------------------------ TABLE SYRUPS, MOLASSES
    1405: dict(name='SYRUP - TABLE', minutes=0, why='syrup poured as sold'),
    1406: dict(name='SYRUP - BERRY/FRUIT TYPE', minutes=0, why='syrup poured as sold'),
    1407: dict(name='MOLASSES', minutes=0, why='sweetener added to other food'),
    1404: dict(name='SYRUP - SORGHUM & SUGAR', minutes=0, why='syrup poured as sold'),
}
