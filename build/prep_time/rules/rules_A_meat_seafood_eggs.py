# Cluster A: meat, seafood, eggs (40 modules). Minutes per cooking session of about four servings; see prep_rules_brief.md.


def W(p):
    """A whole word of an abbreviated description: bounded by spaces or the ends (a hyphen is not a boundary, so CK does not match JN-CK-BF)."""
    return r'(?:^| )(?:' + p + r')(?= |$)'


HAM = W('HM|HAM')
COOKED_T = r'(?<!UN)COOKED|SMOKED|CURED|BARBECUE|\bHS\b'          # type values of cooked, smoked or cured meat
BONELESS_T = r'BNLS|BONELESS|SKINLESS|\bFF\b|X-LN'          # type values of boneless skinless poultry
SMOKED_SG = W(r'SM|SMK|P-KB|KBS|P-K|PL|AD|HS|CKD|F-C|PC|SM/\S+')   # dinner sausage: smoked, kielbasa, polish, andouille, cooked
MIXED_SG = W(r'BCP|BP|BPT|CPB|B&P|BP&VL|B&P&T|BF|CK|CT|CK&PK|PK&TK|CK&TK')   # beef, mixed-meat and chicken links

RULES = {
    # ------------------------------------------------------------------ eggs
    4100: dict(name='EGGS-FRESH', minutes=6, basis='serv4', why='shell eggs: fried or scrambled, pan 5-6 min'),

    # ------------------------------------------------------------------ fresh meat (barcoded packages)
    3561: dict(name='FRESH MEAT', minutes=20, basis='serv4', why='fresh meat, cut not recognized: pan or oven about 20 min', rules=[
        (dict(descr='LIVER'), 15, 'liver: pan-fry with onions 12-15 min'),
        (dict(descr='GIZZARD|HEART'), 60, 'gizzards and hearts: simmer about an hour until tender'),
        (dict(descr='CHITLING|CHTRLNG|CHITTERLING|TRIPE'), 180, 'chitterlings or tripe: clean and simmer about 3 hours', 'pkg'),
        (dict(descr=W('TRKY'), form='^WHOLE$'), 240, 'whole turkey: roast about 4 hours', 'pkg'),
        # ham (cooked or cured)
        (dict(descr=HAM, form='DICED|CUBE|CHOPPED|CHIP|JULIENNE|SHRED|MINCED'), 0, 'cooked ham, diced or cubed: added to a dish as sold'),
        (dict(descr=HAM, form='^WHOLE$|LOAF|SPIRAL'), 90, 'whole boneless cooked ham: heat in the oven', 'pkg'),
        (dict(descr=HAM, form='STEAK|SLICE|CHOP|BISCUIT|CENTER|END|GRILLER|STRIP|MEDALLION|FILLET|PIECE'), 8, 'ham steak or ham slices: heat in a pan'),
        (dict(descr=HAM, form='HALF|CHUNK|PORTION|MINI|SHOULDER|BUTT|ROAST|ROLL'), 60, 'half or chunk of cooked ham, 2-3 lb: heat in the oven about 50 min', 'pkg'),
        (dict(descr=HAM), 8, 'other cooked ham: heat slices in a pan'),
        # other smoked, cured or cooked meat
        (dict(type='SMOKED|CURED', form='PIECE|NECK|HOCK|WING|PORTION|FEET|TAIL|SHANK|LEG'), 90, 'smoked pork or turkey pieces: simmer with beans or greens'),
        (dict(type=COOKED_T, form='^WHOLE$|HALF|LOAF'), 60, 'smoked or cooked whole piece: heat in the oven', 'pkg'),
        (dict(type=COOKED_T), 8, 'smoked chops, cured side meat, cooked meat: heat or fry in a pan'),
        # ground meat
        (dict(form='PATT|BURGER|SLIDER'), 10, 'patties: pan or grill 8-10 min'),
        (dict(form='GROUND|GRND|CHUB|MINCED'), 20, 'ground meat: brown and finish a dish such as tacos or meat sauce'),
        (dict(form='^(ROLL|CHUNK)$', descr=W('BF|A-BF|TRKY|BPV|BISON|VEAL')), 20, 'ground meat in a roll: brown and finish a dish'),
        # whole birds and roasts
        (dict(descr='CRNSH|CORNISH'), 65, 'Cornish hens: roast 50-60 min', 'pkg'),
        (dict(descr='QUAIL'), 30, 'quail: roast or grill 20-25 min', 'pkg'),
        (dict(descr=W('TRKY'), form='BREAST WHOLE|BREAST ROAST|^ROAST'), 120, 'whole boneless turkey breast: roast about 2 hours', 'pkg'),
        (dict(form='^WHOLE$|WHOLE ROAST|^ROAST|ROLL ROAST|^POT$'), 90, 'whole chicken or roast: oven 75-100 min', 'pkg'),
        (dict(descr=W('RST')), 90, 'roast: oven or pot 90 min or more', 'pkg'),
        (dict(descr=W('STEW')), 90, 'stew meat: brown and simmer'),
        (dict(form='^CUT$|CUT UP'), 45, 'cut-up whole chicken: bake 40-45 min', 'pkg'),
        (dict(descr=W('PK|PRK'), form='^TENDERLOIN$|TENDERLOIN ROLL|WHOLE TENDER'), 35, 'pork tenderloin: roast 25 min after the oven heats', 'pkg'),
        (dict(descr=W('PK|PRK'), form='^LOIN$|^LOIN FIL|^PIECE LOIN FIL'), 55, 'pork loin or loin filet, 1.5-2 lb: roast 45 min', 'pkg'),
        (dict(form='RIBLET|RIB TIP|SPARE|^RIB$|BACK RIB|RIB SLICED|NECK|FEET'), 120, 'ribs, riblets, neck bones: bake or simmer about 2 hours'),
        # poultry parts
        (dict(descr=W('TRKY'), form='TENDERLOIN'), 25, 'turkey tenderloins: pan or bake 20-25 min'),
        (dict(form=r'BREAST.*(TENDER|STRIP|CUTLET|SLICED|CUBE|CHUNK|DICED)|BREAST TENDER'), 15, 'chicken or turkey tenders, strips, cutlets: pan 12-15 min'),
        (dict(form='BREAST|THIGH', type=BONELESS_T), 25, 'boneless chicken breast or thighs: pan or bake 20-25 min'),
        (dict(form='BREAST (FILLET|FILET|PORTION|PIECE)|(FILLET|PIECE) BREAST|FILLET PORTION|FILLET THIGH'), 25, 'boneless breast fillets or portions: pan or bake 20-25 min'),
        (dict(form='BREAST|THIGH|LEG|DRUM|WING|QUARTER'), 45, 'bone-in chicken parts: bake 40-45 min'),
        # steaks, chops, thin cuts
        (dict(form='SHAVED|THIN SLICED|WAFER'), 10, 'shaved or thin-sliced steak: pan 8-10 min'),
        (dict(type='SHAVED'), 10, 'shaved steak: pan 8-10 min'),
        (dict(form='STEAK|CHOP|FILLET|FILET|MEDALLION|STRIP|SLICE|CUTLET|PIECE|TENDER|LOIN|PORTION|CUBE|DICED'), 15, 'steaks, chops, thin cuts: pan 10-15 min'),
    ]),

    # ------------------------------------------------------------------ packaged meats, deli
    3577: dict(name='BACON-REFRIGERATED', minutes=10, basis='serv4', why='raw sliced bacon: pan 8-10 min', rules=[
        (dict(descr='F-C', variety='TURKEY'), 5, 'fully cooked turkey bacon: crisp in a pan about 5 min'),
        (dict(descr='F-C|PCKD'), 1, 'fully cooked bacon: microwave 30-60 seconds'),
        (dict(brand='^HORMEL', descr=W('MW')), 4, 'microwave bacon in its tray: microwave 3-4 min'),
        (dict(descr='PANCETTA|' + W('PNC')), 6, 'pancetta: fry 5-6 min'),
        (dict(form='THICK'), 13, 'thick-cut bacon: pan 12-14 min, 3-4 min longer than regular slices'),
    ]),
    3574: dict(name='LUNCHMEAT-SLICED-REFRIGERATED', minutes=0, basis='serv4', why='sliced lunch meat: eaten as sold', rules=[
        (dict(type='CANADIAN', style_not='PIZZA'), 4, 'Canadian bacon: heat slices in a pan 3-4 min'),
        (dict(type='HAM STEAK'), 8, 'ham steak: heat in a pan'),
    ]),
    3618: dict(name='LUNCHMEAT-DELI POUCHES-REFRIGERATED', minutes=0, basis='serv4', why='deli pouch lunch meat: eaten as sold', rules=[
        (dict(descr='CN-B|CNDN'), 4, 'Canadian bacon: heat slices in a pan 3-4 min'),
    ]),
    3572: dict(name='SAUSAGE-DINNER', minutes=18, basis='serv4', why='raw dinner sausage links (Italian and similar): pan or grill 16-20 min', rules=[
        (dict(style='^DRY|DRIED'), 0, 'dry sausage: sliced and eaten as sold'),
        (dict(type='PRECOOKED|^COOKED', form='CRUMBLE'), 2, 'precooked sausage crumbles: warm 1-2 min'),
        (dict(descr='LVR PUD|GOETTA|SCRAPPLE|LVRMUSH|BOUDAIN|BOUDIN|' + W('BDN')), 10, 'liver pudding, goetta, boudin (cooked): slice and fry or steam'),
        (dict(descr=W('CZO')), 10, 'Mexican chorizo: remove the casing and fry 8-10 min'),
        (dict(form='RING|ROPE|COIL|STICK'), 10, 'smoked sausage rope or kielbasa: heat in a pan 8-10 min'),
        (dict(flavor='SMOK|NHS'), 10, 'smoked sausage: heat in a pan 8-10 min'),
        (dict(descr=SMOKED_SG), 10, 'smoked, Polish or cooked sausage: heat in a pan 8-10 min'),
        (dict(type='(?<!UN)COOKED|SMOKED|PREBROWNED'), 10, 'cooked sausage: heat in a pan 8-10 min'),
        (dict(style='LOUISIANA|NEW ORLEANS|HEAT'), 10, 'hot links (cooked): heat in a pan 8-10 min'),
        (dict(form='LINK', descr=MIXED_SG), 10, 'beef, mixed-meat or chicken links (sold cooked): heat 8-10 min'),
        (dict(descr='^$', form='LINK', flavor='MOZZARELLA|SPINACH|ASIAGO|APPLE|PINEAPPLE|FETA|ARTICHOKE|ROASTED'), 10, 'chicken sausage links (sold cooked), no description: heat 8-10 min'),
        (dict(brand='MORNINGSTAR|BOCA|GARDEIN|LIGHTLIFE|TOFURKY|FIELD ROAST'), 4, 'meatless sausage (sold cooked): heat 3-4 min'),
        (dict(form='PATT|BURGER'), 9, 'sausage patties, raw (12) or precooked (6) not stated: average'),
        (dict(form='^ROLL|CHUB|SLICED|BLOCK|STRIP'), 12, 'raw sausage roll sliced into patties: pan 12 min'),
        (dict(descr=W('PT|PAT')), 12, 'raw pork patties: pan or grill 12 min'),
        (dict(form='GROUND|BULK|CRUMBLE'), 20, 'ground sausage: brown and finish a dish such as meat sauce'),
    ]),
    3576: dict(name='FRANKFURTERS-REFRIGERATED', minutes=6, basis='serv4', why='hot dogs and franks: boil or grill 5-6 min'),
    3578: dict(name='SAUSAGE-BREAKFAST', minutes=12, basis='serv4', why='raw breakfast sausage links, patties or roll: pan 12 min', rules=[
        (dict(type='PRECOOKED|PREBROWNED|BROWNED|FULLY COOKED|SMOKED', form='CRUMBLE'), 2, 'precooked sausage crumbles: warm 1-2 min'),
        (dict(type='PRECOOKED|PREBROWNED|BROWNED|FULLY COOKED|SMOKED'), 6, 'precooked links or patties: brown in a pan 5-6 min or microwave 1-2'),
    ]),
    3617: dict(name='LUNCHMEAT-NONSLICED-REFRIGERATED', minutes=0, basis='serv4', why='summer sausage, liverwurst, bologna, cubed ham: sliced and eaten as sold', rules=[
        (dict(descr='CN-B|CNDN'), 4, 'Canadian bacon: slice and heat in a pan 3-4 min'),
        (dict(descr='LVRMUSH|SCRAPPLE'), 8, 'livermush or scrapple: slice and fry 8 min'),
        (dict(descr=W('PCT') + '|PANCETTA'), 6, 'pancetta: fry 5-6 min'),
        (dict(form='ROLL', descr=r'TAYLOR|(?:^| )PK (?:HS |ML |TG |HT |SK )*(?:NSL |TN SL )*RL', descr_not='BRAUN|LVRWST|TEAWURST|' + W('SA|BLG|PPRNI|SG|SOP|CZO|HAM'), type_not='SOP|SALAM|DRY|GENOA'), 6, 'pork roll: slice and fry 5-6 min'),
    ]),
    3567: dict(name='BRATWURST & KNOCKWURST', minutes=18, basis='serv4', why='raw bratwurst: grill or pan 16-20 min', rules=[
        (dict(type='(?<!UN)COOKED|SMOKED|STADIUM|HEAT|S-HS-C'), 10, 'cooked or smoked bratwurst: heat 8-10 min'),
        (dict(descr=W('CKD|SC|SMK|HW-SC|STDM')), 10, 'cooked or smoked bratwurst: heat 8-10 min'),
        (dict(product='KNOCK|KNACK|WEISS|METT|CHEDDARWURST|JAGD'), 10, 'knockwurst and other cooked wurst: simmer or grill 8-10 min'),
        (dict(descr='PATTIE|' + W('PT|PAT')), 12, 'raw bratwurst patties: pan or grill 12 min'),
    ]),
    3582: dict(name='FRANKS-COCKTAIL-REFRIGERATED', minutes=8, basis='serv4', why='cocktail smokies and franks (cooked): heat in a pan or sauce 8 min'),
    3581: dict(name='HAM-CANNED-REFRIGERATED', minutes=45, basis='pkg', why='canned ham: 3-5 lb cans baked 60-90 min, 1 lb cans sliced cold'),
    3584: dict(name='BACON-BEEF & CANNED', minutes=10, basis='serv4', why='beef bacon slices: pan 8-10 min'),
    3585: dict(name='HAM PATTIES-CANNED', minutes=6, basis='serv4', why='cooked ham patties: fry 5-6 min'),

    # ------------------------------------------------------------------ canned seafood
    1209: dict(name='SEAFOOD-TUNA-SHELF STABLE', minutes=0, basis='serv4', why='canned or pouch tuna: eaten as sold'),
    1205: dict(name='SEAFOOD - SALMON - CANNED', minutes=0, basis='serv4', why='canned salmon: eaten as sold'),
    1207: dict(name='SEAFOOD - SARDINES - CANNED', minutes=0, basis='serv4', why='sardines: eaten as sold'),
    1202: dict(name='SEAFOOD - REMAINING - CANNED', minutes=0, basis='serv4', why='kippers, mackerel, herring, gefilte fish in jars: eaten as sold'),
    1200: dict(name='SEAFOOD-CLAMS-CANNED', minutes=0, basis='serv4', why='canned clams: added to a sauce or chowder as sold'),
    1204: dict(name='SEAFOOD - OYSTERS - CANNED', minutes=0, basis='serv4', why='canned oysters: eaten as sold'),
    1203: dict(name='SEAFOOD-CRAB-CANNED', minutes=0, basis='serv4', why='canned crab meat: eaten as sold'),
    1208: dict(name='SEAFOOD - SHRIMP - CANNED', minutes=0, basis='serv4', why='canned shrimp: eaten as sold'),
    1199: dict(name='SEAFOOD - ANCHOVIES', minutes=0, basis='serv4', why='anchovies: eaten as sold'),
    1198: dict(name='ANCHOVY PASTE', minutes=0, basis='serv4', why='anchovy paste: a seasoning'),
    6071: dict(name='RBC SHELF STABLE SEAFOOD', minutes=0, basis='serv4', why='shelf-stable seafood: eaten as sold'),

    # ------------------------------------------------------------------ frozen unprepared meat, poultry, seafood
    2681: dict(name='FROZEN POULTRY', minutes=25, basis='serv4', why='frozen boneless chicken breast: pan or bake 20-25 min once thawed', rules=[
        (dict(descr='CORNISH|CRNSH'), 65, 'Cornish hens: roast 50-60 min', 'pkg'),
        (dict(descr='QUAIL'), 30, 'quail: roast or grill 20-25 min', 'pkg'),
        (dict(descr='LIVER|GIZZARD'), 15, 'chicken livers: pan-fry 12-15 min'),
        (dict(descr=W('TRKY|TK'), form='^WHOLE$', type_not='BNS'), 240, 'whole turkey: roast about 4 hours', 'pkg'),
        (dict(descr=W('TRKY|TK'), form='ROAST|^WHOLE$|^BREAST$'), 120, 'turkey breast or boneless turkey roast, 3-5 lb: roast about 2 hours', 'pkg'),
        (dict(form='WING|WGS|JNTWS|DRUMETTE|DRUMMETTE'), 45, 'chicken wings: bake 40-45 min'),
        (dict(descr='WING'), 45, 'chicken wings: bake 40-45 min'),
        (dict(form='^WHOLE$', style_not='BONELESS'), 90, 'whole chicken: roast 75-100 min', 'pkg'),
        (dict(form='GROUND|CHUB'), 20, 'ground turkey or chicken: brown and finish a dish'),
        (dict(form='BURGER|PATT'), 10, 'turkey or chicken burgers: pan or grill 8-10 min'),
        (dict(form='TENDER|STRIP|CUTLET|THIN|SLICED|NUGGET|CHUNK|CUBE|FINGER'), 15, 'chicken tenderloins, strips, cutlets: pan 12-15 min'),
        (dict(style='BONE-IN'), 45, 'bone-in chicken parts: bake 40-45 min'),
        (dict(form='THIGH|DRUM|LEG|QUARTER|SPLIT', style_not='BONELESS'), 45, 'bone-in chicken parts: bake 40-45 min'),
    ]),
    2682: dict(name='SEAFOOD - SHRIMP - UNBREADED - FROZEN', minutes=4, basis='serv4', why='peeled shrimp, raw or cooked not stated: average of cooked (2) and raw (8)', rules=[
        (dict(type=r'(?<!UN)COOKED|STEAMED|GRILLED|BOILED|\bF-C\b'), 2, 'cooked shrimp: thawed and eaten cold or warmed 2-3 min'),
        (dict(descr=W('CKD|CK|F-C')), 2, 'cooked shrimp: thawed and eaten cold or warmed 2-3 min'),
        (dict(type=r'RAW|UNCOOKED|\bRSOT?\b'), 8, 'raw shrimp: pan or boil 5-8 min'),
        (dict(descr=W('RW|RAW')), 8, 'raw shrimp: pan or boil 5-8 min'),
        (dict(type=r'SHELL|HEADLESS|HEAD LESS|\bARG|R-AR'), 8, 'shell-on shrimp (sold raw): pan or boil 5-8 min'),
        (dict(type='DVN', type_not='PEELED|PDTO|PLD'), 8, 'deveined shell-on shrimp (sold raw): pan or boil 5-8 min'),
    ]),
    2683: dict(name='SEAFOOD-FISH-UNBREADED-FROZEN', minutes=15, basis='serv4', why='fish fillets, portions, steaks: pan or broil 12-15 min', rules=[
        (dict(product='GEFILTE'), 90, 'frozen gefilte fish loaf: simmer 90 min', 'pkg'),
        (dict(type=r'SMOKED|\bS-S\b'), 0, 'smoked salmon: eaten as sold'),
        (dict(product='NOVA|LOX|GRAVLAX'), 0, 'smoked salmon, lox: eaten as sold'),
        (dict(form='BURGER|PATT'), 10, 'salmon burgers: pan 8-10 min'),
        (dict(product='CRAWFISH'), 8, 'crawfish tail meat: saute into a dish 5-8 min'),
        (dict(form='^WHOLE$', type_not='BONELESS'), 25, 'whole fish: bake or fry 20-25 min'),
    ]),
    2688: dict(name='FROZEN MEAT - GROUND BEEF', minutes=20, basis='serv4', why='frozen ground beef: brown and finish a dish such as tacos or meat sauce', rules=[
        (dict(form='PATT|BURGER|SLIDER'), 10, 'frozen beef patties: pan or grill 8-10 min'),
        (dict(descr=W('PT|BGR|PAT|PATS|PTS')), 10, 'frozen beef patties: pan or grill 8-10 min'),
    ]),
    2607: dict(name='SEAFOOD - FISH - BREADED - FROZEN', minutes=22, basis='serv4', why='breaded fish sticks or fillets: oven 12-18 min after it heats'),
    2643: dict(name='SEAFOOD - SHRIMP - BREADED - FROZEN', minutes=22, basis='serv4', why='breaded shrimp: oven 10-14 min after it heats'),
    2686: dict(name='SEAFOOD-REMAINING-UNBREADED-FROZEN', minutes=8, basis='serv4', why='scallops, mussels, clams, squid, cooked crab: pan or steam 5-8 min', rules=[
        (dict(type='IMITATION|SURIMI'), 0, 'imitation crab or lobster: eaten as sold'),
        (dict(product='POLLOCK & CRAB|SURIMI'), 0, 'imitation crab: eaten as sold'),
        (dict(product='LOBSTER', product_not='LANGOSTINO', type_not='(?<!UN)COOKED|F-C'), 15, 'raw lobster tails: split and broil or boil 10-15 min'),
        (dict(product='OCTOPUS'), 60, 'octopus: simmer 45-60 min'),
    ]),
    2690: dict(name='FROZEN MEAT - SANDWICH STEAK', minutes=8, basis='serv4', why='paper-thin frozen sandwich steaks: pan 1-2 min a batch, 8 min for four'),
    2645: dict(name='SEAFOOD-CRAB-UNBREADED-FROZEN', minutes=8, basis='serv4', why='cooked crab legs or clusters: steam or boil 6-8 min', rules=[
        (dict(type='IMITATION|SURIMI'), 0, 'imitation crab: eaten as sold'),
        (dict(form='FLAKE|CHUNK|SHRED|STICK|SALAD|LUMP|CHOPPED'), 0, 'imitation crab or crab meat: eaten as sold'),
        (dict(product='MEAT'), 0, 'crab meat: eaten as sold'),
    ]),
    2687: dict(name='FROZEN MEAT-BEEF STEAK', minutes=15, basis='serv4', why='frozen steaks: pan or grill 10-15 min', rules=[
        (dict(form='BURGER|PATT'), 10, 'chopped steak patties: pan or grill 8-10 min'),
        (dict(form='THIN|WAFER|SHAVED|CHOPPED|^SLICED|STRIP'), 8, 'thin-sliced sandwich steak: pan 6-8 min'),
        (dict(brand='STEAK.?EZE|FAST FIXIN'), 8, 'thin-sliced sandwich steak: pan 6-8 min'),
    ]),
    2680: dict(name='FROZEN MEAT - REMAINING', minutes=15, basis='serv4', why='sliced liver and other cuts: pan 12-15 min', rules=[
        (dict(descr='CHTRLNG|CHITTERLING|CHITLIN|TRIPE'), 180, 'chitterlings or tripe: clean and simmer about 3 hours', 'pkg'),
        (dict(form='PATT|BURGER'), 10, 'patties: pan or grill 8-10 min'),
        (dict(form='GROUND'), 20, 'ground meat: brown and finish a dish'),
        (dict(descr='STEW'), 90, 'stew meat: brown and simmer'),
    ]),
    2679: dict(name='SEAFOOD-REMAINING-BREADED-FROZEN', minutes=22, basis='serv4', why='breaded clams, crab cakes, scallops: oven 12-18 min after it heats'),
    2689: dict(name='FROZEN MEAT - PORK', minutes=15, basis='serv4', why='frozen pork chops and other cuts: pan 12-15 min', rules=[
        (dict(form='CHITTERLING'), 180, 'chitterlings: clean and simmer about 3 hours', 'pkg'),
        (dict(form='GROUND'), 20, 'ground pork: brown and finish a dish'),
        (dict(form='PATT|BURGER|FRITTER'), 10, 'pork patties or fritters: pan 8-10 min'),
        (dict(form='RIB'), 120, 'pork ribs, riblets, rib tips: bake about 2 hours'),
    ]),
    2691: dict(name='FROZEN MEAT - VEAL', minutes=10, basis='serv4', why='veal patties: pan 8-10 min', rules=[
        (dict(descr='GROUND|' + W('GRD')), 20, 'ground veal: brown and finish a dish'),
        (dict(descr='SLICED|CUTLET|' + W('STKS|CHP|VLCS')), 15, 'veal slices, cutlets, chops: pan 10-15 min'),
    ]),
    6065: dict(name='RBC FROZEN AND REFRIGERATED SEAFOOD', minutes=12, basis='serv4', why='seafood, kind not stated: average of fish fillets (15) and shrimp (8)', rules=[
        (dict(descr='SHRP'), 8, 'raw shrimp: pan or boil 5-8 min'),
        (dict(brand='TRANS OCEAN'), 0, 'imitation crab (the brand sells surimi): eaten as sold'),
    ]),
}
