import re


punctuation = re.compile(r"[\W +]+")


number = re.compile(r"\d+")

invalid_prefixes = set([
"RESCUE", "FISHFARMS", "TLF ", "TEL ", "CALL ", "PHONE", "FISHING", "MOB ",
"SAR ", "SEARCH AND RESCUE", "POLICE", "ON DUTY", "WORKING", "SURVEYING", "SEA TRIAL", 
"CH ", "VHF CH", "PESCA ", "DREDGE "
])


invalid_suffixes = set([
"FOR ORDER", "TOWING", "CRUISING", "OIL FIELDS", "OIL FIELD", "DREDGE", "PARADE", "TRADE", "WORK", " TRIAL",
"PESCA", "PECHE", "OIL FIELDS", "HARBOR DUTY", " ESCORT", "SAILING"
    ])


known_false_destinations = set([
'CITY',
'B # A',
'AN',
'NULL',
'T #NKCL KR V',
'A ARO',
'VIGO MOANA VIGO',
'BDAM VIA NOK',
'GILLNETTER #',
'OIL AND GAS ACTIVITY',
'# #',
'HOME',
'A B C O',
'SOUTH FLORIDA',
'# T E D JT',
'O P',
'M Y SPACE OR SHORE',
'OWER BY NAVALMARINE',
'A#B #GD E',
'Y B AND Y YY AND FY AND X',
'# # N # # E',
'ON THE BAY',
'PLY PTOWN PLY',
'SPLIT DUBROVNIK SPLI',
'#O#',
'BBBBBBBBB',
'# # #',
'ON THE BAY',
'NO DESTINATION',
'CLASSIFIED',
])

def normalize(x):
    x = (x
        .upper()
        .replace('&', ' AND '))
    return punctuation.sub(' ', x).strip()

def is_valid_name(x):
    x = number.sub('#', x)
    if len(x) <= 1:
        return False
    for p in invalid_prefixes:
        if x.startswith(p):
            return False
    for s in invalid_suffixes:
        if x.endswith(s):
            return False
    if x in known_false_destinations:
        return False
    return True


def normalized_valid_names(seq):
    for x in seq:
        x = normalize(x)
        if is_valid_name(x):
            yield x

