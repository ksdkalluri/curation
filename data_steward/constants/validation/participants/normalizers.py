import re

# state abbreviations.  used to validate state abbreviations
STATE_ABBREVIATIONS = [
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il',
    'in', 'ia', 'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt',
    'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri',
    'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy',
    # Commonwealth/Territory: 	Abbreviation:
    'as', 'dc', 'fm', 'gu', 'mh', 'mp', 'pw', 'pr', 'vi',
    # Military "State": 	Abbreviation:
    'aa', 'ae', 'ap',
]

AT = '@'

NUMERIC_ENDINGS_REGEX = '(\d+)(st|nd|rd|th)'

COMPILED_NUMERIC_ENDINGS_REGEX = re.compile(NUMERIC_ENDINGS_REGEX)

ALPHA_NUMERIC = '(\d+)[a-zA-Z]+'

COMPILED_ALPHA_NUMERIC = re.compile(ALPHA_NUMERIC)

ADDRESS_ABBREVIATIONS = {
    'aly': 'alley',
    'anx': 'annex',
    'apt': 'apartment',
    'ave': 'avenue',
    'bch': 'beach',
    'bldg': 'building',
    'blvd': 'boulevard',
    'bnd': 'bend',
    'btm': 'bottom',
    'cir': 'circle',
    'ct': 'court',
    'co': 'county',
    'ctr': 'center',
    'dr': 'drive',
    'e': 'east',
    'expy': 'expressway',
    'hts': 'heights',
    'hwy': 'highway',
    'is': 'island',
    'jct': 'junction',
    'lk': 'lake',
    'ln': 'lane',
    'mtn': 'mountain',
    'n': 'north',
    'ne': 'northeast',
    'num': 'number',
    'nw': 'northwest',
    'pkwy': 'parkway',
    'pl': 'place',
    'plz': 'plaza',
    'po': 'post office',
    'rd': 'road',
    'rdg': 'ridge',
    'rr': 'rural route',
    'rm': 'room',
    's': 'south',
    'se': 'southeast',
    'sq': 'square',
    'st': 'street',
    'str': 'street',
    'sta': 'station',
    'ste': 'suite',
    'sw': 'southwest',
    'ter': 'terrace',
    'tpke': 'turnpike',
    'trl': 'trail',
    'vly': 'valley',
    'w': 'west',
    'way': 'way',
}

CITY_ABBREVIATIONS = {
    'st':  'saint',
    'afb': 'air force base',
}
