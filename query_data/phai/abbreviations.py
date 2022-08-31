def getstates():
    state_abbrevs = [
    "ak",
    "al",
    "ar",
    "az",
    "ca",
    "co",
    "ct",
    "de",
    "fl",
    "ga",
    "hi",
    "ia",
    "id",
    "il",
    "in",
    "ks",
    "ky",
    "la",
    "ma",
    "md",
    "me",
    "mi",
    "mn",
    "mo",
    "ms",
    "mt",
    "nc",
    "nd",
    "ne",
    "nh",
    "nj",
    "nm",
    "nv",
    "ny",
    "oh",
    "ok",
    "or",
    "pa",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "va",
    "vt",
    "wa",
    "wi",
    "wv",
    "wy",
]
#US, DC, PR, are not states
    return state_abbrevs
def getnortheast():
    northeast = ["ct", "de" "ma", "me", "nh", "nj", "ny", "pa", "ri", "vt"]
    return northeast
def getsouthwest():
    southwest = ["az", "ca", "co", "nv", "nm", "ok", "tx", "ut"]
    return southwest
def getwest():
    west = [
        "ak",
        "id",
        "mt",
        "wy",
        "wa",
        "or",
        "hi",
    ]
    return west
def getsoutheast():
    southeast = [
        "al",
        "ar",
        "fl",
        "ga",
        "ky",
        "la",
        "md",
        "ms",
        "nc",
        "sc",
        "tn",
        "va",
        "wv",
    ]
    return southeast
def getmidwest():
    midwest = ["ia", "ks", "mo", "ne", "nd", "sd", "il", "in", "mi", "mn", "oh", "wi"]
    return midwest