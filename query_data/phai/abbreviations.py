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
        "hi"
    ]
    return west
def getsoutheast():
    southeast = [
        "AL",
        "AR",
        "FL",
        "GA",
        "KY",
        "LA",
        "MD",
        "MS",
        "NC",
        "SC",
        "TN",
        "VA",
        "WV"
    ]
    return southeast
def getmidwest():
    midwest = ["IA", "KS", "MO", "NE", "ND", "SD", "IL", "IN", "MI", "MN", "OH", "WI"]
    return midwest