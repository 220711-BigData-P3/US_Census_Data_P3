state_abbrevs = [
    "ak",
    "al",
    "ar",
    "az",
    "ca",
    "co",
    "ct",
    "dc",
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

with open("al2020.pl/file1headers.csv", "r") as f1:
    for line in f1:
        file1headers = line.strip()

with open("al2020.pl/geoheaders.csv", "r") as f2:
    for line in f2:
        geoheaders = line.strip()

state_geos = []
state_pops = []

for state in state_abbrevs:
    with open(state + "geo2020.pl", "r") as f3:
        for line in f3:
            algeo = line.strip()
            break

    with open(state + "000012020.pl", "r") as f4:
        for line in f4:
            alpop = line.strip()
            break

with open("2020geo.csv", "w") as newf1:
    newf1.write(geoheaders)
    for row in state_geos:
        newf1.write("\n" + row)

with open("2020pop.csv", "w") as newf2:
    newf2.write(file1headers)
    for row in state_pops:
        newf2.write("\n" + row)