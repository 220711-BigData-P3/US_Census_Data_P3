import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

plt.close("all")

path = "US_Census_Data_P3/query_data/byCategory/"

races = [
    "White",
    "Black",
    "NativeAm",
    "Asian",
    "PacIslander",
    "Other",
    "TwoOrMore"
    ]

races_plus = [
    "Hispanic",
    "White",
    "Black",
    "NativeAm",
    "Asian",
    "PacIslander",
    "Other",
    "TwoOrMore"
    ]

races2 = [
    "White",
    "Black and African American",
    "American Indian and Alaska Native",
    "Asian",
    "Native Hawaiian and Pacific Islander",
    "Other",
    "Two or More Races"
    ]

races_plus2 = [
    "Hispanic",
    "White",
    "Black and African American",
    "American Indian and Alaska Native",
    "Asian",
    "Native Hawaiian and Pacific Islander",
    "Other",
    "Two or More Races"
    ]

hisp = []
nonhisp = []
for elem in races2:
    hisp.append("Hispanic " + elem)
    nonhisp.append("Non-Hispanic " + elem)

# Hispanic population totals by racial category
df1 = pd.read_csv(path + "hispUS.csv", index_col="Year")[races]
hispUS = df1.transpose()
# Hispanic population total of 1 race and of multiple races
hispOneOrMore = pd.read_csv(path + "hispUS.csv", index_col="Year")[["OneRace", "TwoOrMore"]].transpose()
# Non-Hispanic population totals by racial category
df2 = pd.read_csv(path + "nonhispUS.csv", index_col="Year")[races]
nonhispUS = df2.transpose()
# Non-Hispanic population total of 1 race and of multiple races
nonhispOneOrMore = pd.read_csv(path + "nonhispUS.csv", index_col="Year")[["OneRace", "TwoOrMore"]].transpose()
# Hispanic and Non-Hispanic population totals
hispVnonshisp = pd.read_csv(path + "usData_1.csv", index_col="Year")[["Hispanic", "NonHispanic"]].transpose()
# Single-Racial and Multi-Racial population totals
oneVtwoplus = pd.read_csv(path + "usData_1.csv", index_col="Year")[["OneRace", "TwoOrMore"]].transpose()
# Hispanic population total with Non-Hispanic population totals for all racial categories
catsPlus = pd.read_csv(path + "usData_2.csv", index_col="Year")[races_plus].transpose()
# All categories, Hispanic and Non-Hispanic population totals separated
df1.columns = hisp
df2.columns = nonhisp
allCats = df1.join(df2).transpose()

# print(allCats)
# print(hispUS)
# print(nonhispUS)

############################################################################################################################################
''' ---------------------------------------- Hispanic vs. Non-Hispanic ---------------------------------------- 
colors = ['indianred', 'turquoise']
hispVnonshisp.plot.pie(subplots=True, figsize=(20,6), autopct='%1.0f%%', colors=colors, title="Hispanic vs. Non-Hispanic", legend=False)'''

############################################################################################################################################
''' ---------------------------------------- One vs. 2+ Races ---------------------------------------- 
colors = ['indianred', 'turquoise']
oneVtwoplus.plot.pie(subplots=True, figsize=(20,6), autopct='%1.0f%%', colors=colors, title="Number of Racial Categories Identified With", legend=False)'''
############################################################################################################################################
''' ---------------------------------------- All Racial Categories ---------------------------------------- 
# Hispanic total, non-hispanic totals for race
catsPlus.sort_values(by=[2000], inplace=True, ascending=False)
catsPlus.plot.pie(subplots=True, figsize=(18,6), autopct='%1.0f%%', legend=False)'''
# All categories, Hispanic and Non-Hispanic population totals separated
allCats2010 = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple','tab:brown','tab:pink',
	           'tab:grey','tab:olive','tab:blue','tab:cyan','tab:orange','tab:green','tab:red']
allCats2020 = ['tab:blue','tab:orange','tab:red','tab:pink','tab:purple','tab:green','tab:brown',
	           'tab:grey','tab:cyan','tab:blue','tab:olive','tab:orange','tab:green','tab:red']
size = (10,10)
labels = []
for elem in allCats2010:
    labels.append('')
allCats.sort_values(by=[2000], inplace=True, ascending=False)
# allCats.plot.pie(y=2000, labels=labels, figsize=size, autopct='%1.0f%%', legend=False)
# leg = allCats.plot.pie(y=2000, figsize=(15,6), autopct='%1.0f%%')
# plt.legend(title = 'Legend', bbox_to_anchor=(-0.1, 0.8))
# allCats.sort_values(by=[2010], inplace=True, ascending=False)
# allCats.plot.pie(y=2010, labels=labels, figsize=size, autopct='%1.0f%%', colors= allCats2010, legend=False)
# allCats.sort_values(by=[2020], inplace=True, ascending=False)
# allCats.plot.pie(y=2020, labels=labels, figsize=size, autopct='%1.0f%%', colors= allCats2020, legend=False)
allCats.columns = ["C2000", "C2010", "C2020"]
allCats.eval("Change0010 = C2010 - C2000", inplace=True)
allCats.eval("Change1020 = C2020 - C2010", inplace=True)
allCats.eval("Change0020 = C2020 - C2000", inplace=True)
allCats.eval("ChangePerc0010 = C2010/C2000 - 1", inplace=True)
allCats.eval("ChangePerc1020 = C2020/C2010 - 1", inplace=True)
allCats.eval("ChangePerc0020 = C2020/C2000 - 1", inplace=True)
allCatsChange = allCats[["Change0010", "Change1020", "Change0020"]]
allCatsChange.columns = ["2000-2010", "2010-2020", "2000-2020"]
allCatsChangePerc = allCats[["ChangePerc0010", "ChangePerc1020", "ChangePerc0020"]]
allCatsChangePerc.columns = ["2000-2010", "2010-2020", "2000-2020"]
labs = []
for elem in hisp:
    labs.append(elem)
for elem in nonhisp:
    labs.append(elem)
ax = allCatsChange.plot.bar(y=["2000-2010", "2010-2020"], figsize=(16,8), title="Population Change")
plt.ticklabel_format(axis='y', style='plain')
plt.xticks(rotation=30, ha='right')
# for p in ax.patches:
#     width = p.get_width()
#     height = p.get_height()
#     x, y = p.get_xy()
#     ax.annotate(f'{height:,.0f}', (x + width/2, y + height*1.02), ha='center')
# allCatsChange.plot.bar(y="2000-2020", figsize=(16,8), title="Population Change 2000-2020", legend=False)
# plt.ticklabel_format(axis='y', style='plain')
# y = list(allCatsChange["2000-2020"])
# bars = plt.bar(x=labs, height=y)
# plt.bar_label(bars, labels=[f'{x:,.0f}' for x in y])
# plt.xticks(rotation=30, ha='right')
# ax = allCatsChangePerc.plot.bar(y=["2000-2010", "2010-2020"], figsize=(18,10), title="Population % Change")
# plt.xticks(rotation=30, ha='right')
# for p in ax.patches:
#     width = p.get_width()
#     if abs(p.get_height()) > 1:
#         height = f'{100*p.get_height():.0f}%'
#     else:
#         height = f'{100*p.get_height():.1f}%'
#     x, y = p.get_xy()
#     ax.annotate(height, (x + width/2, y + p.get_height()*1.02), ha='center')
# allCatsChangePerc.plot.bar(y="2000-2020", figsize=(16,8), title="Population % Change 2000-2020", legend=False)
# y = list(allCatsChangePerc["2000-2020"])
# bars = plt.bar(x=labs, height=y)
# labels = []
# for x in y:
#     if abs(x) > 1:
#         labels.append(f'{100*x:.0f}%')
#     elif abs(x) > 0.1:
#         labels.append(f'{100*x:.1f}%')
#     else:
#         labels.append(f'{100*x:.2f}%')
# plt.bar_label(bars, labels=labels)
# # plt.bar_label(bars)
# plt.xticks(rotation=30, ha='right')
############################################################################################################################################
''' ---------------------------------------- Hispanic Pop. by Race ---------------------------------------- 
hispUS.sort_values(by=[2000], inplace=True, ascending=False)
hispUS.plot.pie(y=2000, figsize=(6,6), explode=(0.05,0.05,0,0,0,0,0), autopct='%1.0f%%', legend=False)
hispUS.plot.pie(y=2010, figsize=(6,6), explode=(0.05,0.05,0,0,0,0,0), autopct='%1.0f%%', legend=False)
hispUS.sort_values(by=[2020], inplace=True, ascending=False)
colors2020 = ['tab:orange', 'tab:green', 'tab:blue', 'tab:purple', 'tab:red', 'tab:brown', 'tab:pink']
hispUS.plot.pie(y=2020, figsize=(6,6), explode=(0.05,0.05,0.05,0,0,0,0), autopct='%1.0f%%', colors=colors2020, legend=False)'''
############################################################################################################################################
''' ---------------------------------------- One or More Races ---------------------------------------- 
colors = ['indianred', 'turquoise']
hispOneOrMore.plot.pie(subplots=True, figsize=(20,6), autopct='%1.0f%%', colors=colors, title="Hispanic Population by No. of Races", legend=False)'''
############################################################################################################################################
''' ---------------------------------------- Non-Hispanic Pop. by Race ---------------------------------------- 
nonhispUS.sort_values(by=[2000], inplace=True, ascending=False)
nonhispUS.plot.pie(y=2000, figsize=(6,6), explode=(0.05,0,0,0,0,0,0), autopct='%1.0f%%', title="Non-Hispanic Population", legend=False)
nonhispUS.plot.pie(y=2010, figsize=(6,6), explode=(0.05,0,0,0,0,0,0), autopct='%1.0f%%', title="Non-Hispanic Population", legend=False)
nonhispUS.plot.pie(y=2020, figsize=(6,6), explode=(0.05,0,0,0,0,0,0), autopct='%1.0f%%', title="Non-Hispanic Population", legend=False)'''

plt.show()
