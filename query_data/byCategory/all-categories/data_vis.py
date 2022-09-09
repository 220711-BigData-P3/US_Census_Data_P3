import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

plt.close("all")

path = "../"

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
# Non-Hispanic population totals by racial category
df2 = pd.read_csv(path + "nonhispUS.csv", index_col="Year")[races]
nonhispUS = df2.transpose()
# All categories, Hispanic and Non-Hispanic population totals separated
df1.columns = hisp
df2.columns = nonhisp
allCats = df1.join(df2).transpose()

############################################################################################################################################
'''---------------------------------------- All Racial Categories ----------------------------------------'''
# All categories, Hispanic and Non-Hispanic population totals separated
allCats2010 = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple','tab:brown','tab:pink',
	           'tab:grey','tab:olive','tab:blue','tab:cyan','tab:orange','tab:green','tab:red']
allCats2020 = ['tab:blue','tab:orange','tab:red','tab:pink','tab:purple','tab:green','tab:brown',
	           'tab:grey','tab:cyan','tab:blue','tab:olive','tab:orange','tab:green','tab:red']
size = (10,10)
labels = []
for elem in allCats2010:
    labels.append('')
allCats.columns = ["C2000", "C2010", "C2020"]
allCats.eval("Change0010 = C2010 - C2000", inplace=True)
allCats.eval("Change1020 = C2020 - C2010", inplace=True)
allCats.eval("Change0020 = C2020 - C2000", inplace=True)
allCats.eval("ChangePerc0010 = C2010/C2000 - 1", inplace=True)
allCats.eval("ChangePerc1020 = C2020/C2010 - 1", inplace=True)
allCats.eval("ChangePerc0020 = C2020/C2000 - 1", inplace=True)
allCatsChange = allCats[["Change0010", "Change1020", "Change0020"]]
allCatsChange.eval("AbsChange = abs(Change0010) + abs(Change1020)", inplace=True)
allCatsChange.columns = ["2000-2010", "2010-2020", "2000-2020", "AbsChange"]
allCatsChangePerc = allCats[["ChangePerc0010", "ChangePerc1020", "ChangePerc0020", "C2020"]]
allCatsChangePerc.eval("ChPerc0010 = ChangePerc0010 * 100", inplace=True)
allCatsChangePerc.eval("ChPerc1020 = ChangePerc1020 * 100", inplace=True)
allCatsChangePerc.eval("ChPerc0020 = ChangePerc0020 * 100", inplace=True)
allCatsChangePerc.eval("AbsChange = abs(ChPerc0010) + abs(ChPerc1020)", inplace=True)
allCatsChangePerc = allCatsChangePerc[["ChPerc0010", "ChPerc1020", "ChPerc0020", "C2020", "AbsChange"]]
allCatsChangePerc.columns = ["2000-2010", "2010-2020", "2000-2020", "C2020", "AbsChange"]

# print(allCats)
# set colors for graphs
colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan', 'maroon', 'chartreuse', 'teal', 'bisque']
colors.reverse() # reverse for horizontal bar graph for formatting purposes
font = {'family' : 'sans-serif',
        'weight' : 'normal',
        'size'   : 14}

matplotlib.rc('font', **font)

# 2020 population graph
allCats.sort_values(by="C2020", inplace=True, ascending=True)
ax = allCats.plot.barh(y="C2020", figsize=(16,8), color=colors,
                       title="US Population by Category, 2020 Census")
ax.set_xscale('log')
for p in ax.patches:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    ax.annotate(f'{width:,.0f}', (x + width/2, y + height*1.02), ha='center', va='bottom', fontsize=8)
y_axis = ax.axes.get_yaxis()
y_axis.set_visible(False) # Use legend instead of y axis
vals = list(allCats.index.values)
vals.reverse()
revpatches = []
for elem in ax.patches:
    revpatches.append(elem)
revpatches.reverse()
plt.legend(labels=vals, handles=revpatches,
           borderaxespad=0, fontsize=11,
           frameon=False)

# reverse colors list back to original
colors.reverse()

# 2000-2010 change graph
allCatsChange.sort_values(by="2000-2010", inplace=True, ascending=False)
ax = allCatsChange.plot.bar(y="2000-2010", figsize=(16,8), color=colors,
                            title="Population Change 2000-2010")
plt.ticklabel_format(axis='y', style='plain')
for p in ax.patches:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    ax.annotate(f'{height:+,.0f}', (x + width/2, y + height*1.02), ha='center', fontsize=9)
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChange.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

# 2010-2020 change graph
ax = allCatsChange.plot.bar(y="2010-2020", figsize=(16,8), color=colors,
                            title="Population Change 2010-2020")
plt.ticklabel_format(axis='y', style='plain')
for p in ax.patches:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    ax.annotate(f'{height:+,.0f}', (x + width/2, y + height*1.02), ha='center', fontsize=9)
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChange.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

# 2000-2020 change graph
allCatsChange.sort_values(by="2000-2020", inplace=True, ascending=False)
ax = allCatsChange.plot.bar(y="2000-2020", figsize=(16,8), color=colors,
                            title="Population Change 2000-2020")
plt.ticklabel_format(axis='y', style='plain')
for p in ax.patches:
    width = p.get_width()
    height = p.get_height()
    x, y = p.get_xy()
    ax.annotate(f'{height:+,.0f}', (x + width/2, y + height*1.02), ha='center')
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChange.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

# 2000-2010 % change graph
allCatsChangePerc.sort_values(by="2000-2010", inplace=True, ascending=False)
ax = allCatsChangePerc.plot.bar(y="2000-2010", figsize=(16,8), color=colors,
                                title="Population % Change 2000-2010")
ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())
for p in ax.patches:
    width = p.get_width()
    if abs(p.get_height()) > 100:
        height = f'{p.get_height():+.0f}%'
    elif abs(p.get_height()) > 10:
        height = f'{p.get_height():+.1f}%'
    else:
        height = f'{p.get_height():+.2f}%'
    x, y = p.get_xy()
    ax.annotate(height, (x + width/2, y + p.get_height()*1.02), ha='center')
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChangePerc.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

# 2010-2020 % change graph
ax = allCatsChangePerc.plot.bar(y="2010-2020", figsize=(16,8), color=colors,
                                title="Population % Change 2010-2020")
ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())
for p in ax.patches:
    width = p.get_width()
    if abs(p.get_height()) > 100:
        height = f'{p.get_height():+.0f}%'
    elif abs(p.get_height()) > 10:
        height = f'{p.get_height():+.1f}%'
    else:
        height = f'{p.get_height():+.2f}%'
    x, y = p.get_xy()
    ax.annotate(height, (x + width/2, y + p.get_height()*1.02), ha='center')
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChangePerc.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

# 2000-2020 % change graph
allCatsChangePerc.sort_values(by="2000-2020", inplace=True, ascending=False)
ax = allCatsChangePerc.plot.bar(y="2000-2020", figsize=(16,8), color=colors,
                                title="Population % Change 2000-2020")
ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter())
plt.xticks(rotation=30, ha='right')
for p in ax.patches:
    width = p.get_width()
    if abs(p.get_height()) > 100:
        height = f'{p.get_height():+.0f}%'
    elif abs(p.get_height()) > 10:
        height = f'{p.get_height():+.1f}%'
    else:
        height = f'{p.get_height():+.2f}%'
    x, y = p.get_xy()
    ax.annotate(height, (x + width/2, y + p.get_height()*1.02), ha='center')
x_axis = ax.axes.get_xaxis()
x_axis.set_visible(False) # Use legend instead of x axis
plt.legend(labels=list(allCatsChangePerc.index.values),
           handles=ax.patches, bbox_to_anchor=(1.04, 0.5),
           loc='center left', borderaxespad=0, fontsize=15,
           frameon=False)

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
