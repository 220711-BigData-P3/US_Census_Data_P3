import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

plt.close("all")

path = "../"

# COLUMN NAMES IN FILE
races = [
    "White",
    "Black",
    "NativeAm",
    "Asian",
    "PacIslander",
    "Other",
    "TwoOrMore"
    ]

# NEW COLUMN NAMES, BASED ON CENSUS LANGUAGE
races2 = [
    "White",
    "Black and African American",
    "American Indian and Alaska Native",
    "Asian",
    "Native Hawaiian and Pacific Islander",
    "Other",
    "Two or More Races"
    ]

hisp = [] # NEW LABELS FOR hispUS DATA
nonhisp = [] # NEW LABELS FOR nonhispUS DATA
for elem in races2:
    hisp.append("Hispanic " + elem)
    nonhisp.append("Non-Hispanic " + elem)

############################################################################################################################################

# Hispanic population totals by racial category (hispUS.csv)
df1 = pd.read_csv(path + "hispUS.csv", index_col="Year")[races]
# Non-Hispanic population totals by racial category (nonhispUS.csv)
df2 = pd.read_csv(path + "nonhispUS.csv", index_col="Year")[races]
# All categories, Hispanic and Non-Hispanic population totals separated
df1.columns = hisp # Rename columns
df2.columns = nonhisp # Rename columns
allCats = df1.join(df2).transpose() # Want Years as columns

# PIE CHARTS
def autopct(pct):
    if pct > 10:
        return f'{pct:.1f}%'
    elif pct > 2:
        return f'{pct:.2f}%'
    else:
        return ''

# LEGEND
leg = allCats.plot.pie(y=2000, figsize=(15,6))
plt.legend(title = 'Key', bbox_to_anchor=(-0.1, 0.8))

# COLOR ORDER TO MATCH LEGEND
allCats2010 = ['tab:blue','tab:orange','tab:green','tab:red','tab:purple','tab:brown','tab:pink',
	           'tab:grey','tab:olive','tab:blue','tab:cyan','tab:orange','tab:green','tab:red']
allCats2020 = ['tab:blue','tab:orange','tab:red','tab:pink','tab:purple','tab:green','tab:brown',
	           'tab:grey','tab:cyan','tab:blue','tab:olive','tab:orange','tab:green','tab:red']
               
# SET PLOT SIZE
size = (10,8)

# 2000
allCats.sort_values(by=[2000], inplace=True, ascending=False) # Sort by 2020 values
ax = allCats.plot.pie(y=2000, labels=None, figsize=size, autopct=autopct, fontsize=12, title="2000", legend=False)
y_axis = ax.axes.get_yaxis()
y_axis.set_visible(False) # Remove title from y-axis side of plot

# 2010
allCats.sort_values(by=[2010], inplace=True, ascending=False)
ax = allCats.plot.pie(y=2010, labels=None, figsize=size, autopct=autopct, fontsize=12, colors= allCats2010, title="2010", legend=False)
y_axis = ax.axes.get_yaxis()
y_axis.set_visible(False)

# 2010
allCats.sort_values(by=[2020], inplace=True, ascending=False)
ax = allCats.plot.pie(y=2020, labels=None, figsize=size, autopct=autopct, fontsize=12, colors= allCats2020, title="2020", legend=False)
y_axis = ax.axes.get_yaxis()
y_axis.set_visible(False)

# RENAME COLUMNS FOR OPERATIONS
allCats.columns = ["C2000", "C2010", "C2020"]
# CREATE NEW COLUMNS TO SHOW CHANGE B/W CENSUSES
allCats.eval("Change0010 = C2010 - C2000", inplace=True)
allCats.eval("Change1020 = C2020 - C2010", inplace=True)
allCats.eval("Change0020 = C2020 - C2000", inplace=True)
# % CHANGES
allCats.eval("ChangePerc0010 = C2010/C2000 - 1", inplace=True)
allCats.eval("ChangePerc1020 = C2020/C2010 - 1", inplace=True)
allCats.eval("ChangePerc0020 = C2020/C2000 - 1", inplace=True)
# CREATE DF FOR RAW CHANGES
allCatsChange = allCats[["Change0010", "Change1020", "Change0020"]]
allCatsChange.columns = ["2000-2010", "2010-2020", "2000-2020"]
# CREATE DF FOR % CHANGES
allCatsChangePerc = allCats[["ChangePerc0010", "ChangePerc1020", "ChangePerc0020", "C2020"]]
# MULT. BY 100 FOR USE AS PERCENTAGES
allCatsChangePerc.eval("ChPerc0010 = ChangePerc0010 * 100", inplace=True)
allCatsChangePerc.eval("ChPerc1020 = ChangePerc1020 * 100", inplace=True)
allCatsChangePerc.eval("ChPerc0020 = ChangePerc0020 * 100", inplace=True)
allCatsChangePerc = allCatsChangePerc[["ChPerc0010", "ChPerc1020", "ChPerc0020", "C2020"]]
allCatsChangePerc.columns = ["2000-2010", "2010-2020", "2000-2020", "C2020"]

# print(allCats)

############################################################################################################################################

# set colors for graphs
colors = ['blue', 'orange', 'green', 'red', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan', 'maroon', 'chartreuse', 'teal', 'bisque']
colors.reverse() # reverse for horizontal bar graph for formatting purposes

# font settings for graphs
font = {'family' : 'sans-serif',
        'weight' : 'normal',
        'size'   : 14}

matplotlib.rc('font', **font)

############################################################################################################################################

# 2020 population graph
allCats.sort_values(by="C2020", inplace=True, ascending=True)
ax = allCats.plot.barh(y="C2020", figsize=(16,8), color=colors,
                       title="US Population by Category, 2020 Census")
ax.set_xscale('log')
# FOR LOOP TO ADD COLUMN LABELS
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

############################################################################################################################################

# 2000-2010 % change graph
allCatsChangePerc.sort_values(by="2000-2010", inplace=True, ascending=False)
ax = allCatsChangePerc.plot.bar(y="2000-2010", figsize=(16,8), color=colors,
                                title="Population % Change 2000-2010")
ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter()) # SHOW PERCENTAGES ON Y AXIS
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

plt.show()
