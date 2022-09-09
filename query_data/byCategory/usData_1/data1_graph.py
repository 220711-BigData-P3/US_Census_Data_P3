import matplotlib.pyplot as plt
import pandas as pd
import csv

def main():
    fname = "usData_1.csv"

    # Most Graphs
    plt.rcParams["figure.figsize"] = [15, 7]
    # Pie Charts
    # plt.rcParams["figure.figsize"] = [7, 7]

    plt.rcParams["figure.autolayout"] = True

    df = pd.read_csv(fname, header=0)

    df1 = df.loc[:, ~df.columns.isin(['Total', 'Hispanic', 'NonHispanic'])]
    races = [
    "Total",
    "OneRace",
    "White",
    "Black",
    "NativeAm",
    "Asian",
    "PacIslander",
    "Other",
    "TwoOrMore"
    ]
    df2 = df.loc[:, ~df.columns.isin(races)]
    

    ## Bar graph set up
    # my_colors = ['b', 'g', 'r', 'c', 'm', 'y', (0, 0, .5, 1), (0, .5, 0, 1), (.5, 0, 0, 1)]
    # df1 = df1.set_index('Year').transpose()
    # df1.sort_values(by=[2020], inplace=True, ascending=False)

    # ax = df1.plot.bar(y = 2020, legend = False, title = 'Population: Year 2020', color = my_colors)
    # x_axis = ax.axes.get_xaxis()
    # x_axis.set_visible(False)
    # for p in ax.patches:
    #     width = p.get_width()
    #     height = p.get_height()
    #     x, y = p.get_xy()
    #     ax.annotate(f'{height:,.0f}', (x + width/2, y + height*1.02), ha='center')
    # # 'Multiple Races' must be moved to fourth place for 2020, otherwise in sixth place.
    # labels = ['One Race', 'White', 'Black or African American', 'Multiple Races', 'Some Other Race', 'Asian', 'American Indian or Alaska Native', 'Native Hawaiian or Pacific Islander']
    # plt.legend(labels=labels, handles=ax.patches, bbox_to_anchor=(1.04, 0.5), loc='center left', borderaxespad=0, fontsize=12, frameon=False)

    ## Line graph set up
    df1.set_index('Year').plot(linewidth = 3)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)
    # Comment out the following for the full graph, use 50000000 to get the bottom group, 5000000 to get the bottom two.
    plt.ylim(0, 50000000)


    
    # Pie graph set up
    # df2 = df2.set_index('Year').transpose()
    # ax = df2.plot.pie(y = 2020, explode = { 0, .1 }, labels=None, autopct='%1.0f%%', colors = ["tab:orange", "tab:blue"], title = "Hispanic Distribution: Year 2020")
    # y_axis = ax.axes.get_yaxis()
    # y_axis.set_visible(False)

    plt.ticklabel_format(axis='y', style='plain')
    plt.show()


if __name__ == "__main__":
    main()