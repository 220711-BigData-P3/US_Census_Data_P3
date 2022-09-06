import matplotlib.pyplot as plt
import pandas as pd
import csv

def main():
    fname = "usData_1.csv"

    plt.rcParams["figure.figsize"] = [8, 4]
    plt.rcParams["figure.autolayout"] = True

    df = pd.read_csv(fname, header=0)
    

    ## Bar graph set up
    # df.set_index('Year').transpose().plot.bar(y = 2000, legend = False, title = "2000", color = (0, 0, .75, 1))
    # 2000: 0, 0, .75, 1
    # 2010: 0, .75, 0, 1
    # 2020: .75, 0, 0, 1

    ## Line graph set up
    df.set_index('Year').plot()
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0)


    plt.ticklabel_format(axis='y', style='plain')
    plt.show()

if __name__ == "__main__":
    main()