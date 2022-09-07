

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

read = pd.read_csv('usData_2.csv',header=0)
y = read.iloc[:,]
df = pd.DataFrame(y, columns=["Year", "Total", "Hispanic", "White","Black","NativeAm","Asian","Pac","Other","Two Or More"])
df.plot(x='Year', y =["Total", "Hispanic", "White","Black","NativeAm","Asian","Pac","Other","Two Or More"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.show()

