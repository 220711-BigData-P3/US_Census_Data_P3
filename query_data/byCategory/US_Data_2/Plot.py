

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

read = pd.read_csv('usData_2.csv',header=0)
y = read.iloc[:,]

df = pd.DataFrame(y, columns=["Year", "Total", "Hispanic", "White","Black","NativeAm","Asian","PacIslander","Other","TwoOrMore"])
print (df)
#################################################################################################################################
#                                       Population by each race                       
#
df.plot(x='Year', y =["Hispanic", "White","Black","NativeAm","Asian","PacIslander","Other","TwoOrMore"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["Hispanic", "White","Black","NativeAm","Asian","Pac Islander","Other","Two Or More"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Population by each race .png')

#################################################################################################################################
#                                       Total vs White
#                        
df.plot(x='Year', y =["Total","White"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["Total","White"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Total vs White.png')
#################################################################################################################################
#                                       Hispanic vs Black vs Two or More
#                        
df.plot(x='Year', y =["Hispanic","Black","TwoOrMore"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["Hispanic","Black", "Two or More"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Hispanic vs Black vs Two or More.png')

#################################################################################################################################
#                                       Asian
#                        
df.plot(x='Year', y =["Asian"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["Asian"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Asian.png')

#################################################################################################################################
#                                       Other
#                        
df.plot(x='Year', y =["NativeAm","Other"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["NativeAm","Other"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Other.png')

#################################################################################################################################
#                                       PacIslander and TwoOrMore
#                        
df.plot(x='Year', y =["PacIslander"],kind =  'bar')
plt.ticklabel_format(axis='y', style='plain')
plt.legend(["Pacific Islander"],bbox_to_anchor = (1,1))
current_values = plt.gca().get_yticks()
plt.gca().set_yticklabels([np.float64((float(x) /1000000),precision=4) for x in current_values])
plt.ylabel('Population in Millions')
plt.tight_layout()
plt.savefig('Pac Islanderific.png')



