
import pandas as pd


# reading two csv files
data1 = pd.read_csv('file:/mnt/c/Users/Nithia Justin/Desktop/Revature/Revature_Projects/p3-Team-2/2010_1.csv')
data2 = pd.read_csv('file:/mnt/c/Users/Nithia Justin/Desktop/Revature/Revature_Projects/p3-Team-2/Region_file.csv')
  
# using merge function by setting how='inner'
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
output1 = pd.merge(data1, data2, 
                   on='STUSAB', 
                   how='inner')

output1.to_csv('2010_1_Region.csv')

