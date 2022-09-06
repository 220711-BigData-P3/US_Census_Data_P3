from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from pyspark.context import SparkContext
import os


#Used for intial filtering before view is created!
state_abbrevs = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']



path = os.getcwd()
csv_path = "file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/"
spark = SparkSession.builder.master("local").appName("data1").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("Warn")


filenames = ['2000_1.csv', '2010_1.csv', '2020_P1.csv']

def get_tempViews():
    '''
    Iteratively create tempViews using names from "filenames". Filtering is done before view creation.
    Use spark.sql("SHOW VIEWS") to see a table of the views in the program:
    +---------+--------+-----------+
    |namespace|viewName|isTemporary|
    +---------+--------+-----------+
    |         |  2000_1|       true|
    |         |  2010_1|       true|
    |         | 2020_p1|       true|
    +---------+--------+-----------+
    '''
    for filename in filenames:
        year = filename.split('_')[0]
        df_temp = (spark.read.option("header", True).csv(csv_path + filename))
        df_temp = df_temp.withColumn("YEAR", lit(f"01/01/{year}")).filter(col("STUSAB").isin(*state_abbrevs))
        #extract name from filename (everything before '.')
        df_temp.createOrReplaceTempView(filename.split('.')[0])


#Create Views
get_tempViews()

all_years = spark.sql("SELECT 2000_1.STUSAB AS STATE, 2000_1.P0010001 AS POPULATION, YEAR FROM 2000_1 UNION SELECT 2010_1.STUSAB AS STATE, 2010_1.P0010001 AS POPULATION, YEAR FROM 2010_1 UNION SELECT 2020_P1.STUSAB AS STATE, 2020_P1.P0010001 AS POPULATION, YEAR FROM 2020_P1 ")
print(all_years.count()) #50

all_years.show(150)

#Run to get result csv file
# all_years.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Martin/state_population_trendline")