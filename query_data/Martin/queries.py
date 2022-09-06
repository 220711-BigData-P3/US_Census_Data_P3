from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
        df_temp = (spark.read.option("header", True).csv(csv_path + filename))
        df_temp = df_temp.filter(col("STUSAB").isin(*state_abbrevs))
        #extract name from filename (everything before '.')
        df_temp.createOrReplaceTempView(filename.split('.')[0])


#Create Views
get_tempViews()

#Join tables together by common column value (STUSAB)
all_years = spark.sql("SELECT 2000_1.STUSAB AS STATE, 2000_1.P0010001 AS POP_2000, '2000' AS YEAR_2000, 2010_1.P0010001 AS POP_2010, '2010' AS YEAR_2010, 2020_P1.P0010001 AS POP_2020, '2020' AS YEAR_2020 FROM 2000_1 JOIN 2010_1 ON 2000_1.STUSAB = 2010_1.STUSAB JOIN 2020_P1 ON 2000_1.STUSAB = 2020_P1.STUSAB")

print(all_years.count()) #50

all_years.show()
# +-----+--------+--------+--------+
# |STATE|POP_2000|POP_2010|POP_2020|
# +-----+--------+--------+--------+
# |   AK|  626932|  710231|  733391|
# |   AL| 4447100| 4779736| 5024279|
# |   AR| 2673400| 2915918| 3011524|
# |   AZ| 5130632| 6392017| 7151502|
# |   CA|33871648|37253956|39538223|
# |   CO| 4301261| 5029196| 5773714|
# |   CT| 3405565| 3574097| 3605944|
# |   DE|  783600|  897934|  989948|
# |   FL|15982378|18801310|21538187|
# |   GA| 8186453| 9687653|10711908|
# |   HI| 1211537| 1360301| 1455271|
# |   IA| 2926324| 3046355| 3190369|
# |   ID| 1293953| 1567582| 1839106|
# |   IL|12419293|12830632|12812508|
# |   IN| 6080485| 6483802| 6785528|
# |   KS| 2688418| 2853118| 2937880|
# |   KY| 4041769| 4339367| 4505836|
# |   LA| 4468976| 4533372| 4657757|
# |   MA| 6349097| 6547629| 7029917|
# |   MD| 5296486| 5773552| 6177224|
# +-----+--------+--------+--------+

# all_years.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Martin/state_population_trendline")