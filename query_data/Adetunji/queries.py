from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# path = 'file:/home/strumunix/Rev-P3/US_Census_Data_P3/query_data/Adetunji/'
path = "file:/mnt/c/Users/mofob/OneDrive/Desktop/p3/US_Census_Data_P3/query_data/Adetunji/"

#######READ 2000 CENSUS  DATA LOAD ###########
census2000_1 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2000_1.csv")

census2000_2 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2000_2.csv")

census2000_1.createTempView("census2000_1")
census2000_2.createTempView("census2000_2")

########READ 2010 CENSUS  DATA LOAD ###########
census2010_1 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2010_1.csv")


census2010_2 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2010_2.csv")

census2010_1.createTempView("census2010_1")
census2010_2.createTempView("census2010_2")

#######READ 2020 CENSUS  DATA LOAD ###########
census2020_1 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2020_1.csv")


census2020_2 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2020_2.csv")

census2020_1.createTempView("census2020_1")
census2020_2.createTempView("census2020_2")
'''
print("2000")
census2000_1.printSchema()
census2000_2.printSchema()
print("2010")
census2010_1.printSchema()
census2010_2.printSchema()
print("2020")
census2020_1.printSchema()
census2020_2.printSchema()
'''

#########>>>>>>>>>>QUERIES <<<<<<<<<<<<<<<<#########

#POPULATION DATA 2010 TO 2020
GROWTH2010TO2020 = spark.sql("SELECT census2020_1.STUSAB as STATE , census2020_1.P0010001 as 2020_Population ,\
        census2010_1.P0010001 as 2010_Population , census2020_1.P0010001 - census2010_1.P0010001 as Difference ,\
        100 * (census2020_1.P0010001 - census2010_1.P0010001)/census2010_1.P0010001 as Percentage_change from census2020_1 \
        join census2010_1 on census2010_1.STUSAB = census2020_1.STUSAB WHERE census2020_1.STUSAB != 'US' and census2020_1.STUSAB != 'PR'   order by Percentage_change DESC")

#Population Increase 2020 to 2010
Pop_Incr_2010_to_2020 = GROWTH2010TO2020.filter(func.col("Percentage_change") > 0).withColumn("Pop_Growth_Rate(2010-2020)",func.round("Percentage_change",2))\
    .select(["STATE","Pop_Growth_Rate(2010-2020)" ])

#Population Decrease 2020 to 2010
Pop_Decr_2010_to_2020 = GROWTH2010TO2020.filter(func.col("Percentage_change") < 0).withColumn("Pop_Reduction_Rate(2010-2020)",func.round("Percentage_change",2))\
    .orderBy("Percentage_change").select(["STATE","Pop_Reduction_Rate(2010-2020)" ])


#POPULATION DATA 2000 TO 2010
GROWTH2000TO2010 = spark.sql("SELECT census2010_1.STUSAB as STATE , census2010_1.P0010001 as 2010_Population ,\
        census2000_1.P0010001 as 2000_Population , census2010_1.P0010001 - census2000_1.P0010001 as Difference ,\
        100 * (census2010_1.P0010001 - census2000_1.P0010001)/census2000_1.P0010001 as Percentage_change from census2010_1 \
        join census2000_1 on census2010_1.STUSAB = census2000_1.STUSAB WHERE census2010_1.STUSAB != 'US' and census2010_1.STUSAB != 'PR' order by Percentage_change DESC")

#Population Increase 2010 to 2000
Pop_Incr_2000_to_2010 = GROWTH2000TO2010.filter(func.col("Percentage_change") > 0).withColumn("Pop_Growth_Rate(2000-2010)",func.round("Percentage_change",2))\
    .select(["STATE","Pop_Growth_Rate(2000-2010)" ])

#Population Decrease 2010 to 2000
Pop_Decr_2000_to_2010 =GROWTH2000TO2010.filter(func.col("Percentage_change") < 0).withColumn("Pop_Reduction_Rate(2000-2010)",func.round("Percentage_change",2)) \
    .orderBy("Percentage_change").select(["STATE","Pop_Reduction_Rate(2000-2010)" ])


#POPULATION DATA 2000 TO 2020
GROWTH2000TO2020 = spark.sql("SELECT census2020_1.STUSAB as STATE , census2020_1.P0010001 as 2020_Population ,\
        census2000_1.P0010001 as 2000_Population , census2020_1.P0010001 - census2000_1.P0010001 as Difference ,\
        100 * (census2020_1.P0010001 - census2000_1.P0010001)/census2000_1.P0010001 as Percentage_change from census2020_1 \
        join census2000_1 on census2020_1.STUSAB = census2000_1.STUSAB  WHERE census2020_1.STUSAB != 'US' and census2020_1.STUSAB != 'PR' order by Percentage_change DESC")

#POPULATION DATA 2000 TO 2020
AVGGROWTH2000TO2020 = spark.sql("SELECT census2020_1.STUSAB as STATE , census2020_1.P0010001 as 2020_Population ,\
        census2000_1.P0010001 as 2000_Population , census2020_1.P0010001 - census2000_1.P0010001 as Difference ,\
        50 * (census2020_1.P0010001 - census2000_1.P0010001)/census2000_1.P0010001 as Percentage_change from census2020_1 \
        join census2000_1 on census2020_1.STUSAB = census2000_1.STUSAB  WHERE census2020_1.STUSAB != 'US' and census2020_1.STUSAB != 'PR' order by Percentage_change DESC")

#Population Increase 2020 to 2000
Pop_Incr_2000_to_2020 = GROWTH2000TO2020.filter(func.col("Percentage_change") > 0).withColumn("Pop_Growth_Rate(2000-2020)",func.round("Percentage_change",2))\
    .select(["state","Pop_Growth_Rate(2000-2020)"])

#Population Decrease 2020 to 2000
Pop_Decr_2000_to_2020 = GROWTH2000TO2020.filter(func.col("Percentage_change") < 0).withColumn("Pop_Reduction_Rate(2000-2020)",func.round("Percentage_change",2)) \
    .orderBy("Percentage_change").select(["STATE","Pop_Reduction_Rate(2000-2020)" ])

Pop_Rate_2000_2020= GROWTH2000TO2020.withColumn("Rate", func.round("Percentage_change",2)).select(["state","Rate"])
Pop_Rate_2000_2010=GROWTH2000TO2010.withColumn("Rate", func.round("Percentage_change",2)).select(["state","Rate"])
Pop_Rate_2010_2020=GROWTH2010TO2020.withColumn("Rate", func.round("Percentage_change",2)).select(["state","Rate"])
Avg_Rate_2000_2020=AVGGROWTH2000TO2020.withColumn("Rate", func.round("Percentage_change",2)).select(["state","Rate"])

print("Population rate change through the decades ")
Avg_Rate_2000_2020.show()
Pop_Rate_2000_2020.show()
Pop_Rate_2000_2010.show()
Pop_Rate_2010_2020.show()

print("Population increase and decrease through the decades ")
Pop_Incr_2010_to_2020.show()
Pop_Decr_2010_to_2020.show()

Pop_Incr_2000_to_2010.show()
Pop_Decr_2000_to_2010.show()

Pop_Incr_2000_to_2020.show()
Pop_Decr_2000_to_2020.show()


#### WRITING THE DATA FRAME RESULTS TO FILE FOR POPULATION INCREASE AND DECREASE


Pop_Incr_2010_to_2020.write.csv(path + "/Pop_Incr_2010_to_2020")
Pop_Decr_2010_to_2020.write.csv(path + "/Pop_Decr_2010_to_2020")

Pop_Incr_2000_to_2010.write.csv(path + "/Pop_Incr_2000_to_2010")
Pop_Decr_2000_to_2010.write.csv(path + "/Pop_Decr_2000_to_2010")

Pop_Incr_2000_to_2020.write.csv(path + "/Pop_Incr_2000_to_2020")
Pop_Decr_2000_to_2020.write.csv(path + "/Pop_Decr_2000_to_2020")

#### WRITING THE DATA FRAME RESULTS TO FILE FOR POPULATION RATE CHANGE

Pop_Rate_2000_2020.write.csv(path + "/Total_Pop_Rate_2000_to_2020")
Pop_Rate_2000_2010.write.csv(path + "/Total_Pop_Rate_2000_to_2010")
Pop_Rate_2010_2020.write.csv(path + "/Total_Pop_Rate_2010_to_2020")
Avg_Rate_2000_2020.write.csv(path + "/AVG_Pop_Rate_2010_to_2020")
spark.stop()