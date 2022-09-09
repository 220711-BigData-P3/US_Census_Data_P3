from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .config("spark.debug.maxToStringFields", "30") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

path = "file:/mnt/c/Users/keyno/Desktop/Capstone/State.Tables" # MODIFY PATH IF YOU USE THIS ON YOUR MACHINE

# 2020 Data
"""geodata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .options(delimiter="|") \
    .csv(path + "alabama_geo.csv")"""


categorydata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "/2020_P1.csv")

#geodata2020.createOrReplaceTempView("geodata2020")
categorydata2020.createOrReplaceTempView("categorydata2020")

totalpop2020 = spark.sql("SELECT SUM(P0010001) AS Total_Population_2020 FROM categorydata2020")
totalpop2020.show()
#totalpop2020.coalesce(1).write.option("header", True).csv(path+ "/TotalUSPop2020")

#2010 Data
"""geodata2010 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .options(delimiter="|") \
    .csv(path + "alabama_geo.csv")"""


categorydata2010 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "/2010_1.csv")

#geodata2010.createOrReplaceTempView("geodata2020")
categorydata2010.createOrReplaceTempView("categorydata2010")

Totalpop2010 = spark.sql("SELECT SUM(P0010001) AS Total_Population_2010 FROM categorydata2010 WHERE STUSAB='US'")
Totalpop2010.show()
#Totalpop2010.coalesce(1).write.option("header", True).csv(path+ "/TotalUSPop2010")

#2000 Data
# No Geo Data no need for joins. Be sure to structure queries with that in mind
"""geodata2000 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .options(delimiter="|") \
    .csv(path + "alabama_geo.csv")"""


categorydata2000 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "/2000_1.csv")

#geodata2000.createOrReplaceTempView("geodata2020")
categorydata2000.createOrReplaceTempView("categorydata2020")

Totalpop2000= spark.sql("SELECT SUM(P0010001) AS Total_Population_2000 FROM categorydata2020 WHERE STUSAB='US'")
Totalpop2000.show()
#Totalpop2000.coalesce(1).write.option("header", True).csv(path+ "/TotalUSPop2000")
spark.stop()
print("Complete!")