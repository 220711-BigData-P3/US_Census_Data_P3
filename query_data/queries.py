from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

path = "file:/mnt/c/Users/jan94/OneDrive/Desktop/Work/proj3/" # MODIFY PATH IF YOU USE THIS ON YOUR MACHINE

geodata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .options(delimiter="|") \
    .csv(path + "alabama_geo.csv")


categorydata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .options(delimiter="|") \
    .csv(path + "alabama_pop.csv")

geodata2020.createOrReplaceTempView("geodata2020")
categorydata2020.createOrReplaceTempView("categorydata2020")

# geodata2020.show()
# categorydata2020.show()

# spark.sql("SELECT g.LOGRECNO, P0010001 FROM geodata2020 g JOIN categorydata2020 c ON g.LOGRECNO=c.LOGRECNO").show()
# select the log record number, total population from the alabama 2020 census tables, join the geodata with the population data on the logrecno column, select only the state (logrecno=1)
spark.sql("SELECT NAME, P0010001 AS TotalPopulation FROM geodata2020 g JOIN categorydata2020 c ON g.LOGRECNO=c.LOGRECNO WHERE g.LOGRECNO=1").show()
spark.sql("SELECT NAME, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS BlackAfricanAmerican, P0010005 AS AmericanIndianAlaskaNative, "
    "P0010006 AS Asian, P0010007 AS NativeHawaiianPacificIslander, P0010008 AS Other, P0010009 AS TwoOrMore FROM geodata2020 g JOIN categorydata2020 c ON g.LOGRECNO=c.LOGRECNO WHERE g.LOGRECNO=1").show()

spark.stop()