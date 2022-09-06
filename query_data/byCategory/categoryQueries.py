from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, lit

# ---------- BUILD SPARK SESSION ---------- #
spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .getOrCreate()

# ---------- SPARK CONTEXT ---------- #
sc = spark.sparkContext
sc.setLogLevel("WARN")

# ---------- SET FILEPATH ---------- #
mypath = "file:/mnt/c/Users/jan94/OneDrive/Desktop/Work/proj3/" # MODIFY MYPATH TO LOCATION OF YOUR LOCAL CLONE OF THE REPOSITORY
path = mypath + "US_Census_Data_P3/"

# ---------- CREATE DATAFRAMES FROM FILES ---------- #
    # ----- 2020 ----- #
popdata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2020_P1.csv")

    # ----- 2010 ----- #
popdata2010 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2010_1.csv")

    # ----- 2000 ----- #
popdata2000 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2000_1.csv")

# REMOVE NATIONAL SUMMARY AND PUERTO RICO FROM DATA
popdata2020.createOrReplaceTempView("popdata2020_ex")
spark.sql("SELECT * FROM popdata2020_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2020)).createOrReplaceTempView("popdata2020")
popdata2010.createOrReplaceTempView("popdata2010_ex")
spark.sql("SELECT * FROM popdata2010_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2010)).createOrReplaceTempView("popdata2010")
popdata2000.createOrReplaceTempView("popdata2000_ex")
spark.sql("SELECT * FROM popdata2000_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2000)).createOrReplaceTempView("popdata2000")

# ---------------------------------------- 2020 DATA ---------------------------------------- #
    # --------------- usData2020_1 (Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
          "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
          "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2020 ORDER BY Total DESC").createOrReplaceTempView("cat2020_1")

usData2020_1 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore, SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2020_1")

    # --------------- usData2020_2 (Non-Hispanic Totals for all categories, Hispanic as separate category) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC") \
          .createOrReplaceTempView("cat2020_2")

usData2020_2 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore FROM cat2020_2")

    # --------------- hispUS2020 (Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
          "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
          "P0010009-P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC").createOrReplaceTempView("hisp2020")

hispUS2020 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                       "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                       "SUM(TwoOrMore) AS TwoOrMore FROM hisp2020")

    # --------------- nonhispUS2020 (Non-Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC") \
          .createOrReplaceTempView("nonhisp2020")

nonhispUS2020 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                          "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                          "SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2020")

# ---------------------------------------- 2010 DATA ---------------------------------------- #
    # --------------- usData2010_1 (Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
          "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
          "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2010 ORDER BY Total DESC").createOrReplaceTempView("cat2010_1")

usData2010_1 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore, SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2010_1")

    # --------------- usData2010_2 (Non-Hispanic Totals for all categories, Hispanic as separate category) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC") \
          .createOrReplaceTempView("cat2010_2")

usData2010_2 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore FROM cat2010_2")

    # --------------- hispUS2010 (Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
          "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
          "P0010009-P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC").createOrReplaceTempView("hisp2010")

hispUS2010 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                       "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                       "SUM(TwoOrMore) AS TwoOrMore FROM hisp2010")

    # --------------- nonhispUS2010 (Non-Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC") \
          .createOrReplaceTempView("nonhisp2010")

nonhispUS2010 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                          "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                          "SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2010")

# ---------------------------------------- 2000 DATA ---------------------------------------- #
    # --------------- usData2000_1 (Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
          "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
          "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2000 ORDER BY Total DESC").createOrReplaceTempView("cat2000_1")

usData2000_1 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore, SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2000_1")

    # --------------- usData2000_2 (Non-Hispanic Totals for all categories, Hispanic as separate category) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC") \
          .createOrReplaceTempView("cat2000_2")

usData2000_2 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, "
                         "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                         "SUM(TwoOrMore) AS TwoOrMore FROM cat2000_2")

    # --------------- hispUS2000 (Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
          "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
          "P0010009-P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC").createOrReplaceTempView("hisp2000")

hispUS2000 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                       "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                       "SUM(TwoOrMore) AS TwoOrMore FROM hisp2000")

    # --------------- nonhispUS2000 (Non-Hispanic Totals for all categories) --------------- #
spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
          "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC") \
          .createOrReplaceTempView("nonhisp2000")

nonhispUS2000 = spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, "
                          "SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, "
                          "SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2000")

# -------------------- COMBINE AND REPARTITION DATAFRAMES -------------------- #
usData_1 = usData2000_1.union(usData2010_1).union(usData2020_1).repartition(1) # usData_1 (Totals for all categories)
usData_2 = usData2000_2.union(usData2010_2).union(usData2020_2).repartition(1) # usData_2 (Non-Hispanic Totals for all categories, Hispanic as separate category)
hispUS = hispUS2000.union(hispUS2010).union(hispUS2020).repartition(1) # hispUS (Hispanic Totals for all categories)
nonhispUS = nonhispUS2000.union(nonhispUS2010).union(nonhispUS2020).repartition(1) # nonhispUS (Non-Hispanic Totals for all categories)

# ---------- UNCOMMENT TO VERIFY DATAFRAMES ---------- #
usData_1.show()
usData_2.show()
print("Hispanic Population")
hispUS.show()
print("Non-Hispanic Population")
nonhispUS.show()

# # --------------- SAVE FILES --------------- #
# savepath = path + "query_data/byCategory/"
# usData_1.write.csv(savepath + "usData_1", header=True)
# usData_2.write.csv(savepath + "usData_2", header=True)
# hispUS.write.csv(savepath + "hispUS", header=True)
# nonhispUS.write.csv(savepath + "nonhispUS", header=True)

# TO MAKE QUERIES EASIER, RENAME THE RESULTING CSV FILES TO THE NAMES OF THE DATAFRAMES THEY WERE
# CREATED FROM, THEN MOVE THEM TO THE "/byCategory" FOLDER.

print("Complete!")

spark.stop()