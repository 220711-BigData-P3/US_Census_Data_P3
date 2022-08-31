from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number, lit

# TODO:
#     - Load 2020 census files
#     - Save dataframes to csv

spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# set path
path = "file:/mnt/c/Users/jan94/OneDrive/Desktop/Work/proj3/US_Census_Data_P3/" # MODIFY PATH IF YOU USE THIS ON YOUR MACHINE

# create dataframes from files
popdata2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2020_P1.csv")
popdata2010 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2010_1.csv")
popdata2000 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2000_1.csv")

# filter unused data out of dataframes
popdata2020.createOrReplaceTempView("popdata2020_ex")
spark.sql("SELECT * FROM popdata2020_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2020)).createOrReplaceTempView("popdata2020")
popdata2010.createOrReplaceTempView("popdata2010_ex")
spark.sql("SELECT * FROM popdata2010_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2010)).createOrReplaceTempView("popdata2010")
popdata2000.createOrReplaceTempView("popdata2000_ex")
spark.sql("SELECT * FROM popdata2000_ex WHERE STUSAB != 'PR' AND STUSAB != 'US'").withColumn("Year", lit(2000)).createOrReplaceTempView("popdata2000")

# ---------------------------------------- 2020 DATA ---------------------------------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
            "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
            "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2020 ORDER BY Total DESC").createOrReplaceTempView("cat2020_1")

spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC") \
                .createOrReplaceTempView("cat2020_2")

spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
            "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
            "P0010009-P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC").createOrReplaceTempView("hisp2020")

spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2020 ORDER BY Total DESC") \
            .createOrReplaceTempView("nonhisp2020")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, "
            "SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore, "
            "SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2020_1").createOrReplaceTempView("usData2020_1")
usData2020_1 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAM, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T, Hispanic, "
                    "Hispanic/Total AS H, NonHispanic, NonHispanic/Total AS NH FROM usData2020_1")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAM, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM cat2020_2").createOrReplaceTempView("usData2020_2")

usData2020_2 = spark.sql("SELECT Year, Total, Hispanic, Hispanic/Total AS H, White, White/Total AS W, Black, Black/Total AS B, NativeAM, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM usData2020_2")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM hisp2020").createOrReplaceTempView("hispUS2020")
hispUS2020 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM hispUS2020")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2020").createOrReplaceTempView("nonhispUS2020")
nonhispUS2020 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM nonhispUS2020")

usData2020_1 = usData2020_1.withColumn("S", format_number("S", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4)) \
                           .withColumn("H", format_number("H", 4)) \
                           .withColumn("NH", format_number("NH", 4))

usData2020_2 = usData2020_2.withColumn("H", format_number("H", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4))

hispUS2020 = hispUS2020.withColumn("S", format_number("S", 4)) \
                       .withColumn("W", format_number("W", 4)) \
                       .withColumn("B", format_number("B", 4)) \
                       .withColumn("N", format_number("N", 4)) \
                       .withColumn("A", format_number("A", 4)) \
                       .withColumn("P", format_number("P", 4)) \
                       .withColumn("O", format_number("O", 4)) \
                       .withColumn("T", format_number("T", 4))

nonhispUS2020 = nonhispUS2020.withColumn("S", format_number("S", 4)) \
                             .withColumn("W", format_number("W", 4)) \
                             .withColumn("B", format_number("B", 4)) \
                             .withColumn("N", format_number("N", 4)) \
                             .withColumn("A", format_number("A", 4)) \
                             .withColumn("P", format_number("P", 4)) \
                             .withColumn("O", format_number("O", 4)) \
                             .withColumn("T", format_number("T", 4))


# ---------------------------------------- 2010 DATA ---------------------------------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
            "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
            "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2010 ORDER BY Total DESC").createOrReplaceTempView("cat2010_1")

spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC") \
            .createOrReplaceTempView("cat2010_2")

spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
            "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
            "P0010009-P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC").createOrReplaceTempView("hisp2010")

spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2010 ORDER BY Total DESC") \
            .createOrReplaceTempView("nonhisp2010")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, "
            "SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore, "
            "SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2010_1").createOrReplaceTempView("usData2010_1")
usData2010_1 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAM, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T, Hispanic, "
                    "Hispanic/Total AS H, NonHispanic, NonHispanic/Total AS NH FROM usData2010_1")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAM, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM cat2010_2").createOrReplaceTempView("usData2010_2")

usData2010_2 = spark.sql("SELECT Year, Total, Hispanic, Hispanic/Total AS H, White, White/Total AS W, Black, Black/Total AS B, NativeAM, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM usData2010_2")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM hisp2010").createOrReplaceTempView("hispUS2010")
hispUS2010 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM hispUS2010")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2010").createOrReplaceTempView("nonhispUS2010")
nonhispUS2010 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM nonhispUS2010")

usData2010_1 = usData2010_1.withColumn("S", format_number("S", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4)) \
                           .withColumn("H", format_number("H", 4)) \
                           .withColumn("NH", format_number("NH", 4))

usData2010_2 = usData2010_2.withColumn("H", format_number("H", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4))

hispUS2010 = hispUS2010.withColumn("S", format_number("S", 4)) \
                       .withColumn("W", format_number("W", 4)) \
                       .withColumn("B", format_number("B", 4)) \
                       .withColumn("N", format_number("N", 4)) \
                       .withColumn("A", format_number("A", 4)) \
                       .withColumn("P", format_number("P", 4)) \
                       .withColumn("O", format_number("O", 4)) \
                       .withColumn("T", format_number("T", 4))

nonhispUS2010 = nonhispUS2010.withColumn("S", format_number("S", 4)) \
                             .withColumn("W", format_number("W", 4)) \
                             .withColumn("B", format_number("B", 4)) \
                             .withColumn("N", format_number("N", 4)) \
                             .withColumn("A", format_number("A", 4)) \
                             .withColumn("P", format_number("P", 4)) \
                             .withColumn("O", format_number("O", 4)) \
                             .withColumn("T", format_number("T", 4))


# ---------------------------------------- 2000 DATA ---------------------------------------- #
spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0010002 AS OneRace, P0010003 AS White, P0010004 AS Black, "
            "P0010005 AS NativeAm, P0010006 AS Asian, P0010007 AS PacIslander, P0010008 AS Other, P0010009 AS TwoOrMore, "
            "P0020002 AS Hispanic, P0020003 AS NonHispanic FROM popdata2000 ORDER BY Total DESC").createOrReplaceTempView("cat2000_1")

spark.sql("SELECT Year, STUSAB AS State, P0010001 AS Total, P0020002 AS Hispanic, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC") \
            .createOrReplaceTempView("cat2000_2")

spark.sql("SELECT Year, STUSAB AS State, P0020002 AS Total, P0010002-P0020004 AS OneRace, P0010003-P0020005 AS White, P0010004-P0020006 AS Black, "
            "P0010005-P0020007 AS NativeAm, P0010006-P0020008 AS Asian, P0010007-P0020009 AS PacIslander, P0010008-P0020010 AS Other, "
            "P0010009-P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC").createOrReplaceTempView("hisp2000")

spark.sql("SELECT Year, STUSAB AS State, P0020003 AS Total, P0020004 AS OneRace, P0020005 AS White, P0020006 AS Black, P0020007 AS NativeAm, "
            "P0020008 AS Asian, P0020009 AS PacIslander, P0020010 AS Other, P0020011 AS TwoOrMore FROM popdata2000 ORDER BY Total DESC") \
            .createOrReplaceTempView("nonhisp2000")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, "
            "SUM(Asian) AS Asian, SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore, "
            "SUM(Hispanic) AS Hispanic, SUM(NonHispanic) AS NonHispanic FROM cat2000_1").createOrReplaceTempView("usData2000_1")
usData2000_1 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T, Hispanic, "
                    "Hispanic/Total AS H, NonHispanic, NonHispanic/Total AS NH FROM usData2000_1")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(Hispanic) AS Hispanic, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM cat2000_2").createOrReplaceTempView("usData2000_2")
usData2000_2 = spark.sql("SELECT Year, Total, Hispanic, Hispanic/Total AS H, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM usData2000_2")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM hisp2000").createOrReplaceTempView("hispUS2000")
hispUS2000 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM hispUS2000")

spark.sql("SELECT MIN(Year) AS Year, SUM(Total) AS Total, SUM(OneRace) AS OneRace, SUM(White) AS White, SUM(Black) AS Black, SUM(NativeAm) AS NativeAm, SUM(Asian) AS Asian, "
            "SUM(PacIslander) AS PacIslander, SUM(Other) AS Other, SUM(TwoOrMore) AS TwoOrMore FROM nonhisp2000").createOrReplaceTempView("nonhispUS2000")
nonhispUS2000 = spark.sql("SELECT Year, Total, OneRace, OneRace/Total AS S, White, White/Total AS W, Black, Black/Total AS B, NativeAm, NativeAm/Total AS N, "
                    "Asian, Asian/Total AS A, PacIslander, PacIslander/Total AS P, Other, Other/Total AS O, TwoOrMore, TwoOrMore/Total AS T "
                    "FROM nonhispUS2000")

usData2000_1 = usData2000_1.withColumn("S", format_number("S", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4)) \
                           .withColumn("H", format_number("H", 4)) \
                           .withColumn("NH", format_number("NH", 4))

usData2000_2 = usData2000_2.withColumn("H", format_number("H", 4)) \
                           .withColumn("W", format_number("W", 4)) \
                           .withColumn("B", format_number("B", 4)) \
                           .withColumn("N", format_number("N", 4)) \
                           .withColumn("A", format_number("A", 4)) \
                           .withColumn("P", format_number("P", 4)) \
                           .withColumn("O", format_number("O", 4)) \
                           .withColumn("T", format_number("T", 4))

hispUS2000 = hispUS2000.withColumn("S", format_number("S", 4)) \
                       .withColumn("W", format_number("W", 4)) \
                       .withColumn("B", format_number("B", 4)) \
                       .withColumn("N", format_number("N", 4)) \
                       .withColumn("A", format_number("A", 4)) \
                       .withColumn("P", format_number("P", 4)) \
                       .withColumn("O", format_number("O", 4)) \
                       .withColumn("T", format_number("T", 4))

nonhispUS2000 = nonhispUS2000.withColumn("S", format_number("S", 4)) \
                             .withColumn("W", format_number("W", 4)) \
                             .withColumn("B", format_number("B", 4)) \
                             .withColumn("N", format_number("N", 4)) \
                             .withColumn("A", format_number("A", 4)) \
                             .withColumn("P", format_number("P", 4)) \
                             .withColumn("O", format_number("O", 4)) \
                             .withColumn("T", format_number("T", 4))


# ---------------------------------------- NOT IN USE ---------------------------------------- #
'''catPercDF2000 = spark.sql("SELECT State, Total, OneRace/Total AS OneRace, White/Total AS White, Black/Total AS Black, NativeAm/Total AS NativeAm, "
                            "Asian/Total AS Asian, PacIslander/Total AS PacIslander, Other/Total AS Other, TwoOrMore/Total AS TwoOrMore, "
                            "Hispanic/Total AS Hispanic, NonHispanic/Total AS NonHispanic FROM cat2000_1 ORDER BY Total DESC")

hispPercDF2000 = spark.sql("SELECT State, Total, Hispanic/Total AS Hispanic, White/Total AS White, Black/Total AS Black, NativeAm/Total AS NativeAm, "
                            "Asian/Total AS Asian, PacIslander/Total AS PacIslander, Other/Total AS Other, TwoOrMore/Total AS TwoOrMore "
                            "FROM cat2000_2 ORDER BY Total DESC")

catPercDF2000 = catPercDF2000.withColumn("OneRace", format_number("OneRace", 4)) \
                                .withColumn("White", format_number("White", 4)) \
                                .withColumn("Black", format_number("Black", 4)) \
                                .withColumn("NativeAm", format_number("NativeAM", 4)) \
                                .withColumn("Asian", format_number("Asian", 4)) \
                                .withColumn("PacIslander", format_number("PacIslander", 4)) \
                                .withColumn("Other", format_number("Other", 4)) \
                                .withColumn("TwoOrMore", format_number("TwoOrMore", 4)) \
                                .withColumn("Hispanic", format_number("Hispanic", 4)) \
                                .withColumn("NonHispanic", format_number("NonHispanic", 4))

hispPercDF2000 = hispPercDF2000.withColumn("Hispanic", format_number("Hispanic", 4)) \
                                .withColumn("White", format_number("White", 4)) \
                                .withColumn("Black", format_number("Black", 4)) \
                                .withColumn("NativeAm", format_number("NativeAM", 4)) \
                                .withColumn("Asian", format_number("Asian", 4)) \
                                .withColumn("PacIslander", format_number("PacIslander", 4)) \
                                .withColumn("Other", format_number("Other", 4)) \
                                .withColumn("TwoOrMore", format_number("TwoOrMore", 4))

catPercDF2000.show(52)
hispPercDF2000.show(52)'''

usData_1 = usData2000_1.union(usData2010_1).union(usData2020_1).repartition(1)
usData_2 = usData2000_2.union(usData2010_2).union(usData2020_2).repartition(1)
hispUS = hispUS2000.union(hispUS2010).union(hispUS2020).repartition(1)
nonhispUS = nonhispUS2000.union(nonhispUS2010).union(nonhispUS2020).repartition(1)

usData_1.show()
usData_2.show()
print("Hispanic Population")
hispUS.show()
print("Non-Hispanic Population")
nonhispUS.show()

# --------------- SAVE FILES --------------- #
savepath = path + "query_data/byCategory/"
usData_1.write.csv(savepath + "usData_1", header=True)
usData_2.write.csv(savepath + "usData_2", header=True)
hispUS.write.csv(savepath + "hispUS", header=True)
nonhispUS.write.csv(savepath + "nonhispUS", header=True)

print("Complete!")

spark.stop()