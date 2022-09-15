from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .master("local") \
    .appName("project3") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")


path = "file:/mnt/c/Users/Nithia Justin/Desktop/Revature/Revature_Projects/p3-Team-2/"

allData2000 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2000_1_Region.csv")

allData2010 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2010_1_Region.csv")

allData2020 = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "2020_1_Region.csv")

Fastest_growing_Region_2000_Data = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "Fastest_growing_Region_2000.csv")

Fastest_growing_Region_2010_Data = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "Fastest_growing_Region_2010.csv")

Fastest_growing_Region_2020_Data = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "Fastest_growing_Region_2020.csv")

Fastest_growing_Region_Final = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "Fastest_growing_Region.csv")

Fastest_growing_Region_Final_Year = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(path + "Fastest_growing_Region_Differance.csv")


allData2000.createOrReplaceTempView("allData2000")
allData2010.createOrReplaceTempView("allData2010")
allData2020.createOrReplaceTempView("allData2020")

Fastest_growing_Region_2000_Data.createOrReplaceTempView("Fastest_growing_Region_2000_Data")
Fastest_growing_Region_2010_Data.createOrReplaceTempView("Fastest_growing_Region_2010_Data")
Fastest_growing_Region_2020_Data.createOrReplaceTempView("Fastest_growing_Region_2020_Data")

Fastest_growing_Region_Final.createOrReplaceTempView("Fastest_growing_Region_Final")
Fastest_growing_Region_Final_Year.createOrReplaceTempView("Fastest_growing_Region_Final_Year")

Pop_Data_2000 = spark.sql("select Region, sum(P0010001) as TotalPopulation_2000, sum(P0010001)/285230516*100 as percentage_2000 from allData2000 group by Region order by sum(P0010001) Desc")
Pop_Data_2010 = spark.sql("select Region, sum(P0010001) as TotalPopulation_2010, sum(P0010001)/312471327*100 as percentage_2010 from allData2010 group by Region order by TotalPopulation_2010 Desc")
Pop_Data_2020 = spark.sql("select Region, sum(P0010001) as TotalPopulation_2020, sum(P0010001)/334735155*100 as percentage_2020 from allData2020 group by Region order by TotalPopulation_2020 Desc")

Fastest_growing_Region = spark.sql("select Fastest_growing_Region_2000_Data.Region, Fastest_growing_Region_2000_Data.TotalPopulation_2000, Fastest_growing_Region_2000_Data.percentage_2000, Fastest_growing_Region_2010_Data.TotalPopulation_2010, Fastest_growing_Region_2010_Data.percentage_2010, Fastest_growing_Region_2020_Data.TotalPopulation_2020, Fastest_growing_Region_2020_Data.percentage_2020 from ((Fastest_growing_Region_2000_Data inner join Fastest_growing_Region_2010_Data on Fastest_growing_Region_2000_Data.Region = Fastest_growing_Region_2010_Data.Region) inner join Fastest_growing_Region_2020_Data on Fastest_growing_Region_2000_Data.Region = Fastest_growing_Region_2020_Data.Region)")
Fastest_growing_Region_Differance = spark.sql("select Region, TotalPopulation_2000, percentage_2000, TotalPopulation_2010,percentage_2010, TotalPopulation_2020, percentage_2020, percentage_2010 - percentage_2000 as Differance_2000_2010, percentage_2020 - percentage_2010 as Differance_2010_2020, percentage_2020 - percentage_2000 as Differance_2000_2020 from Fastest_growing_Region_Final order by Differance_2000_2010 desc")

Fastest_growing_Region_Final_2000_2010 = spark.sql("select Region, Differance_2000_2010 from Fastest_growing_Region_Final_Year order by Differance_2000_2010 desc")
Fastest_growing_Region_Final_2010_2020 = spark.sql("select Region, Differance_2010_2020 from Fastest_growing_Region_Final_Year order by Differance_2010_2020 desc")
Fastest_growing_Region_Final_2000_2020 = spark.sql("select Region, Differance_2000_2020 from Fastest_growing_Region_Final_Year order by Differance_2000_2020 desc")

Fastest_growing_Region.show()
Fastest_growing_Region_Differance.show()
Fastest_growing_Region_Final_2000_2010.show()
Fastest_growing_Region_Final_2010_2020.show()
Fastest_growing_Region_Final_2000_2020.show()

spark.stop()