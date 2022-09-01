from abbreviations import getmidwest, getnortheast, getsoutheast, getsouthwest, getwest, getstates
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_str, substring, lit
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local").appName("data").getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

states = getstates()
midwest = getmidwest()
northeast = getnortheast()
southeast = getsoutheast()
southwest = getsouthwest()
west = getwest()

#_filepath = 'file:/home/strumunix/Rev-P3/US_Census_Data_P3/'
_filepath = "file:/mnt/c/Users/phait/Desktop/Revature/220711-BigData-P3/"
file_name = "2000_1.csv"
rdd_2000 = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(_filepath + file_name)
)


#   $$$$$$\   $$$$$$\   $$$$$$\   $$$$$$\  
#  $$  __$$\ $$$ __$$\ $$$ __$$\ $$$ __$$\ 
#  \__/  $$ |$$$$\ $$ |$$$$\ $$ |$$$$\ $$ |
#   $$$$$$  |$$\$$\$$ |$$\$$\$$ |$$\$$\$$ |
#  $$  ____/ $$ \$$$$ |$$ \$$$$ |$$ \$$$$ |
#  $$ |      $$ |\$$$ |$$ |\$$$ |$$ |\$$$ |
#  $$$$$$$$\ \$$$$$$  /\$$$$$$  /\$$$$$$  /
#  \________| \______/  \______/  \______/ 
#                                          
#                                          
#                                          


#######################################################################################
#                                                                                     #
#                                                                                     #
#                   QUERYING EACH INDIVIDUAL REGION                                   #
#                                                                                     #
#                                                                                     #
#######################################################################################
data_view = rdd_2000.createOrReplaceTempView("2000_all")
midwest_df = rdd_2000.filter(rdd_2000.STUSAB.isin(midwest))
# midwest_df = rdd_2000.filter((rdd_2000.STUSAB == "IA") | (rdd_2000.STUSAB == "KS") 
#                             | (rdd_2000.STUSAB == "MO")| (rdd_2000.STUSAB == "NE") 
#                             | (rdd_2000.STUSAB == "ND") | (rdd_2000.STUSAB == "SD") 
#                             | (rdd_2000.STUSAB == "IL") | (rdd_2000.STUSAB == "IN") 
#                             | (rdd_2000.STUSAB == "MI") | (rdd_2000.STUSAB == "MN") 
#                             | (rdd_2000.STUSAB == "OH") | (rdd_2000.STUSAB == "WI"))
midwest_view = midwest_df.createOrReplaceTempView("midwest_view")

northeast_df = rdd_2000.filter(rdd_2000.STUSAB.isin(northeast)) 
# northeast_df = rdd_2000.filter((rdd_2000.STUSAB == "CT") | (rdd_2000.STUSAB == "DE") 
#                               | (rdd_2000.STUSAB == "MA") | (rdd_2000.STUSAB == "ME") 
#                               | (rdd_2000.STUSAB == "NH") | (rdd_2000.STUSAB == "NJ") 
#                               | (rdd_2000.STUSAB == "NY") | (rdd_2000.STUSAB == "PA") 
#                               | (rdd_2000.STUSAB == "RI") | (rdd_2000.STUSAB == "VT"))
northeast_view = northeast_df.createOrReplaceTempView("northeast_view")

southeast_df = rdd_2000.filter(rdd_2000.STUSAB.isin(southeast))
# southeast_df = rdd_2000.filter((rdd_2000.STUSAB == "AL") | (rdd_2000.STUSAB == "AR") 
#                               | (rdd_2000.STUSAB == "FL") | (rdd_2000.STUSAB == "GA") 
#                               | (rdd_2000.STUSAB == "KY") | (rdd_2000.STUSAB == "LA") 
#                               | (rdd_2000.STUSAB == "MD") | (rdd_2000.STUSAB == "MS") 
#                               | (rdd_2000.STUSAB == "NC") | (rdd_2000.STUSAB == "SC") 
#                               | (rdd_2000.STUSAB == "TN") | (rdd_2000.STUSAB == "VA") | (rdd_2000.STUSAB == "WV"))
southeast_view = southeast_df.createOrReplaceTempView("southeast_view")

southwest_df = rdd_2000.filter(rdd_2000.STUSAB.isin(southwest))
# southwest_df = rdd_2000.filter((rdd_2000.STUSAB == "AZ") | (rdd_2000.STUSAB == "CA") 
#                               | (rdd_2000.STUSAB == "CO") | (rdd_2000.STUSAB == "NV") 
#                               | (rdd_2000.STUSAB == "NM") | (rdd_2000.STUSAB == "OK") 
#                               | (rdd_2000.STUSAB == "TX") | (rdd_2000.STUSAB == "UT"))
southwest_view = southwest_df.createOrReplaceTempView("southwest_view")

west_df = rdd_2000.filter(rdd_2000.STUSAB.isin(west))
west_view = west_df.createOrReplaceTempView("west_view")



midwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM midwest_view")
northeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM northeast_view")
southeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southeast_view")
southwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southwest_view")
west_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM west_view")

#######################################################################################
#                                                                                     #
#                                                                                     #
#                 COMBINING THE INDIVIDUAL QUERIES TO DF                              #
#                                                                                     #
#                                                                                     #
#######################################################################################

midwest_final = midwest_pop.withColumn("region", lit("midwest"))
northeast_final = northeast_pop.withColumn("region", lit("northeast"))
southeast_final = southeast_pop.withColumn("region", lit("southeast"))
southwest_final = southwest_pop.withColumn("region", lit("southwest"))
west_final = west_pop.withColumn("region", lit("west"))



all_regions_2000 = sc.union([northeast_final.rdd, midwest_final.rdd, southeast_final.rdd, southwest_final.rdd, west_final.rdd]).toDF()
all_regions_view = all_regions_2000.createOrReplaceTempView("all_regions_view")
all_regions_2000_query = spark.sql("SELECT region, total_population FROM all_regions_view")
print("Regional data for 2000: ")
all_regions_2000_query.show()
highest_pop_2000 = spark.sql("SELECT region, total_population FROM all_regions_view WHERE total_population = (SELECT MAX(total_population) FROM all_regions_view)")
print("Most populous region for 2000: ")
highest_pop_2000.show()


#######################################################################################
#                                                                                     #
#                                                                                     #
#                               WRITING TO FILE                                       #
#                                                                                     #
#                                                                                     #
#######################################################################################
'''
file = open("pop_by_region_2000.csv", "w")
for i in range(len(all_regions_2000.collect())):
    tmp_region = all_regions_2000.collect()[i]['region']
    tmp_pop = all_regions_2000.collect()[i]['total_population']
    file.write(f"{tmp_region},{tmp_pop}\n")

file.write(f"max,{highest_pop_2000.collect()[0]['region']}")

file.close()
'''


#                                                          
#                                                          
#        ,----,       ,----..         ,---,     ,----..    
#      .'   .' \     /   /   \     ,`--.' |    /   /   \   
#    ,----,'    |   /   .     :   /    /  :   /   .     :  
#    |    :  .  ;  .   /   ;.  \ :    |.' '  .   /   ;.  \ 
#    ;    |.'  /  .   ;   /  ` ; `----':  | .   ;   /  ` ; 
#    `----'/  ;   ;   |  ; \ ; |    '   ' ; ;   |  ; \ ; | 
#      /  ;  /    |   :  | ; | '    |   | | |   :  | ; | ' 
#     ;  /  /-,   .   |  ' ' ' :    '   : ; .   |  ' ' ' : 
#    /  /  /.`|   '   ;  \; /  |    |   | ' '   ;  \; /  | 
#  ./__;      :    \   \  ',  /     '   : |  \   \  ',  /  
#  |   :    .'      ;   :    /      ;   |.'   ;   :    /   
#  ;   | .'          \   \ .'       '---'      \   \ .'    
#  `---'              `---`                     `---`      
#                                                          

file_name = "2010_1.csv"
rdd_2010 = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(_filepath + file_name)
)
#######################################################################################
#                                                                                     #
#                                                                                     #
#                   QUERYING EACH INDIVIDUAL REGION                                   #
#                                                                                     #
#                                                                                     #
#######################################################################################

data_view = rdd_2010.createOrReplaceTempView("2010_all")
midwest_df = rdd_2010.filter(rdd_2010.STUSAB.isin(midwest))
# midwest_df = rdd_2010.filter((rdd_2010.STUSAB == "IA") | (rdd_2010.STUSAB == "KS") 
#                             | (rdd_2010.STUSAB == "MO")| (rdd_2010.STUSAB == "NE")
#                             | (rdd_2010.STUSAB == "ND") | (rdd_2010.STUSAB == "SD") 
#                             | (rdd_2010.STUSAB == "IL") | (rdd_2010.STUSAB == "IN") 
#                             | (rdd_2010.STUSAB == "MI") | (rdd_2010.STUSAB == "MN") 
#                             | (rdd_2010.STUSAB == "OH") | (rdd_2010.STUSAB == "WI"))
midwest_view = midwest_df.createOrReplaceTempView("midwest_view")

northeast_df = rdd_2010.filter(rdd_2010.STUSAB.isin(northeast))
# northeast_df = rdd_2010.filter((rdd_2010.STUSAB == "CT") | (rdd_2010.STUSAB == "DE") 
#                               | (rdd_2010.STUSAB == "MA") | (rdd_2010.STUSAB == "ME") 
#                               | (rdd_2010.STUSAB == "NH") | (rdd_2010.STUSAB == "NJ") 
#                               | (rdd_2010.STUSAB == "NY") | (rdd_2010.STUSAB == "PA") 
#                               | (rdd_2010.STUSAB == "RI") | (rdd_2010.STUSAB == "VT"))
northeast_view = northeast_df.createOrReplaceTempView("northeast_view")

southeast_df = rdd_2010.filter(rdd_2010.STUSAB.isin(southeast))
# southeast_df = rdd_2010.filter((rdd_2010.STUSAB == "AL") | (rdd_2010.STUSAB == "AR") | (rdd_2010.STUSAB == "FL") 
#                               | (rdd_2010.STUSAB == "GA") | (rdd_2010.STUSAB == "KY") 
#                               | (rdd_2010.STUSAB == "LA") | (rdd_2010.STUSAB == "MD") 
#                               | (rdd_2010.STUSAB == "MS") | (rdd_2010.STUSAB == "NC") 
#                               | (rdd_2010.STUSAB == "SC") | (rdd_2010.STUSAB == "TN") 
#                               | (rdd_2010.STUSAB == "VA") | (rdd_2010.STUSAB == "WV"))
southeast_view = southeast_df.createOrReplaceTempView("southeast_view")

southwest_df = rdd_2010.filter(rdd_2010.STUSAB.isin(southwest))
southwest_df = rdd_2010.filter((rdd_2010.STUSAB == "AZ") | (rdd_2010.STUSAB == "CA") 
                              | (rdd_2010.STUSAB == "CO") | (rdd_2010.STUSAB == "NV") 
                              | (rdd_2010.STUSAB == "NM") | (rdd_2010.STUSAB == "OK") 
                              | (rdd_2010.STUSAB == "TX") | (rdd_2010.STUSAB == "UT"))
southwest_view = southwest_df.createOrReplaceTempView("southwest_view")

west_df = rdd_2010.filter(rdd_2010.STUSAB.isin(west))
west_df = rdd_2010.filter((rdd_2010.STUSAB == "AK") | (rdd_2010.STUSAB == "ID") 
                         | (rdd_2010.STUSAB == "MT") | (rdd_2010.STUSAB == "WY") 
                         | (rdd_2010.STUSAB == "WA") | (rdd_2010.STUSAB == "OR") | (rdd_2010.STUSAB == "HI"))
west_view = west_df.createOrReplaceTempView("west_view")

midwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM midwest_view")
northeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM northeast_view")
southeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southeast_view")
southwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southwest_view")
west_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM west_view")

#######################################################################################
#                                                                                     #
#                                                                                     #
#                 COMBINING THE INDIVIDUAL QUERIES TO DF                              #
#                                                                                     #
#                                                                                     #
#######################################################################################

midwest_final = midwest_pop.withColumn("region", lit("midwest"))
northeast_final = northeast_pop.withColumn("region", lit("northeast"))
southeast_final = southeast_pop.withColumn("region", lit("southeast"))
southwest_final = southwest_pop.withColumn("region", lit("southwest"))
west_final = west_pop.withColumn("region", lit("west"))

all_regions_2010 = sc.union([northeast_final.rdd, midwest_final.rdd, southeast_final.rdd, southwest_final.rdd, west_final.rdd]).toDF()
all_regions_view = all_regions_2010.createOrReplaceTempView("all_regions_view")
all_regions_2010_query = spark.sql("SELECT region, total_population FROM all_regions_view")
print("Regional data for 2010: ")
all_regions_2010_query.show()
highest_pop_2010 = spark.sql("SELECT region, total_population FROM all_regions_view WHERE total_population = (SELECT MAX(total_population) FROM all_regions_view)")
print("Most populous region for 2010: ")
highest_pop_2010.show()

#######################################################################################
#                                                                                     #
#                                                                                     #
#                               WRITING TO FILE                                       #
#                                                                                     #
#                                                                                     #
#######################################################################################
'''
file = open("pop_by_region_2010.csv", "w")

for i in range(len(all_regions_2010.collect())):
    tmp_region = all_regions_2010.collect()[i]['region']
    tmp_pop = all_regions_2010.collect()[i]['total_population']

    file.write(f"{tmp_region},{tmp_pop}\n")
file.write(f"max,{highest_pop_2010.collect()[0]['region']}")
file.close()
'''

#   .----------------.  .----------------.  .----------------.  .----------------. 
#  | .--------------. || .--------------. || .--------------. || .--------------. |
#  | |    _____     | || |     ____     | || |    _____     | || |     ____     | |
#  | |   / ___ `.   | || |   .'    '.   | || |   / ___ `.   | || |   .'    '.   | |
#  | |  |_/___) |   | || |  |  .--.  |  | || |  |_/___) |   | || |  |  .--.  |  | |
#  | |   .'____.'   | || |  | |    | |  | || |   .'____.'   | || |  | |    | |  | |
#  | |  / /____     | || |  |  `--'  |  | || |  / /____     | || |  |  `--'  |  | |
#  | |  |_______|   | || |   '.____.'   | || |  |_______|   | || |   '.____.'   | |
#  | |              | || |              | || |              | || |              | |
#  | '--------------' || '--------------' || '--------------' || '--------------' |
#   '----------------'  '----------------'  '----------------'  '----------------' 

file_name = "2020_P1.csv"
rdd_2020 = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(_filepath + file_name)
)

#######################################################################################
#                                                                                     #
#                                                                                     #
#                   QUERYING EACH INDIVIDUAL REGION                                   #
#                                                                                     #
#                                                                                     #
#######################################################################################

data_view = rdd_2020.createOrReplaceTempView("2020_all")
midwest_df = rdd_2020.filter(rdd_2020.STUSAB.isin(midwest))
# midwest_df = rdd_2020.filter((rdd_2020.STUSAB == "IA") | (rdd_2020.STUSAB == "KS") 
#                             | (rdd_2020.STUSAB == "MO")| (rdd_2020.STUSAB == "NE") 
#                             | (rdd_2020.STUSAB == "ND") | (rdd_2020.STUSAB == "SD") 
#                             | (rdd_2020.STUSAB == "IL") | (rdd_2020.STUSAB == "IN") 
#                             | (rdd_2020.STUSAB == "MI") | (rdd_2020.STUSAB == "MN") 
#                             | (rdd_2020.STUSAB == "OH") | (rdd_2020.STUSAB == "WI"))
midwest_view = midwest_df.createOrReplaceTempView("midwest_view")

northeast_df = rdd_2020.filter(rdd_2020.STUSAB.isin(northeast))
# northeast_df = rdd_2020.filter((rdd_2020.STUSAB == "CT") | (rdd_2020.STUSAB == "DE") 
#                               | (rdd_2020.STUSAB == "MA") | (rdd_2020.STUSAB == "ME") 
#                               | (rdd_2020.STUSAB == "NH") | (rdd_2020.STUSAB == "NJ") 
#                               | (rdd_2020.STUSAB == "NY") | (rdd_2020.STUSAB == "PA") 
#                               | (rdd_2020.STUSAB == "RI") | (rdd_2020.STUSAB == "VT"))
northeast_view = northeast_df.createOrReplaceTempView("northeast_view")

southeast_df = rdd_2020.filter(rdd_2020.STUSAB.isin(southeast))
# southeast_df = rdd_2020.filter((rdd_2020.STUSAB == "AL") | (rdd_2020.STUSAB == "AR") 
#                               | (rdd_2020.STUSAB == "FL") | (rdd_2020.STUSAB == "GA") 
#                               | (rdd_2020.STUSAB == "KY") | (rdd_2020.STUSAB == "LA") 
#                               | (rdd_2020.STUSAB == "MD") | (rdd_2020.STUSAB == "MS") 
#                               | (rdd_2020.STUSAB == "NC") | (rdd_2020.STUSAB == "SC") 
#                               | (rdd_2020.STUSAB == "TN") | (rdd_2020.STUSAB == "VA") | (rdd_2020.STUSAB == "WV"))
southeast_view = southeast_df.createOrReplaceTempView("southeast_view")

southwest_df = rdd_2020.filter(rdd_2020.STUSAB.isin(southwest))
# southwest_df = rdd_2020.filter((rdd_2020.STUSAB == "AZ") | (rdd_2020.STUSAB == "CA") 
#                               | (rdd_2020.STUSAB == "CO") | (rdd_2020.STUSAB == "NV") 
#                               | (rdd_2020.STUSAB == "NM") | (rdd_2020.STUSAB == "OK") 
#                               | (rdd_2020.STUSAB == "TX") | (rdd_2020.STUSAB == "UT"))
southwest_view = southwest_df.createOrReplaceTempView("southwest_view")

west_df = rdd_2020.filter(rdd_2020.STUSAB.isin(west))
# west_df = rdd_2020.filter((rdd_2020.STUSAB == "AK") | (rdd_2020.STUSAB == "ID") | (rdd_2020.STUSAB == "MT") 
#                          | (rdd_2020.STUSAB == "WY") | (rdd_2020.STUSAB == "WA") 
#                          | (rdd_2020.STUSAB == "OR") | (rdd_2020.STUSAB == "HI"))                         
west_view = west_df.createOrReplaceTempView("west_view")

midwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM midwest_view")
northeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM northeast_view")
southeast_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southeast_view")
southwest_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM southwest_view")
west_pop = spark.sql("SELECT SUM(P0010001) AS total_population FROM west_view")

#######################################################################################
#                                                                                     #
#                                                                                     #
#                 COMBINING THE INDIVIDUAL QUERIES TO DF                              #
#                                                                                     #
#                                                                                     #
#######################################################################################

midwest_final = midwest_pop.withColumn("region", lit("midwest"))
northeast_final = northeast_pop.withColumn("region", lit("northeast"))
southeast_final = southeast_pop.withColumn("region", lit("southeast"))
southwest_final = southwest_pop.withColumn("region", lit("southwest"))
west_final = west_pop.withColumn("region", lit("west"))

all_regions_2020 = sc.union([northeast_final.rdd, midwest_final.rdd, southeast_final.rdd, southwest_final.rdd, west_final.rdd]).toDF()
all_regions_view = all_regions_2020.createOrReplaceTempView("all_regions_view")
all_regions_2020_query = spark.sql("SELECT region, total_population FROM all_regions_view")
print("Regional data for 2020: ")
all_regions_2020_query.show()
highest_pop_2020 = spark.sql("SELECT region, total_population FROM all_regions_view WHERE total_population = (SELECT MAX(total_population) FROM all_regions_view)")
print("Most populous region for 2020: ")
highest_pop_2020.show()

#######################################################################################
#                                                                                     #
#                                                                                     #
#                               WRITING TO FILE                                       #
#                                                                                     #
#                                                                                     #
#######################################################################################
'''
file = open("pop_by_region_2020.csv", "w")
for i in range(len(all_regions_2020.collect())):
    tmp_region = all_regions_2020.collect()[i]['region']
    tmp_pop = all_regions_2020.collect()[i]['total_population']
    file.write(f"{tmp_region},{tmp_pop}\n")

file.write(f"max,{highest_pop_2010.collect()[0]['region']}")
file.close()
'''
spark.stop()