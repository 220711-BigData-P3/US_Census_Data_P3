import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.context import SparkContext


#Create SparkSession
spark = SparkSession.builder\
            .master("local")\
            .appName("2020_population_prediction")\
            .getOrCreate()
            
sc = SparkContext.getOrCreate()
sc.setLogLevel("Warn")
            
state_abbrevs = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']


#Path Building
project_root_path = "file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/"
print(project_root_path)

#######################VIEWS##################
#Creating TempViews
filenames = ['2000_1.csv', '2010_1.csv', '2020_P1.csv']

def createTempViews():
    for file in filenames:
        year = file.split("_")[0]
        filepath = project_root_path + f'/{file}'
        print(filepath)
        df = spark.read.option('header', True).option("inferSchema", True).csv(filepath)
        filtered_df = df.select(col("STUSAB"), col("P0010001").alias(f"population")).filter(col("STUSAB").isin(*state_abbrevs))
        filtered_df.createOrReplaceTempView(f"{file.split('.')[0]}_view")
        
createTempViews()
spark.sql('SHOW VIEWS').show()
'''
    Output of show():
        +---------+------------+-----------+
        |namespace|    viewName|isTemporary|
        +---------+------------+-----------+
        |         | 2000_1_view|       true|
        |         | 2010_1_view|       true|
        |         |2020_p1_view|       true|
        +---------+------------+-----------+
    
'''
    
#Join 2000, 2010 views on "STUSAB" into 1 dataframe
populations = spark.sql("""
                      SELECT 
                        v1.STUSAB as State,
                        v1.population AS 2000_population,
                        v2.population AS 2010_population,
                        v3.population AS 2020_population
                      FROM 
                        2000_1_view AS v1 
                        JOIN 2010_1_view AS v2
                        JOIN 2020_P1_view AS v3
                      ON 
                        v1.STUSAB = v2.STUSAB
                      AND v1.STUSAB = v3.STUSAB  
                      """)

print(populations.count()) #50
'''
    Sample show of populations dataframe:
    
        |State|2000_population|2010_population|
        +-----+---------------+---------------+
        |   AK|         626932|         710231|
        |   AL|        4447100|        4779736|
        |   AR|        2673400|        2915918|
        |   AZ|        5130632|        6392017|
        |   CA|       33871648|       37253956|
        |   CO|        4301261|        5029196|
        |   CT|        3405565|        3574097|
        |   DE|         783600|         897934|
        |   FL|       15982378|       18801310|
        
'''

#Create view of joined populations table

populations.createOrReplaceTempView("populations")
# populations.show()


#Use analytic function on each row to calculate the percentage of increase/decrease
'''
#Finding percentage increase/decrease between 2 points
    Using AK data
        point2000 = 626932
        point2010 = 710231
        
        1. Subtract point2000 from point2010: 710231 - 626932
        2. Divide the amount by point 2010 (decimal) ROUND (2) * 100 for percentage
'''

populations_with_change = spark.sql("""
                                    SELECT 
                                        state,
                                        2000_population,
                                        2010_population,
                                        ROUND(((2010_population - 2000_population) / 2010_population), 6) AS 2000_to_2010_change,
                                        (CASE
                                            WHEN 2000_population > 2010_population THEN "Decrease"
                                            WHEN 2000_population < 2010_population THEN "Increase"
                                            ELSE "No Change"
                                        END) as 2000_to_2010_direction,
                                        2020_population,
                                        (CASE
                                            WHEN 2010_population > 2020_population THEN "Decrease"
                                            WHEN 2010_population < 2020_population THEN "Increase"
                                            ELSE "No Change"
                                        END) as 2010_to_2020_direction
                                        
                                    FROM 
                                        populations
                                    """)

populations_with_change.show(50)
populations_with_change.createOrReplaceTempView("populations_with_change_view")
# prediction_comparison = spark.sql("""
#                                   SELECT 
#                                     state,
#                                     2000_population,
#                                     2010_population,
#                                     2000_to_2010_change,
#                                     FLOOR((2010_population * 2000_to_2010_change) + 2010_population) AS 2020_population_prediction,
#                                     2000_to_2010_direction AS 2020_direction_prediction,
#                                     2020_population,
#                                   FROM 
#                                     populations_with_change_view;
#                                   """)
prediction_comparison = spark.sql("""
                                SELECT 
                                    state,
                                    2000_population,
                                    2010_population,
                                    2000_to_2010_change,
                                    2000_to_2010_direction AS 2020_direction_prediction,
                                    FLOOR((2010_population * 2000_to_2010_change) + 2010_population) AS 2020_predicted,
                                    2010_to_2020_direction,
                                    2020_population
                                FROM 
                                    populations_with_change_view;
                                  
                                  """)

prediction_comparison.printSchema()
prediction_comparison.show(50)
prediction_comparison.createOrReplaceTempView("prediction_comparison_view")

trend_following_states = spark.sql("""
                                SELECT
                                    state,
                                    2000_population,
                                    2010_population,
                                    2000_to_2010_change,
                                    2020_predicted,
                                    2020_population
                                FROM
                                    prediction_comparison_view
                                WHERE
                                    2020_direction_prediction = 2010_to_2020_direction
                                   
                                   """)

trend_breaker_states = spark.sql("""
                                SELECT
                                    state,
                                    2000_population,
                                    2010_population,
                                    2000_to_2010_change,
                                    2020_predicted,
                                    2020_population
                                FROM
                                    prediction_comparison_view
                                WHERE
                                    2020_direction_prediction != 2010_to_2020_direction
                                 """)


#Get AVG rate of change for predictions that met. compare that to avg rate of change of actual results

trend_following_states.show(50)
trend_breaker_states.show(50)


