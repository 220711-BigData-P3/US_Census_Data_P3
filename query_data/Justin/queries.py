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

#Create view of joined populations table

populations.createOrReplaceTempView("populations")
# populations.show()


#Scalar/SRF to calculate the percentage of change from 2000 to 2010 for each state. 
#2000_to_2010_direction ouputs the direction of change (increase, decrease). This value will be used to compare 2020 prediction to 2020 population
'''
#Finding percentage increase/decrease between 2 points
    Using AK data
        point2000 = 626932
        point2010 = 710231
        
        1. Subtract point2000 from point2010: 710231 - 626932
        2. Divide the amount by point 2000 (decimal) ROUND (2) * 100 for percentage
'''

populations_with_change = spark.sql("""
                                    SELECT 
                                        state,
                                        2000_population,
                                        2010_population,
                                        ROUND(((2010_population - 2000_population) / 2000_population), 6) AS 2000_to_2010_change,
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

# populations_with_change.show(50)
populations_with_change.createOrReplaceTempView("populations_with_change_view")

# 2020_predicted will show the predicted 2020 population for each state based on 2000 - 2010.
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
# prediction_comparison.show(50)
prediction_comparison.createOrReplaceTempView("prediction_comparison_view")

#Compares 2020_direction_prediction to the ACTUAL direction of 2020 - 2020 population change. If they are equal, the trend prediction is correct. If not, leave out of the ouput
trend_states_bound_values = spark.sql("""
                                   SELECT
                                        state,
                                        2000_population,
                                        2010_population,
                                        2000_to_2010_change,
                                        2020_predicted,
                                        FLOOR((2010_population * 2000_to_2010_change / 1.5) + 2010_population) AS left_bound,
                                        FLOOR((2010_population * 2000_to_2010_change * 1.5) + 2010_population) AS right_bound,
                                        2020_population
                                    FROM
                                        prediction_comparison_view;
                                    """)

trend_states_bound_values.createOrReplaceTempView("trend_states_bound_values_view")


#Using the LEFT_BOUND and RIGHT_BOUND to determine whether a 2020_population vs 2020_predicted relationship is considered to be "close". 
#The "1.5" in the declaration of the columns above can be changed to vary the size of the bounds.
trend_following_states = spark.sql("""
                                   SELECT *
                                   FROM trend_states_bound_values_view
                                   WHERE 
                                    (2020_population BETWEEN left_bound AND right_bound);
                                   """)

trend_following_states.createOrReplaceTempView("trend_following_states_view")

# The dataframes below create a table out of 3 datasets. 
# Each dataset houses a decade's (2000, 2010, 2020) population as well as a column made to show the year for each state's population record (for line graph construction in tableu, a date column is necessary).
# The first shows ACTUAL (2000-2020) population data, while the second shows ACTUAL (2000-2010) data then PREDICTED 2020 Data. 
union_trend_following_states_populations = spark.sql("""
                                                        SELECT
                                                            state,
                                                            2000_population AS population,
                                                            "01/01/2000" AS year
                                                        FROM
                                                            trend_following_states_view
                                                        UNION
                                                        SELECT
                                                            state,
                                                            2010_population AS population,
                                                            "01/01/2010" AS year
                                                        FROM
                                                            trend_following_states_view
                                                        UNION
                                                        SELECT 
                                                            state,
                                                            2020_population AS population,
                                                            "01/01/2020" AS year
                                                        FROM 
                                                            trend_following_states_view;
                                                    """)

union_trend_following_states_predictions = spark.sql("""
                                                        SELECT
                                                            state,
                                                            2000_population AS population,
                                                            "01/01/2000" AS year
                                                        FROM
                                                            trend_following_states_view
                                                        UNION
                                                        SELECT
                                                            state,
                                                            2010_population AS population,
                                                            "01/01/2010" AS year
                                                        FROM
                                                            trend_following_states_view
                                                        UNION
                                                        SELECT
                                                            state,
                                                            2020_predicted AS population,
                                                            "01/01/2020" AS year
                                                        FROM 
                                                            trend_following_states_view;                            
                                                    """)

##Using the LEFT_BOUND and RIGHT_BOUND to determine whether a 2020_population vs 2020_predicted relationship is considered to be "close". 
#The "1.5" in the declaration of the columns above can be changed to vary the size of the bounds.
#Because we are looking for states that DID NOT follow the prediction, the NOT keyword is used with BETWEEN in the WHERE clause.
trend_breaking_states = spark.sql("""
                                   SELECT *
                                   FROM trend_states_bound_values_view
                                   WHERE 
                                    (2020_population NOT BETWEEN left_bound AND right_bound);
                                   """)

trend_breaking_states.createOrReplaceTempView("trend_breaking_states_view")

union_trend_breaking_states_populations = spark.sql("""
                                                        SELECT
                                                            state,
                                                            2000_population AS population,
                                                            "01/01/2000" AS year
                                                        FROM
                                                            trend_breaking_states_view
                                                        UNION
                                                        SELECT
                                                            state,
                                                            2010_population AS population,
                                                            "01/01/2010" AS year
                                                        FROM
                                                            trend_breaking_states_view
                                                        UNION
                                                        SELECT 
                                                            state,
                                                            2020_population AS population,
                                                            "01/01/2020" AS year
                                                        FROM 
                                                            trend_breaking_states_view
                                                    """)
union_trend_breaking_states_predictions = spark.sql("""
                                                        SELECT
                                                            state,
                                                            2000_population AS population,
                                                            "01/01/2000" AS year
                                                        FROM
                                                            trend_breaking_states_view
                                                        UNION
                                                        SELECT
                                                            state,
                                                            2010_population AS population,
                                                            "01/01/2010" AS year
                                                        FROM
                                                            trend_breaking_states_view
                                                        UNION
                                                        SELECT 
                                                            state,
                                                            2020_predicted AS population,
                                                            "01/01/2020" AS year
                                                        FROM 
                                                            trend_breaking_states_view
                                                    """) 


#Check to make sure total number of states is 50
print(trend_following_states.count())
print(trend_breaking_states.count())






#Write 4 Dataframes to csv:
# union_trend_following_states_populations.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Justin/union_trend_following_states_populations")
# union_trend_following_states_predictions.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Justin/union_trend_following_states_predictions")

# union_trend_breaking_states_populations.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Justin/union_trend_breaking_states_populations")
# union_trend_breaking_states_predictions.write.option("header", True).csv("file:/mnt/c/Users/jchou/Desktop/Us_Census_Data_P3/query_data/Justin/union_trend_breaking_states_predictions")