import os
from pyspark.sql import SparkSession
import pyspark.sql.functions


#Create SparkSession
spark = SparkSession.builder\
            .master("local")\
            .appName("2020_population_prediction")\
            .getOrCreate()

            
state_abbrevs = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']


#Path Building
project_root_path = os.getcwd().split('Us_Census_Data')[0] + 'Us_Census_Data'
