import zipfile
import upload_s3
import os

filenames_dict = {
    "2000_1": "2000_1.csv",
    "2000_2": "2000_2.csv",
    "2010_1": "2010_1.csv",
    "2010_2": "2010_2.csv",
    "2020_1": "2020_1.csv",
    "2020_2": "2020_2.csv"
}

directories = ["//wsl$/Ubuntu/home/jcho/census_data/zip_2000", 
               "//wsl$/Ubuntu/home/jcho/census_data/zip_2010",
               "//wsl$/Ubuntu/home/jcho/census_data/zip_2020"
               ]

def main():
    pass