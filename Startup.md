# Running the Program:

## Data Injestion
Packages needed:
```
pip install python-dotenv
pip install boto3
```
### Goals
1. Programmatically download/unzip all zip files from census website urls by decade (2000, 2010, 2020)
2. Gather summary rows from each state to generate 2 csv files for each decade (The second file will house data under the "Over 18" category)
3. Upload generated csv files into a AWS S3 Bucket
### Steps
* [Scraping Zip Files From URL](#web-crawler-python-script-for-US-Census-Redistricting-Data-2000/2010)
* [Ingesting 2000 Data](#ingesting-data-for-2000)
* [Ingesting 2010 Data](#ingesting-data-for-2010)
* [Injesting 2020 Data](#ingesting-data-for-2020)

## Web Crawler Python Script for US Census Redistricting Data 2000/2010

Location: Ingest_Data/main.py

```
 --change url depending on the year:
 ---2000: https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/
 ---2010: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/
 ---2020: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 [url]')
```
1. Navigate to the directory where zip files should be placed
2. Run python3/python [path_to_repo_on_local_machine]/Ingest_Data/main.py
3. The script will download every .zip file on the webpage onto the current directory

Example:

```
mkdir 2010_zipfiles
cd 2010_zipfiles
python3 /path/to/localmachine/repo/Ingest_Data/main.py
ls
```
![alt text](documentation_screenshots/zip_files.png "zip files in ubuntu")

## S3 Bucket Environment Variables
Credentials for AWS S3 access are not included in the remote repository. Contact project owner for access and place received credentials inside a .env file in the root level of repository.

## Ingesting Data for 2000

## Ingesting Data for 2010
Location: Ingest_Data/2010_unzip_write.py

1. Set ```directory ``` to reference the directory containing the zip files for 2010

```
def main():
    # assign directory
    directory = '[path_to_2010_zip_files_directory]'
    initial_create()

```

2. Execute the python script
From root level:
```
python Ingest_Data/2010_unzip_write.py
```

3. Executing the script will result in 2 csv files (2010_1.csv and 2010_2.csv) to be uploaded into the S3 Bucket

## Ingesting Data for 2020

## Data Analysis

### Goals
1. Placeholder goal
### Steps Below
* [Table of content item placeholder](#placeholder-link)





