# Running the Program:

## Table of Contents
* [Scraping Zip Files From URL](#Web Crawler Script for US Census Redistricting Data 2000/2010)
* [Ingesting 2000 Data](#technologies-used)
* [Ingesting 2010 Data](#features)
* [Injesting 2020 Data](#screenshots)

## Web Crawler Script for US Census Redistricting Data 2000/2010

Ingest_Data/main.py
```
 --change url depending on the year:
 ---2000: https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/
 ---2010: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/
 ---2020: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 [url]')

```
1. Navigate to the directory where zip files should be placed
2. Run python3/python [path_to_repo_on_local_machine]/Ingest_Data/main.py
3. The script will download every .zip file on the webpage

example:

![alt text](documentation_screenshots/zip_files.png "zip files in ubuntu")


