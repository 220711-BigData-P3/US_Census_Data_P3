import os

'''
wget webcrawler command for files:

 --change url depending on the year:
 ---2000: https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/
 ---2010: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/
 ---2020: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

 --run inside of directory where the files will be stored  
 '''
 
#command:
os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 https://www2.census.gov/census_2000/datasets/redistricting_file_--p1_94-171/')