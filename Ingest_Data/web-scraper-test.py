import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

'''
wget webcrawler command for files:

 --change url depending on the year:
 ---2000: https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/
 ---2010: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/
 ---2020: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

 --run inside of directory where the files will be stored  
 '''
 
def url_scrape(directory):
    urls = ['https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/', 'https://www2.census.gov/census_2010/redistricting_file--pl_94-171/', 'https://www2.census.gov/programs-surveys/decennial/2020/data/']
    
    for url in urls:
        if '2000' in url:
            os.system(f'rm -r /{directory}/2000_zips; mkdir {directory}/2000_zips')
            os.system(f'wget -P/{directory}/2000_zips --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 {url}')
        elif '2010' in url:
            os.system(f'rm -r /{directory}/2010_zips; mkdir {directory}/2010_zips')
            os.system(f'wget -P/{directory}/2010_zips --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 {url}')
        elif '2020' in url:
            os.system('rm -r 2020_zips; mkdir 2020_zips')
            os.system(f'wget -P/{directory}/2020_zips --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 {url}')
        else:
            return False
        
       
    return True

def unzip_and_upload(directory, year):
    header_files = {
        "2000_pl1_header_file": '../2000-1Header.csv',
        "2000_pl2_header_file": '../2000-2Header.csv',
        "2010_pl1_header_file": '../2010-1Header.csv',
        "2010_pl1_header_file": '../2010-2Header.csv',
        "2020_pl1_header_file": '../2020-1Header.csv',
        "2020_pl2_header_file": '../2020-2Header.csv'
    }
    
    headers= []
    
    for header_path in header_files.values():
        if header_path
    
    
    
    
    
    
    
        
def main():
    print("directory:")
    dir_name = input()
    os.system(f'cd {dir_name}')
    scrape_result = url_scrape(dir_name)
    if scrape_result == False:
        print('Please check urls and try again.')
        return False
    else:
        print('Data Scraping Success')
        return True
    
    

#command:
# os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 https://www2.census.gov/census_2000/datasets/redistricting_file_--p1_94-171/')

if __name__=='__main__':
    main()