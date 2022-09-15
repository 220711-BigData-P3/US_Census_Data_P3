import os

'''
HLO
    This command scrapes the US Census website for .zip files and downloads them to the cwd.
    If the .zip files have already been retrieved, it is not necessary to run this again.


Details:
    Wget is a free webscraper package developed by GNU that works on the most widely used internet protocols
    (HTTP, HTTPS, FTP, FTPS).

    The decenial census data required for this project lies on webpages that are children of the following 3 webpages:

        2000: https://www2.census.gov/census_2000/datasets/redistricting_file--pl_94-171/
        2010: https://www2.census.gov/census_2010/redistricting_file--pl_94-171/
        2020: https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171/

    We scrape them by including modifiers that specify our needs:

        --no directories           keeps recurisve file downloading from creating a hierachy of directories. All files will get saved to the current directory.
      
        --content-disposition      Uses the server's suggested name for file naming as opposed to using the tail end of the URL.

        -e robots=off              Execute a `.wgetrc'-style command (in this case, don't download 'robots' text file that usually comes when using a scraper)

        -A.zip                     Only allows .zip files to be downloaded

        -r                         Downloads recursively (searches child pages w.r.t. given link)

        --no-parent                Restricts accessing links in webpages above the current directory

        -l 3                       Restricts directory depth of files to be downloaded (all required files are 3 or fewer pages beneath either directory)


Instructions:
    
    1.  In a terminal, cd to the directory you want to download the .zip files to.

    2.  Either:
        
        a.  Run this Python script or

        b.  Copy the argument of os.system(), paste it in the terminal, and run the command.
'''
 
#command:
os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 https://www2.census.gov/census_2000/datasets/redistricting_file_--p1_94-171/')