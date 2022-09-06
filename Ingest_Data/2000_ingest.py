"""
HLO:
    This program reads year-2000 redistricting census data from zip files and creates
    two new csv documents, '2000_1.csv' and '2000_2.csv.' If these files already exist,
    there is no need to run this program again.

    If upload_to_AWS = True, the two csv files will be uploaded to AWS S3 and deleted
    from local storage afterwards.
    If upload_to_AWS = False, the two .csv files will be saved to the cwd.
    The default setting is False. No zip files are extracted during runtime regardless
    of this choice.

Instructions:
    1. Set the 'zip_directory' variable to the location of the year-2000 .zip files
       on local storage. If this has not been done already, first follow the
       instructions for running 'web-scraper.py'.
    
    2. Keep the variable 'upload_to_AWS' set to 'False' if you want the generated
       .csv files to be saved to your local working directory.
       Set it to 'True' if you want to upload the files to an AWS S3 bucket instead (and
       follow the instructions for running 'upload_s3.py' before running this program).
    
    3. Run this program.
"""


import zipfile
import upload_s3
import os

def main():  
    fname1 = "2000_1.csv"
    fname2 = "2000_2.csv"
    ############################################################################
    #                                                                          #
    #                 VERIFY zip_directory BEFORE RUNNING                      #
    #                                                                          #
    ############################################################################
    zip_directory = '../2000/'
    # zip_directory = '//wsl$/Ubuntu/home/benjaminserio/P3_Census/'
    # iterate over files in that directory

    ############################################################################
    #                                                                          #
    #   upload_to_AWS = True:   Save generated csv files to cwd.               #
    #   upload_to_AWS = False:  Upload generated csv files to AWS S3 and       #
    #                           delete them locally afterwards.                #
    #                                                                          #
    ############################################################################
    upload_to_AWS = False

    with open(fname1, "w") as f1:
        with open("2000-1Header.csv", "r") as header_01:
            f1.write(header_01.readline())
            f1.write("\n")

    with open(fname2, "w") as f2:
        with open("2000-2Header.csv", "r") as header_02:
            f2.write(header_02.readline())
            f2.write("\n")
        

    for filename in sorted(os.listdir(zip_directory)):
        if "00001" in filename:
            f = os.path.join(zip_directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                with open(fname1, "a") as f1:
                    f1.write(root.open(name).readline().decode("utf-8"))
                root.close()
        elif "00002" in filename:
            f = os.path.join(zip_directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                with open(fname2, "a") as f2:
                    f2.write(root.open(name).readline().decode("utf-8"))
                root.close()

    if upload_to_AWS == True:
        upload_s3.upload_file_s3(fname1)
        os.remove(fname1)
        
        upload_s3.upload_file_s3(fname2)
        os.remove(fname2)

if __name__ == "__main__":
    main()
