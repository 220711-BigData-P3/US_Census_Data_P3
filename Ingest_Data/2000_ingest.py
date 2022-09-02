'''
HLO: This program reads census data directly from zip files and joins relevant
lines together in two csv documents. The documents are deleted from local storage
after they are uploaded to AWS S3. No zip files are extracted during runtime and
no new files created by this program remain on local storage afterwards.

Instructions:
    1. set the zip_directory to the location of the census .zip files on local storage
    2. run program.
'''


import zipfile
import upload_s3
# import required module
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

    upload_s3.upload_file_s3(fname1)
    os.remove(fname1)
    
    upload_s3.upload_file_s3(fname2)
    os.remove(fname2)

if __name__ == "__main__":
    main()