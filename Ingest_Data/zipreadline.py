import csv
import zipfile

# import required module
import os

def read2000():
    # assign directory
    directory = '//wsl$/Ubuntu/home/benjaminserio/P3_Census/'
    # iterate over files in
    # that directory
    for filename in os.listdir(directory):
        if "al00001" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                print(root.open(name).readline())
            root.close()
            # checking if it is a file
            if os.path.isfile(f):
                print(f)

def read2010():
    # assign directory
    directory = '//wsl$/Ubuntu/home/joredelma/2010data/'
    # iterate over files in
    # that directory
    for filename in os.listdir(directory):
        if "2010" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                if not "packing" in name and not "geo" in name:
                    s = str(root.open(name).readline().decode('utf-8'))
                    with open(os.path.join(directory, "line1"), 'a', encoding='utf-8') as csv_file:
                        csv_file.write(s)
            root.close()


if __name__ == "__main__":
    #read2000()
    read2010()