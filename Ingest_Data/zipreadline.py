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
    head1 = ''
    with open("//wsl$/Ubuntu/home/joredelma/2010-PL1Header.csv") as b:
        head1 = b.readline()[3:]
    head2 = ''
    with open("//wsl$/Ubuntu/home/joredelma/2010-PL2Header.csv") as b:
        head2 = b.readline()[3:]
    c1 = 0
    c2 = 0
    for filename in os.listdir(directory):
        if "2010" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                if not "packing" in name and not "geo" in name:
                    s = str(root.open(name).readline().decode('utf-8'))
                    if "12" in name:
                        with open(os.path.join(directory, "line1"), 'a', encoding='utf-8') as csv_file:
                            if c1 == 0:
                                c1 = c1 + 1
                                csv_file.write(head1)
                            csv_file.write(s)
                    if "22" in name:
                        with open(os.path.join(directory, "line2"), 'a', encoding='utf-8') as csv_file:
                            if c2 == 0:
                                c2 = c2 + 1
                                csv_file.write(head2)
                            csv_file.write(s)
            root.close()


if __name__ == "__main__":
    #read2000()
    read2010()