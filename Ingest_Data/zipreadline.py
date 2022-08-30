
import zipfile

# import required module
import os

def main():
    fname1 = "2000_1.csv"
    fname2 = "2000_2.csv"
    # assign directory
    directory = '//wsl$/Ubuntu/home/benjaminserio/P3_Census/'
    # iterate over files in
    # that directory
    with open(fname1, "w") as z:
        with open("2000-1Header.csv", "r") as header_01:
            z.write(header_01.readline())
            z.write("\n")

    with open(fname2, "w") as y:
        with open("2000-2Header.csv", "r") as header_02:
            y.write(header_02.readline())
            y.write("\n")
        

    for filename in os.listdir(directory):
        if "us" in filename:
            pass
        elif "00001" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                with open(fname1, "a") as y:
                    y.write(root.open(name).readline().decode("utf-8"))
                root.close()

    for filename in os.listdir(directory):
        if "us" in filename:
            pass
        elif "00002" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                with open(fname2, "a") as z:
                    z.write(root.open(name).readline().decode("utf-8"))
                root.close()

if __name__ == "__main__":
    main()