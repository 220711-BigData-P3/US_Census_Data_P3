
import zipfile

# import required module
import os

def main():
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

if __name__ == "__main__":
    main()