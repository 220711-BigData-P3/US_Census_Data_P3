
import zipfile

# import required module
import os

def main():
    # assign directory
    # directory = '//wsl$/Ubuntu/home/benjaminserio/P3_Census/'
    # iterate over files in
    # that directory

    cwd = os.getcwd()  # Get the current working directory (cwd)
    files = os.listdir(cwd)  # Get all the files in that directory
    print("Files in %r: %s" % (cwd, files))
    directory = str(cwd) + '/2000/'


    for filename in sorted(os.listdir(directory)):
        if "00001" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                line = str(root.open(name).readline().decode('utf-8'))

                with open(directory + 'summaries.csv', 'a', encoding='utf-8') as f:
                    f.write(line)
                root.close()

if __name__ == "__main__":
    main()