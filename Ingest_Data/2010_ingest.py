# import required module
import os
import zipfile
import upload_s3

pl1_header_file = '../2010-1Header.csv'
pl1_data_file = '../2010_1.csv'
pl2_header_file = '../2010-2Header.csv'
pl2_data_file = '../2010_2.csv'


def main():
    # assign directory
    directory = '//wsl$/Ubuntu/home/jcho/census_data/2010_zips/'
    # iterate over files in
    # that directory
    initial_create()

    for filename in sorted(os.listdir(directory)):
        f = os.path.join(directory, filename)
        root = zipfile.ZipFile(f, "r")
     
        for name in root.namelist():
            if "packinglist" not in name and "geo" not in name:
                line = root.open(name).readline().decode('utf-8')
        
                if '12' in name:
                    append_line(line, 1)    
                if '22' in name:
                    append_line(line, 2)
                    
        root.close()
            
    upload_s3.upload_file_s3(pl1_data_file)
    os.remove(pl1_data_file)
    
    upload_s3.upload_file_s3(pl2_data_file)
    os.remove(pl2_data_file)
    
    

def append_line(line, plNumber):
    if plNumber == 1:
        with open(pl1_data_file, 'a') as p1:
            p1.write(line)
    
    if plNumber == 2:
        with open(pl2_data_file, 'a') as p2:
            p2.write(line)
        
def initial_create():
    with open(pl1_header_file, 'r') as f1:
        header = f1.readline()
        
    with open(pl1_data_file, 'w') as f2:
        f2.write(header)
        f2.write('\n')
    
    with open(pl2_header_file, 'r') as f3:
        header = f3.readline()
        
    with open(pl2_data_file, 'w') as f4:
        f4.write(header)
        f4.write('\n')

if __name__ == "__main__":
    main()