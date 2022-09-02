# US Census Redistricting Data Analysis 
Analyzation of US redistricting data for the years 2000, 2010 and 2020


<!-- ## Table of Contents
* [General Info](#general-information)
* [Technologies Used](#technologies-used)
* [Features](#features)
* [Screenshots](#screenshots)
* [Setup](#setup)
* [Room for Improvement](#room-for-improvement)
* [Contact](#contact) -->


## General Information
The goal of this project is to gather insights and aggregates from redistricting data provided by the [US Census website](https://www.census.gov/data.html). This was achieved with the following steps: 

    1. Programmatically scrape all relevant data files from 3 (one for each decade from 2000) webpages
    2. Unzip and remodel data to include only necessary information
    3. Upload new data files into AWS S3 cloud storage
    4. Gather the following insights by querying/aggregating data from S3:

        * Which region is the most densely populated?
        * What are the populations for different race/ethnicities present in the United States?
        * Total population of the United States in the given years
        * Which regions are growing the fastest?
        * Which states have the highest rate of growth?
        * Are any states decreasing in population?

<!-- - Why did you undertake it? -->
<!-- You don't have to answer all the questions - just the ones relevant to your project. -->


## Technologies Used
- Python - 3.10.5
- AWS S3
- pySpark


<!-- ## Screenshots
![Example screenshot](./img/screenshot.png) -->
<!-- If you have screenshots you'd like to share, include them here. -->


## Setup
<!-- What are the project requirements/dependencies? Where are they listed? A requirements.txt or a Pipfile.lock file perhaps? Where is it located?

Proceed to describe how to install / setup one's local environment / get started with the project. -->
To run the program, clone or copy project folder onto local machine. Then:

Installing packages:
```
pip install boto3
pip install python-dotenv
pip install zipfile

```

## Usage

#### Web Scraping for Downloading Zip Files
```
os.system('wget --no-directories --content-disposition -e robots=off -A.zip -r --no-parent -l 3 https://www2.census.gov/census_2000/datasets/redistricting_file_--p1_94-171/')
```
```wget``` is the optimal way to download files recursively on a webpage, bypassing all html elements and text. Explanation of options below:
    
     ```--no directories``` keeps recurisve file downloading from creating a hierachy of directories. All files will get saved to the current directory. 
     ```--content-disposition``` uses the server's suggested name for file naming as opposed to using the tail end of the URL.
     ```-A.zip``` only allows .zip files to be downloaded
     ```-r``` downloads recursively
     ```no-parent``` restricts retrieval of links that refer to a hierarchy above the current directory
     ``` -l 3``` restricts directory depth of files to be downloaded 

#### Unzipping, Extracting and Uploading Files to S3

```zipfile``` allows for the quick unzipping of zip files in python. The package was used to unzip every zip file downloaded from the ```wget``` command, while providing access to the ```namelist()``` function which returns to us an iterable datatype containing the names of files in a .zip. Then the ```.open``` function was used iteratively to access these files, followed by writing the needed data into a csv file. 

```python
    for filename in sorted(os.listdir(directory)):
        if "usa" in filename:
            pass
        elif "00001" in filename:
            f = os.path.join(directory, filename)
            root =  zipfile.ZipFile(f, "r")
            for name in root.namelist():
                print(name)
                with open(fname1, "a") as y:
                    y.write(root.open(name).readline().decode("utf-8"))
                root.close()
```

After writing the necessary data into a csv file, the ```boto3``` package was implemented to access AWS S3 storage. An instance of boto3's ```Session``` class provides authentication and connectivity to a specific S3 bucket (client, resource). To upload a csv file, ```upload_file()``` was used.

```python
    def upload_file_s3(file_name, object_name=None):
        load_dotenv()

        access_key = os.getenv("ACCESS_KEY_ID")
        secret_access_key = os.getenv("SECRET_ACCESS_KEY")
        bucket_name = os.getenv("BUCKET_NAME")
        region_name = os.getenv("REGION_NAME")

        session = boto3.Session(
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_access_key,
            region_name = region_name
        )

        client = session.client('s3')
        """
        Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        try:
            response = client.upload_file(file_name, bucket_name, object_name)
        except ClientError as e:
            print(e)
            return False
        return True
```

3. examples of queries (3?)


<!-- ## Usage
How does one go about using it?
Provide various use cases and code examples here.

`write-your-code-here` -->


<!-- ## Project Status
Project is: _in progress_ / _complete_ / _no longer being worked on_. If you are no longer working on it, provide reasons why. -->


<!-- ## Acknowledgements
Give credit here.
- This project was inspired by...
- This project was based on [this tutorial](https://www.example.com).
- Many thanks to... -->


## Contributors
Names here!



<!-- Optional -->