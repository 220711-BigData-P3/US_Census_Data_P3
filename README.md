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
- The goal of this project is to gather insights and aggregates from redistricting data provided by the [US Census website](https://www.census.gov/data.html). This was achieved with the following steps: 
    1. Programmatically scrape all relevant data files from 3 (one for each decade from 2000) webpages
    2. Unzip and remodel data to include only necessary information
    3. Upload new data files into AWS S3 cloud storage
    4. Gather the following insights by querying/aggregating data from S3
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

```

## Screenshots
1. wget command
2. unzipping, extracting, upload process
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