# Data Engineering Capstone Project: 
# i-94 Immigration Analtyics 


##### PROJECT SUMMARY: 
The objective of this project was to create an Elt pipeline for I94 immigration, global land temperatures and happiness and Human development index datsets to form an analytics database on immigration events. 

A use case for this analytics database is to find immigration patterns for the US immigration department.

For example, they could try to find answears to questions such as,

- Do people from countries with warmer or cold climate immigrate to the US in large numbers?
- Do people come from developed countries?
- Does Freedom and Human development imply in the number of people coming in to the us?

##### Data and Code
All the data for this project was loaded into S3 prior to commencing the project. 

The project workspace includes:

etl.py - reads data from S3, processes that data using Spark, and writes processed data as a set of dimensional tables back to S3
etl_functions.py - these modules contains the functions for creating fact and dimension tables, data visualizations and cleaning.
config.cfg - contains configuration that allows the ETL pipeline to access AWS EMR cluster.
Jupyter Notebooks - jupyter notebook that was used for building the ETL pipeline.

## Open-Ended Project
If you decide to design your own project, you can find useful information in the Project Resources section. Rather than go through steps below with the data Udacity provides, you'll gather your own data, and go through the same process.

## Instructions
To help guide your project, we've broken it down into a series of steps.

### Step 1: Scope the Project and Gather Data
Since the scope of the project will be highly dependent on the data, these two things happen simultaneously. In this step, you’ll:

Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

### Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data

### Step 3: Define the Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model

### Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness

### Step 5: Complete Project Write Up
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.
Document the steps of the process.
Propose how often the data should be updated and why.
Post your write-up and final data model in a GitHub repo.
Include a description of how you would approach the problem differently under the following scenarios:
If the data was increased by 100x.
If the pipelines were run on a daily basis by 7am.
If the database needed to be accessed by 100+ people.
Rubric

In the Project Rubric, you'll see more detail about the requirements. Use the rubric to assess your own project before you submit to Udacity for review. As with other projects, Udacity reviewers will use this rubric to assess your project and provide feedback. If your project does not meet specifications, you can make changes and resubmit.