# The final project - Capstone project of Udacity Data Engineer.
-----
## Project Summary
This project is about designing an ETL pipeline in order to integrate data from different sources and transform then loading into a datawarehouse for data analytics purpose. Datasets are ones provided from Udacity. 

- This project is followed by the 5 steps suggested, and the dataset provided - I94 Immigration dataset.
- Please follow the file Capstone Project Template to see my thought process as well as code snipets to solve this project. 
- I find there's more room to improve this project (such as testing, automation and migrate data to AWS Redshift/Snowflake). These ideas will be initiated in the future.

- The project follows the follow steps:
   * Step 1: Scope the Project and Gather Data
   * Step 2: Explore and Assess the Data
   * Step 3: Define the Data Model
   * Step 4: Run ETL to Model the Data
   * Step 5: Complete Project Write Up
   
# The rationale for the choice of tools and tech in the project
- Python and its libraries: Easy to handle and process data.
- Spark: its capability to deal with large volume of data in distributed system.
- AWS: s3 bucket because of its cost efficiency in storage on Cloud. Therefore, easy to access to. 

# How often data should be updated
- Given the context (US Immigration - often dealing with daily large volume of data),fact and dim_person data should be updated every 4 hours. 
- However for other dim tables requiring fewer updates could be considered to update fortnightly, monthly or even yearly (ie: dim_transport).
 
# How to approach with following scenarios
## The data was increases by 100x
- If spark in a stand alone mode could not handle the volume of such large data, it should move up to AWS EMR cluster, which is a distributed data cluster for processing large data. 

## Dashboard updated on a daily basis by 7am
- ETL pipeline should be scheduled to run at least twice daily. and the tool to be proposed for such scenari is Apache Airflow. 

## Database needed to be accessed by 100+ people
- Currently, Snowflake is fairly popular due to its friendliness to non-tech end-users and the tool is working On cloud, easy to connect with AWS services. 
- Else, AWS Redshift is an option worthy of consideration.  