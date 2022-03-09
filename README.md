# Data Modeling with Postgres in Python: Building a Song Play Analysis ETL Pipeline and Database

**Sparkify**, a music streaming service, would like to perform analytics on data collected from their platform. In this project, I implement an ETL pipeline in Python that extracts data from platform logs and song data stored in .json files and stores it in a PostgreSQL database that has been modeled to perform well with expected queries. 

## Project Overview

The two main components of this project are the the data model that has been executed in PostgreSQL, and the ETL pipeline.

#### Data Model

The database for Sparkify's analytics data uses a star schema. In accordance with star schema, there is one fact table - **songplays** - and four dimension tables - **songs**, **artists**, **users**, and **time**. **Songplays** contains foreign keys for each dimension table, enabling joins. The star schema was chosen due to the use case; because analytics will be performed on this data, we require a schema that allows for fast aggregations and simplified queries.

#### ETL Pipeline

**Sparkify's** data is currently stored in two JSON datasets - songs and logs. The ETL pipeline performs the follwing steps:
1. Extracts data from JSON files and stores them in memory via Python's Pandas package
2. Performs filtering (log actions) and dtype conversions (timestamp to datetime)
3. Transforms data to fit the data model described above
4. Loads data into PostgreSQL tables, applying constraints to the UPSERT operation in order to enforce data quality

## Repository Overview

```
├───data  
│   ├───log_data
│   │   └───2018
│   │       └───11
│   │           └───.json
│   └───song_data 
│       └───A
│           ├───A
│           │   ├───A
│           │   │   └───.json
│           │   ├───B
│           │   │   └───.json
│           │   └───C
│           │       └───.json
│           └───B
│               ├───A
│               │   └───.json
│               ├───B
│               │   └───.json
│               └───C
│                   └───.json
├───create_tables.py  
├───etl.py  
├───README.md  
└───sql_queries.py  
```  

* The **data folders** contain log and song metadata files as .json.
* **create_tables.py** contains the logic for creation of data model in PostgreSQL.
* **etl.py** contains the logic for the ETL pipeline.
* **sql_queries.py** contains all of the PostgreSQL queries used in **etl.py** and **create_tables.py**.