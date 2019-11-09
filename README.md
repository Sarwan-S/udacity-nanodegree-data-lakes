# Description
This application performs Spark based ELT to read in songs and log data files in S3 and process them using Spark to create fact and dimension files which are then stored back on S3. The files containing the fact and dimensions can then be used for queries on song plays analysis for Sparkify. Sparkify would like to understand their users' usage patterns on their music streaming app.

Data collected by Sparkify include songs data files as well as event log data files, all of which are in JSON format and all of which sit on an Amazon S3 bucket. Once processed, the fact and dimension files are stored in parquet format, some of which are partitioned. These parquet files may then be read from big data tools like Spark/Python for querying.

# Purpose of Database
This data lake would allow the Sparkify analytics team to perform queries to find the following type insights, among others:

1. Most active users based on 
2. Most popular songs based on numer of times the song is played
3. Most popular artists based on number of times their songs are played
4. Songs that specific users like listening to
5. Popular songs in specific geographies

# Database Schema Design
A STAR schema design has been implemented. This simplifies the queries the analytics team will write through it's denormalised design. It also allows for faster aggregations as relevant information is captured in a denormalised structure.

This data lake consists of one fact table and five dimension tables. This design allows for the analytics team to run quick queries on the fact table to understand business process metrics on song plays by users in sessions. It also allows for queries to answer when, where and what type of questions (when were the songs played? which location were they played from? what songs are most popular?) using queries that combine the fact table with the dimension tables.


## Fact Table
### songplays
A fact table named **songplays** captures information from the log data files about which songs were played by which user during a session. The files are partitioned by year and month

The following attributes are captured in this fact table:

| Column      |
|-------------|
| songplay_id |
| start_time  |
| user_id     |
| level       |
| song_id     |
| artist_id   |
| session_id  |
| location    |
| user_agent  |
| month       |
| year        |

## Dimension Tables
### users
The **users** dimension table captures information from the log data files about the users of the music streaming app.

The following attributes are captured in this dimension table:

| Column     |
|------------|
| user_id    |
| first_name |
| last_name  |
| gender     |
| level      |

If a user changes his level from free to paid or vice versa, the database only takes the latest record.

### songs
The **songs** dimension table captures information from the songs data files about songs in the music database. The files are partitioned by year and artist_id.

The following attributes are captured in this dimension table:

| Column    |
|-----------|
| song_id   |
| title     |
| artist_id |
| year      |
| duration  |

### artists
The **artists** dimension table captures information from the songs data files about artists in the music database.

The following attributes are captured in this dimension table:

| Column    |
|-----------|
| artist_id |
| name      |
| location  |
| latitude  |
| longitude |

### time
The **time** dimension table captures information from the log data files about timestamps of records in **songplays** broken down into specific time units. The files are partitioned by year and month.

The following attributes are captured in this dimension table:

| Column     |
|------------|
| start_time |
| hour       |
| day        |
| week       |
| month      |
| year       |
| weekday    |

# ETL Pipeline
## Files
The following files are present in this project:

| File/Folder      | Description                                                                                 |
|------------------|---------------------------------------------------------------------------------------------|
| dl.cfg           | Configuration file to place all keys needed to connect to aws                               |
| data             | Folder containing sample songs and log data files for testing the application               |
| etl.py           | Python file which reads and processes all files using spark and saves them in parquets      |
| README.md        | A read me file which provides discussion on this spark ELT data lake application            |

## Usage

1. Update dl.cfg with aws secret and access keys needed to connect and read/write from/to aws S3 bucket

2. Update etl.py with output data location on S3 e.g "s3a://my-s3-bucketname/"

3. Run the etl.py file in a terminal using the following command:
```python etl.py```

