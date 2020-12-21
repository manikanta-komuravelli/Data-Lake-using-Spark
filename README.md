Project 4 Date Lake with Amazon S3, EMR, Spark.

Context:

A Music streaming start up Sparkify has a growing user base and song database; their user activity and songs metadata resides in json files in an S3 bucket. My goal for this project is to build an ETL pipeline to extract the data from the Data Lake on S3, process them using Spark, and load them back into S3 as dimension tables in parquet format. Which can be later used by analytical team to perform their analysis in the pursuit of finding the insights of various songs their users are listening to.

Steps Follwed:
Updated the credentials to access the S3 bucket in the configuration file
Created the Spark Session
Loaded the data from S3 buckets to the Spark Dataframes and processed them and loaded them back as dimension tables in parquet format back into the S3.

Files used:
dl.cfg -> Configuration file with AWS credentials
etl.py -> Python script to extract, transform and load the data from S3 and back to S3
Readme.md -> Steps describing the actions performed in the project.

ETL creation:
With the help of the configuration file and the credentials, The input data is loaded from the S3 bucket.
Input Data consists of json logs for Songs and Logs.

Song Data: 
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

Log Data:
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}


Created Fact and Dimension tables from the about data, which are categorised as below:
Fact Table: Songs_play table
            songplay_id, start_time, user_id , level, song_id , artist_id, session_id, location , user_agent are the columns used in the fact table.
Dimension Tables: songs table, artists table, users table, time table.

Execution Steps followed:

Read the configuration file to load the credentials
Created the Spark Job to retreive and process the data from S3.
Created the dimension tables by doing some transformation using Spark
Created the Fact table with the help of the Song Data, Log Data.
Written back the dimensional tables and the Fact tables back to S3 in the parquet format.

Documented Process in the README.md file.