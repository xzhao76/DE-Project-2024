# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```

## My Code for HW 5
```
import os
import pyspark
from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = "/home/ximin/anaconda3/lib/python3.12/site-packages/pyspark"
os.environ["PYSPARK_PYTHON"] = "/home/ximin/anaconda3/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "notebook"

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
# Question 1
spark.version

import pandas as pd

df = pd.read_parquet("data/raw/yellow/2024/10/yellow_tripdata_2024_10.parquet")

df = df.head()

print(df.dtypes)

df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype('datetime64[ns]')
df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype('datetime64[ns]')
df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype(str)

print(df.dtypes)

spark.createDataFrame(df).schema

# Question 2

df_yellow = spark.read \
    .option("header","true")\
    .parquet('data/raw/yellow/2024/10/yellow_tripdata_2024_10.parquet')

df_yellow.printSchema()

df_yellow = df_yellow.repartition(4)

df_yellow.show(5)

from pyspark.sql import functions as F

df_yellow.write.parquet('output', mode='overwrite')

# Question 3
df_yellow.select('tpep_pickup_datetime')\
         .filter(F.to_date("tpep_pickup_datetime") == F.lit("2024-10-15"))\
         .groupBy(F.to_date("tpep_pickup_datetime").alias("date"))\
         .agg(F.count("*").alias("number_of_trips")).show()

# Question 4
df_yellow.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
).agg(F.max("duration_hours").alias("max_duration_hours")).show()

zlu = pd.read_csv("taxi_zone_lookup/taxi_zone_lookup.csv")

zlu.head()

# Question 6
zlu = spark.read \
    .option("header","true")\
    .csv("taxi_zone_lookup/taxi_zone_lookup.csv")

df_yellow\
        .join(zlu, df_yellow["PULocationID"]==zlu["LocationID"], how="left")\
        .groupBy(["PULocationID", "Zone"])\
        .agg(F.count("Zone").alias("number_of_pu_location_zone"))\
        .orderBy("number_of_pu_location_zone", ascending=True)\
        .show()

print(df_yellow)
print(zlu)  
```

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```
spark.version
```
Return result: 3.5.5

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB (Check)
- 75MB
- 100MB


## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567  (Check)
- 145,567


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162 (Check)
- 182


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040 (Check)
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island (Check)
- Arden Heights
- Rikers Island
- Jamaica Bay


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
