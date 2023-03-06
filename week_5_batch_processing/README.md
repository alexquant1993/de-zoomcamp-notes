# Week 5: Batch Processing

## Processing data

- Batch processing: processing chunks of data at regular intervals. Example: processing taxi trips each month.
- Streaming processing: processing data on the fly. Example: processing a taxi trip as soon as it's generated.

## Types of batch jobs
A *batch job* is a job (a unit of work) that will process data in batches.
Batch jobs may be scheduled in many ways: Weekly, daily, hourly, 3 times per hour, etc.

Batch jobs may also be carried out using different technologies:

- Python scripts (like the data pipelines in lesson 1).
- Python scripts can be run anywhere (Kubernetes, AWS Batch, ...)
- SQL (like the dbt models in lesson 4).
- Spark (what we will use for this lesson)
- Flink
- Etc...

## Orchestrating batch jobs
Batch jobs are commonly orchestrated with tools such as Airflow / Prefect.
A common workflow for batch jobs may be the following:
- DataLake CSV -> Python -> SQL-dbt -> Spark -> Python

## Pros and cons of batch jobs
- *Advantages:*
    - Easy to manage. There are multiple tools to manage them (the technologies we already mentioned)
    - Re-executable. Jobs can be easily retried if they fail.
    - Scalable. Scripts can be executed in more capable machines; Spark can be run in bigger clusters, etc.
- *Disadvantages:*
    - Delay. Each task of the workflow in the previous section may take a few minutes; assuming the whole workflow takes 20 minutes, we would need to wait those 20 minutes until the data is ready for work.

However, the advantages of batch jobs often compensate for its shortcomings, and as a result most companies that deal with data tend to work with batch jobs mos of the time (probably 90%).

## Spark

### What is Spark?

Spark is a framework for processing data in a distributed manner. It's written in Scala, but it has APIs for Python, Java, R, and SQL. In other words, it's an open-source multi-language unified analytics engine for large-scale data processing. Why engine? Because it processes data. It process data in a distributed manner because it can be run in clusters with multiple nodes, each pulling and transforming data.

- Spark can be ran in clusters with multiple nodes, each pulling and transforming data.
- Spark is multi-language because we can use Java and Scala natively, and there are wrappers for Python, R and other languages.
- The wrapper for Python is called PySpark and for R sparklyr.
- Spark can deal with both batches and streaming data. The technique for streaming data is seeing a stream of data as a sequence of small batches and then applying similar techniques on them to those used on regular badges. We will cover streaming in detail in the next lesson.

### Why do we need Spark?

Spark is used for transforming data in a Data Lake. There are tools such as Hive, Presto or Athena (a AWS managed Presto) that allow you to express jobs as SQL queries. However, there are times where you need to apply more complex manipulation which are very difficult or even impossible to express with SQL (such as ML models); in those instances, Spark is the tool to use.

- Data Lake -> Can the Job be expressed as SQL? -> Yes: Use Hive, Presto, Athena -> No: Use Spark -> Data Lake

A typical workflow may combine both tools. Here's an example involving ML:

- Raw data -> Data Lake -> SQL Athena job -> Spark job -> Use a model or Train a model with a Python job (ML) -> Model -> Spark job to apply model -> Save output to Data Lake

## Installing Spark on Windows

- Install JDK (Java) 11 from this [link](https://www.oracle.com/de/java/technologies/javase/jdk11-archive-downloads.html). Download the "Windows x64 Compressed Archive". Use version 11, given that Spark doesn't support newer versions. Add the path to the environment variable JAVA_HOME.
- Install Hadoop:
    - Create directory: `mkdir -p /c/tools/hadoop-3.2.0/bin` and `cd /c/tools/hadoop-3.2.0/bin`
    - Dowload hadoop winutils files and make sure to add the path to the environment variables: 
```bash
HADOOP_VERSION="3.2.0"
PREFIX="https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-${HADOOP_VERSION}/bin/"

FILES="hadoop.dll hadoop.exp hadoop.lib hadoop.pdb libwinutils.lib winutils.exe winutils.pdb"

for FILE in ${FILES}; do
  wget "${PREFIX}/${FILE}"
done
```

- Install spark:
    - Move to tools directory: `cd /c/tools`
    - `wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz`
    - Unzip folder
- Make sure to add the path to the environment variables: JAVA_HOME, HADOOP_HOME, SPARK_HOME and add `%SPARK_HOME%\bin` to the PATH variable.
- Check if installation was successful: `spark-shell`

### Install PySpark
- Add PYTHONPATH to environment variables in CMD:
    - `setx PYTHONPATH "%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.9.5-src.zip"`
- Testing pyspark installation - make sure you restart your terminal before checking. First get, CSV file for testing: `wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "True") \
    .csv('taxi+_zone_lookup.csv')

df.show()
df.write.parquet('zones')
```
- Access to SpARK UI: `http://localhost:4040/`

## Creating a Spark Session

We first need to import PySpark to our code:

```python
import pyspark
from pyspark.sql import SparkSession
```

We now need to instantiate a Spark session, an object that we use to interact with Spark.

```python	
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

- SparkSession is the class of the object that we instantiate. builder is the builder method.
- master() sets the Spark master URL to connect to. The local string means that Spark will run on a local cluster. [*] means that Spark will run with as many CPU cores as possible.
- appName() defines the name of our application/session. This will show in the Spark UI.
- getOrCreate() will create the session or recover the object if it was previously created.

Once we've instantiated a session, we can access the Spark UI by browsing to localhost:4040. The UI will display all current jobs. Since we've just created the instance, there should be no jobs currently running.

## Reading data with Pyspark

Similarlly to Pandas, Spark can read CSV files into dataframes, a tabular data structure. Unlike Pandas, Spark can handle much bigger datasets but it's unable to infer the datatypes of each column.

> Note: Spark dataframes use custom data types; we cannot use regular Python types.

```python
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Download data: High Volume For-Hire Vehicle (FHV) Trip Records for January 2021
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-01.parquet

# Read data
df = spark.read \
    .option("header", "True") \
    .parquet('fhvhv_tripdata_2021-01.parquet')

# Show data
df.head()
# Get schema. Note that the datatypes are not inferred
df.schema

# Infer datatypes with Pandas
# Limit rows in order to infer the datatypes
df_head = df.limit(1000)
df_pandas = df_head.toPandas()
df_pandas.dtypes

# Create a dataframe with Spark 
schema = types.StructType([
    types.StructField('hvfhs_license_num',types.StringType(),True),
    types.StructField('dispatching_base_num',types.StringType(),True),
    types.StructField('originating_base_num',types.StringType(),True),
    types.StructField('request_datetime',types.TimestampType(),True),
    types.StructField('on_scene_datetime',types.TimestampType(),True),
    types.StructField('pickup_datetime',types.TimestampType(),True),
    types.StructField('dropoff_datetime',types.TimestampType(),True),
    types.StructField('PULocationID',types.LongType(),True),
    types.StructField('DOLocationID',types.LongType(),True),
    types.StructField('trip_miles',types.DoubleType(),True),
    types.StructField('trip_time',types.LongType(),True),
    types.StructField('base_passenger_fare',types.DoubleType(),True),
    types.StructField('tolls',types.DoubleType(),True),
    types.StructField('bcf',types.DoubleType(),True),
    types.StructField('sales_tax',types.DoubleType(),True),
    types.StructField('congestion_surcharge',types.DoubleType(),True),
    types.StructField('airport_fee',types.DoubleType(),True),
    types.StructField('tips',types.DoubleType(),True),
    types.StructField('driver_pay',types.DoubleType(),True),
    types.StructField('shared_request_flag',types.StringType(),True),
    types.StructField('shared_match_flag',types.StringType(),True),
    types.StructField('access_a_ride_flag',types.StringType(),True),
    types.StructField('wav_request_flag',types.StringType(),True),
    types.StructField('wav_match_flag',types.StringType(),True)
])
# Now read data with the proper schema
df = spark.read \
    .option("header", "True") \
    .schema(schema) \
    .parquet('fhvhv_tripdata_2021-01.parquet')

# Check data
df.head(10)
```

> Useful: to kill a spark process use spark.stop() or find the process and kill it with taskkill /F /PID <PID> and find the PID with `netstat -ano | findstr :4040`

## Partitions
In PySpark, a partition is a logical division of a large distributed dataset into smaller, more manageable parts. A partition consists of a subset of the data, along with information about its location within the cluster.

Partitions are used to parallelize the processing of data across multiple nodes in a cluster. Each partition is processed by a separate task running on a different node in the cluster. By dividing the data into partitions, PySpark can distribute the processing workload across the nodes in the cluster, allowing the data to be processed faster and more efficiently.

PySpark allows you to specify the number of partitions when creating a DataFrame or RDD. By default, PySpark uses the value of `spark.default.parallelism` configuration parameter as the number of partitions. This value is typically set to the number of cores on the cluster.

You can also use the `repartition()` and `coalesce()` methods to control the number of partitions of a DataFrame or RDD after it has been created. The `repartition()` method shuffles the data and creates a new DataFrame or RDD with a specified number of partitions. The `coalesce()` method merges partitions without shuffling the data, creating a new DataFrame or RDD with fewer partitions.

In general, the number of partitions should be chosen to balance the workload across the cluster and minimize the amount of data that needs to be shuffled between nodes. If there are too few partitions, the processing workload may not be distributed evenly, leading to slow processing times. If there are too many partitions, there may be too much overhead associated with coordinating the processing of each partition. Therefore, it's important to experiment with different numbers of partitions to find the optimal balance for your specific use case.

We will now partition the dataframe and parquetize it. This will create multiple files in parquet format.

```python
df = df.repartition(24)
df.write.parquet('fhvhv/2021/01/')
```

The opposite of partitioning (joining multiple partitions into a single partition) is called coalescing.

## Spark dataframes
Unlike CSV files, parquet files contain the schema of the dataset, so there is no need to specify a schema like we previously did when reading the CSV file.
```python
# Read parquet file and print schema
df = spark.read.parquet('fhvhv/2021/01/')
df.printSchema()
```	
(One of the reasons why parquet files are smaller than CSV files is because they store the data according to the datatypes, so integer values will take less space than long or string values.)

There are many Pandas-like operations that we can do on Spark dataframes, such as:

- Column selection - returns a dataframe with only the specified columns.
```python
new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID')
```
- Filtering by value - returns a dataframe whose records match the condition stated in the filter.
```python
new_df = df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df.hvfhs_license_num == 'HV0003')
```
- And many more. The official Spark documentation website contains a [quick guide for dataframes](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html).

## Actions and transformations

Some Spark methods are "lazy", meaning that they are not executed right away. You can test this with the last instructions we run in the previous section: after running them, the Spark UI will not show any new jobs. However, running df.show() right after will execute right away and display the contents of the dataframe; the Spark UI will also show a new job.

These lazy commands are called *transformations* and the eager commands are called *actions*. Computations only happen when actions are triggered.

```python
df.select(...).filter(...).show()
```
Both `select()` and `filter()` are transformations, but `show()` is an action. The whole instruction gets evaluated only when the `show()` action is triggered.

List of transformations (lazy):
- Selecting columns
- Filtering
- Joins
- Group by
- Partitions

List of actions (eager):

- Show, take, head
- Write, read, etc.

```python
# Shows the result of the transformations
df.show()	
```

## Functions and User Defined Functions (UDFs)

Besides the SQL and Pandas-like commands we've seen so far, Spark provides additional built-in functions that allow for more complex data manipulation. By convention, these functions are imported as follows:

```python	
from pyspark.sql import functions as F
```

Here's an example of built-in function usage:

```python
df.withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

- withColumn() is a transformation that adds a new column to the dataframe.
- select() is another transformation that selects the stated columns.
- F.to_date() is a built-in Spark function that converts a timestamp to date format (year, month and day only, no hour and minute).

A list of built-in functions is available in the [official Spark documentation page](https://spark.apache.org/docs/latest/api/sql/index.html).

Besides these built-in functions, Spark allows us to create *User Defined Functions (UDFs)* with custom behavior for those instances where creating SQL queries for that behaviour becomes difficult both to manage and test.

UDFs are regular functions which are then passed as parameters to a special builder. Let's create one:

```python
# A crazy function that changes values when they're divisible by 7 or 3
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'

# Creating the actual UDF
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
```
- F.udf() takes a function (crazy_stuff() in this example) as parameter as well as a return type for the function (a string in our example).
- While crazy_stuff() is obviously non-sensical, UDFs are handy for things such as ML and other complex operations for which SQL isn't suitable or desirable. Python code is also easier to test than SQL.

We can then use our UDF in transformations just like built-in functions:

```python
df.withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```	

## Spark Internals

- Local setup: use a local cluster to run Spark code, it is defined when the SparkSession is created, using: `master('local[*]')`.
- Cluster setup: 
    - Create a driver:
        - Python/Scala/Java Spark script that will be executed by the Spark cluster.
        - Airflow DAG.
    - The spark job will be submitted to the Spark master (inside the cluster) through a given port (default is 7077, 4040 was for the local cluster) using the `spark-submit` command.
    - The executors inside the cluster will execute the code and return the results to the driver. The master is the coordinator of the cluster, it will assign tasks to the executors and reassign them if they fail.
- Overall, when a job is sent, the master assigns tasks to the executors and then each executor will fetch a dataframe partition stored in a Data Lake (usually S3, GCS or a similar cloud provider), do something with it and then store it somewhere, which could be the same Data Lake or somewhere else. If there are more partitions than executors, executors will keep fetching partitions until every single one has been processed.
- This is in contrast to Hadoop/HFDS, another data analytics engine, whose executors locally store the data they process. Partitions in Hadoop are duplicated across several executors for redundancy, in case an executor fails for whatever reason (Hadoop is meant for clusters made of commodity hardware computers). However, data locality has become less important as storage and data transfer costs have dramatically decreased and nowadays it's feasible to separate storage from computation, so Hadoop has fallen out of fashion.

## GroupBy in Spark

This query will output the total revenue and amount of trips per hour per zone. We need to group by hour and zones in order to do this.
```python
df_green_revenue = spark.sql("""
SELECT 
    date_trunc('hour', lpep_pickup_datetime) AS hour, 
    PULocationID AS zone,

    SUM(total_amount) AS amount,
    COUNT(1) AS number_records
FROM
    green
WHERE
    lpep_pickup_datetime >= '2020-01-01 00:00:00'
GROUP BY
    1, 2  
""")
```
Since the data is split along partitions, it's likely that we will need to group data which is in separate partitions, but executors only deal with individual partitions. Spark solves this issue by separating the grouping in 2 stages:

- First stage - partitioned processing: each executor groups the data in the partition they're working on and stores the results in a temporary partition that can be called intermediate results.
- Second stage - reshuffling: Spark will put all records with the same keys (in this case, the GROUP BY keys which are hour and zone) in the same partition. The algorithm to do this is called *external merge sort*. Once the shuffling has finished, we can once again apply the GROUP BY to these new partitions and reduce the records to the final output.
> Note that the shuffled partitions may contain more than one key, but all records belonging to a key should end up in the same partition.

- By default, Spark will repartition the dataframe to 200 partitions after shuffling data. For the kind of data we're dealing with in this example this could be counterproductive because of the small size of each partition/file, but for larger datasets this is fine.
- Shuffling is an expensive operation, so it's in our best interest to reduce the amount of data to shuffle when querying.
- Keep in mind that repartitioning also involves shuffling data.

## Joins in Spark

### Joining 2 large tables

Given all records for yellow taxis `Y1, Y2, ... , Yn` and for green taxis `G1, G2, ... , Gn` and knowing that the resulting composite key is key `K = (hour H, zone Z)`, we can express the resulting complex records as `(Kn, Yn)` for yellow records and `(Kn, Gn)` for green records. Spark will first shuffle the data like it did for grouping **(using the external merge sort algorithm)** and then it will reduce the records by joining yellow and green data for matching keys to show the final output.
- Initial partitioning:
    - Yellow taxis: `(K1, Y1); (K2, Y2); ... ; (Kn, Yn)`
    - Green taxis: `(K4, G3); (K2, G1); ... ; (Km, Gm)`
- Shuffling partitions - grouping partitions with the same key:
    - `(K1,Y1);(K4,G3)`
    - `(K2,Y2);(K2,G1)`
    - `(K3,Y3);(K3,G2)`
- Reduce partitions: join partitions with the same key:
    - `(K1, Y1, null);(K4, null, G3)`
    - `(K2, Y2, G1)`
    - `(K3, Y3, G2)`
- Because we're doing an outer join, keys which only have yellow taxi or green taxi records will be shown with empty fields for the missing data, whereas keys with both types of records will show both yellow and green taxi data.
> If we did an inner join instead, the records such as (K1, Y1, Ø) and (K4, Ø, G3) would be excluded from the final result.

### Joining a large and a small table
When a dataset is small enough to fit in memory, Spark will broadcast it to all of the executors. This is done to avoid shuffling the data and to avoid the overhead of the external merge sort algorithm. This is called a *broadcast join*. In other words: Spark sends a copy of the complete table to all of the executors and each executor then joins each partition of the big table in memory by performing a lookup on the local broadcasted table.

> Shuffling isn't needed because each executor already has all of the necessary info to perform the join on each partition, thus speeding up the join operation by orders of magnitude.

## Resilient Distributed Datasets - RDDs

Resilient Distributed Datasets (RDDs) are the main abstraction provided by Spark and consist of collection of elements partitioned accross the nodes of the cluster.

Dataframes are actually built on top of RDDs and contain a schema as well, which plain RDDs do not.

Spark dataframes contain a rdd field which contains the raw RDD of the dataframe. The RDD's objects used for the dataframe are called rows.

Manipulating RDDs to perform SQL-like queries is complex and time-consuming. Ever since Spark added support for dataframes and SQL, manipulating RDDs in this fashion has become obsolete, but since dataframes are built on top of RDDs, knowing how they work can help us understand how to make better use of Spark.

Most important methods of RDDs:
- 'map': apply a function to each element of the RDD.
- 'filter': filter out elements of the RDD based on a condition.
- 'reduce': reduce the RDD to a single element by applying a function to each element of the RDD.

### Spark RDD mapPartitions

The mapPartitions() function behaves similarly to map() in how it receives an RDD as input and transforms it into another RDD with a function that we define but it transforms partitions rather than elements. In other words: map() creates a new RDD by transforming every single element, whereas mapPartitions() transforms every partition to create a new RDD.

mapPartitions() is a convenient method for dealing with large datasets because it allows us to separate it into chunks that we can process more easily, which is handy for workflows such as Machine Learning.

mapPartitions() operates in the whole partition and not on individual elements, if we want to apply a function to each element of the partition we need to use map().

## Spark in the Cloud
### Local Cluster and Spark-Submit
Spark can be run locally on a single machine, but it's also possible to run it on a cluster of machines. This is useful for large datasets that can't fit in a single machine's memory.

This code will start a local cluster, but once the notebook kernel is shut down, the cluster will disappear.
```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```
We will see now how to run Spark on a [standalone cluster](https://spark.apache.org/docs/latest/spark-standalone.html) and how to submit jobs to it.

- Open a CMD terminal in administrator mode
- `cd %SPARK_HOME%`
- Start a master node: `bin\spark-class org.apache.spark.deploy.master.Master`
- Start a worker node: `bin\spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077`
    - spark://<master_ip>:<port>: copy the address from the previous command, in my case it was spark://localhost:7077
    - Use --host <IP_ADDR> if you want to run the worker on a different machine. For now leave it empty.
- Now you can access Spark UI through localhost:8080

### Setting up a Dataproc Cluster

#### Creating the cluster

**Submit a job using the Google UI**

- Enable Cloud Dataproc API
- Create cluster on Compute Engine:
    - Choose a name and select the same region as the GCS bucket
    - Cluster type: single node for testing purposes
    - Specify additional components: Jupyter Notebook and Docker
    - Use all other options as default
    - Create cluster
- Submit a job:
    - Job type: PySpark
    - Create a folder in GCS and save the python file: `gsutil cp test_spark_sql.py gs://dtc_data_lake_dtc-data-engineering-377412/code/`
    - Paste GCS Python file path: `gs://dtc_data_lake_dtc-data-engineering-377412/code/test_spark_sql.py`
    - Pass parameters with the GCS location:
        - `--input_green=gs://dtc_data_lake_dtc-data-engineering-377412/pq/green/2021/*/`
        - `--input_yellow=gs://dtc_data_lake_dtc-data-engineering-377412/pq/yellow/2021/*/`
        - `--output=gs://dtc_data_lake_dtc-data-engineering-377412/report-2021`

**Submit a job using a Google Cloud SDK**
- Set up a service account with credentials to submit Pyspark jobs. Add Dataproc admin role.
- Submit job using SDK:
```bash
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-dezoomcamp \
    --region=europe-southwest1 \
    gs://dtc_data_lake_dtc-data-engineering-377412/code/test_spark_sql.py \
        -- \
            --input_green=gs://dtc_data_lake_dtc-data-engineering-377412/pq/green/2020/*/ \
            --input_yellow=gs://dtc_data_lake_dtc-data-engineering-377412/pq/yellow/2020/*/ \
            --output=gs://dtc_data_lake_dtc-data-engineering-377412/report-2020
```

### Spark with BigQuery

> Check this [tutorial](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark).

- Copy BQ Python file to GCS: `gsutil cp test_spark_sql_bq.py gs://dtc_data_lake_dtc-data-engineering-377412/code/`


```bash
gcloud dataproc jobs submit pyspark \
    --cluster=cluster-dezoomcamp \
    --region=europe-southwest1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
    gs://dtc_data_lake_dtc-data-engineering-377412/code/test_spark_sql_bq.py \
        -- \
            --input_green=gs://dtc_data_lake_dtc-data-engineering-377412/pq/green/2020/*/ \
            --input_yellow=gs://dtc_data_lake_dtc-data-engineering-377412/pq/yellow/2020/*/ \
            --output=trips_data_all.reports-2020
```
