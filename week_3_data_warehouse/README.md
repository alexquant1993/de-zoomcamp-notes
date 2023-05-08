## OLAP vs OLTP
- OLTP: Online Transaction Processing. Used for day-to-day business operations. Normalized databases. Short and fast updates initiated by users. Generally small if historical data is archived. User examples: customer-facing personnel, sales, online shoppers, etc.
- OLAP: Online Analytical Processing. Used for data analysis. Denormalized databases. Long and slow updates initiated by data analysts. Generally large. User examples: data analysts, data scientists, etc.

## Data Warehouse
- OLAP solution meant for reporting and data analysis. They follow the ETL process: Extract, Transform, Load. They are usually built on top of a relational database management system (RDBMS) such as MySQL, PostgreSQL, Oracle, etc.
- DW receive data from different sources such as operational systems and flat lines. Which is then processed in a staging area and then loaded into the DW.
- DW then can feed data to separate Data Marts, which are smaller database systems which end users may use for different purposes: analysis, reporting, mining, etc.

## BigQuery
- BigQuery is a serverless, highly scalable, and cost-effective data warehouse. It is a fully managed service, which means that Google takes care of the infrastructure and maintenance.
- Easy scale up from GB to PB of data.
- Some building features that really outshine: machine learning, geospatial analysis, time series analysis, business intelligence, etc.
- BigQuery maximizes flexibility by separating the compute engine that analyzes data from the storage engine that stores it. This allows you to scale each independently, so you can choose the right mix of speed and cost for your workloads.
- Some interesting features:
    - Cache results by default.
    - bigquery-public-data: access to public datasets such as citibike stations, etc.
    - Quick preview of data, perform SQL queries, store the results and explore them in data studio.
- Costs:
    - On demand pricing model: 1 TB of data processed is 5$.
    - Flat rate pricing model: based on numer of pre requested slots. 100 slots per 2000$ per month = 400 TB of data processed on demand pricing. It makes sense until unless you are scanning above 200 TB. You get a fixed number of slots and if you have to compute more than that, your query will be queued until a slot is available.
- Partition in BQ: split table into multiple smaller tables. This is useful when you have a large table and you want to query a small subset of it. You can partition the table by date, for example, and then query only the data you need. This will make your query faster and cheaper.
- Clustering in BQ: similar to partitioning, but it is used to improve the performance of queries that filter by a column. It is useful when you have a large table and you want to query a small subset of it. You can cluster the table by a given tag, for example, and then query only the data you need. This will make your query faster and cheaper. Partitioning come first and then clustering.

```sql	
-- Playing with BigQuery
-- Query public available table
-- This table is only available in the US or query this with automatic location selection
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-data-engineering-377412.dezoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_dtc-data-engineering-377412/yellow/yellow_tripdata_2019-*.parquet', 'gs://dtc_data_lake_dtc-data-engineering-377412/yellow/yellow_tripdata_2020-*.parquet']
);

-- Check yellow trip data
SELECT * FROM `dtc-data-engineering-377412.dezoomcamp.external_yellow_tripdata`;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_non_partitioned` AS
SELECT * FROM `dtc-data-engineering-377412.dezoomcamp.external_yellow_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `dtc-data-engineering-377412.dezoomcamp.external_yellow_tripdata`;


-- Impact of partition
-- Scanning 1.62GB of data
SELECT DISTINCT(VendorID)
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning 105MB of data
SELECT DISTINCT(VendorID)
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `dezoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
-- How I am querying the data? Given that filtering by pickup time and vendor id is a common query, then I should use those columns as partitions and clusters
CREATE OR REPLACE TABLE `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `dtc-data-engineering-377412.dezoomcamp.external_yellow_tripdata`;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 847 MB. It will improve better as we deal with more data.
SELECT count(*) as trips
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
```

### More on BigQuery Partitioning
- You can choose between time-unit column, ingestion time and integer range partioning.
- When using time unit or ingestion time, you can choose between daily (default), hourly (might need to consider the number of partitions to be created), monthly or yearly.
- Number of partitions is limited to 4000. If hourly is chosen, then an expiration strategy might need to be considered.
- Cost benefit is known upfront.
- You need partition-level management: creating, deleting and moving partitions between storages.
- Filter or aggregate on a sigle column.

### More on BigQuery Clustering
- Columns specified are used to colocate related data.
- The order of the column is important. It specifies the sort order of the data.
- Clustring improves performance of: filter and aggregate queries.
- If table < 1 GB, there's no significant performance improvement with partitioning and clustering. It is not recommended to use it for small datasets because it increases metadata reads and mantainance.
- You can specify up to 4 clustering columns.
- Clustering columns must be top-level, non-repeated columns: date, bool, geography, int64, numeric, bignumeric, string, timestamp and datetime.
- Cost benefit is unknown.
- You need more granularity than partitioning alone provides.
- Your queries commonly use filters or aggregation against multiple particular columns.
- The cardinality of the number of values in a column or group of columns is large. This becomes a hindrance in partitioning due to the 4000 partition limit.

### When do you choose clustering over partition?

- Partitioning results in a small amount of data per partition: approx less than 1 GB.
- Partitioning results in a large number of partitions beyond the limits on partitioned tables.
- Partitioning results in your mutation operations modifying the majority of partitions in the table frequently (for example, every few minutes). For instance: querying hourly that modifies all your partitions.

## Automatic reclustering
As data is added to a clustered table:
- The newly inserted data can be written to blocks that contain key ranges that overlap with the key ranges in previously written blocks.
- These overlapping keys weaken the sort property of the table.

To mantain the performance characteristics of a clustered table:
- BigQuery performs automatic re-clustering in the background to restore the sort property of the table.
- For partitioned tables, clusterins is mainted for data within the scope of each partition.
- It's free.

## BigQuery Best Practices
- Cost reduction:
    - Avoid SELECT *
    - Price your queries before running them
    - Use clustered or partitioned tables
    - Use streaming inserts with caution
    - Materialize query results in stages. Try to place query results into cache.

- Query performance:
    - Filter on partitioned or cluster columns
    - Denormalize data by having more nested or repeated columns in order to reduce the amount of joins and therefore having simpler queries.
    - Use nested or repeated columns in clase of complex structures.
    - Use external data sources appropriately:  don't do it too much, it might be more expensive and less performant.
    - Reduce data before using a JOIN.
    - Do not treat WITH claused as prepared statements.
    - Avoid oversharding tables: avoid having too many partitions.
    - Avoid JavaScript user-defined functions
    - Use approximate aggregation functions (HyperLogLog ++)
    - Order Last, for query operations to maximize performance.
    - Optimize your join patterns.
    - As a best practice, place the table with the largest number of rows first, followed by the table with the fewest rows, and then place the remaining tables by decreasing size.

## BigQuery internals
- BigQuery stores our data into a separate storage called colossus.
- Colossus is generally a cheap storage, stores data in a columnar format. Because BigQuery has separated storage from compute, it is significantly cheaper. If your data size increases tomorrow, you just have to pay for the increase in storage, which is generally very cheap. Most of the costs are allocated reading data and processing data (running queries): compute.
- Given that storage and compute are located in different hardware, how do they communicate? They communicate through a network. If the network is slow, then the performance of the query will be slow. This is where Jupiter Network comes into play. Jupiter (inside BQ datacenters) is a high-speed network that connects colossus and the compute engine. It provides approximately 1TBs of bandwidth.
- Dremel is the query execution engine. It generally divides your query into a tree structure. It separates the query in such a way that each node can execute an individual subset of the query and then add the results together.
- Different structures:
    - Record-oriented: each row is stored as a single record. It is the most common structure. It is the one used by most databases.
    - Column-oriented: each column is stored as a single record. It is the structure used by BigQuery. It is more efficient for analytical queries. It provides better aggregations on columns. Main uses in OLAP systems: filter and aggregate on certain columns and query few columns.

## ML in BigQuery

- Target audience: data analysts and managers
- No need for Python or Java knowledge
- No need to export data into a different system
- Free:
    - 10GB/month of data storage
    - 1TB/month of QUERIES processed
    - ML create model step: first 10GB/month is free
- After the free tier, you pay the following depending on the algorithm used for the US:
    - 250$/TB: logistic regression, linear regression, k-means and time series model
    - 5$/TB + Vertex AI training cost: AutoML Tables, DNN model, Boosted tree model
- Setup example:
    - Install requirements: `pip install pandas pyarrow google-cloud-storage`
    - Create a service account and download the JSON key file.
    - Set the environment variable: `set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\aarro\OneDrive\Escritorio\Elearning\14. Data Engineering Bootcamp\data-engineering-zoomcamp\week_3_data_warehouse\.gc\ny-rides.json`
- Set GCP_GCS_BUCKET with your created bucket name.
- Run the web_to_gcs.py script to upload the required data to GCS.
- Create a linear model in BigQuery
```sql
-- SELECT THE COLUMNS INTERESTED FOR YOU
SELECT passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount, tolls_amount, tip_amount
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0;

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `dtc-data-engineering-377412.dezoomcamp.tip_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
*
FROM
`dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `dtc-data-engineering-377412.dezoomcamp.tip_model`);

-- EVALUATE THE MODEL
SELECT
*
FROM
ML.EVALUATE(MODEL `dtc-data-engineering-377412.dezoomcamp.tip_model`,
(
SELECT
*
FROM
`dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
));

-- PREDICT THE MODEL
SELECT
*
FROM
ML.PREDICT(MODEL `dtc-data-engineering-377412.dezoomcamp.tip_model`,
(
SELECT
*
FROM
`dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
));

-- PREDICT AND EXPLAIN
SELECT
*
FROM
ML.EXPLAIN_PREDICT(MODEL `dtc-data-engineering-377412.dezoomcamp.tip_model`,
(
SELECT
*
FROM
`dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `dtc-data-engineering-377412.dezoomcamp.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT
*
FROM
`dtc-data-engineering-377412.dezoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;
```
- Extract the model and use it to predict with TensorFlow
```cmd
gcloud auth login
# Extract ML model from BigQuery and store in GCS
bq --project_id dtc-data-engineering-377412 extract -m dezoomcamp.tip_model gs://dtc_data_lake_dtc-data-engineering-377412/ml_model
# Create temporary folder
mkdir %TEMP%\model
# Copy model from GCS to local folder
gsutil cp -r gs://dtc_data_lake_dtc-data-engineering-377412/ml_model %TEMP%\model
# Create serving folder
mkdir serving_dir\tip_model\1
# Copy model to serving folder
xcopy %TEMP%\model\ml_model\* serving_dir\tip_model\1 /E
```
- Run in GitBash:
```bash
# Pull Tensorflow serving docker image
docker pull tensorflow/serving
# Run docker container with TensorFlow serving
docker run -p 8501:8501 --mount type=bind,source="`pwd`/serving_dir/tip_model",target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving
curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict
```

- Check prediction results: http://localhost:8501/v1/models/tip_model
- By default, Tensorflow Serving doesn't provide a GUI, it only provides a RESTful API endpoint that you can use to send requests and receive predictions.