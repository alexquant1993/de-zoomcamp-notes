## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442
- 51648442
- 61648442 ANSWER
- 71648442

Solution:
There is an issue in using tripid as the key column. This column is created by combining the vendorid and the pickup datetime. This means that there can be multiple records with the same tripid. I.e., two different trips that had the same vendorid and pickup datetime. This causes that the fact_trips table changes the number of records in each execution.
```sql
-- Question 1
SELECT count(1) FROM `dtc-data-engineering-377412.production.fact_trips` WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-12-31'; 

SELECT count(1) FROM `dtc-data-engineering-377412.dbt_development.fact_trips` WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-12-31';
```

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- 89.9/10.1 ANSWER
- 94/6
- 76.3/23.7
- 99.1/0.9

Solution:
Check the dashboard created in Google Data Studio.

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696 ANSWER
- 53244696
- 63244696

Solution:
- Create the stg_fhv_tripdata model
```sql
{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as tlc_license,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_flag as integer) as shared_flag
from tripdata
-- To avoid repeated trips
-- where rn = 1
where date(pickup_datetime) between '2019-01-01' and '2019-12-31' 

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
- Run the model: `dbt build --select stg_fhv_tripdata --var 'is_test_run: false'`
### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722 ANSWER
- 32998722
- 42998722

Solution
- Create the fact_fhv_trips model
```sql
{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_data.tripid, 
    fhv_data.tlc_license, 
    fhv_data.service_type,
    fhv_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime, 
    fhv_data.shared_flag
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
```
- Run the model: `dbt build --select fact_fhv_trips --var 'is_test_run: false'`
- In BigQuery, count the number of records in the table: `SELECT count(1) FROM `dtc-data-engineering-377412.dbt_development.fact_fhv_trips` WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31'`


### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January ANSWER
- December

SOLUTION
It can be done in Google Data Studio with a bar chart or with sql in Google BigQuery obtaining the same result.
```sql
SELECT EXTRACT(YEAR FROM pickup_datetime) AS year, EXTRACT(MONTH FROM pickup_datetime) AS month, COUNT(*) AS num_records
FROM `dtc-data-engineering-377412.dbt_development.fact_fhv_trips`
GROUP BY year, month
ORDER BY year, month;	
```

## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here