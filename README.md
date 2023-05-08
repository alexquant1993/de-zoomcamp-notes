# ZoomCamp: Data Engineering Course

This repository contains the materials and projects from the ZoomCamp Data Engineering Course, where I learned essential data engineering skills and mastered various tools to manage data pipelines.

## Course Overview
The course was divided into 9 weeks, covering a wide range of topics and tools related to data engineering:

1. Introduction & Prerequisites: Setup of GCP, Docker, and Terraform
2. Workflow Orchestration: Data Lake, Prefect, and ETL with GCP
3. Data Warehouse: BigQuery, Partitioning, Clustering, and BigQuery ML
4. Analytics Engineering: dbt, BigQuery, Postgres, Data Studio, and Metabase
5. Batch Processing: Apache Spark, DataFrames, and Spark SQL
6. Streaming: Apache Kafka, Avro, Kafka Streams, Kafka Connect, and KSQL
7. Project: Apply the learned concepts and tools to a real-world project.

> I have developed a real estate data pipeline that sources data from a leading Spanish real estate portal, Idealista. The pipeline is designed to efficiently extract data from the portal, transform it, and load it into Google Cloud Storage (GCS) before ultimately storing it in BigQuery. This entire process is executed daily through an automated workflow orchestrated using Prefect. To further refine and analyze the data, I employed dbt for analytics engineering tasks. The transformed data is then visualized using Looker, enabling users to gain valuable insights into the real estate market trends and dynamics. Check my repo [here](https://github.com/alexquant1993/real_estate_spain/tree/do_droplets).

## Technologies and Tools

Throughout the course, I gained hands-on experience with the following technologies and tools:

- Google Cloud Platform (GCP)
- Google Cloud Storage (GCS)
- BigQuery
- Terraform
- Docker
- SQL
- Prefect
- dbt
- Apache Spark
- Apache Kafka