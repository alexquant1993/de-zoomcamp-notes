# Analytics Engineering

## What is Analytics Engineering?
Latest developments in the data domain:
1. Massively parallel processing (MPP) databases: Vertica, Greenplum, Netezza, Teradata, Exadata (Oracle).
2. Cloud data warehouses: BigQuery, Redshift, Snowflake. They lowered the cost of storage and computation.
3. Data-pipelines-as-a-service: Stitch, Fivetran, Segment. Simplified the ETL process.
4. SQL-first approach: data model and database schema are designed and implemented before the application's code is written. Looker, Mode Analytics, Periscope, Chartio, Redash, etc.
5. Introduction of version control system to the data workflow.
6. Self-service analytics: BI tools.
7. Data governance: the work approach of data teams and the way stakeholders were consuming data changed.

## Roles in a data team
1. Data Engineer: responsible for the data pipeline, data warehouse, and data infrastructure. They are trained software engineers but do not write application code. 
2. Data analyst: responsible for data analysis and data visualization. Use data to anser business questions and solve problems.
3. Analytics engineer: software engineering best practices to the efforts of data analysts and data scientists.

## Tooling for the analytics engineer
- Data loading: Fivetran, Stitch, Segment, etc.
- Data storing: Redshift, BigQuery, Snowflake, etc.
- Data modeling: dbt, Looker, Mode Analytics, Periscope, Chartio, Redash, etc.
- Data presentation: Google Data Studio, Tableau, Looker or Mode Analytics, etc.

## Data modeling concepts

- ETL: extract, transform, load. The data is extracted from the source, transformed, and loaded into the data warehouse. Slightly more stable and compliant data analysis. Higher storage and compute costs.
- ELT: extract, load, transform. The data is extracted from the source, loaded into the data warehouse, and transformed. More flexible and agile data analysis. Lower storage and compute costs and lower maintenance.

## Kimball's Dimensional Modeling

- Objective: deliver data understanble to business users and deliver fast query performance.
- Approach: prioritise user understandability and query performance over non redundant data (3NF). We don't mind to have some redundant data, as long as the data is understandable to business users and the query performance is fast.
- Other approaches: Bill Inmon and Data Vault.

### Elements of dimensional modeling

- Also known as star schema.
- Fact table: measurment, metrics or facts. Business process. "Verbs": sales, orders, etc.
- Dimension table: corresponds to business identity. They provide context to the fact table. "Nouns": products, customers, etc.

### Arquitecture of Dimensional Modeling

- Restaurant analogy: the datawarehouse is the kitchen.
- Stage area: the raw data is stored here. Not meant to be exposed to everyone. In the case of the restaurant it would be the food.
- Processing area: this would be the kitchen in the restaurant. We take the raw data and make data models. Limited again only to the cooks: data engineers. Focuses in efficiency and ensuring standards.
- Presentation area: dining hall. Final presentation of the data. Exposure to the business stakeholders.

## Data modeling with dbt

- dbt stands for data build tool. It helps us to transform our raw data to later expose it to our business stakeholders and be able to perform the analysis.
- It is a transformation tool that allows anyone that knows SQL to deploy analytics code following software engineering best practices like modularity, portability, CI/CD, and documentation.
    - Development workflow
    - Test and document
    - Deployment: version control and CI/CD

### How dbt works?

- A model layer is added where the data is transformed. This model with the transformed data is persisted back to the data warehouse.
- Each model is a *.sql file, with no DDL or DML. It is a file that dbt will complile by creating the DDL or DML file and it's going to push that compute to our datawarehose. At the end, we will be able to see the table results of the transformation in our data warehouse.

### How to use dbt?

1. dbt Core:
    - Open-source project that allows the data transformation.
    - Builds and runs a dbt project (.sql and .yml files).
    - Includes SQL compilation logic, macros and database adapters (change the project to work with different databases).
    - Includes a CLI interface to run dbt commands.
2. dbt Cloud:
    - SaaS application to develop and manage dbt projects.
    - Web-based IDE to develop, run and test a dbt project.
    - Jobs orchestration.
    - Logging and alerting.
    - Integrated documentation.
    - Free for individuals.

## How to setup the deployment of dbt?
### Production environment

- Environment: a set of configurations that define how dbt should interact with your data warehouse.
- Choose deployment in the UI and then environment.
- Create a new environment and select the production dataset.

### Create jobs

- Jobs: a set of commands that dbt will run in a specific order.
- Create a new job and select the production environment.
    - Threads: number of threads to use when running the job. 4 is default.
    - Target name: default = it will be use the schema I define.
- Execution settings:
    - Generate docs on run
    - List commands
    - Run timeout
    - Run source freshness
- Triggers:
    - Schedule: run the job at a specific time.
    - CI: run on pull request.
- Save and run the job. You will see the logs and the created docs.
- Add the documentation into the project given a job. Edit project and select job. Then you will be able to see a documentation tab.

