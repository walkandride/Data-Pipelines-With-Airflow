## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce 
more automation and monitoring to their data warehouse ETL pipelines and come 
to the conclusion that the best tool to achieve this is Apache Air􀃓ow.

They have decided to bring you into the project and expect you to create high 
grade data pipelines that are dynamic and built from reusable tasks, can be 
monitored, and allow easy backfills. They have also noted that the data quality 
plays a big part when analyses are executed on top the data warehouse and want 
to run tests against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data 
warehouse in Amazon Redshift. The source datasets consist of JSON logs that 
tell about user activity in the application and JSON metadata about the songs 
the users listen to.

## Overview
Build an automated Apache Airflow process that runs hourly.  Utilize custom 
operators to load the staging tables, populate the fact table, populate the 
dimension tables, and perform data quality checks.

Configure the task dependencies so that after the dependencies are set, the
graph view displays ![Workflow DAG](./example-dag.png)

### Files:
`README.md`
:  this file

`example-dag.png`
:  image of DAG workflow

`iac_redshift.py`
:  Script to build Redshift cluster

`dags/udac_example_dag.py`
:  Main DAG

`dags/create_tables.sql`
:  SQL script to build Redshift tables

`plugins/helpers/sql_queries.py`
:  *Select* statements used to populate tables

`plugins/operators/data_quality.py`
:  DAG operator used for data quality checks

`plugins/operators/load_dimensions.py`
:  DAG operator used to populate dimension tables in a STAR schema

`plugins/operators/load_fact.py`
:  DAG operator used to populate fact tables

`plugins/operators/stage_redshift.py`
:  DAG operator to populate staging tables from source files

### Prerequisites
1.  Start Redshift cluster
2.  Start Airflow, e.g. /opt/airflow/start.sh
3.  Open Airflow web browser and *create* the following:

        Airflow AWS Connection
                    Conn id:  aws_credentials
                    Login:  [REDACTED]
                    Password: [REDACTED]

        Airflow Redshift Connection
                    Conn id:  redshift
                    Conn Type:  Postgres
                    Host:   [REDACTED]
                    Schema: dev
                    Login:  awsuser
                    Password: Passw0rd
                    Port: 5439

        Airflow Variable
                    Key:  s3_bucket
                    Value:  udacity-dend
4.  From the DAGs page, click the *Trigger Dag* link for `udac_example`.

Notes:
- Create a `dwh.cfg` file and run `iac_redshift.py` to create (and teardown) a Redshift cluster and identify Airflow connections and variables.

        # ---------------------------------
        # Example dwh.cfg file
        # ---------------------------------
        [AWS]
        # Define IAM credentials
        KEY=[REDACTED]
        SECRET=[REDACTED]

        [DWH] 
        # define Redshift cluster attributes
        DWH_CLUSTER_TYPE=multi-node
        DWH_NUM_NODES=4
        DWH_NODE_TYPE=dc2.large

        DWH_IAM_ROLE_NAME=dwhRole
        DWH_CLUSTER_IDENTIFIER=dwhCluster
        DWH_DB=dev
        DWH_DB_USER=awsuser
        DWH_DB_PASSWORD=Passw0rd

        REGION=us-west-2


        (base) E:\project_5>python iac_redshift.py
        [C]reate or [D]elete Redshift cluster? Enter C for create and D for delete: c
                            Param       Value
        0        DWH_CLUSTER_TYPE  multi-node
        1           DWH_NUM_NODES           4
        2           DWH_NODE_TYPE   dc2.large
        3  DWH_CLUSTER_IDENTIFIER  dwhCluster
        4                  DWH_DB         dev
        5             DWH_DB_USER     awsuser
        6         DWH_DB_PASSWORD    Passw0rd
        7       DWH_IAM_ROLE_NAME     dwhRole
        1.1 Creating a new IAM Role
        An error occurred (EntityAlreadyExists) when calling the CreateRole operation: Role with name dwhRole already exists.
        1.2 Attaching Policy
        ARN: [REDACTED]:role/dwhRole
        row = ('PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc (GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), Redshift 1.0.16505',)
        0
        connection to cluster is successful
        ===== Airflow Initialization =====
        Airflow AWS Connection
                    Conn id:  aws_credentials
                    Login:  [AWS Key]
                    Password: [AWS Secret]

        Airflow Redshift Connection
                    Conn id:  redshift
                    Conn Type:  Postgres
                    Host:   [REDACTED].us-west-2.redshift.amazonaws.com
                    Schema: dev
                    Login:  awsuser
                    Password: [Database password]
                    Port: 5439

        Airflow Variable
                    Key:  s3_bucket
                    Value:  udacity-dend

        (base) E:\project_5>python iac_redshift.py
        [C]reate or [D]elete Redshift cluster? Enter C for create and D for delete: d
                            Param       Value
        0        DWH_CLUSTER_TYPE  multi-node
        1           DWH_NUM_NODES           4
        2           DWH_NODE_TYPE   dc2.large
        3  DWH_CLUSTER_IDENTIFIER  dwhCluster
        4                  DWH_DB         dev
        5             DWH_DB_USER     awsuser
        6         DWH_DB_PASSWORD    Passw0rd
        7       DWH_IAM_ROLE_NAME     dwhRole
        8                  REGION   us-west-2
        problem with deleting cluster
        cluster is deleted

### Notes / Lessons Learned
1.  Add LOG_JSONPATH to COPY command to prevent empty fields in staging_events 
table.
2.  Copied original `create_tables.sql` to dags directory.  Updated `CREATE TABLE` 
statements to `CREATE TABLE IF NOT EXISTS`.  Incorporated this file into a task 
and added to workflow.
3.  Data quality checks allow two types of checks.  `check_sql` runs a SQL 
statement and compares the result against an expected result.  The `dual_sql1` 
and `dual_sql2` tags executes both statements and compares the results against 
each other.

        check_sql
                The results of the `check_sql` statement are compared with the
                expected_result value.

                format:
                {'check_sql': [sql statement to verify]",
                'expected_result': [expected result],
                'descr': [description of check]}

        dual_sql1 and dual_sql2
                The results of `dual_sql1` is compared with the results of
                `dual_sql2`.

                format:
                {'dual_sql1': [first sql statement],
                'dual_sql2': [sql statement to compare first sql statement with],
                'descr': [description of check]}


***
###### Udacity's Data Engineering Project:  Project: Data Pipelines with Airflow