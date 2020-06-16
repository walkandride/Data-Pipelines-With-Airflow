from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers import SqlQueries
from airflow.models import Variable
from airflow.contrib.hooks.aws_hook import AwsHook

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

def getS3Bucket():
    return Variable.get('s3_bucket')

def getAWSKey():
    return AwsHook("aws_credentials").get_credentials().access_key

def getAWSSecret():
    return AwsHook("aws_credentials").get_credentials().secret_key

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='staging_events',
    s3_bucket=getS3Bucket(),
    s3_path='log_data/2018/11/2018-11-01-events.json', # "log_data/{{ execution_date.strftime('%Y') }}/{{ execution_date.strftime('%m') }}/{{ ds }}-events.json" or "log_data/{{ ds }}-events.csv"
    aws_key=getAWSKey(),
    aws_secret=getAWSSecret(),
    region = 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table_name='staging_songs',
    s3_bucket=getS3Bucket(),
    s3_path='song_data/A/A/*/*.json',
    aws_key=getAWSKey(),
    aws_secret=getAWSSecret(),
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

##start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
