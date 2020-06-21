import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
                               LoadFactOperator, StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}


def getS3Bucket():
    return Variable.get("s3_bucket")


def getRegion():
    return 'us-west-2'


def getRedshiftConnId():
    return 'redshift'


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

dq_checks = [
    {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
     'expected_result': 0,
     'descr': "null values in users.userid column"},
    {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
     'expected_result': 0,
     'descr': "null values in songs.songid column"},
    {'check_sql': "SELECT COUNT(*) FROM(SELECT cnt, count(*) FROM (SELECT "
        "songid, count(*) cnt FROM songs GROUP BY songid) GROUP BY cnt)",
     'expected_result': 1,
     'descr': "duplicate song ids found"},
    {'dual_sql1': "SELECT COUNT(*) songs_cnt FROM songs",
     'dual_sql2': "SELECT COUNT(DISTINCT song_id) st_sng_song_cnt "
        "FROM staging_songs",
     'descr': "# records in songs table and # DISTINCT staging_songs records"}
]

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id=getRedshiftConnId(),
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='staging_events',
    s3_bucket=getS3Bucket(),
    s3_path='log_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region=getRegion(),
    copy_json_option='s3://' + getS3Bucket() + '/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='staging_songs',
    s3_bucket=getS3Bucket(),
    s3_path='song_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region=getRegion(),
    copy_json_option='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='songplays',
    insert_sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='users',
    insert_sql=SqlQueries.user_table_insert,
    truncate_table='Y'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='songs',
    insert_sql=SqlQueries.song_table_insert,
    truncate_table='Y'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='artists',
    insert_sql=SqlQueries.artist_table_insert,
    truncate_table='Y'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    table_name='time',
    insert_sql=SqlQueries.time_table_insert,
    truncate_table='Y'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id=getRedshiftConnId(),
    checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> \
    create_tables_task >> [stage_events_to_redshift,
                           stage_songs_to_redshift] >> \
    load_songplays_table >> [load_song_dimension_table,
                             load_user_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table] >> \
    run_quality_checks >> \
    end_operator
