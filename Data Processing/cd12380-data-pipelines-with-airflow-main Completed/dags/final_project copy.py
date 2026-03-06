from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage events and songs
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='my-dend-project-bucket',
        s3_key='log_data',
        json_path='s3://my-dend-project-bucket/log_json_path.json',
        region='us-east-1'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='my-dend-project-bucket',
        s3_key='song_data',
        json_path='auto',
        region='us-east-1'
    )

    # Load fact table
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    # Load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dimension_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dimension_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dimension_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert
    )

    # Run quality checks
    run_quality_checks = DataQualityOperator(
        task_id='run_quality_checks',
        redshift_conn_id='redshift',
        test_cases=[
            {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id IS NULL;", 'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL;", 'expected_result': 0},
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')

    # Set task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
