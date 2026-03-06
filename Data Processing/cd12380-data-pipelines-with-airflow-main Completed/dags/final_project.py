
from datetime import datetime, timedelta
import pendulum
import os
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

    # Stage 1
    start_operator = DummyOperator(task_id='Begin_execution')

    # Stage 2
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events_to_redshift',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs_to_redshift',
    )

    # Stage 3
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_table',
    )

    # Stage 4
    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dimension_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dimension_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dimension_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
    )

    # Stage 5
    run_quality_checks = DataQualityOperator(
        task_id='run_quality_checks',
    )

    # Stage 6
    end_operator = DummyOperator(task_id='End_execution')

    # Task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] 

    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()





