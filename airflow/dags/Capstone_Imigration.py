from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import loadQuery

default_args = {
    'owner': 'vitor',
    'start_date': datetime(2016, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retries_delay': timedelta(minutes=1),
    'email_on_retry': False,
    'catchup':False
}

dag = DAG('capstone_imigration',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 7 * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_task=PostgresOperator(
    task_id='create_tables',
    dag=dag,
    sql='create_capstone_tables.sql',
    postgres_conn_id='redshift'
)

stage_i94_toRedshift=StageToRedshiftOperator(
    task_id='Stage_i94_data',
    dag=dag,
    table='stage_i94',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='I94DataIm/',
    input_format='parquet'  
)

stage_land_temp= StageToRedshiftOperator(
    task_id='Stage_land_temp',
    dag=dag,
    table='stage_global_temp',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='GlobalLandTemperaturesByCity.csv/',
    input_format='CSV'
)

stage_airport = StageToRedshiftOperator(
    task_id='Stage_Airport',
    dag=dag,
    table='stage_airport_codes',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='airport-codes_csv.csv',
    input_format='CSV'
)

stage_us_city = StageToRedshiftOperator(
    task_id='Stage_US_city',
    dag=dag,
    table='stage_us_demo',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='us-cities-demographics.csv',
    input_format='CSV'
)

stage_country_label = StageToRedshiftOperator(
    task_id='Stage_Country_label',
    dag=dag,
    table='stage_country_label',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='country_label.csv',
    input_format='CSV'
)

stage_port_label = StageToRedshiftOperator(
    task_id='Stage_port_label',
    dag=dag,
    table='stage_port_label',
    redshift_conn='redshift',
    aws_credentials='aws_credentials',
    s3_bucket= 'capstone-data-udacity',
    s3_key='port_label.csv',
    input_format='CSV'
)

load_i94_table = LoadFactOperator(
    task_id='Load_i94_fact_table',
    dag=dag,
    table='imigration_table',
    redshift_conn='redshift',
    sql=loadQuery.load_imigration_table
)

load_global_temp = LoadDimensionOperator(
    task_id='Load_globalTemp_dim_table',
    dag=dag,
    overwrite=True,
    table='land_temp_table',
    redshift_conn='redshift',
    sql=loadQuery.load_temperature_table
)

load_airport_table = LoadDimensionOperator(
    task_id='Load_airport_dim_table',
    dag=dag,
    overwrite=True,
    table='airport_table',
    redshift_conn='redshift',
    sql=loadQuery.load_airport_table
)

load_us_demo = LoadDimensionOperator(
    task_id='Load_usDemo_dim_table',
    dag=dag,
    overwrite=True,
    table='us_city_demo',
    redshift_conn='redshift',
    sql=loadQuery.load_us_city_demo
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn='redshift',
    #tables=['users','time','songplays','songs','artists'],
    quality_check= [
            {'check_type': 'null_records', 
             'table':'imigration_table',
             'quality_check':'SELECT COUNT(*) FROM imigration_table WHERE admnum is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'land_temp_table',
             'quality_check':'SELECT COUNT(*) FROM land_temp_table WHERE avg_temp is NULL',
             'expected_value': 0,
             'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'airport_table',
             'quality_check':'SELECT COUNT(*) FROM airport_table WHERE ident is NULL',
             'expected_value': 0,
             'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'us_city_demo',
             'quality_check':'SELECT COUNT(*) FROM us_city_demo WHERE city_name is NULL',
             'expected_value': 0,
             'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'stage_country_label',
             'quality_check':'SELECT COUNT(*) FROM stage_country_label WHERE country_code is NULL',
             'expected_value': 0,
             'comparison':'!='},
            {'check_type': 'null_records', 
             'table':'stage_port_label',
             'quality_check':'SELECT COUNT(*) FROM stage_port_label WHERE code is NULL',
             'expected_value': 0,
              'comparison':'!='},
            {'check_type':'qnt_records',
             'table':'imigration_table',
             'quality_check':'SELECT COUNT(*) FROM imigration_table',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'stage_port_label',
             'quality_check':'SELECT COUNT(*) FROM stage_port_label',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'stage_country_label',
             'quality_check':'SELECT COUNT(*) FROM stage_country_label',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'us_city_demo',
             'quality_check':'SELECT COUNT(*) FROM us_city_demo',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'airport_table',
             'quality_check':'SELECT COUNT(*) FROM airport_table',
             'expected_value':1,
             'comparison': '<'},
            {'check_type':'qnt_records',
             'table':'land_temp_table',
             'quality_check':'SELECT COUNT(*) FROM land_temp_table',
             'expected_value':1,
             'comparison': '<'}
            ]
)
start_operator>> create_table_task
create_table_task>> [stage_i94_toRedshift, stage_land_temp, stage_airport, stage_us_city, stage_country_label, stage_port_label]
stage_i94_toRedshift>>load_i94_table
[stage_country_label, stage_land_temp]>>load_global_temp
[stage_i94_toRedshift, stage_airport, stage_country_label, stage_port_label]>>load_airport_table
stage_us_city>>load_us_demo
[load_i94_table, load_global_temp, load_airport_table, load_us_demo]>>run_quality_checks
