"""
DAG создает  таблицы в слоях CDM, DDS, STG
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = os.path.basename(__file__).split('.')[0]

args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    dag_id=DAG_ID,
    description='transfer data from mongo to pg',
    catchup=False,
    start_date=pendulum.datetime(2022, 8, 6, tz="UTC"),
)

begin = DummyOperator(task_id='begin', dag=dag)

create_cdm_tables = PostgresOperator(
        task_id="create_cdm_tables",
        postgres_conn_id= "pg_conn",
        sql='sql/p4_create_cdm_tables.sql',
        dag=dag)

create_dds_tables = PostgresOperator(
        task_id="create_dds_tables",
        postgres_conn_id= "pg_conn",
        sql='sql/p4_create_dds_tables.sql',
        dag=dag)

create_stg_tables = PostgresOperator(
        task_id="create_stg_tables",
        postgres_conn_id= "pg_conn",
        sql='sql/p4_create_stg_tables.sql',
        dag=dag)

end = DummyOperator(task_id='end', dag=dag)

begin >> create_cdm_tables >> create_dds_tables >> create_stg_tables >> end
