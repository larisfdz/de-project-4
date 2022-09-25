"""
DAG обновляет данные в слое STG.

Обновление данных происходит следующим образом:
- выгрузка по курьерам загружается каждый раз полностью, при дублировании уникальных значений _id в stg.del_couriers строки заменяются;
- выгрузка по ресторанам загружается каждый раз полностью, при дублировании уникальных значений _id в stg.del_restaurants строки заменяются;
- выгрузка доставок осуществляется от последней записанной в stg.del_deliveries временной метки доставки. В связи с урезанием метки до секунд, а также в связи с тем, что метод api, видимо, берет время, включая указанное в "from", здесь также осуществляется проверка на уникальные значения delivery_id, и в случае нахождения дубликатов строки заменяются.

Запросы для создания таблиц хранятся в файле sql/p4_update_tables.sql
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils.p4_func import get_val, load_file_to_pg

DAG_ID = os.path.basename(__file__).split('.')[0]

args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

r_params = {'table_name':'restaurants', 'sort_field':'id', 'sort_direction':'asc', 'limit':50, 'offset':0}
c_params = {'table_name':'couriers', 'sort_field':'_id', 'sort_direction':'asc', 'limit':50, 'offset':0}
d_params = {'table_name':'deliveries', 'sort_field':'_id', 'sort_direction':'asc', 'limit':50, 'offset':0, 'from_d':'2022-09-25 00:01:01'}

dag = DAG(
    dag_id=DAG_ID,
    description='transfer data from mongo to pg',
    catchup=False,
    start_date=pendulum.datetime(2022, 8, 6, tz="UTC"),
)

begin = DummyOperator(task_id='begin', dag=dag)

get_delivery_ts = PythonOperator(
        task_id='get_delivery_ts',
        python_callable=get_val,
        op_kwargs={'exp': 'SELECT MAX(delivery_ts) FROM stg.del_deliveries;'},
        dag = dag)

get_restaurants_task = PythonOperator(
        task_id = 'get_restaurants_task',
        python_callable = load_file_to_pg,
        op_kwargs = {'pg_table': 'stg.del_restaurants',
                    'params': r_params,
                    'u_col': '_id',
                    'set_stmt': '''SET _id = EXCLUDED._id,
                                    name = EXCLUDED.name,
                                    update_ts = EXCLUDED.update_ts;
                                '''},
        dag = dag)

get_couriers_task = PythonOperator(
        task_id = 'get_couriers_task',
        python_callable = load_file_to_pg,
        op_kwargs = {'pg_table': 'stg.del_couriers',
                    'params': c_params, 
                    'u_col': '_id',
                    'set_stmt': '''SET _id = EXCLUDED._id,
                                    name = EXCLUDED.name,
                                    update_ts = EXCLUDED.update_ts;
                                '''
                                },
        dag = dag)

get_deliveries_task = PythonOperator(
        task_id = 'get_deliveries_task',
        python_callable = load_file_to_pg,
        op_kwargs = {'pg_table': 'stg.del_deliveries',
                    'params': d_params,
                    'u_col': 'delivery_id',
                    'set_stmt': '''SET order_id = EXCLUDED.order_id,
                                    order_ts = EXCLUDED.order_ts,
                                    delivery_id = EXCLUDED.delivery_id,
                                    rate = EXCLUDED.rate,
                                    sum = EXCLUDED.sum,
                                    tip_sum = EXCLUDED.tip_sum;
                                '''},
        dag = dag)

end = DummyOperator(task_id='end', dag=dag)

begin >> get_delivery_ts >> get_restaurants_task >> get_couriers_task >> get_deliveries_task >> end
