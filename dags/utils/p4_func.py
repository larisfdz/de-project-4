import logging
from datetime import datetime


import pandas as pd
import psycopg2
import requests
from airflow.hooks.postgres_hook import PostgresHook

headers={'X-Nickname': 'abaelardus',
         'X-Cohort': '1',
         'X-Project': 'True',
         'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
        }
url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

    
def get_tables(params):
    '''
    функция возвращает датафрейм с выборкой элементов, указанной в table_name
    '''
    df = pd.DataFrame()
    table_name = params['table_name']
    sort_field = params['sort_field']
    sort_direction = params['sort_direction']
    limit = params['limit']
    offset = params['offset']
    len_report = params['limit']
    while len_report == limit:
        if 'from_d' in params:
            from_d = params['from_d']
            url_method =f'''/{table_name}?from={from_d}&sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'''
            print(url_method)
        else:
            url_method = f'/{table_name}?sort_field={sort_field}&sort_direction={sort_direction}&limit={limit}&offset={offset}'
        report = requests.get(url+url_method, headers=headers).json()
        df = pd.concat([df, pd.DataFrame(report)])
        len_report = len(report)
        offset += limit
    logging.info(f'df_columns: {df.columns}')
    logging.info(f'df_length: {len(df.index)}')
    return df


#подключение к БД
def pg_execute_query(query):
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def load_file_to_pg(ti, pg_table, params, u_col, set_stmt):
    '''
    загрузка отчета в БД
    pg_table - таблица, куда загружаются данные
    params - параметры api-запроса
    u_col - поле с уникальными значениями
    set_stmt - как обновлять поля при дублировании строк с уникальным значением
    '''
    if 'from_d' in params:
        params = update_val(ti, params, 'from_d')
    logging.info(f'params: {params}')
    df = get_tables(params)
    if pg_table != 'stg.del_deliveries':
        df['update_ts'] = pd.to_datetime(datetime.now()).floor('s') #datetime.now()
    if 'delivery_ts' in df.columns:
        df['delivery_ts'] = df['delivery_ts'].astype('datetime64[s]').round('1s')
    df = df.drop_duplicates().reset_index(drop = True)
    cols = ','.join(list(df.columns))
    logging.info(f'cols: {cols}')
    insert_stmt = f'''
    INSERT INTO {pg_table} ({cols}) VALUES %s ON CONFLICT ({u_col}) DO UPDATE
    '''+ set_stmt

    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    psycopg2.extras.execute_values(cur, insert_stmt, df.values)
    conn.commit()
    cur.close()
    conn.close() 


#дата последнего загруженного заказа
def get_val(exp):
    hook = PostgresHook(postgres_conn_id='pg_conn')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(f"{exp}")
    result = str((cur.fetchall()[0][0]))
    logging.info(f'update_ts: {result}')
    if result == 'None':
        return '2022-09-25 00:01:01'
    else:
        return result

#обновление параметров фильтрации для запроса доставок
def update_val(ti, params, col):
    val = ti.xcom_pull(task_ids="get_delivery_ts", key="return_value")
    logging.info(f'val: {val}')
    logging.info(f'dict_key: {params[col]}')
    params[col] = val
    return params
