from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
# Вместо старого импорта:
from airflow.providers.common.sql.hooks.sql import DbApiHook

# Внутри функции используй такой вызов (через стандартный Get Connection):
from airflow.hooks.base import BaseHook
import clickhouse_connect

def refresh_clickhouse_table():
    conn = BaseHook.get_connection('clickhouse_default')

    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port,
        username=conn.login,
        password=conn.password,
        database=conn.schema
    )

    # 1. Очищаем таблицу перед загрузкой
    client.command("TRUNCATE TABLE learn_db.f_transaction_details")

    # 2. Заливаем свежие данные напрямую из Postgres View
    # Замени хост, порт, бд, юзера и пароль на свои (или используй переменные Airflow)
    sql = """
    insert into learn_db.f_transaction_details
    SELECT *
    FROM postgresql(
        'postgres_dwh:5432', 
        'dwh_db', 
        'v_transactions_reporting',
        'user', 
        'password', 
        'dm'
    )
    """
    client.command(sql)
    print("Данные успешно перелиты в ClickHouse!")

def on_failure_callback(context):
    print(f"Ошибка в таске: {context['task_instance'].task_id}")

default_args = {
    'owner': 'Mazoriev Umar'
}

with DAG(
        dag_id='postgre_to_ch',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        max_active_runs=1,
        default_args=default_args
) as dag:
    start = EmptyOperator(task_id='start')

    wait_for_load_to_core = ExternalTaskSensor(
        task_id='wait_for_load_to_core',
        external_dag_id='staging_to_core',  # ID первого дага
        external_task_id='end',  # ID конкретной задачи (опционально)
        allowed_states=['success'],
        execution_delta=timedelta(hours=0),  # Ждем запуск за то же время
        timeout=600  # Ждем максимум 10 минут
    )

    load_task = PythonOperator(
        task_id='debug_s3_paths',
        python_callable=refresh_clickhouse_table
    )

    end = EmptyOperator(task_id='end')

    start >> wait_for_load_to_core >> load_task >> end
