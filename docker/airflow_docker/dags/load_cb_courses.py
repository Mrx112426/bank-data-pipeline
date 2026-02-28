import requests
import xml.etree.ElementTree as ET
import pandas as pd
from io import StringIO
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_from_cbr_to_s3(ds, **kwargs):
    date_dt = datetime.strptime(ds, '%Y-%m-%d')
    cbr_date = date_dt.strftime('%d/%m/%Y')

    url = f"http://www.cbr.ru/scripts/XML_daily.asp?date_req={cbr_date}"
    response = requests.get(url)
    root = ET.fromstring(response.content)

    data = []
    for valute in root.findall('Valute'):
        char_code = valute.find('CharCode').text
        if char_code in ['USD', 'EUR', 'CNY']:
            value = float(valute.find('Value').text.replace(',', '.'))
            nominal = int(valute.find('Nominal').text)
            data.append({
                'currency_code': char_code,
                'rate_value': value / nominal,
                'rate_date': ds  # Дата в формате YYYY-MM-DD
            })

    df = pd.DataFrame(data)
    csv_buffer = df.to_csv(index=False)

    s3_hook = S3Hook(aws_conn_id='minio_conn_id')
    s3_key = f"raw/cbr/rates_{ds}.csv"
    s3_hook.load_string(csv_buffer, s3_key, 'bank-analytics-lake', replace=True)
    return s3_key


def load_s3_to_postgres(ti, ds, **kwargs):  # Добавили ds (дата запуска)
    s3_key = ti.xcom_pull(task_ids='extract_cbr')
    s3_hook = S3Hook(aws_conn_id='minio_conn_id')
    content = s3_hook.read_key(s3_key, bucket_name='bank-analytics-lake')
    df = pd.read_csv(StringIO(content))

    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    engine = pg_hook.get_sqlalchemy_engine()

    # Удаляем данные за ЭТУ конкретную дату перед вставкой,
    # чтобы не плодить дубли при перезапусках таски
    with engine.connect() as conn:
        conn.execute(f"DELETE FROM staging.exchange_rates WHERE rate_date = '{ds}'")

    df.to_sql('exchange_rates', engine, schema='staging', if_exists='append', index=False)


default_args = {
    'owner': 'Umar Mazoriev',
}

with DAG(
    dag_id='load_cb_courses',     # ID дага (должен быть уникальным)
    default_args=default_args,
    start_date=datetime(2025, 1, 1), # Объект даты
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=3
) as dag:
    start = EmptyOperator(task_id='start')
    t1 = PythonOperator(
        task_id='extract_cbr',
        python_callable=extract_from_cbr_to_s3
    )
    t2 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_s3_to_postgres
    )
    end = EmptyOperator(task_id='end')
    start >> t1 >> t2 >> end