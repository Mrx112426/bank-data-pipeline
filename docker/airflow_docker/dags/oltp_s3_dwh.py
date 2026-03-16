from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import io
import pandas as pd

def upload_to_s3(table_name, date_column, ds, **kwargs):
    '''
    :param table_name: Название таблицы источника
    :param date_column: Названия колонки таблицы, где есть дата создания записи
    :param ds: Логическая дата airflow
    :param kwargs:
    '''
    oltp_postgre_hook = PostgresHook(postgres_conn_id='postgres_oltp')
    s3_hook = S3Hook(aws_conn_id='minio_conn_id')
    bucket_name = 'bank-analytics-lake'
    conn_info = oltp_postgre_hook.get_connection('postgres_oltp')
    print(f"DEBUG: Host: {conn_info.host}")
    print(f"DEBUG: DB Name: {conn_info.schema}")

    dt = datetime.strptime(ds, '%Y-%m-%d')
    year = dt.strftime('%Y')
    month = dt.strftime('%m')

    # 1. Формирование sql запроса
    if date_column:
        sql_query = f"select * from {table_name} where date_trunc('day', {date_column}::timestamptz) = date_trunc('day','{ds}'::timestamptz)"
        s3_path = f"raw/not_dict/{table_name}/{year}/{month}/{ds}.parquet"
    else:
        sql_query = f"select * from {table_name}"
        s3_path = f"raw/dicts/{table_name}/snapshot_{ds}.parquet"

    # ДОБАВЬ ЭТУ СТРОКУ
    print(f"DEBUG: Генерируемый SQL-запрос: {sql_query}")

    # 2. Чтение данных в pandas df
    df = oltp_postgre_hook.get_pandas_df(sql_query)

    if df.empty:
        print(f'В таблице {table_name} нет данных за логическую дату {ds}')
        return

    # 3. df -> parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    #4. Загрузка в s3
    s3_hook.load_file_obj(
        file_obj = buffer,
        key=s3_path,
        bucket_name = bucket_name,
        replace = True
    )

    print(f'За {ds} в S3 загружено - {df.shape[0]} строк')


def upload_s3_to_dwh(table_name, date_column, ds, **kwargs):
    dwh_postgre_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    s3_hook = S3Hook(aws_conn_id='minio_conn_id')
    bucket_name = 'bank-analytics-lake'

    dt = datetime.strptime(ds, '%Y-%m-%d')
    year = dt.strftime('%Y')
    month = dt.strftime('%m')

    # Определяем путь
    if date_column:
        s3_path = f"raw/not_dict/{table_name}/{year}/{month}/{ds}.parquet"
    else:
        s3_path = f"raw/dicts/{table_name}/snapshot_{ds}.parquet"

    if not s3_hook.check_for_key(s3_path, bucket_name):
        print(f"Файл {s3_path} не найден. Пропускаем.")
        return

    # Чтение из S3
    file_from_s3 = s3_hook.get_key(s3_path, bucket_name)
    data = file_from_s3.get()['Body'].read()
    df = pd.read_parquet(io.BytesIO(data))

    # Подготовка данных для COPY (табуляция)
    output = io.StringIO()
    df.to_csv(output, sep='\t', index=False, header=False)
    output.seek(0)

    conn = dwh_postgre_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("SET search_path TO staging;")

        # --- ЛОГИКА ОЧИСТКИ ---
        if not date_column:
            # Справочники: полностью перезаписываем
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            print(f"TRUNCATE для справочника {table_name}")
        else:
            # Факты: удаляем только данные за текущий месяц (чтобы перегрузить их)
            # Это делает задачу идемпотентной (можно перезапускать без дублей)
            cursor.execute(f"""
                DELETE FROM {table_name} 
                WHERE date_trunc('day', {date_column}::timestamptz) = date_trunc('day', '{ds}'::timestamptz);
            """)
            print(f"DELETE среза данных для {table_name} за {ds}")
        # ----------------------

        columns = ", ".join(df.columns)
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV DELIMITER '\t' NULL ''"
        cursor.copy_expert(sql, output)

        conn.commit()
        print(f"Successfully loaded {table_name}")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def_args = {
    'owner' : 'Umar Mazoriev'
}

with DAG (
    dag_id = 'oltp_s3_dwh',
    start_date = datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup = True,
    max_active_runs = 1
) as dag:
    tables = [
        ('transactions', 'created_at'),
        ('accounts', None),
        ('branches', None),
        ('counterparties', None),
        ('currencies', None),
        ('customers', None),
        ('operation_types', None),
        ('customer_products', None),
        ('products', None),
        ('categories', None)
    ]
    start = EmptyOperator(task_id=f'start')
    end = EmptyOperator(task_id=f'end')
    for table_name, column_date in tables:
        upload_postgre_to_s3_task = PythonOperator(
            task_id = f'upload_postgre_to_s3_{table_name}',
            python_callable = upload_to_s3,
            op_kwargs = {
                'table_name' : table_name,
                'date_column' : column_date,
                'ds' : '{{ds}}'
            }
        )

        upload_s3_to_dwh_task = PythonOperator(
            task_id = f'upload_s3_to_dwh_{table_name}',
            python_callable = upload_s3_to_dwh,
            op_kwargs = {
                'table_name' : table_name,
                'date_column': column_date,
                'ds': '{{ds}}'
            }
        )


        start >> upload_postgre_to_s3_task >> upload_s3_to_dwh_task >> end