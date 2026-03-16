import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Инициализируем логгер
logger = logging.getLogger("airflow.task")


def on_failure_callback(context):
    print(f"Ошибка в таске: {context['task_instance'].task_id}")


default_args = {
    'owner': 'Mazoriev Umar',
    'on_failure_callback': on_failure_callback,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='staging_to_core',
        start_date=datetime(2025, 1, 1),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs=1,
        default_args=default_args
) as dag:
    # --- 1. ФУНКЦИЯ ОТЛАДКИ (ПОМОЖЕТ ПОНЯТЬ, ЧТО ИЩЕТ СЕНСОР) ---
    def debug_s3_paths(ds, **kwargs):
        # Генерируем пути точно так же, как сенсор
        dt = datetime.strptime(ds, '%Y-%m-%d')
        year = dt.strftime('%Y')
        month = dt.strftime('%m')

        dict_path = f"raw/dicts/currencies/snapshot_{ds}.parquet"
        fact_path = f"raw/not_dict/transactions/{year}/{month}/{ds}.parquet"

        logger.info("=" * 50)
        logger.info(f"DEBUG LOG FOR DATE: {ds}")
        logger.info(f"EXPECTED DICT PATH: {dict_path}")
        logger.info(f"EXPECTED FACT PATH: {fact_path}")
        logger.info("=" * 50)


    debug_task = PythonOperator(
        task_id='debug_s3_paths',
        python_callable=debug_s3_paths,
        op_kwargs={'ds': '{{ ds }}'}
    )


    def generate_upsert_sql(table_name, pk, update_cols, date_col, ds):
        all_cols = update_cols + [pk]
        update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        where_clause = f"WHERE {date_col}::date = '{ds}'::date" if date_col else ""

        return f"""
            INSERT INTO core.{table_name} ({", ".join(all_cols)})
            SELECT DISTINCT ON ({pk}) {", ".join(all_cols)} 
            FROM staging.{table_name}
            {where_clause}
            ORDER BY {pk}
            ON CONFLICT ({pk}) DO UPDATE SET {update_str};
        """


    def create_load_tier(table_name, pk, update_cols, date_col):
        if date_col:
            # Для фактов
            s3_path = (
                f"raw/not_dict/{table_name}/"
                "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}/"
                "{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}/"
                "{{ ds }}.parquet"
            )
        else:
            # Для справочников (ИСПОЛЬЗУЕМ ДВОЙНЫЕ СКОБКИ БЕЗ F-СТРОКИ ДЛЯ НАДЕЖНОСТИ)
            s3_path = "raw/dicts/" + table_name + "/snapshot_{{ ds }}.parquet"

        wait_file = S3KeySensor(
            task_id=f'wait_for_s3_{table_name}',
            bucket_name='bank-analytics-lake',
            bucket_key=s3_path,
            aws_conn_id='minio_conn_id',
            poke_interval=30,
            timeout=3600,
            mode='reschedule'
        )

        load_task = PostgresOperator(
            task_id=f'stg_to_core_{table_name}',
            postgres_conn_id='postgres_dwh',
            # Тут тоже исправляем передачу ds
            sql=generate_upsert_sql(table_name, pk, update_cols, date_col, '{{ ds }}')
        )

        return wait_file >> load_task


    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    with TaskGroup("layer_1_independent_dicts") as tg1:
        dicts_1 = [
            ('currencies', 'currency_code', ['name'], None),
            ('categories', 'category_id', ['category_name'], None),
            ('operation_types', 'operation_type_id', ['operation_name'], None),
            ('branches', 'branch_id', ['branch_name', 'city', 'region'], None),
            ('counterparties', 'counterparty_id', ['name', 'category'], None),
            ('products', 'product_id', ['product_name', 'product_type'], None)
        ]
        for table, pk, cols, d_col in dicts_1:
            create_load_tier(table, pk, cols, d_col)

    with TaskGroup("layer_2_customers") as tg2:
        create_load_tier('customers', 'customer_id', ['first_name', 'last_name', 'birth_date', 'region'], None)

    with TaskGroup("layer_3_accounts_and_products") as tg3:
        create_load_tier('accounts', 'account_id',
                         ['customer_id', 'account_number', 'currency_code', 'balance', 'account_type', 'status',
                          'closed_at'], None)

        cp_table = 'customer_products'

        # 1. Сенсор (убедись, что тут нет f-строки, только обычная строка)
        wait_cp = S3KeySensor(
            task_id=f'wait_for_s3_{cp_table}',
            bucket_name='bank-analytics-lake',
            bucket_key="raw/dicts/" + cp_table + "/snapshot_{{ ds }}.parquet",
            aws_conn_id='minio_conn_id',
            mode='reschedule'
        )

        # 2. Оператор с явным SQL (поменяли порядок колонок на классический)
        load_cp = PostgresOperator(
            task_id=f'stg_to_core_{cp_table}',
            postgres_conn_id='postgres_dwh',
            sql="""
                        INSERT INTO core.customer_products (customer_id, product_id, start_date, end_date)
                        SELECT DISTINCT ON (customer_id, product_id) 
                               customer_id, product_id, start_date, end_date
                        FROM staging.customer_products
                        ORDER BY customer_id, product_id
                        ON CONFLICT (customer_id, product_id) 
                        DO UPDATE SET 
                            start_date = EXCLUDED.start_date, 
                            end_date = EXCLUDED.end_date;
                    """
        )
        wait_cp >> load_cp

    with TaskGroup("layer_4_transactions") as tg4:
        create_load_tier('transactions', 'transaction_id',
                         ['account_id', 'branch_id', 'operation_type_id', 'category_id', 'currency_code',
                          'transaction_date', 'created_at', 'amount', 'counterparty_id', 'status', 'description'],
                         'created_at')

    # Связываем всё вместе
    start >> debug_task >> [tg1, tg2, tg3, tg4] >> end