from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain

default_args = {
    'owner' : 'Mazoriev Umar'
}

with DAG(
    dag_id = 'staging_to_core',
    start_date=datetime(2025, 1, 1),
    schedule_interval = '@monthly',
    catchup=True,
    max_active_runs=1
) as dag:

    # Порядок в списке ВАЖЕН: сначала таблицы без FK, потом таблицы с зависимостями
    TABLES_CONFIG = [
        # (имя_таблицы, первичный_ключ, список_полей_для_обновления)
        ('dict_regions', 'region_id', ['region_name', 'city', 'timezone']),
        ('dict_mcc', 'mcc_code', ['category_name', 'description']),
        ('dict_tariffs', 'tariff_id', ['tariff_name', 'monthly_cost', 'cashback_percent']),
        ('dict_merchants', 'merchant_id', ['merchant_name', 'mcc_code', 'region_id']),
        ('users', 'user_id', ['full_name', 'gender', 'birth_date', 'region_id', 'is_active']),
        ('accounts', 'account_id', ['user_id', 'tariff_id', 'currency_code', 'balance', 'is_blocked']),
        ('cards', 'card_id', ['account_id', 'card_number_masked', 'card_type', 'expiry_date', 'is_virtual']),
        ('transactions', 'transaction_id', ['amount_rub', 'cashback_amount', 'status'])
    ]

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id='end')

    # Создаем список для цепочки задач
    task_list = [start]

    for table_name, pk, update_cols in TABLES_CONFIG:
        wait_for_load_to_staging = ExternalTaskSensor(
            task_id = f'wait_for_{table_name}',
            external_dag_id = 'oltp_s3_dwh',
            external_task_id = f'upload_s3_to_dwh_{table_name}',
            allowed_states = ['success'],
            execution_delta = timedelta(0),
            timeout = 600,
            mode = 'reschedule'
        )

        update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        sql = f"""
                    INSERT INTO core.{table_name}
                    SELECT * FROM staging.{table_name}
                    ON CONFLICT ({pk}) 
                    DO UPDATE SET
                        {update_str};
                """

        transform_to_core = PostgresOperator(
            task_id = f'transform_{table_name}_to_core',
            postgres_conn_id = 'postgres_dwh',
            sql = sql
        )

        # Добавляем в список для цепочки
        task_list.append(wait_for_load_to_staging)
        task_list.append(transform_to_core)

    task_list.append(end)

    # Выстраиваем всё в одну строгую очередь: start >> сенсор1 >> таска1 >> сенсор2 >> таска2... >> end
    chain(*task_list)