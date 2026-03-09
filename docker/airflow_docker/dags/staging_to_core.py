from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain
from airflow.utils.task_group import TaskGroup

def on_failure_callback(context):
    # Тут можно добавить логику отправки сообщения в Telegram/Slack
    print(f"Ошибка в таске: {context['task_instance'].task_id}")

default_args = {
    'owner' : 'Mazoriev Umar',
    'on_failure_callback': on_failure_callback # Все таски в DAG будут слать алерт при ошибке
}

with DAG(
    dag_id = 'staging_to_core',
    start_date=datetime(2025, 1, 1),
    schedule_interval = '@monthly',
    catchup=True,
    max_active_runs=1,
    default_args=default_args
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
        ('transactions', 'transaction_id', [
            'card_id',
            'account_id',
            'merchant_id',
            'transaction_type',
            'amount_rub',
            'cashback_amount',
            'transaction_time',
            'status'
        ])
    ]

    start = EmptyOperator(task_id = 'start')
    end = EmptyOperator(task_id='end')

    # Создаем список для цепочки задач

    def create_task_group(tables, group_id):
        with TaskGroup(group_id) as tg:
            for table_name, pk, update_cols in tables:
                wait_task = ExternalTaskSensor(
                    task_id=f'wait_for_{table_name}',
                    external_dag_id='oltp_s3_dwh',
                    external_task_id=f'upload_s3_to_dwh_{table_name}',
                    allowed_states=['success'],
                    execution_delta=timedelta(0),
                    timeout=600,
                    mode='reschedule'
                )

                all_cols = update_cols + [pk]
                update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])

                # --- УЛУЧШЕННАЯ ЛОГИКА SQL ---
                # 1. Для транзакций добавляем фильтр по месяцу и дедупликацию
                if table_name == 'transactions':
                    sql = f"""
                        INSERT INTO core.{table_name} ({", ".join(all_cols)})
                        SELECT DISTINCT ON ({pk}) {", ".join(all_cols)} 
                        FROM staging.{table_name}
                        WHERE date_trunc('month', transaction_time::timestamptz) = date_trunc('month', '{{{{ ds }}}}'::timestamptz)
                        ORDER BY {pk}, transaction_time DESC
                        ON CONFLICT ({pk}) 
                        DO UPDATE SET {update_str};
                    """
                # 2. Для справочников просто дедупликация (на случай повторов в staging)
                else:
                    sql = f"""
                        INSERT INTO core.{table_name} ({", ".join(all_cols)})
                        SELECT DISTINCT ON ({pk}) {", ".join(all_cols)} 
                        FROM staging.{table_name}
                        ORDER BY {pk}
                        ON CONFLICT ({pk}) 
                        DO UPDATE SET {update_str};
                    """
                # -----------------------------

                transform_to_core = PostgresOperator(
                    task_id=f'transform_{table_name}_to_core',
                    postgres_conn_id='postgres_dwh',
                    sql=sql
                )

                wait_task >> transform_to_core

        return tg

    # Группы согласно зависимостям
    tg1 = create_task_group([
        ('dict_regions', 'region_id', ['region_name', 'city', 'timezone']),
        ('dict_mcc', 'mcc_code', ['category_name', 'description']),
        ('dict_tariffs', 'tariff_id', ['tariff_name', 'monthly_cost', 'cashback_percent'])
    ], "layer_1_dicts")

    tg2 = create_task_group([
        ('dict_merchants', 'merchant_id', ['merchant_name', 'mcc_code', 'region_id']),
        ('users', 'user_id', ['full_name', 'gender', 'birth_date', 'region_id', 'is_active'])
    ], "layer_2_users_merch")

    tg3 = create_task_group([
        ('accounts', 'account_id', ['user_id', 'tariff_id', 'currency_code', 'balance', 'is_blocked'])
    ], "layer_3_accounts")

    tg4 = create_task_group([
        ('cards', 'card_id', ['account_id', 'card_number_masked', 'card_type', 'expiry_date', 'is_virtual'])
    ], "layer_4_cards")

    tg5 = create_task_group([
        ('transactions', 'transaction_id', [
            'card_id',
            'account_id',
            'merchant_id',
            'transaction_type',
            'amount_rub',
            'cashback_amount',
            'transaction_time',
            'status'
        ])
    ], "layer_5_transactions")

    # Выстраиваем цепочку
    start >> tg1 >> tg2 >> tg3 >> tg4 >> tg5 >> end

