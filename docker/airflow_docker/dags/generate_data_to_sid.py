from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import io
import csv
import random
from faker import Faker

# Настройки
fake = Faker('ru_RU')


def bulk_copy(cursor, table_name, columns, data_list):
    """Универсальная вставка через COPY внутри существующей транзакции"""
    if not data_list:
        return
    f = io.StringIO()
    writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    for row in data_list:
        writer.writerow(row)
    f.seek(0)

    query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL 'NULL')"
    cursor.copy_expert(query, f)


def fill_reference_data(conn):
    """Заполнение справочников (выполняется один раз)"""
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM currencies LIMIT 1);")
        if cur.fetchone()[0]:
            return

        print("Заполняем справочники...")
        bulk_copy(cur, 'currencies', ('currency_code', 'name'),
                  [('RUB', 'Рубль'), ('USD', 'Доллар'), ('EUR', 'Евро'), ('CNY', 'Юань')])
        bulk_copy(cur, 'categories', ('category_name',),
                  [('Продукты',), ('Кафе',), ('Электроника',), ('Транспорт',), ('ЖКХ',)])
        bulk_copy(cur, 'operation_types', ('operation_name',),
                  [('Перевод',), ('Покупка',), ('Снятие',), ('Пополнение',)])
        bulk_copy(cur, 'branches', ('branch_name', 'city', 'region'),
                  [('Центральный', 'Москва', 'МСК'), ('Невский', 'Санкт-Петербург', 'СПБ'),
                   ('Уральский', 'Екатеринбург', 'УРАЛ')])
        bulk_copy(cur, 'products', ('product_name', 'product_type'),
                  [('Дебетовая карта Мир', 'Карта'), ('Кредитная карта Platinum', 'Карта'),
                   ('Вклад Доходный', 'Депозит')])
        bulk_copy(cur, 'counterparties', ('name', 'category'), [
            ('Магнит', 'Shop'), ('Яндекс Такси', 'Service'), ('Wildberries', 'Shop'),
            ('Вкусно и Точка', 'Cafe'), ('М.Видео', 'Shop'), ('Налоговая служба', 'Government')
        ])


def generate_data_func(ds, **kwargs):
    # Airflow передает ds как строку 'YYYY-MM-DD'
    target_date = datetime.strptime(ds, '%Y-%m-%d')

    # Используем PostgresHook (connection_id 'postgres_oltp' должен быть создан в Airflow UI)
    pg_hook = PostgresHook(postgres_conn_id='postgres_oltp')
    conn = pg_hook.get_conn()

    try:
        with conn.cursor() as cur:
            # 1. Справочники
            #fill_reference_data(conn)

            # 2. Идемпотентность: удаляем данные за этот день перед вставкой
            cur.execute("DELETE FROM transactions WHERE transaction_date::date = %s", (target_date.date(),))

            # 3. Получаем справочные ID
            cur.execute("SELECT product_id, product_type FROM products")
            all_products = cur.fetchall()
            cur.execute("SELECT branch_id FROM branches")
            all_branches = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT category_id FROM categories")
            all_categories = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT operation_type_id FROM operation_types")
            all_ops = [r[0] for r in cur.fetchall()]
            cur.execute("SELECT counterparty_id FROM counterparties")
            all_counterparties = [r[0] for r in cur.fetchall()]

            # --- Логика новых клиентов ---
            num_new_customers = random.randint(30, 60)
            new_cust_prods = []
            available_currencies = ['RUB', 'USD', 'EUR', 'CNY']
            for _ in range(num_new_customers):
                first, last = fake.first_name(), fake.last_name()
                birth = fake.date_of_birth(minimum_age=18, maximum_age=70)
                cur.execute("""
                    INSERT INTO customers (first_name, last_name, birth_date, region, created_at) 
                    VALUES (%s, %s, %s, %s, %s) RETURNING customer_id
                """, (first, last, birth, fake.region(), target_date))
                c_id = cur.fetchone()[0]

                acc_num = fake.numerify(text='40817810############')
                prod_id, prod_type = random.choice(all_products)
                acc_type = 'Кредитный' if 'Кредитная' in prod_type else 'Текущий'

                # ВЫБИРАЕМ ВАЛЮТУ (например, 80% RUB, остальные — валюта)
                currency = random.choices(available_currencies, weights=[70, 10, 10, 10], k=1)[0]

                cur.execute("""
                    INSERT INTO accounts (customer_id, account_number, currency_code, balance, account_type, opened_at) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (c_id, acc_num, currency, round(random.uniform(5000, 100000), 2), acc_type, target_date))
                new_cust_prods.append([c_id, prod_id, target_date.date(), 'NULL'])

            bulk_copy(cur, 'customer_products', ('customer_id', 'product_id', 'start_date', 'end_date'), new_cust_prods)

            # --- Транзакции ---
            cur.execute("SELECT account_id, currency_code FROM accounts WHERE status = 'active'")
            active_accounts = cur.fetchall()

            if active_accounts:
                num_tx = random.randint(1000, 3000)
                tx_data = []
                statuses = ['completed', 'declined', 'pending', 'reversed']
                status_weights = [94, 4, 1, 1]
                for _ in range(num_tx):
                    acc_id, curr = random.choice(active_accounts)
                    h, m, s = random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
                    tx_time = target_date.replace(hour=h, minute=m, second=s)
                    tx_status = random.choices(statuses, weights=status_weights, k=1)[0]
                    created_at = tx_time.replace(second=random.randint(s, 59) if s < 59 else 59)

                    tx_data.append([
                        acc_id, random.choice(all_branches), random.choice(all_ops),
                        random.choice(all_categories), curr, tx_time, round(random.uniform(50, 15000), 2),
                        random.choice(all_counterparties), tx_status, fake.sentence(nb_words=3), created_at
                    ])

                bulk_copy(cur, 'transactions',
                          ('account_id', 'branch_id', 'operation_type_id', 'category_id', 'currency_code',
                           'transaction_date', 'amount', 'counterparty_id', 'status', 'description', 'created_at'),
                          tx_data)

        conn.commit()
        print(f"Successfully generated data for {ds}")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


# Описание DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'generate_oltp_data_daily',
        default_args=default_args,
        schedule_interval='@daily',  # Каждый день в полночь
        catchup=True,  # Заполнит данные с 2025-01-01 до текущей даты
        max_active_runs=1,  # Чтобы не перегрузить базу одновременными вставками
        tags=['oltp', 'generator'],
) as dag:
    start = EmptyOperator(task_id = 'start')
    generate_data_task = PythonOperator(
        task_id='generate_daily_batch',
        python_callable=generate_data_func,
        op_kwargs={'ds': '{{ ds }}'},  # Передаем дату из Airflow
    )

    end = EmptyOperator(task_id='end')
    start >> generate_data_task >> end