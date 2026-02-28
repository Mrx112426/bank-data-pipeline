import psycopg2
import random
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta

# Настройки подключения
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",  # Проверь порт! У тебя было 5433
    "dbname": "oltp_db",
    "user": "user",
    "password": "password"
}

fake = Faker('ru_RU')


def populate():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    print("--- Начало процесса генерации данных ---")

    # 1. Очистка
    tables = ['transactions', 'cards', 'accounts', 'users', 'dict_merchants', 'dict_mcc', 'dict_tariffs',
              'dict_regions']
    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
    print("Таблицы очищены.")

    # 2. Наполнение справочников
    # Регионы
    regions = [('Северная', 'Санкт-Петербург', 'UTC+3'), ('Центральная', 'Москва', 'UTC+3'),
               ('Южная', 'Ростов', 'UTC+3')]
    region_ids = []  # Создаем пустой список заранее
    for r in regions:
        cur.execute("INSERT INTO dict_regions (region_name, city, timezone) VALUES (%s, %s, %s) RETURNING region_id", r)
        region_ids.append(cur.fetchone()[0])  # Добавляем ID в список сразу

    # Тарифы
    tariffs = [('Gold', 500, 5.0), ('Premium', 1000, 10.0), ('Standard', 0, 1.0)]
    tariff_data = {}
    for t in tariffs:
        cur.execute(
            "INSERT INTO dict_tariffs (tariff_name, monthly_cost, cashback_percent) VALUES (%s, %s, %s) RETURNING tariff_id, cashback_percent",
            t)
        res = cur.fetchone()
        tariff_data[res[0]] = res[1]  # ИСПРАВЛЕНО: было res[2], стало res[1]

    # MCC
    mccs = [(5411, 'Супермаркеты', 'Еда'), (5812, 'Рестораны', 'Питание'), (4829, 'Переводы', 'Банки')]
    for m in mccs:
        cur.execute("INSERT INTO dict_mcc (mcc_code, category_name, description) VALUES (%s, %s, %s)", m)
    mcc_codes = [m[0] for m in mccs]

    # Мерчанты
    merchant_ids = []
    for _ in range(100):
        cur.execute(
            "INSERT INTO dict_merchants (merchant_name, mcc_code, region_id) VALUES (%s, %s, %s) RETURNING merchant_id",
            (fake.company(), random.choice(mcc_codes), random.choice(region_ids)))
        merchant_ids.append(cur.fetchone()[0])

    conn.commit()
    print("Справочники заполнены.")

    # 3. Пользователи, Счета, Карты (Dimension data)
    print("Создаем 10 000 пользователей...")
    for _ in range(10000):
        cur.execute(
            "INSERT INTO users (full_name, gender, birth_date, region_id) VALUES (%s, %s, %s, %s) RETURNING user_id",
            (fake.name(), random.choice(['M', 'F']), fake.date_of_birth(minimum_age=18, maximum_age=70),
             random.choice(region_ids)))
        uid = cur.fetchone()[0]

        t_id, cashback_pct = random.choice(list(tariff_data.items()))
        cur.execute(
            "INSERT INTO accounts (user_id, tariff_id, currency_code, balance) VALUES (%s, %s, %s, %s) RETURNING account_id",
            (uid, t_id, 'RUB', random.uniform(10000, 50000)))
        aid = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO cards (account_id, card_number_masked, card_type, expiry_date) VALUES (%s, %s, %s, %s) RETURNING card_id",
            (aid, f"****{random.randint(1000, 9999)}", random.choice(['Visa', 'MasterCard', 'MIR']),
             (datetime.now() + timedelta(days=365 * 2)).date()))

    conn.commit()
    print("Пользователи и карты созданы.")

    # 4. Транзакции (Fact data) - ВНЕ ЦИКЛА ПОЛЬЗОВАТЕЛЕЙ!
    cur.execute(
        "SELECT c.card_id, c.account_id, a.tariff_id FROM cards c JOIN accounts a ON c.account_id = a.account_id")
    card_info = cur.fetchall()  # Список всех доступных карт

    trans_types = ['Payment', 'Transfer', 'ATM Withdrawal']

    batch_size = 10000
    total_records = 1000000
    print(f"--- Начало массовой вставки {total_records} транзакций ---")

    insert_query = """
        INSERT INTO transactions 
        (card_id, account_id, merchant_id, transaction_type, amount_rub, cashback_amount, transaction_time, status) 
        VALUES %s
    """

    for i in range(0, total_records, batch_size):
        batch = []
        for _ in range(batch_size):
            c_id, a_id, t_id = random.choice(card_info)
            amount = random.uniform(100, 100000)
            cashback = round(amount * (float(tariff_data[t_id]) / 100), 2)
            m_id = random.choice(merchant_ids)

            batch.append((c_id, a_id, m_id, random.choice(trans_types), amount, cashback,
                          fake.date_time_between(start_date='-1y'), 'completed'))

        execute_values(cur, insert_query, batch)
        conn.commit()  # Фиксируем пачку
        print(f"Загружено {i + batch_size} строк...")

    cur.close()
    conn.close()
    print("--- Успешно! База данных готова к работе ---")


if __name__ == "__main__":
    populate()