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

for table, pk, update_cols in TABLES_CONFIG:
    update_str = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])


    sql=f"""
        INSERT INTO core.{table}
        SELECT * FROM staging.{table}
        ON CONFLICT ({pk}) 
        DO UPDATE SET
            {update_str};
    """

    print(sql)