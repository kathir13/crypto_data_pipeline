from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests


# ======================
# PYTHON FUNCTION FIRST
# ======================

def extract_transform_load():
    API_URL = "https://api.coingecko.com/api/v3/simple/price"
    COINS = ["bitcoin", "ethereum"]
    CURRENCIES = ["usd", "inr"]

    response = requests.get(
        API_URL,
        params={
            "ids": ",".join(COINS),
            "vs_currencies": ",".join(CURRENCIES)
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30
    )

    data = response.json()
    run_ts = datetime.utcnow()

    rows = []
    for coin, prices in data.items():
        for currency, price in prices.items():
            rows.append((run_ts, coin, currency, price))

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    insert_sql = """
        INSERT INTO DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES_STG
        (run_ts, coin_id, vs_currency, price)
        VALUES (%s, %s, %s, %s)
    """

    conn = hook.get_conn()
    cur = conn.cursor()
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


# ======================
# DAG DEFINITION SECOND
# ======================

default_args = {"owner": "airflow"}

with DAG(
    dag_id="crypto_prices_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    extract_load = PythonOperator(
        task_id="extract_transform_load",
        python_callable=extract_transform_load,
    )

merge_final = SQLExecuteQueryOperator(
    task_id="merge_into_final",
    conn_id="snowflake_default",
    sql="""
    MERGE INTO DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES f
    USING (
        SELECT
            DATE(run_ts) AS run_date,
            coin_id,
            vs_currency,
            price,
            run_ts AS last_updated_ts
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY DATE(run_ts), coin_id, vs_currency
                       ORDER BY run_ts DESC
                   ) rn
            FROM DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES_STG
        )
        WHERE rn = 1
    ) s
    ON f.run_date = s.run_date
    AND f.coin_id = s.coin_id
    AND f.vs_currency = s.vs_currency
    WHEN MATCHED THEN UPDATE SET
        f.price = s.price,
        f.last_updated_ts = s.last_updated_ts
    WHEN NOT MATCHED THEN INSERT (
        run_date, coin_id, vs_currency, price, last_updated_ts
    )
    VALUES (
        s.run_date, s.coin_id, s.vs_currency, s.price, s.last_updated_ts
    );
    """
)

truncate_stg = SQLExecuteQueryOperator(
    task_id="truncate_staging",
    conn_id="snowflake_default",
    sql="""
    TRUNCATE TABLE DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES_STG;
    """
)


extract_load >> merge_final >> truncate_stg
