from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests
import pandas as pd

API_URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = ["bitcoin", "ethereum"]
CURRENCIES = ["usd", "inr"]

def extract_transform_load():
    # 1️⃣ Fetch API
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

    # 2️⃣ Transform
    run_ts = datetime.utcnow()
    rows = []

    for coin, prices in data.items():
        for currency, price in prices.items():
            rows.append((
                run_ts,
                coin,
                currency,
                price
            ))

    # 3️⃣ Load into Snowflake STG
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    insert_sql = """
        INSERT INTO DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES_STG
        (run_ts, coin_id, vs_currency, price)
        VALUES (%s, %s, %s, %s)
    """

    hook.run(
        insert_sql,
        parameters=rows
    )
