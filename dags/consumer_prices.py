from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from kafka import KafkaConsumer
import json
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# ----------------------
# Config
# ----------------------
DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "1234",
    "database": "thesis"
}

BROKER = "kafka:9092"
TOPIC = "crypto-prices"
GROUP_ID = "price-consumer-group"

# ----------------------
# Helper functions
# ----------------------
def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)

def get_or_create_id(cursor, table, id_col, name_col, name_value, conn):
    """Lấy hoặc tạo id trong bảng dim"""
    cursor.execute(f"SELECT {id_col} FROM {table} WHERE {name_col}=%s", (name_value,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(f"INSERT INTO {table} ({name_col}) VALUES (%s)", (name_value,))
    conn.commit()
    return cursor.lastrowid

# ----------------------
# Consumer & Insert
# ----------------------
def consume_and_insert(**context):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    conn = get_db_conn()
    cursor = conn.cursor()

    print("⏳ Consumer started...")

    for msg in consumer:
        data = msg.value

        symbol_name = data["symbol"]
        interval_name = data.get("interval", "1h")  # mặc định "1h" nếu producer không gửi

        # Lấy symbol_id & interval_id từ bảng dim
        symbol_id = get_or_create_id(cursor, "symbol_dim_1", "symbol_id", "symbol_name", symbol_name, conn)
        interval_id = get_or_create_id(cursor, "interval_dim_1", "interval_id", "interval_name", interval_name, conn)

        # Chèn vào fact table
        cursor.execute("""
            INSERT IGNORE kline_fact_1
            (symbol_id, interval_id, open_time, open_price, high_price, low_price, close_price, volume, close_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol_id, interval_id,
            datetime.fromtimestamp(data["open_time"]/1000), data["open"], data["high"],
            data["low"], data["close"], data["volume"], datetime.fromtimestamp(data["close_time"]/1000)
        ))

        conn.commit()
        consumer.commit()  # commit Kafka offset
        print(f"💾 Inserted {symbol_name}-{interval_name} at {datetime.now()}")

    cursor.close()
    conn.close()
    consumer.close()

# ----------------------
# DAG setup
# ----------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="consume_binance_prices",
    default_args=default_args,
    schedule_interval="@hourly",  # chạy theo DAG, vẫn liên tục nếu producer gửi dữ liệu
    start_date=datetime(2025, 9, 9),
    catchup=False,
) as dag:

    consume_task = PythonOperator(
        task_id="consume_and_insert",
        python_callable=consume_and_insert,
        provide_context=True
    )

    compute_indicators_1 = SparkSubmitOperator(
        task_id="Compute_indicators_1",
        application="/opt/airflow/dags/spark_job_1.py",
        conn_id="spark_default",
        name="arrow-spark",
        conf={"spark.master": "local[*]"},
        deploy_mode="client",
        packages="mysql:mysql-connector-java:8.0.33",
        dag=dag,
    )

consume_task >> compute_indicators_1