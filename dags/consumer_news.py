# airflow_dag_consume_kafka.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from kafka import KafkaConsumer
import json

# DB config
DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "user": "root",
    "password": "1234",
    "database": "thesis"
}

# Kafka
BROKER = "kafka:9092"
TOPIC = "crypto-news"
GROUP_ID = "news-consumer-group"

def get_db_conn():
    return mysql.connector.connect(**DB_CONFIG)

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

    # Lấy dữ liệu hiện có, timeout 1s nếu không có
    msgs = consumer.poll(timeout_ms=1000, max_records=100)
    for tp, records in msgs.items():
        for msg in records:
            article = msg.value
            url = article["url"]

            # Kiểm tra trùng
            cursor.execute("SELECT id FROM news_fact_1 WHERE url=%s", (url,))
            if cursor.fetchone():
                consumer.commit()
                continue

            tag_id = None
            if article.get("tag"):
                cursor.execute("INSERT IGNORE INTO dim_tag_1 (tag_name) VALUES (%s)", (article["tag"],))
                conn.commit()
                cursor.execute("SELECT tag_id FROM dim_tag_1 WHERE tag_name=%s", (article["tag"],))
                tag_id = cursor.fetchone()[0]

            cursor.execute("""
                INSERT INTO news_fact_1 (title, url, sentiment_score, created_date, view_number, tag_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                article["title"], url, article["sentiment_score"],
                article["created_date"], None, tag_id
            ))

            conn.commit()
            consumer.commit()

    conn.close()
    consumer.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="consume_crypto_news_kafka",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2025, 9, 9),
    catchup=False,
) as dag:

    consume_task = PythonOperator(
        task_id="consume_and_insert",
        python_callable=consume_and_insert,
        provide_context=True
    )
