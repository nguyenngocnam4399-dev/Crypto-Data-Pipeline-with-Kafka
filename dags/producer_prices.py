# producer_binance_multi.py
from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

BROKER = "kafka:9092"
TOPIC = "crypto-prices"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda m: json.dumps(m).encode("utf-8")
)

# Danh sách Symbol và Interval
SYMBOLS = ["BTCUSDT","ETHUSDT","BNBUSDT","PEPEUSDT","XRPUSDT","SOLUSDT","DOGEUSDT","LINKUSDT"]
INTERVALS = ["1h","1d"]
LIMIT = 1000

def fetch_klines(symbol, interval, limit):
    # Gọi API Binance để lấy dữ liệu Kline
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    return res.json()

def produce_loop():
    # Vòng lặp gửi dữ liệu liên tục vào Kafka
    last_timestamp = {}  # lưu timestamp cuối mỗi symbol+interval
    print("🚀 Producer started — sending data to Kafka...")

    try:
        while True:
            for symbol in SYMBOLS:
                for interval in INTERVALS:
                    key = f"{symbol}-{interval}"
                    klines = fetch_klines(symbol, interval, LIMIT)

                    for k in klines:
                        open_time = k[0]

                        # Bỏ qua dữ liệu cũ đã gửi
                        if last_timestamp.get(key) and open_time <= last_timestamp[key]:
                            continue  

                        msg = {
                            "symbol": symbol,
                            "interval": interval,
                            "open_time": k[0],
                            "open": k[1],
                            "high": k[2],
                            "low": k[3],
                            "close": k[4],
                            "volume": k[5],
                            "close_time": k[6],
                            "fetched_at": datetime.now().isoformat()
                        }

                        # Gửi message vào topic tương ứng (ở đây là topic klines)
                        producer.send(TOPIC,
                            key=bytes(key, "utf-8"),  # key giúp Kafka group theo symbol-interval
                            value=msg)
                        # Cập nhật timestamp cuối    
                        last_timestamp[key] = open_time

                        # Log ra console
                        print(f"[{datetime.now()}] Sent {symbol}-{interval} @ {msg['close']}")

            producer.flush()
            time.sleep(10)  # crawl mỗi 10s

    except KeyboardInterrupt:
        print("\n🛑 Producer stopped manually.")
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        producer.close()
        print("✅ Kafka producer closed cleanly.")

if __name__ == "__main__":
    produce_loop()
