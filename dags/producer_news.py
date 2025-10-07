#docker exec -it airflow-airflow-worker-1 bash
#python /opt/airflow/dags/producer.py

# producer_streaming.py
import time
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import re
from datetime import datetime

# ===== NLTK setup =====
nltk.download("vader_lexicon")
sia = SentimentIntensityAnalyzer()

# ===== Kafka setup =====
BROKER = "kafka:9092"
TOPIC = "crypto-news"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# ===== RSS validator =====
COINDESK_REGEX = re.compile(r"^https:\/\/www\.coindesk\.com\/[a-z0-9-]+")
NEWSBTC_REGEX = re.compile(r"^https:\/\/www\.newsbtc\.com\/[a-z0-9-]+")

def is_valid_coindesk(url: str) -> bool:
    return COINDESK_REGEX.match(url) is not None

def is_valid_newsbtc(url: str) -> bool:
    return NEWSBTC_REGEX.match(url) is not None

# ===== RSS feeds =====
RSS_FEEDS = [
    ("https://www.coindesk.com/arc/outboundfeeds/rss/", is_valid_coindesk),
    ("https://www.newsbtc.com/feed/", is_valid_newsbtc)
]

# ===== Lưu URLs đã gửi để tránh trùng =====
sent_urls = set()

# ===== Producer streaming =====
while True:
    for rss_url, validator in RSS_FEEDS:
        try:
            resp = requests.get(rss_url, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(resp.text, "xml")

            for item in soup.find_all("item"):
                link = item.find("link").text.strip()
                if not validator(link) or link in sent_urls:
                    continue

                title = item.find("title").text.strip()
                pub_date = item.find("pubDate").text.strip()
                created_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
                category_el = item.find("category") or item.find("dc:creator")
                tag = category_el.text.strip() if category_el else None

                # Crawl body + sentiment
                try:
                    body_resp = requests.get(link, headers={"User-Agent": "Mozilla/5.0"})
                    body_soup = BeautifulSoup(body_resp.text, "html.parser")
                    paragraphs = [p.text.strip() for p in body_soup.find_all("p")]
                    content = " ".join(paragraphs)
                    score = sia.polarity_scores(content)["compound"]
                except:
                    content = ""
                    score = 0

                data = {
                    "title": title,
                    "url": link,
                    "created_date": str(created_date),
                    "tag": tag,
                    "content": content,
                    "sentiment_score": score
                }

                producer.send(TOPIC, value=data)
                sent_urls.add(link)
                print(f"Sent to Kafka: {title}")

        except Exception as e:
            print(f"Error crawling {rss_url}: {e}")

    # Delay nhỏ để tránh spam request, crawl liên tục gần real-time
    time.sleep(30)  # crawl mỗi 30 giây
