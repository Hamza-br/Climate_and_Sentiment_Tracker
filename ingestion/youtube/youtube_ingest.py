import os
import json
import time
import googleapiclient.discovery
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
from logging_utils import json_log

# Load API key from environment or .env file
load_dotenv(".env.youtube")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
SLEEP_SECONDS = int(os.getenv("YOUTUBE_SLEEP_SECONDS", "300"))  # default 5 min
COMPONENT = "youtube_ingest"

# Validate API key
if not YOUTUBE_API_KEY:
    json_log(COMPONENT, "config_warning", message="YOUTUBE_API_KEY not set")

# Define search queries
SEARCH_QUERIES = [
    "climate change",
    "global warming",
    "air pollution",
    "heat wave",
    "environment"
]

# Kafka setup
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )

# YouTube API client
def get_youtube_client():
    from googleapiclient.discovery import build
    json_log(COMPONENT, "api_client_create")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

from googleapiclient.errors import HttpError

def fetch_comments(youtube, producer, cycle):
    sent = 0
    for query in SEARCH_QUERIES:
        json_log(COMPONENT, "search_begin", query=query, cycle=cycle)
        search_response = youtube.search().list(
            q=query,
            part="id",
            type="video",
            maxResults=5
        ).execute()

        for item in search_response.get("items", []):
            video_id = item["id"]["videoId"]

            try:
                comments_response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=20,
                    textFormat="plainText"
                ).execute()

                for comment_thread in comments_response.get("items", []):
                    comment = comment_thread["snippet"]["topLevelComment"]["snippet"]
                    record = {
                        "video_id": video_id,
                        "query": query,
                        "comment_id": comment_thread["id"],
                        "author": comment["authorDisplayName"],
                        "text": comment["textDisplay"],
                        "like_count": comment["likeCount"],
                        "published_at": comment["publishedAt"],
                        "retrieved_at": datetime.utcnow().isoformat()
                    }
                    producer.send("youtube_raw", record)
                    json_log(COMPONENT, "send_success", comment_id=record["comment_id"], video_id=video_id, cycle=cycle)
                    sent += 1

            except HttpError as e:
                json_log(COMPONENT, "video_skip", video_id=video_id, error=str(e))
                continue

        time.sleep(2)  # prevent rate limit
    return sent


def main():
    json_log(COMPONENT, "start", broker=KAFKA_BROKER)
    producer = get_producer()
    json_log(COMPONENT, "producer_ready")
    youtube = get_youtube_client()
    json_log(COMPONENT, "client_ready")

    cycle = 0
    while True:
        cycle += 1
        cycle_start = time.time()
        json_log(COMPONENT, "cycle_begin", cycle=cycle)
        sent = fetch_comments(youtube, producer, cycle)
        elapsed = round(time.time() - cycle_start, 3)
        throughput = round((sent / elapsed) if elapsed > 0 else 0.0, 3)
        json_log(COMPONENT, "cycle_complete", cycle=cycle, elapsed_sec=elapsed, records_sent=sent, throughput_rps=throughput, sleep_sec=SLEEP_SECONDS)
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
