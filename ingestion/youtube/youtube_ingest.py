import os
import json
import time
import googleapiclient.discovery
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
from logging_utils import json_log
from kafka.errors import NoBrokersAvailable

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

def get_producer(max_retries=12, wait_s=5):
    for attempt in range(1, max_retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            print(f"producer ready -> {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt}/{max_retries}), sleeping {wait_s}s")
            time.sleep(wait_s)
    raise RuntimeError("Kafka not available after retries")

# YouTube API client
def get_youtube_client():
    from googleapiclient.discovery import build
    json_log(COMPONENT, "api_client_create")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)

from googleapiclient.errors import HttpError

def fetch_comments(youtube, producer, cycle):
    query = "climate change"

    try:
        search_response = (
            youtube.search()
            .list(
                q=query,
                part="id",
                type="video",
                maxResults=5,
            )
            .execute()
        )

    except HttpError as e:
        error_content = str(e)

        # --- YOUTUBE QUOTA EXCEEDED ---
        if "quotaExceeded" in error_content:
            print("❌ YouTube API quota exceeded — waiting 1 hour before retrying...")
            time.sleep(3600)   # 1 hour sleep
            return []          # Skip this ingestion cycle safely

        # --- 403 but not quota related ---
        if e.resp.status == 403:
            print(f"❌ Forbidden (403). Response: {error_content}")
            time.sleep(120)  # wait 2 minutes
            return []

        # --- NETWORK OR MISC ERRORS ---
        print(f"❌ HttpError during YouTube search: {error_content}")
        time.sleep(30)
        return []

    except Exception as e:
        print(f"❌ Unexpected error while searching: {e}")
        time.sleep(10)
        return []

    # If we reach here → Search worked, proceed to handle videos
    video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]
    all_comments = []

    for vid in video_ids:
        try:
            comments_req = (
                youtube.commentThreads()
                .list(
                    part="snippet",
                    videoId=vid,
                    maxResults=20,
                    textFormat="plainText",
                )
            )
            comments_resp = comments_req.execute()

        except HttpError as e:
            err = str(e)

            if "quotaExceeded" in err:
                print("❌ Quota exceeded while fetching comments — skipping video.")
                continue

            print(f"❌ HttpError on video {vid}: {err}")
            continue

        except Exception as e:
            print(f"❌ Unexpected error for video {vid}: {e}")
            continue

        # Process comments
        for item in comments_resp.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]
            payload = {
                "video_id": vid,
                "author": comment.get("authorDisplayName"),
                "text": comment.get("textDisplay"),
                "published_at": comment.get("publishedAt"),
                "cycle": cycle,
            }

            producer.send("youtube_raw", payload)
            all_comments.append(payload)

    return all_comments



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
        comments = fetch_comments(youtube, producer, cycle)
        sent = len(comments)
        elapsed = round(time.time() - cycle_start, 3)
        throughput = round((sent / elapsed) if elapsed > 0 else 0.0, 3)
        json_log(COMPONENT, "cycle_complete", cycle=cycle, elapsed_sec=elapsed, records_sent=sent, throughput_rps=throughput, sleep_sec=SLEEP_SECONDS)
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
