import os
import sys
import json
import time
import logging
from datetime import datetime, timezone

import requests
from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reddit-producer")

TOPIC = "reddit.drug_mentions"
POLL_INTERVAL_SECONDS = 120  # 2 min between cycles
SUBREDDITS = [
    "AskDocs",
    "pharmacy",
    "drugnerds",
    "Nootropics",
    "ChronicPain",
    "addiction",
    "mentalhealth",
]
SAFETY_TERMS = [
    "side effects",
    "adverse reaction",
    "overdose",
    "withdrawal",
    "drug interaction",
    "stopped taking",
    "started taking",
]
DRUG_KEYWORDS = [
    "Adderall", "Oxycodone", "Xanax", "Metformin", "Lisinopril",
    "Atorvastatin", "Ibuprofen", "Amoxicillin", "Methadone", "Fentanyl",
]
SEARCH_LIMIT = 100

REDDIT_CLIENT_ID = os.environ["REDDIT_CLIENT_ID"]
REDDIT_CLIENT_SECRET = os.environ["REDDIT_CLIENT_SECRET"]
REDDIT_USER_AGENT = os.environ["REDDIT_USER_AGENT"]

BOOTSTRAP_SERVERS = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
API_KEY = os.environ["CONFLUENT_API_KEY"]
API_SECRET = os.environ["CONFLUENT_API_SECRET"]

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    "client.id": "pharmawatch-reddit-producer",
}

producer = Producer(producer_config)

seen_ids = set()

_token_cache = {"access_token": None, "expires_at": 0.0}


def delivery_report(err, msg):
    if err is not None:
        logger.error("Delivery failed for %s: %s", msg.key(), err)


def get_access_token():
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 30:
        return _token_cache["access_token"]

    auth = HTTPBasicAuth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
    data = {"grant_type": "client_credentials"}
    headers = {"User-Agent": REDDIT_USER_AGENT}
    response = requests.post(
        "https://www.reddit.com/api/v1/access_token",
        auth=auth,
        data=data,
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    _token_cache["access_token"] = payload["access_token"]
    _token_cache["expires_at"] = now + float(payload.get("expires_in", 3600))
    return _token_cache["access_token"]


def reddit_get(path, params):
    token = get_access_token()
    headers = {
        "Authorization": "Bearer {}".format(token),
        "User-Agent": REDDIT_USER_AGENT,
    }
    url = "https://oauth.reddit.com{}".format(path)
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def iso_from_epoch(epoch):
    if epoch is None:
        return None
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def extract_drug_mentions(title, body):
    haystack = "{} {}".format(title or "", body or "").lower()
    matches = []
    for drug in DRUG_KEYWORDS:
        if drug.lower() in haystack:
            matches.append(drug)
    if not matches:
        return "unknown"
    return ",".join(matches)


def build_post_record(child):
    data = child.get("data", {}) or {}
    title = data.get("title") or ""
    body = data.get("selftext") or ""
    return {
        "post_id": data.get("id"),
        "subreddit": data.get("subreddit"),
        "title": title,
        "body": body,
        "score": data.get("score"),
        "created_utc": iso_from_epoch(data.get("created_utc")),
        "ingestion_ts": now_iso(),
        "drug_mentions": extract_drug_mentions(title, body),
    }


def build_comment_record(child):
    data = child.get("data", {}) or {}
    body = data.get("body") or ""
    return {
        "post_id": data.get("id"),
        "subreddit": data.get("subreddit"),
        "title": "",
        "body": body,
        "score": data.get("score"),
        "created_utc": iso_from_epoch(data.get("created_utc")),
        "ingestion_ts": now_iso(),
        "drug_mentions": extract_drug_mentions("", body),
    }


def search_posts(subreddit, query):
    path = "/r/{}/search".format(subreddit)
    params = {
        "q": query,
        "restrict_sr": "true",
        "sort": "new",
        "limit": SEARCH_LIMIT,
        "type": "link",
    }
    payload = reddit_get(path, params)
    return payload.get("data", {}).get("children", []) or []


def search_comments(subreddit, query):
    path = "/r/{}/search".format(subreddit)
    params = {
        "q": query,
        "restrict_sr": "true",
        "sort": "new",
        "limit": SEARCH_LIMIT,
        "type": "comment",
    }
    payload = reddit_get(path, params)
    return payload.get("data", {}).get("children", []) or []


def publish(record):
    post_id = record.get("post_id")
    key = (post_id or "").encode("utf-8")
    value = json.dumps(record).encode("utf-8")
    producer.produce(TOPIC, key=key, value=value, callback=delivery_report)


def poll_and_publish():
    count = 0
    for subreddit in SUBREDDITS:
        for term in SAFETY_TERMS:
            query = '"{}"'.format(term)
            posts = search_posts(subreddit, query)
            for child in posts:
                record = build_post_record(child)
                pid = record.get("post_id")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                publish(record)
                count += 1

            comments = search_comments(subreddit, query)
            for child in comments:
                record = build_comment_record(child)
                pid = record.get("post_id")
                if not pid or pid in seen_ids:
                    continue
                seen_ids.add(pid)
                publish(record)
                count += 1

    producer.flush()
    print("published {} records".format(count))
    sys.stdout.flush()


def main():
    while True:
        try:
            poll_and_publish()
        except requests.RequestException as exc:
            logger.error("Reddit API error: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
