import os
import sys
import json
import logging
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reddit-consumer")

TOPIC = "reddit.drug_mentions"
S3_BUCKET = "pharmawatch-data-lake"
S3_PREFIX = "raw/reddit/"
BATCH_SIZE = 100  # flush every N messages
POLL_TIMEOUT = 1.0
BATCH_WAIT_SECONDS = 10

BOOTSTRAP_SERVERS = os.environ["CONFLUENT_BOOTSTRAP_SERVERS"]
API_KEY = os.environ["CONFLUENT_API_KEY"]
API_SECRET = os.environ["CONFLUENT_API_SECRET"]

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_REGION"]

consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
    "group.id": "pharmawatch-reddit-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC])

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def validate(record):
    if not record.get("post_id"):
        return False, "post_id is null"
    body = record.get("body")
    if body is None or body == "":
        return False, "body is null or empty"
    if not record.get("created_utc"):
        return False, "created_utc is null"
    return True, None


def write_batch_to_s3(records):
    if not records:
        return 0
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = "{}reddit_{}.json".format(S3_PREFIX, ts)
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
    return len(records)


def process_batch(messages):
    valid_records = []
    for msg in messages:
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            print("INVALID: could not decode message - {}".format(exc))
            sys.stdout.flush()
            continue

        ok, reason = validate(record)
        if not ok:
            print("INVALID: {} - record={}".format(reason, record))
            sys.stdout.flush()
            continue
        valid_records.append(record)

    written = write_batch_to_s3(valid_records)
    consumer.commit(asynchronous=False)
    print("wrote {} to s3".format(written))
    sys.stdout.flush()


def main():
    buffer = []
    last_flush = datetime.now(timezone.utc)
    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                elapsed = (datetime.now(timezone.utc) - last_flush).total_seconds()
                if buffer and elapsed >= BATCH_WAIT_SECONDS:
                    process_batch(buffer)
                    buffer = []
                    last_flush = datetime.now(timezone.utc)
                continue
            if msg.error():
                raise KafkaException(msg.error())

            buffer.append(msg)
            if len(buffer) >= BATCH_SIZE:
                process_batch(buffer)
                buffer = []
                last_flush = datetime.now(timezone.utc)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        if buffer:
            try:
                process_batch(buffer)
            except Exception as exc:
                logger.error("Failed final flush: %s", exc)
        consumer.close()


if __name__ == "__main__":
    main()
