import argparse
import json
import os
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv


TABLE_KEYS = {
    "suppliers": "supplier_id",
    "products": "product_id",
    "purchase_orders": "purchase_order_id",
    "shipments": "shipment_id",
    "inventory": "inventory_id",
}

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


def process_message(db, message):
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    table = topic.rsplit(".", 1)[-1]

    key = json.loads(message.key().decode("utf-8"))
    record_id = key["payload"][TABLE_KEYS[table]]

    value = (
        json.loads(message.value().decode("utf-8"))
        if message.value() is not None
        else None
    )

    payload = value["payload"] if value is not None else None
    operation = payload["op"] if payload is not None else None
    source = payload.get("source", {}) if payload is not None else {}

    if payload is not None and operation not in {"r", "c", "u", "d"}:
        raise ValueError(f"Unsupported operation at {topic}:{offset}")

    if operation in {"r", "c", "u"}:
        record_data = payload["after"]
        if record_data is None:
            raise ValueError(f"Missing after value at {topic}:{offset}")
        if record_data[TABLE_KEYS[table]] != record_id:
            raise ValueError(f"Key mismatch at {topic}:{offset}")
    else:
        record_data = None

    with db.transaction():
        db.execute(
            """
            INSERT INTO raw.cdc_events (
                topic, kafka_partition, kafka_offset,
                message_key, message_value, operation,
                source_lsn, source_timestamp_ms
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (topic, kafka_partition, kafka_offset)
            DO NOTHING
            """,
            (
                topic, partition, offset,
                Jsonb(key),
                Jsonb(value) if value is not None else None,
                operation,
                source.get("lsn"),
                source.get("ts_ms"),
            ),
        )

        # Tombstones are retained in raw storage but do not
        # replace the delete marker created by the delete event.
        if payload is not None:
            db.execute(
                """
                INSERT INTO staging.current_records (
                    source_table, record_id, record_data,
                    is_deleted, last_topic, last_partition, last_offset
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_table, record_id)
                DO UPDATE SET
                    record_data = EXCLUDED.record_data,
                    is_deleted = EXCLUDED.is_deleted,
                    last_topic = EXCLUDED.last_topic,
                    last_partition = EXCLUDED.last_partition,
                    last_offset = EXCLUDED.last_offset,
                    updated_at = CURRENT_TIMESTAMP
                WHERE staging.current_records.last_topic =
                          EXCLUDED.last_topic
                  AND staging.current_records.last_partition =
                          EXCLUDED.last_partition
                  AND staging.current_records.last_offset <
                          EXCLUDED.last_offset
                """,
                (
                    table, record_id,
                    Jsonb(record_data) if record_data is not None else None,
                    operation == "d",
                    topic, partition, offset,
                ),
            )

        db.execute(
            """
            INSERT INTO audit.consumer_checkpoints (
                topic, kafka_partition, next_offset
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (topic, kafka_partition)
            DO UPDATE SET
                next_offset = GREATEST(
                    audit.consumer_checkpoints.next_offset,
                    EXCLUDED.next_offset
                ),
                updated_at = CURRENT_TIMESTAMP
            """,
            (topic, partition, offset + 1),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Read retained events from the beginning to test replay safety.",
    )
    args = parser.parse_args()

    password = os.getenv("WAREHOUSE_DB_PASSWORD")
    if not password:
        raise RuntimeError("WAREHOUSE_DB_PASSWORD is missing from .env")

    db = psycopg.connect(
        host="localhost",
        port=5434,
        dbname="pharma_warehouse",
        user="warehouse",
        password=password,
        autocommit=True,
    )

    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:29092",
        "broker.address.family": "v4",
        "group.id": "pharma-warehouse-loader",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "error",
    })

    processed = 0

    try:
        # Only one loader may write this warehouse at a time.
        locked = db.execute(
            "SELECT pg_try_advisory_lock(731001)"
        ).fetchone()[0]
        if not locked:
            raise RuntimeError("Another warehouse loader is already running")

        assignments = []

        for table in TABLE_KEYS:
            topic = f"pharma.operational.{table}"
            metadata = consumer.list_topics(topic, timeout=15)
            topic_metadata = metadata.topics.get(topic)

            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Cannot access topic: {topic}")

            partitions = sorted(topic_metadata.partitions)
            if partitions != [0]:
                raise RuntimeError(
                    f"{topic}: expected partition 0 only; "
                    "review ordering before changing partition counts"
                )

            saved = db.execute(
                """
                SELECT next_offset
                FROM audit.consumer_checkpoints
                WHERE topic = %s AND kafka_partition = 0
                """,
                (topic,),
            ).fetchone()

            start_offset = (
                0 if args.replay or saved is None else saved[0]
            )

            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, 0),
                timeout=15,
                cached=False,
            )

            if not low <= start_offset <= high:
                raise RuntimeError(
                    f"{topic}: checkpoint {start_offset} outside "
                    f"retained range [{low}, {high}]; recovery required"
                )

            assignments.append(TopicPartition(topic, 0, start_offset))
            print(f"{table}: starting at offset {start_offset}", flush=True)

        consumer.assign(assignments)
        print("Consumer running. Press Ctrl+C to stop.", flush=True)

        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))

            process_message(db, message)
            processed += 1

            if processed % 1000 == 0:
                print(
                    f"Processed {processed:,} messages this run",
                    flush=True,
                )

    except KeyboardInterrupt:
        print(f"\nStopped after {processed:,} messages.", flush=True)
    finally:
        consumer.close()
        db.close()


if __name__ == "__main__":
    main()