import os
import time

import psycopg
from confluent_kafka import Consumer, TopicPartition


TABLES = [
    "suppliers",
    "products",
    "purchase_orders",
    "shipments",
    "inventory",
]


def main():
    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "pharma-catchup-check",
        "enable.auto.commit": False,
    })

    db = None

    try:
        db = psycopg.connect(
            host=os.environ["DBT_HOST"],
            port=int(os.environ["DBT_PORT"]),
            dbname="pharma_warehouse",
            user="warehouse",
            password=os.environ["DBT_ENV_SECRET_WAREHOUSE_PASSWORD"],
            autocommit=True,
            connect_timeout=10,
        )

        targets = {}

        for table in TABLES:
            topic = f"pharma.operational.{table}"
            metadata = consumer.list_topics(topic, timeout=15)
            info = metadata.topics.get(topic)

            if info is None or info.error is not None:
                raise RuntimeError(f"Cannot access topic: {topic}")

            if sorted(info.partitions) != [0]:
                raise RuntimeError(f"Expected one partition for {topic}")

            _, end_offset = consumer.get_watermark_offsets(
                TopicPartition(topic, 0),
                timeout=15,
                cached=False,
            )
            targets[topic] = end_offset

        print(f"Kafka target offsets: {targets}", flush=True)
        deadline = time.monotonic() + 120

        while True:
            behind = []

            for topic, target in targets.items():
                row = db.execute(
                    """
                    SELECT next_offset
                    FROM audit.consumer_checkpoints
                    WHERE topic = %s AND kafka_partition = 0
                    """,
                    (topic,),
                ).fetchone()

                checkpoint = row[0] if row else 0

                if checkpoint < target:
                    behind.append(
                        f"{topic}: checkpoint={checkpoint}, target={target}"
                    )

            if not behind:
                print(
                    "PASS: warehouse reached all captured Kafka offsets",
                    flush=True,
                )
                return

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "CDC catch-up timed out. " + "; ".join(behind)
                )

            print("Waiting: " + "; ".join(behind), flush=True)
            time.sleep(5)

    finally:
        consumer.close()
        if db is not None:
            db.close()


if __name__ == "__main__":
    main()