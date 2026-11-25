
import argparse
import json
import time

from kafka import KafkaProducer
from traffic_accident_generator.event_builder_example import iter_events


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--folder", default="data/sample_7000")
    parser.add_argument("--delay", type=float, default=3)
    parser.add_argument("--count", type=int, default=5)

    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda event: json.dumps(
            event, ensure_ascii=False, default=str
        ).encode("utf-8"),
        acks="all",
        retries=5
    )

    print("Connected to Kafka successfully!")
    print("Topic: raw-accidents")
    print(f"Delay: {args.delay} seconds")

    sent = 0

    try:
        for event in iter_events(args.folder):
            producer.send("raw-accidents", value=event).get(timeout=30)

            sent += 1

            accident_id = event["accident"]["accident_id"]

            print(f"[{sent}] Sent accident: {accident_id}")

            if sent >= args.count:
                break

            time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")

    finally:
        producer.flush()
        producer.close()
        print(f"Total accidents sent: {sent}")


if __name__ == "__main__":
    main()
