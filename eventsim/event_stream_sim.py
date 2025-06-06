# This script simulates a stream of events and sends them to a Kafka topic.
# It generates random events and sends them at random intervals.

import json
import time
import random
from datetime import datetime
import os

from kafka import KafkaProducer

KAFKA_BROKER = "kafka:9093"
KAFKA_TOPIC = "event_stream"

EVENT_INTERVAL = os.getenv("EVENT_INTERVAL", 1)  # seconds
STREAM_DURATION = os.getenv("STREAM_DURATION", 600)  # seconds

event_types = ["play", "pause", "skip"]
tracks = [
    {"id": "track_101", "title": "Song of the Wind", "artist": "Skyline"},
    {"id": "track_102", "title": "Electric Heartbeat", "artist": "Synthmaster"},
    {"id": "track_103", "title": "Lo-Fi Vibes", "artist": "CalmCode"},
    {"id": "track_104", "title": "Morning Coffee", "artist": "JazzLoops"},
]


# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_stream_event():
    """Generate a random event for the stream."""
    event = {
        "event_type": random.choice(event_types),
        "track": random.choice(tracks),
        "user_id": "user_" + str(random.randint(1, 100)),
        "timestamp": datetime.now().isoformat(),
    }
    return event


def send_event(event):
    """Send an event to the Kafka topic."""
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()


def main():
    """Main function to simulate event stream."""
    try:
        print("Starting event stream simulation.")
        start_time = time.time()
        while time.time() - start_time < STREAM_DURATION:
            event = generate_stream_event()
            send_event(event)
            time.sleep(
                EVENT_INTERVAL if EVENT_INTERVAL else random.uniform(0.5, 2.0)
            )  # Random interval if not set
    except KeyboardInterrupt:
        print("Stopping event stream simulation.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
