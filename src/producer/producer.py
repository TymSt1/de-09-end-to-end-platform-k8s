"""
NYC Taxi Ride Event Producer
Generates synthetic taxi ride events and publishes them to Kafka.
"""
import json
import random
import time
import os
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configuration from environment
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "platform-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "taxi-rides")
EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "10"))

# NYC Taxi zones (simplified — 20 popular zones)
ZONES = [
    {"id": 1, "name": "Newark Airport", "borough": "EWR"},
    {"id": 43, "name": "Central Park", "borough": "Manhattan"},
    {"id": 48, "name": "Clinton East", "borough": "Manhattan"},
    {"id": 79, "name": "East Village", "borough": "Manhattan"},
    {"id": 100, "name": "Garment District", "borough": "Manhattan"},
    {"id": 107, "name": "Greenwich Village North", "borough": "Manhattan"},
    {"id": 113, "name": "Greenwich Village South", "borough": "Manhattan"},
    {"id": 114, "name": "Astoria", "borough": "Queens"},
    {"id": 132, "name": "JFK Airport", "borough": "Queens"},
    {"id": 138, "name": "LaGuardia Airport", "borough": "Queens"},
    {"id": 148, "name": "Lower East Side", "borough": "Manhattan"},
    {"id": 161, "name": "Midtown Center", "borough": "Manhattan"},
    {"id": 162, "name": "Midtown East", "borough": "Manhattan"},
    {"id": 163, "name": "Midtown North", "borough": "Manhattan"},
    {"id": 170, "name": "Murray Hill", "borough": "Manhattan"},
    {"id": 186, "name": "Penn Station", "borough": "Manhattan"},
    {"id": 230, "name": "Times Square", "borough": "Manhattan"},
    {"id": 234, "name": "Union Square", "borough": "Manhattan"},
    {"id": 236, "name": "Upper East Side North", "borough": "Manhattan"},
    {"id": 263, "name": "Williamsburg", "borough": "Brooklyn"},
]

PAYMENT_TYPES = ["credit_card", "cash", "no_charge", "dispute"]
PAYMENT_WEIGHTS = [0.7, 0.25, 0.03, 0.02]
RATE_CODES = ["standard", "jfk", "newark", "nassau", "negotiated", "group"]


def generate_ride_event():
    """Generate a single synthetic taxi ride event."""
    pickup_zone = random.choice(ZONES)
    dropoff_zone = random.choice(ZONES)

    # Ride duration: 5-60 minutes
    duration_minutes = random.uniform(5, 60)

    # Distance correlates loosely with duration
    distance_miles = max(0.5, duration_minutes * random.uniform(0.2, 0.6))

    # Base fare calculation
    base_fare = 3.50 + (distance_miles * 2.50) + (duration_minutes * 0.50)
    tip = base_fare * random.uniform(0, 0.3) if random.random() > 0.2 else 0
    tolls = random.choice([0, 0, 0, 0, 6.55, 17.50])  # Most rides no toll
    total = round(base_fare + tip + tolls + 1.0, 2)  # +1.0 surcharge

    # Timestamp: current time with slight random offset
    event_time = datetime.utcnow() - timedelta(seconds=random.randint(0, 300))

    return {
        "ride_id": f"ride_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "event_timestamp": event_time.isoformat() + "Z",
        "pickup_zone_id": pickup_zone["id"],
        "pickup_zone_name": pickup_zone["name"],
        "pickup_borough": pickup_zone["borough"],
        "dropoff_zone_id": dropoff_zone["id"],
        "dropoff_zone_name": dropoff_zone["name"],
        "dropoff_borough": dropoff_zone["borough"],
        "passenger_count": random.choices([1, 2, 3, 4, 5, 6], weights=[60, 20, 8, 5, 4, 3])[0],
        "trip_distance_miles": round(distance_miles, 2),
        "duration_minutes": round(duration_minutes, 2),
        "payment_type": random.choices(PAYMENT_TYPES, weights=PAYMENT_WEIGHTS)[0],
        "rate_code": random.choices(RATE_CODES, weights=[85, 5, 3, 2, 3, 2])[0],
        "fare_amount": round(base_fare, 2),
        "tip_amount": round(tip, 2),
        "tolls_amount": tolls,
        "total_amount": total,
    }


def main():
    """Main producer loop."""
    logger.info(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
    logger.info(f"Publishing to topic: {TOPIC}")
    logger.info(f"Rate: {EVENTS_PER_SECOND} events/second")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    event_count = 0
    try:
        while True:
            event = generate_ride_event()
            producer.send(
                TOPIC,
                key=str(event["pickup_zone_id"]),
                value=event,
            )
            event_count += 1

            if event_count % 100 == 0:
                logger.info(f"Published {event_count} events")
                producer.flush()

            time.sleep(1.0 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        logger.info(f"Shutting down after {event_count} events")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()