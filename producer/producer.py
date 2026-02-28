"""
Weather Kafka Producer
Fetches real-time weather data from OpenWeatherMap API
and publishes it to a Kafka topic every 60 seconds.
"""

import json
import time
import os
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
API_KEY = os.getenv("OWM_API_KEY", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "weather-data"
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", "60"))  # seconds

OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

# 10 European cities for rich streaming data
CITIES = [
    {"name": "Paris",     "country": "FR"},
    {"name": "London",    "country": "GB"},
    {"name": "Berlin",    "country": "DE"},
    {"name": "Madrid",    "country": "ES"},
    {"name": "Rome",      "country": "IT"},
    {"name": "Amsterdam", "country": "NL"},
    {"name": "Brussels",  "country": "BE"},
    {"name": "Vienna",    "country": "AT"},
    {"name": "Zurich",    "country": "CH"},
    {"name": "Stockholm", "country": "SE"},
]


# ─── Kafka ────────────────────────────────────────────────────────────────────
def create_producer(retries: int = 15, delay: int = 5) -> KafkaProducer:
    """Create a Kafka producer with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                linger_ms=100,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as exc:
            logger.warning(f"Kafka connection attempt {attempt}/{retries} failed: {exc}")
            if attempt < retries:
                time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")


# ─── OpenWeatherMap ───────────────────────────────────────────────────────────
def fetch_weather(city: dict) -> dict | None:
    """Fetch current weather for a city from OpenWeatherMap API."""
    params = {
        "q": f"{city['name']},{city['country']}",
        "appid": API_KEY,
        "units": "metric",
    }
    try:
        resp = requests.get(OWM_URL, params=params, timeout=10)
        resp.raise_for_status()
        raw = resp.json()

        return {
            # Identity
            "city":        city["name"],
            "country":     city["country"],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            # Temperature
            "temperature": raw["main"]["temp"],
            "feels_like":  raw["main"]["feels_like"],
            "temp_min":    raw["main"]["temp_min"],
            "temp_max":    raw["main"]["temp_max"],
            # Atmosphere
            "humidity":    raw["main"]["humidity"],
            "pressure":    raw["main"]["pressure"],
            # Wind
            "wind_speed":  raw["wind"]["speed"],
            "wind_deg":    raw["wind"].get("deg", 0),
            "wind_gust":   raw["wind"].get("gust", 0.0),
            # Sky
            "clouds":      raw["clouds"]["all"],
            "visibility":  raw.get("visibility", 10000),
            # Condition
            "weather_id":          raw["weather"][0]["id"],
            "weather_main":        raw["weather"][0]["main"],
            "weather_description": raw["weather"][0]["description"],
        }
    except requests.exceptions.HTTPError as exc:
        logger.error(f"HTTP error for {city['name']}: {exc}")
    except requests.exceptions.RequestException as exc:
        logger.error(f"Request failed for {city['name']}: {exc}")
    return None


# ─── Main loop ────────────────────────────────────────────────────────────────
def run():
    if not API_KEY:
        raise ValueError("OWM_API_KEY environment variable is not set. Copy .env.example to .env and add your key.")

    producer = create_producer()
    logger.info(f"Producer ready — polling {len(CITIES)} cities every {FETCH_INTERVAL}s")

    while True:
        batch_start = time.time()
        sent = 0

        for city in CITIES:
            record = fetch_weather(city)
            if record:
                try:
                    future = producer.send(KAFKA_TOPIC, key=city["name"], value=record)
                    future.get(timeout=10)
                    sent += 1
                    logger.info(
                        f"  ✓ {city['name']:12s} | {record['temperature']:6.1f}°C "
                        f"| {record['weather_main']:15s} | humidity={record['humidity']}%"
                    )
                except KafkaError as exc:
                    logger.error(f"  ✗ Failed to send {city['name']} to Kafka: {exc}")

            time.sleep(0.5)  # small delay between API calls

        producer.flush()
        elapsed = time.time() - batch_start
        logger.info(f"Batch done: {sent}/{len(CITIES)} records sent in {elapsed:.1f}s")

        sleep_for = max(0.0, FETCH_INTERVAL - elapsed)
        logger.info(f"Next batch in {sleep_for:.0f}s …\n")
        time.sleep(sleep_for)


if __name__ == "__main__":
    run()
