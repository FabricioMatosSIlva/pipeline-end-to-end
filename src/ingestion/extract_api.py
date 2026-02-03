import requests
import json
import logging
from datetime import datetime
from pathlib import Path

# Configure logging

BASE_URL = "https://fakestoreapi.com"
ENDPOINTS = ["products", "users", "carts"]
RAW_PATH = Path("data/raw")

# Create directories

RAW_PATH.mkdir(parents=True, exist_ok=True)
Path("logs").mkdir(exist_ok=True)

# Set up logging

logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Function to fetch and save data

def fetch_data(endpoint: str) -> list:
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def save_raw_data(endpoint: str, data: list):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = RAW_PATH / f"{endpoint}_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logging.info(f"Saved raw data: {file_path}")


def main():
    for endpoint in ENDPOINTS:
        try:
            logging.info(f"Starting ingestion for endpoint: {endpoint}")
            data = fetch_data(endpoint)
            save_raw_data(endpoint, data)
            logging.info(f"Completed ingestion for endpoint: {endpoint}")

        except Exception as e:
            logging.error(f"Error during ingestion for endpoint {endpoint}")


if __name__ == "__main__":
    main()
