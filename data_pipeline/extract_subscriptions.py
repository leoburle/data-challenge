import requests
import logging
from datetime import datetime as datetime
from pathlib import Path


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.info("Extracting data from sources...")
    extract_api_data("fenster/subscriptions", "csv")
    extract_api_data("apfel/subscriptions", "json")
    extract_api_data("exchange-rates", "csv")
    logging.info("Extraction from fenster, apfel, and exchange-rates completed.")


def extract_api_data(path: str, file_format: str) -> dict:
    """Extract data from the API and save it to the local file system."""
    api_endpoint = f"http://localhost:5050/{path}"
    print(f"Extracting data from {api_endpoint}")
    file_name = f"{path.replace('/', '_').replace('-', '_')}_{datetime.today().strftime('%Y_%m_%d')}.{file_format}"
    target_location = f"./output_data/s3/bronze/{path}/{file_name}"
    try:
        with requests.get(f"{api_endpoint}", stream=True) as r:
            r.raise_for_status()
            Path(target_location).parent.mkdir(parents=True, exist_ok=True)
            with open(target_location, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        raise Exception(f"Failed retrieving {api_endpoint} data: {e}")


if __name__ == "__main__":
    main()
