import requests
from datetime import datetime

def main():
    extract_api_data("fenster/subscriptions", "csv")
    extract_api_data("apfel/subscriptions", "json")
    extract_api_data("exchange-rates", "csv")

def extract_api_data(path:str, file_format:str) -> dict:
    api_endpoint = f"http://localhost:5050/{path}"
    file_name = f"{path.replace("/", "_")}_{datetime.today().strftime("%Y_%m_%d")}.{file_format}"
    target_location = f"./s3/bronze/{file_name}"
    try:
        with requests.get(f"{api_endpoint}", stream=True) as r:
            r.raise_for_status()
            with open(target_location, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception as e:
        raise Exception(f"Failed retrieving {api_endpoint} data: {e}")


if __name__=='__main__':
    main()