import os
import json
import time
import boto3
import requests

from datetime import datetime, timedelta

FDA_API_KEY = os.getenv("FDA_API_KEY")
bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
folder_name = os.getenv("AWS_S3_FOLDER_NAME")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

LIMIT = 100
REQUEST_DELAY = 60 / 200
current_year = datetime.now().year
current_month = datetime.now().month
BASE_URL = "https://api.fda.gov/food/enforcement.json"

def save_data(data, filename):
    with open(filename, "a") as f:
        json.dump(data, f)
        f.write("\n")

def fetch_data(start_date, end_date, skip=1):
    params = {
        "api_key": FDA_API_KEY,
        "search": f"report_date:[{start_date} TO {end_date}]",
        "limit": LIMIT,
        "skip": skip
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data if "results" in data else None
    except requests.exceptions.HTTPError as err:
        print(f"Request failed with status code: {response.status_code} - {err}")
    except Exception as e:
        print(f"An error occurred: {e}")

def upload_df_to_s3_with_date_check(bucket_name, folder_name, base_filename, aws_access_key, aws_secret_key, region_name="ap-southeast-2"):
    s3_client = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)
    s3_key = f"{folder_name}/{base_filename}"
    new_file_size = os.path.getsize(base_filename)
    existing_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name).get('Contents', [])
    existing_file_size = next((file['Size'] for file in existing_files if base_filename in file['Key']), None)

    if existing_file_size is None or new_file_size > existing_file_size:
        try:
            s3_client.upload_file(Filename=base_filename, Bucket=bucket_name, Key=s3_key)
            print("File uploaded successfully.")
        except Exception as e:
            print(f"Error uploading file: {e}")
    else:
        print("Existing file is larger or equal in size; no upload performed.")

def fetch_data_from_api():
    current_date = datetime.now()
    total_fetched = 0

    for month in range(current_month - 1, current_month):
        start_date = f"{current_year}{str(month).zfill(2)}01"
        end_date = (current_date if current_year == current_date.year and month == current_date.month 
                    else (datetime(current_year, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)).strftime("%Y%m%d")
        
        skip, monthly_fetched, filename = 1, 0, f"fda_data_{current_year}_{str(month).zfill(2)}.json"
        
        while True:
            data = fetch_data(start_date, end_date, skip)
            if not data or not data.get("results"):
                print(f"No data returned or error in fetching for {start_date} to {end_date}.")
                break

            save_data(data["results"], filename)
            num_records = len(data["results"])
            monthly_fetched += num_records
            total_fetched += num_records

            if num_records < LIMIT:
                break
            skip += num_records
            print(f"Fetched {num_records} records for {start_date} to {end_date} (Monthly total: {monthly_fetched}).")
            time.sleep(REQUEST_DELAY)
        
        print(f"Data fetching complete for {start_date} to {end_date}. Monthly total records fetched: {monthly_fetched}")

if __name__ == '__main__':
    fetch_data_from_api()
    upload_df_to_s3_with_date_check(bucket_name, folder_name, f"fda_data_{current_year}_{str(current_month).zfill(2)}.json", aws_access_key, aws_secret_key)
