import os
import boto3
import requests
import pandas as pd
 
from re import sub
from io import StringIO
from bs4 import BeautifulSoup
from datetime import datetime

website_url = 'https://financesone.worldbank.org/ibrd-subscriptions-and-voting-power-of-member-countries/DS00051'
source_url = 'https://financesonefiles.worldbank.org/f-one/DS00051/RS00053/IBRD_Subscriptions_and_Voting_Power_of_Member_Countries.json'

bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
folder_name = os.getenv("AWS_S3_FOLDER_NAME")
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

# Function to upload DataFrame to S3
def upload_df_to_s3_with_date_check(df, bucket_name, folder_name, base_filename, latest_date, aws_access_key, aws_secret_key, region_name="ap-southeast-2"):
    # Initialize the S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region_name
    )

    # Define the S3 key (folder path + filename with date)
    filename = f"{base_filename}_{latest_date}.csv"
    s3_key = f"{folder_name}/{filename}"

    # List all files in the target folder to check for an existing file with the same date
    existing_files = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name).get('Contents', [])
    
    # Check if a file with the same name (and date) already exists
    file_exists = any(file['Key'] == s3_key for file in existing_files)
    
    # If file with same date exists, do not upload; otherwise, delete old files and upload new one
    if file_exists:
        print(f"File with date {latest_date} already exists. Skipping upload.")
    else:
        # Delete older versions of the file with the same base filename
        files_to_replace = [file['Key'] for file in existing_files if base_filename in file['Key']]
        if files_to_replace:
            print("Replacing older files:", files_to_replace)
            delete_objects = [{"Key": key} for key in files_to_replace]
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_objects})
        
        # Convert DataFrame to CSV in-memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the new CSV to S3
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"File uploaded to s3://{bucket_name}/{s3_key}")

def get_latest_updated_date():
    response = requests.get(website_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the specific section with the class 'listActive inactive'
    section = soup.find('section', class_='listActive inactive')

    if section:
        # Within this section, find all divs with class 'details_listData__IjKgc'
        details_divs = section.find_all('div', class_='details_listData__IjKgc')
        
        # Loop through each 'details_listData__IjKgc' div to find the 'Last Updated' label
        for div in details_divs:
            label = div.find('label')
            if label and label.text == 'Last Updated':
                # Get the date from the sibling div next to the label
                updated_date_div = label.find_next_sibling('div')
                if updated_date_div:
                    updated_date = updated_date_div.text.strip()
                    print("Last Updated Date:", updated_date)
                    formatted_date = datetime.strptime(updated_date, "%b %d, %Y")
                    date_formatted_str = formatted_date.strftime("%Y-%m-%d")
                    return date_formatted_str

    print("No update information found.")
    return None

def title_case_with_exceptions(name):
    words = name.lower().split()
    exceptions = {'and', 'of', 'the', 'in', 'for', 'on', 'at', 'by', 'with'}
    return ' '.join([word.capitalize() if word not in exceptions else word for word in words])

def get_data():
    response = requests.get(source_url)
    data = response.json()
    df = pd.DataFrame(data)
    return df

def split_idbr_financial_contribution(df):
    df['member'] = df['member'].apply(title_case_with_exceptions)
    df['as_of_date'] = pd.to_datetime(df['as_of_date'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

    # Dataset 1: Membership and Financial Contributions
    dataset1 = df[["member", "subscription_amount", "percentage_of_total_subscriptions", "total_amounts", "as_of_date"]]

    # Dataset 2: Voting Power Information
    dataset2 = df[["member", "number_of_votes", "percentage_of_total_voting_power", "as_of_date"]]

    return dataset1, dataset2

if __name__ == '__main__':
    df = get_data()

    date = get_latest_updated_date()

    df1, df2 =  split_idbr_financial_contribution(df)

    filename1, filename2 = "idbr_financial_contribution", "idbr_voting_power"
    
    for df, filename in zip([df1, df2], [filename1, filename2]):
        upload_df_to_s3_with_date_check(df, bucket_name, folder_name, filename, date, aws_access_key, aws_secret_key)
        