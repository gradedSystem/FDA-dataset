"""
    AUTHOR: Yedige Ashmet
    LINKEDIN: https://www.linkedin.com/in/yedige2000/
    DESCRIPTION:
        This is the main asset of the Food and Drug Administration (FDA) ETL project.
        The main source of the data comes from the official website of the https://www.fda.gov/
        The data is about the food recalls and the drug recalls.
        The data is in the form of the JSON files.        
"""

# Import libraries
import boto3
import json 
import requests
import pandas as pd
import constants

from datetime import datetime
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dagster import asset, multi_asset, Definitions, AssetIn, AssetKey, AssetOut

@asset(
    description = "Create a Merged DataFrame of Food and Drug Recalls",
)
def create_merged_dataframe() -> pd.DataFrame:
    # Create a Session
    spark = SparkSession.builder \
        .appName('ETL_job_01') \
        .getOrCreate()

    # Configure AWS credentials (replace with your access keys or use Databricks secrets)
    spark.conf.set("fs.s3a.access.key", "***")
    spark.conf.set("fs.s3a.secret.key", "***")
    spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
    # Define the bucket and path for reading and saving data
    bucket = "***"
    s3_base_path = f"s3a://{bucket}/"
    output_path = f"s3a://{bucket}/merged_data/output"  # Define where to save merged data
    json_files_path = f"{s3_base_path}*/"
    
    merged_df = spark.read.option("multiline","true").json(json_files_path)
    merged_df = merged_df \
        .withColumn("event_id", F.col("event_id").cast("int")) \
        .withColumn("center_classification_date", F.to_date(F.col("center_classification_date"), "yyyyMMdd")) \
        .withColumn("recall_initiation_date", F.to_date(F.col("recall_initiation_date"), "yyyyMMdd")) \
        .withColumn("termination_date", F.to_date(F.col("termination_date"), "yyyyMMdd")) \
        .withColumn("report_date", F.to_date(F.col("report_date"), "yyyyMMdd"))

    with open(constants.MERGED_FILE_PATH, 'w') as f:
        merged_df.toPandas().to_csv(f, header=True, index=False)

@asset(
    deps = ['create_merged_dataframe'],
)
def create_recall_summary_by_product(df) -> None:
    df = df.select("product_type", "classification", "recall_number", 
                     "product_description", "reason_for_recall", "recall_initiation_date", 
                     "center_classification_date", "termination_date")
    with open(constants.MERGED_FILE_PATH, 'r') as f:
        df = pd.read_csv(f)

@asset(
    deps = ['create_merged_dataframe'],
)
def create_geographic_distribution(df) -> None:
    df = df.select("recall_number", "state", "country", "city", "distribution_pattern", 
                     "recall_initiation_date")
    with open(constants.MERGED_FILE_PATH, 'r') as f:
            df = pd.read_csv(f)

@asset(
    deps = ['create_merged_dataframe'],
)
def create_recall_timeline_analysis(df) -> None:
    df = df.select("recall_number", "recalling_firm", "recall_initiation_date", 
                     "termination_date", "status", "center_classification_date", "event_id")
    with open(constants.MERGED_FILE_PATH, 'r') as f:
        df = pd.read_csv(f)
@asset(
    deps = ['create_merged_dataframe'],
)
def create_recall_firm_analysis(df) -> None:
    df = df.select("recalling_firm", "product_type", "status", "recall_number", "product_description", 
                     "voluntary_mandated")
    with open(constants.MERGED_FILE_PATH, 'r') as f:
            df = pd.read_csv(f)

@asset(
    deps = ['create_recall_summary_by_product',
            'create_geographic_distribution',
            'create_recall_timeline_analysis',
            'create_recall_firm_analysis'],
)
def process