from pyspark.sql import functions as F

def enrich_data(df):
    df = df.withColumn("recall_duration", F.datediff(F.coalesce("termination_date", F.lit("9999-12-31")), "recall_initiation_date")) \
           .withColumn("is_high_risk", F.when(F.col("classification") == "Class I", True).otherwise(False))
    geo_df = df.withColumn("distribution_location", F.explode(F.split(F.col("distribution_pattern"), ","))) \
               .withColumn("distribution_location", F.trim(F.col("distribution_location")))
    return df, geo_df