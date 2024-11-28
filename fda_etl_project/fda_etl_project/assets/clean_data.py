from pyspark.sql import functions as F
def clean_data(df):
    df = df.withColumn("country", F.initcap(F.trim(F.col("country")))) \
           .withColumn("state", F.upper(F.trim(F.col("state"))))
    for date_col in ["center_classification_date", "recall_initiation_date", "termination_date"]:
        df = df.withColumn(date_col, F.to_date(F.col(date_col), "yyyy-MM-dd"))
    df = df.fillna({"product_description": "Unknown", "termination_date": "9999-12-31"})
    return df
