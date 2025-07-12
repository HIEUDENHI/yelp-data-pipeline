import argparse
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

def flatten_schema(schema: T.StructType, prefix: str = "") -> list:
    """Recursively flatten a StructType into a list of field paths."""
    fields = []
    for field in schema.fields:
        name = f"{prefix}{field.name}"
        if isinstance(field.dataType, T.StructType):
            fields.extend(flatten_schema(field.dataType, f"{name}."))
        else:
            fields.append(name)
    return fields

def clean_json_columns(df: DataFrame, columns: list) -> DataFrame:
    """
    Standardize JSON-like string columns:
      - Remove leading/trailing quotes or u'
      - Normalize True/False -> true/false
      - Convert "none" -> null
    Drops the original struct/string column.
    """
    for col_name in columns:
        new_col = col_name.replace(".", "_").lower()
        df = df.withColumn(new_col, F.regexp_replace(F.col(col_name), r"^u?'|'?$", ""))
        df = df.withColumn(new_col, F.regexp_replace(F.col(new_col), "True", "true"))
        df = df.withColumn(new_col, F.regexp_replace(F.col(new_col), "False", "false"))
        df = df.withColumn(
            new_col,
            F.when(F.lower(F.col(new_col)) == "none", None).otherwise(F.col(new_col))
        )
    return df.drop(*columns)

def get_json_string_columns(df: DataFrame, columns: list) -> list:
    """Return only those columns whose values look like JSON strings."""
    json_cols = []
    for c in columns:
        sample = df.filter(F.col(c).startswith("{") & F.col(c).endswith("}")).limit(1).collect()
        if sample:
            json_cols.append(c)
    return json_cols

def convert_string_columns_to_json(df: DataFrame, json_columns: list) -> DataFrame:
    """Parse JSON strings into StructType columns."""
    for c in json_columns:
        sample = df.filter(F.col(c).isNotNull() & F.col(c).startswith("{")).limit(1).collect()
        if sample:
            inferred = F.schema_of_json(F.lit(sample[0][c]))
            df = df.withColumn(c, F.from_json(F.col(c), inferred))
        else:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df

def clean_hour_columns(df: DataFrame, columns: list) -> DataFrame:
    """Split "hours.Day" columns like "09:00-17:00" into opening & closing times."""
    for col_name in columns:
        day = col_name.split(".")[-1].lower()
        df = df.withColumn(f"{day}_opening_time", F.split(F.col(col_name), "-").getItem(0))
        df = df.withColumn(f"{day}_closing_time", F.split(F.col(col_name), "-").getItem(1))
    return df.drop(*columns)

def process_business(df: DataFrame) -> DataFrame:
    """Clean and flatten the Yelp business table."""
    df = df.withColumn("is_open", F.col("is_open").cast("boolean"))
    hours_cols = [p for p in flatten_schema(df.schema) if p.startswith("hours.")]
    df = clean_hour_columns(df, hours_cols)
    attr_cols = [p for p in flatten_schema(df.schema) if p.startswith("attributes.")]
    df = clean_json_columns(df, attr_cols)
    new_attrs = [c for c in df.columns if c.startswith("attributes_")]
    json_attrs = get_json_string_columns(df, new_attrs)
    df = convert_string_columns_to_json(df, json_attrs)
    nested = [p for p in flatten_schema(df.select(json_attrs).schema) if p.startswith("attributes.")]
    df = clean_json_columns(df, nested)
    return df

def process_checkins(df: DataFrame) -> DataFrame:
    """Explode check-in dates into individual timestamp rows."""
    df = df.withColumn("date_array", F.split(F.col("date"), ","))
    df = df.select("business_id", F.explode("date_array").alias("date"))
    return (
        df.withColumn("date",    F.to_timestamp("date"))
          .withColumn("year",    F.year("date"))
          .withColumn("month",   F.month("date"))
    )
def process_users(df: DataFrame) -> DataFrame:
    """Clean and derive fields for users."""
    df = df.withColumn(
        "elite_years",
        F.when(F.col("elite") == "", None).otherwise(F.split("elite", ","))
    ).drop("elite")
    df = df.withColumn(
        "friends_list",
        F.when(F.col("friends") == "", None).otherwise(F.split("friends", ","))
    ).drop("friends")
    df = df.withColumn("yelping_since", F.to_timestamp("yelping_since"))
    df = df.withColumn(
        "friends_count",
        F.when(F.col("friends_list").isNull(), 0).otherwise(F.size("friends_list"))
    )
    df = df.withColumn(
        "elite_years_count",
        F.when(F.col("elite_years").isNull(), 0).otherwise(F.size("elite_years"))
    )
    return df

def process_reviews_and_tips(df: DataFrame) -> DataFrame:
    """Convert date to timestamp and add year/month for partitioning."""
    df = df.withColumn("date", F.to_timestamp("date"))
    return df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze → Silver monthly (MinIO)"
    )
    parser.add_argument("--bronze-base", required=True,
                        help="Ex: s3a://yelp-data/bronze")
    parser.add_argument("--silver-base", required=True,
                        help="Ex: s3a://yelp-data/silver")
    parser.add_argument("--year",       required=True,
                        help="Ex: 2025")
    parser.add_argument("--month",      required=True,
                        help="Ex: 07")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
            .appName("BronzeToSilverYelpOnMinIO")
            .config("spark.driver.userClassPathFirst",   "true")
            .config("spark.executor.userClassPathFirst", "true")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
    )

    bronze_base = args.bronze_base.rstrip("/")
    silver_base = args.silver_base.rstrip("/")
    year  = args.year
    month = args.month

    df_business = spark.read.json(f"{bronze_base}/business/yelp_academic_dataset_business.json")
    df_checkin  = spark.read.json(f"{bronze_base}/checkin")
    df_review   = spark.read.json(
        f"{bronze_base}/review/year={year}/month={month}/*.json"
    )
    df_tip      = spark.read.json(
        f"{bronze_base}/tip/year={year}/month={month}/*.json"
    )
    df_user     = spark.read.json(
        f"{bronze_base}/user/year={year}/month={month}/*.json"
    )

    # --- PROCESS TO SILVER ---
    business_clean = process_business(df_business)
    checkins_clean = process_checkins(df_checkin)
    reviews_clean  = process_reviews_and_tips(df_review)
    tips_clean     = process_reviews_and_tips(df_tip)
    users_clean    = process_users(df_user)


    
    # --- WRITE SILVER DELTA TO MINIO ---
    business_clean.write \
        .mode("overwrite") \
        .format("delta") \
        .save(f"{silver_base}/business")

    checkins_clean.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("year", "month") \
        .format("delta") \
        .save(f"{silver_base}/checkins")

    reviews_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("delta") \
        .save(f"{silver_base}/reviews")

    tips_clean.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("delta") \
        .save(f"{silver_base}/tips")

    users_clean.write \
        .mode("overwrite") \
        .format("delta") \
        .save(f"{silver_base}/users")

    spark.stop()
