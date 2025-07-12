import argparse
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
from typing import Tuple
from delta.tables import DeltaTable


def create_spark(app_name: str = "SilverToGoldYelpOnMinIO") -> SparkSession:
    """Initialize a SparkSession configured for S3A / Delta."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
spark = create_spark()

def build_dim_time(reviews: DataFrame, checkins: DataFrame) -> DataFrame:
    """Build a dim_time table from all timestamps in reviews & checkins."""
    dates = (
        reviews.select(F.to_date("date").alias("date"))
        .union(checkins.select(F.to_date("date").alias("date")))
        .distinct()
    )
    w = Window.orderBy("date")
    return (
        dates
        .withColumn("date_id", F.row_number().over(w))
        .withColumn("year",      F.year("date"))
        .withColumn("month",     F.month("date"))
        .withColumn("day",       F.dayofmonth("date"))
        .withColumn("weekday",   F.date_format("date", "E"))
        .withColumn("quarter",   F.quarter("date"))
        .withColumn("is_weekend", F.col("weekday").isin("Sat", "Sun"))
    )

def build_dim_business(business: DataFrame) -> DataFrame:
    """Select only the necessary fields for dim_business."""
    return (
        business.select(
            "business_id",
            "name",
            "city",
            "state",
            "postal_code",
            "latitude",
            "longitude",
            "is_open",
            "review_count",
            F.col("stars").alias("stars_avg")
        )
    )

def build_bridge_business_category(business: DataFrame) -> DataFrame:
    """
    Return a bridge_business_category table with three columns:
      - business_id
      - category_id   (sequential ID assigned based on category_name)
      - category_name (trimmed)
    Ensures distinct pairs of (business_id, category_id).
    """
    # explode và trim category
    exploded = (
        business
        .select("business_id", "categories")
        .withColumn("category_name",
            F.explode(F.split(F.col("categories"), ",\\s*"))
        )
        .withColumn("category_name", F.trim(F.col("category_name")))
        .filter(F.col("category_name") != "")
    )
    distinct_cats = (
        exploded
        .select("category_name")
        .distinct()
        .withColumn("category_id",
                    F.row_number().over(Window.orderBy("category_name")))
    )
    bridge = (
        exploded
        .join(distinct_cats, "category_name", "inner")
        .select("business_id", "category_id", "category_name")
        .distinct()  # loại bỏ duplicate nếu có
    )
    return bridge

def build_fact_review(reviews: DataFrame, dim_time: DataFrame) -> DataFrame:
    r = reviews.alias("r")
    d = dim_time.select("date_id","date","year","month").alias("d")

    return (
        r.withColumn("review_date", F.to_date("r.date"))
         .join(d, F.col("review_date") == F.col("d.date"), "left")
         .select(
             F.col("r.business_id"),
             F.col("r.user_id"),
             F.col("d.date_id"),
             F.col("d.year").alias("year"),
             F.col("d.month").alias("month"),
             F.col("r.stars"),
             F.col("r.useful"),
             F.col("r.funny"),
             F.col("r.cool")
         )
)
def build_fact_checkin(checkins: DataFrame, dim_time: DataFrame) -> DataFrame:
    agg = (
        checkins
        .withColumn("date", F.to_date("date"))
        .groupBy("business_id","date")
        .agg(F.count("*").alias("checkin_count"))
    )
    d = dim_time.select("date_id","date","year","month").alias("d")
    return (
        agg.alias("a")
           .join(d, F.col("a.date") == F.col("d.date"), "left")
           .select(
               F.col("a.business_id"),
               F.col("d.date_id"),
               F.col("d.year").alias("year"),
               F.col("d.month").alias("month"),
               F.col("a.checkin_count")
           )
    )


def upsert_delta(df_new, target_path, merge_condition: str):
    """
    Upsert df_new into a Delta table at target_path using merge_condition
    (SQL expression for matching target t and source u).
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        df_new.write.format("delta").mode("overwrite").save(target_path)
    else:
        delta_tbl = DeltaTable.forPath(spark, target_path)
        (delta_tbl.alias("t")
                  .merge(
                      df_new.alias("u"),
                      merge_condition
                  )
                  .whenMatchedUpdateAll()
                  .whenNotMatchedInsertAll()
                  .execute()
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Silver → Gold monthly (MinIO)"
    )
    parser.add_argument("--silver-base", required=True,
                        help="Ex: s3a://yelp-data/silver")
    parser.add_argument("--gold-base",   required=True,
                        help="Ex: s3a://yelp-data/gold")
    parser.add_argument("--year",        required=True,
                        help="Ex: 2025")
    parser.add_argument("--month",       required=True,
                        help="Ex: 07")
    args = parser.parse_args()

    sb = args.silver_base.rstrip("/")
    gb = args.gold_base.rstrip("/")
    year  = args.year
    month = args.month

    silver_business = spark.read.format("delta").load(f"{sb}/business")
    silver_reviews  = spark.read.format("delta") \
                             .load(f"{sb}/reviews/year={year}/month={month}")
    silver_checkins = spark.read.format("delta") \
                             .load(f"{sb}/checkins") \
                             .filter(
                                (F.year("date") == int(year)) &
                                (F.month("date") == int(month))
                             )

    # ----- Dimension -----
    dim_time      = build_dim_time(silver_reviews, silver_checkins)
    dim_business  = build_dim_business(silver_business)
    bridge_bc = build_bridge_business_category(silver_business)

    # ----- Fact -----
    fact_review  = build_fact_review(silver_reviews, dim_time)
    fact_checkin = build_fact_checkin(silver_checkins, dim_time)

    (dim_time
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .save(f"{gb}/dim_time")
    )

    upsert_delta(dim_business,
             f"{gb}/dim_business",
             merge_condition="t.business_id = u.business_id")

    upsert_delta(
        bridge_bc,
        f"{gb}/bridge_business_category",
        merge_condition="t.business_id = u.business_id AND t.category_id = u.category_id"
    )

    (fact_review
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("year", "month")
    .save(f"{gb}/fact_review")
)

    (fact_checkin
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .save(f"{gb}/fact_checkin")
    )

    spark.stop()

if __name__ == "__main__":
    main()
