"""Checks that the data is sane. Each check counts bad rows; 0 means it passed."""

from pyspark.sql import functions as F

from . import config

CHECKS_TABLE = f"{config.SILVER}.quality_checks"

KNOWN_ROOM_TYPES = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]

TECH_COLS = {"city", "snapshot_date", "_ingested_at"}


def run(spark, city):
    """Run the checks for one city and return {check name: number of bad rows}.

    Ideas: listing_id is unique, price is above 0, no listing lost between
    bronze and silver (or every dropped one is explained).

    A non-zero number is not always a bug: e.g. listings without a price exist in the
    source data. The point is that we see the number, instead of finding out later.
    """
    bronze = spark.table(config.RAW_TABLE).filter(F.col("city") == city)
    silver = spark.table(config.LISTINGS_TABLE).filter(F.col("city") == city)

    n_bronze = bronze.count()
    n_silver = silver.count()

    bronze_id = F.expr("try_cast(trim(id) AS BIGINT)")
    n_bronze_no_id = bronze.filter(bronze_id.isNull()).count()
    n_bronze_dup_id = (n_bronze - n_bronze_no_id) - bronze.select(bronze_id.alias("id")).dropna().distinct().count()

    s = silver.agg(
        (F.count("*") - F.countDistinct("id")).alias("duplicate_id"),
        F.sum(F.when(F.col("price").isNull(), 1).otherwise(0)).alias("price_missing"),
        F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias("price_not_positive"),
        F.sum(F.when(F.col("room_type").isNull(), 1).otherwise(0)).alias("room_type_missing"),
        F.sum(F.when(F.col("accommodates").isNull() | (F.col("accommodates") <= 0), 1).otherwise(0))
         .alias("accommodates_missing_or_not_positive"),
        F.sum(F.when(~F.col("is_valid_coords") | F.col("is_valid_coords").isNull(), 1).otherwise(0))
         .alias("bad_coordinates"),
        F.sum(F.when(~F.col("availability_365").between(0, 365), 1).otherwise(0))
         .alias("availability_365_out_of_range"),
        F.sum(F.when(~F.col("review_scores_rating").between(0, 5), 1).otherwise(0))
         .alias("review_score_out_of_range"),
    ).first().asDict()

    unseen = silver.filter(F.col("room_type").isNotNull() & ~F.col("room_type").isin(KNOWN_ROOM_TYPES))
    unseen_room_types = [r["room_type"] for r in unseen.select("room_type").distinct().collect()]

    data_cols = [c for c in silver.columns if c not in TECH_COLS]
    non_null = silver.select([F.count(F.col(c)).alias(c) for c in data_cols]).first().asDict()
    all_null_columns = sorted(c for c, n in non_null.items() if n == 0)

    results = {
        "rows_lost_unexplained": n_bronze - n_silver - n_bronze_no_id - n_bronze_dup_id,
        **{k: int(v or 0) for k, v in s.items()},
        "room_type_unseen": unseen.count(),
        "all_null_columns": len(all_null_columns),
    }

    if unseen_room_types:
        print(f"[validation] WARNING {city}: unseen room types {unseen_room_types}")
    if all_null_columns:
        print(f"[validation] WARNING {city}: columns that are entirely NULL: {all_null_columns}")

    rows = [(city, name, int(bad), n_silver) for name, bad in results.items()]
    (spark.createDataFrame(rows, "city STRING, check_name STRING, bad_rows BIGINT, total_rows BIGINT")
        .withColumn("checked_at", F.current_timestamp())
        .write.mode("append").saveAsTable(CHECKS_TABLE))

    print(f"[validation] {city}: bronze={n_bronze}, silver={n_silver}, "
          f"dropped: no id={n_bronze_no_id}, duplicate id={n_bronze_dup_id}")
    return results