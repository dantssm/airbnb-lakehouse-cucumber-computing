"""Silver: the same listings, cleaned and with proper types."""

import math
import re

from pyspark.sql import Window
from pyspark.sql import functions as F

from . import common, config
STRING_COLS = ["name", "neighbourhood_cleansed", "property_type", "room_type",
               "bathrooms_text", "amenities", "license", "host_response_time"]
INT_COLS = ["id", "host_id", "host_listings_count", "host_total_listings_count",
            "accommodates", "bedrooms", "beds", "minimum_nights", "maximum_nights",
            "availability_30", "availability_60", "availability_90", "availability_365",
            "number_of_reviews", "number_of_reviews_ltm", "calculated_host_listings_count",
            "estimated_occupancy_l365d"]
DOUBLE_COLS = ["latitude", "longitude", "bathrooms", "reviews_per_month",
               "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness",
               "review_scores_checkin", "review_scores_communication", "review_scores_location",
               "review_scores_value",
               "hosts_time_as_host_months"]
MONEY_COLS = ["price", "estimated_revenue_l365d"] 
PERCENT_COLS = ["host_response_rate", "host_acceptance_rate"]
BOOL_COLS = ["host_is_superhost", "host_identity_verified", "host_has_profile_pic",
             "instant_bookable", "has_availability"]
DATE_COLS = ["host_since", "first_review", "last_review", "last_scraped"]

PLACEHOLDER_PRICES = [99999, 90000, 50000]
LONG_STAY_NIGHTS = 30
OUTLIER_IQR_MULT = 3.0 
OUTLIER_MIN_GROUP = 30 

ALL_COLS = STRING_COLS + INT_COLS + DOUBLE_COLS + MONEY_COLS + PERCENT_COLS + BOOL_COLS + DATE_COLS
META_COLS = ["city", "snapshot_date", "_ingested_at"]


def _normalize(name):
    """'Host Response Rate' -> 'host_response_rate'.
    A leading underscore is kept, so our own technical columns like _ingested_at stay as they are."""
    prefix = "_" if name.startswith("_") else ""
    return prefix + re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")


def _clean_str(c):
    """Empty strings and 'N/A' are missing values too."""
    v = F.trim(F.col(c))
    return F.when((v == "") | (F.upper(v) == "N/A"), None).otherwise(v)


def _upper_fence(q):
    """q = [Q1, Q3] of log(price) -> upper fence on the log scale."""
    return q[1] + OUTLIER_IQR_MULT * (q[1] - q[0])


def _flag_price_outliers(silver):
    """Add is_price_outlier. Returns (dataframe, {room_type: fence in currency})."""
    quartiles = "percentile_approx(log_price, array(0.25, 0.75), 10000)"
    valid = silver.filter(F.col("is_valid_price")).withColumn("log_price", F.log("price"))

    city_q = valid.agg(F.expr(quartiles).alias("q")).first()["q"]
    if not city_q:  # no valid prices at all
        return silver.withColumn("is_price_outlier", F.lit(False)), {}
    city_fence = _upper_fence(city_q)

    groups = valid.groupBy("room_type").agg(F.count("*").alias("n"), F.expr(quartiles).alias("q")).collect()
    fences = {
        g["room_type"]: _upper_fence(g["q"]) if g["n"] >= OUTLIER_MIN_GROUP else city_fence
        for g in groups if g["room_type"] is not None
    }

    fence_col = F.lit(city_fence)
    for room_type, fence in fences.items():
        fence_col = F.when(F.col("room_type") == room_type, F.lit(fence)).otherwise(fence_col)

    silver = silver.withColumn(
        "is_price_outlier",
        F.col("is_valid_price")
        & ((F.log("price") > fence_col) | F.col("price").isin(PLACEHOLDER_PRICES)),
    )
    readable = {rt: round(math.exp(f), 2) for rt, f in fences.items()}
    readable["_city"] = round(math.exp(city_fence), 2)
    return silver, readable


def build(spark, city):
    """Turn this city's bronze rows into clean silver rows.

    Steps:
    1. Read this city's rows from config.RAW_TABLE.
    2. Make column names consistent (lower case, words joined with _).
    3. Convert text to real types. Example: price "$1,234.00" becomes 1234.0.
    4. Deal with missing values. The rules come from notebooks/01_eda_leakage.
    5. Save with common.write_city into config.LISTINGS_TABLE.

    Missing values stay NULL here: filling them in (imputation) belongs to the model
    and must be learned on the training split only, otherwise it leaks test data.
    Suspicious rows are flagged, not silently dropped.
    """
    df = spark.table(config.RAW_TABLE).filter(F.col("city") == city)
    n_bronze = df.count()
    if n_bronze == 0:
        raise ValueError(f"No rows for '{city}' in {config.RAW_TABLE}. Run bronze.load first.")

    df = df.toDF(*[_normalize(c) for c in df.columns])

    missing_columns = [c for c in ALL_COLS if c not in df.columns]
    for c in missing_columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

    typed = {}
    for c in STRING_COLS:
        typed[c] = _clean_str(c)
    for c in INT_COLS:
        typed[c] = F.expr(f"try_cast(trim(`{c}`) AS BIGINT)")
    for c in DOUBLE_COLS:
        typed[c] = F.expr(f"try_cast(trim(`{c}`) AS DOUBLE)")
    for c in MONEY_COLS:
        typed[c] = F.expr(f"try_cast(regexp_replace(`{c}`, '[$,\\\\s]', '') AS DOUBLE)")
    for c in PERCENT_COLS:
        typed[c] = F.expr(f"try_cast(regexp_replace(`{c}`, '%', '') AS DOUBLE)") / 100
    for c in BOOL_COLS:
        v = F.lower(F.trim(F.col(c)))
        typed[c] = F.when(v.isin("t", "true"), True).when(v.isin("f", "false"), False)
    for c in DATE_COLS:
        typed[c] = F.expr(f"try_to_timestamp(trim(`{c}`), 'yyyy-MM-dd')").cast("date")

    silver = df.select(*[F.col(c) for c in META_COLS], *[e.alias(c) for c, e in typed.items()])

    check_cols = INT_COLS + DOUBLE_COLS + MONEY_COLS + PERCENT_COLS + DATE_COLS
    before = df.select([F.count(_clean_str(c)).alias(c) for c in check_cols]).first()
    after = silver.select([F.count(F.col(c)).alias(c) for c in check_cols]).first()
    cast_failures = {c: before[c] - after[c] for c in check_cols if before[c] - after[c] > 0}

    bt = F.lower(F.col("bathrooms_text"))
    silver = (
        silver
        .withColumn("bathrooms", F.coalesce(
            F.col("bathrooms"),
            F.expr("try_cast(regexp_extract(bathrooms_text, '(\\\\d+(\\\\.\\\\d+)?)', 1) AS DOUBLE)"),
            F.when(bt.contains("half-bath"), F.lit(0.5))))
        .withColumn("is_shared_bath", bt.contains("shared"))
        .withColumn("amenities_count", F.size(F.from_json(F.col("amenities"), "array<string>")))
        .withColumn("amenities_count", F.when(F.col("amenities_count") >= 0, F.col("amenities_count")))
        .withColumn("host_tenure_days", F.datediff(F.col("snapshot_date"), F.col("host_since")))
        .withColumn("is_long_stay", F.col("minimum_nights") > LONG_STAY_NIGHTS)
        .withColumn("has_reviews", F.col("number_of_reviews") > 0)
    )

    silver = silver.filter(F.col("id").isNotNull())
    w = Window.partitionBy("id").orderBy(F.col("last_scraped").desc_nulls_last())
    silver = silver.withColumn("_rn", F.row_number().over(w))
    n_duplicates = silver.filter(F.col("_rn") > 1).count()
    silver = silver.filter(F.col("_rn") == 1).drop("_rn")

    silver = (
        silver
        .withColumn("is_valid_price", F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("is_valid_coords",
                    F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
    )

    silver, outlier_fences = _flag_price_outliers(silver)

    common.write_city(silver, config.LISTINGS_TABLE, city)

    report = {
        "city": city,
        "n_bronze": n_bronze,
        "n_silver": spark.table(config.LISTINGS_TABLE).filter(F.col("city") == city).count(),
        "n_duplicates_removed": n_duplicates,
        "missing_columns": missing_columns,
        "cast_failures": cast_failures,
        "price_outlier_fences": outlier_fences,
    }
    print(f"[silver] {report}")
    return report