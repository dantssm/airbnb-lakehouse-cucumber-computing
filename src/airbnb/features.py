"""Feature lists and feature building."""

from pyspark.sql import Window
from pyspark.sql import functions as F

from . import config

# computed from the price: never features
LEAKAGE_COLS = [
    "estimated_revenue_l365d",  # = price x occupancy
    "is_valid_price", "is_price_outlier",
    "price_quote_price_per_night", "price_quote_total_price", "price_quote_raw",
    "price_quote_checkin_date", "price_quote_checkout_date",
]

# ids, scrape info, empty columns: never features
NON_FEATURE_COLS = [
    "id", "host_id", "name", "license", "last_scraped", "snapshot_date", "_ingested_at", "is_valid_coords",
    "host_tenure_days", "host_response_time", "host_response_rate", "host_acceptance_rate", "instant_bookable",
    "hosts_time_as_host_years", "hosts_time_as_host_months",  # use host_tenure_months
]

# react to the price over time: only used if they clearly help
CAUTION_COLS = ["estimated_occupancy_l365d", "availability_30", "availability_60",
                "availability_90", "availability_365", "number_of_reviews_ltm"]

CATEGORICAL = ["room_type", "property_group"]
NUMERIC = ["accommodates", "bedrooms", "bathrooms", "amenities_count", "minimum_nights", "number_of_reviews",
           "calculated_host_listings_count", "has_reviews", "rating", "host_tenure_months",
           "dist_to_centre_km", "density_pct"]
CAUTION = ["availability_365"]
NEIGHBOURHOOD = ["neighbourhood_cleansed"]  # only for an experiment: other cities have other names

REQUIRED_SILVER = {"is_price_outlier", "is_long_stay", "has_reviews", "host_tenure_months"}

BUILDING = F.regexp_replace("property_type", "^(Entire|Private room in|Shared room in|Room in) ", "")
FILL = ["bedrooms", "bathrooms", "host_tenure_months"]


def check_no_leakage(columns):
    bad = set(columns) & set(LEAKAGE_COLS + NON_FEATURE_COLS)
    assert not bad, f"leakage or non-feature columns used: {sorted(bad)}"


def fit_stats(rows):
    """Numbers learned from training rows. Reused as they are on a new city."""
    med = rows.select([F.percentile_approx(c, 0.5).alias(c) for c in FILL]).first().asDict()
    types = rows.groupBy(BUILDING.alias("b")).count().filter("count >= 100").collect()
    return {"rating_mean": rows.agg(F.avg("review_scores_rating")).first()[0],
            "medians": {k: v for k, v in med.items() if v is not None},
            "common_types": [r[0] for r in types]}


def make_features(d, st):
    """Add the feature columns. Same code for every city."""
    # density: listings in the same ~1 km cell, as a percentile within the city
    cell = Window.partitionBy("city", F.round("latitude", 2), F.round("longitude", 2))
    d = (d.withColumn("cell_n", F.count("*").over(cell))
          .withColumn("density_pct", F.percent_rank().over(Window.partitionBy("city").orderBy("cell_n"))))
    # distance to the city centre from config, in km
    mid = d.sparkSession.createDataFrame([(c, *s["centre"]) for c, s in config.CITIES.items()], "city string, mlat double, mlon double")
    dy = (F.col("latitude") - F.col("mlat")) * 111.2
    dx = (F.col("longitude") - F.col("mlon")) * 111.2 * F.cos(F.radians("latitude"))
    return (d.join(mid, "city")
             .withColumn("dist_to_centre_km", F.sqrt(dx * dx + dy * dy))
             .withColumn("dist_pct", F.percent_rank().over(Window.partitionBy("city").orderBy("dist_to_centre_km")))  # experiment
             .withColumn("building_type", BUILDING)
             .withColumn("property_group", F.when(F.col("building_type").isin(st["common_types"]), F.col("building_type")).otherwise("Other"))
             .withColumn("has_reviews", F.coalesce(F.col("has_reviews").cast("int"), F.lit(0)))
             .withColumn("rating", F.coalesce("review_scores_rating", F.lit(st["rating_mean"])))
             .withColumn("minimum_nights", F.least("minimum_nights", F.lit(30)))
             .fillna(st["medians"])
             .drop("cell_n", "mlat", "mlon"))