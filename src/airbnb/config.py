"""Settings used everywhere: where tables live, which region, which cities."""

CATALOG = "airbnb"
BRONZE = f"{CATALOG}.bronze"    # raw data
SILVER = f"{CATALOG}.silver"    # cleaned data
GOLD = f"{CATALOG}.gold"        # tables for the model, dashboard and Genie
VOLUME = f"{BRONZE}.raw_files"  # where the downloaded .csv.gz files are kept

# Table names. Use these instead of typing names by hand.
RAW_TABLE = f"{BRONZE}.listings_raw"
LISTINGS_TABLE = f"{SILVER}.listings"
PREDICTIONS_TABLE = f"{GOLD}.price_predictions"
METRICS_TABLE = f"{GOLD}.model_metrics"
SUMMARY_TABLE = f"{GOLD}.city_summary"

REGION = "Australia"
CURRENCY = "AUD"

# One entry per city. To add a city, add one line here and nothing else.
# TODO: check the paths and fill in the snapshot dates on insideairbnb.com/get-the-data
CITIES = {
    "sydney":    {"path": "australia/nsw/sydney",    "snapshot": "2026-09-16", "centre": (-33.8688, 151.2093)},
    "melbourne": {"path": "australia/vic/melbourne", "snapshot": "2026-09-16", "centre": (-37.8136, 144.9631)},
    # "brisbane": {"path": "australia/qld/brisbane",  "snapshot": "TODO", "centre": (-27.4698, 153.0251)},  # city C: uncomment in week 2, not before
}

TRAIN_CITIES = ["sydney", "melbourne"]  # A and B
TEST_FRACTION = 0.2   # share of listings kept aside for testing
SEED = 42             # fixed number, so results are the same on every run


def ensure_schemas(spark):
    """Create the catalog, the three schemas and the Volume if they don't exist.

    Run once by the workspace owner. Safe to run again.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for schema in (BRONZE, SILVER, GOLD):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")