"""Which columns the model uses, and which it must never see."""

# Columns that are calculated from the price, or give it away.
# The model must never train on them. Write the reason next to each one.
# Filled in from the leakage review (notebooks/01_eda_leakage).
LEAKAGE_COLS = [
    "estimated_revenue_l365d",  # = price x estimated_occupancy_l365d; NULL whenever price is NULL (01_eda_leakage, section 4)
    "is_valid_price",           # computed from price
]

# Not leakage, but never features: they describe the scrape, not the listing,
# and ids let a model memorise rows instead of learning prices.
NON_FEATURE_COLS = [
    "id", "host_id", "name", "license", "last_scraped", "snapshot_date", "_ingested_at",
    "is_valid_coords",
]

# Allowed, but kept out of the first model: not computed from price, but they react
# to it over time (expensive listings book less). Add only if they help on the test set.
CAUTION_COLS = [
    "estimated_occupancy_l365d", "availability_30", "availability_60",
    "availability_90", "availability_365", "number_of_reviews_ltm",
]

NUMERIC = []      # numeric features, e.g. accommodates, bedrooms
CATEGORICAL = []  # category features, e.g. room_type, neighbourhood


def build(df):
    """Return one row per listing: listing_id, city, price and the feature columns.

    The same function runs for every city. The features must not depend on
    which city it is, otherwise the model can't work on a new city.
    """
    raise NotImplementedError