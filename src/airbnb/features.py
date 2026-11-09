"""Which columns the model uses, and which it must never see."""

# Columns that are calculated from the price, or give it away.
# The model must never train on them. Write the reason next to each one.
# Filled in from the leakage review (notebooks/01_eda_leakage).
LEAKAGE_COLS = []

NUMERIC = []      # numeric features, e.g. accommodates, bedrooms
CATEGORICAL = []  # category features, e.g. room_type, neighbourhood


def build(df):
    """Return one row per listing: listing_id, city, price and the feature columns.

    The same function runs for every city. The features must not depend on
    which city it is, otherwise the model can't work on a new city.
    """
    raise NotImplementedError