"""Gold: final tables that the dashboard and Genie read."""


def write_predictions(spark, df):
    """Save a prediction for every listing to config.PREDICTIONS_TABLE.

    Columns: listing_id, city, split (train/test), price,
    baseline_pred, model_pred.
    """
    raise NotImplementedError


def write_metrics(spark, metrics):
    """Save the errors to config.METRICS_TABLE.

    One row per city and per model (baseline or model): the MAE and how many
    listings it was measured on. Also a row for all cities together.
    """
    raise NotImplementedError


def write_city_summary(spark):
    """Save per-city numbers to config.SUMMARY_TABLE.

    Listings, median price, availability, review scores, hosts.
    This is what the dashboard's "City overview" page uses.
    """
    raise NotImplementedErrors