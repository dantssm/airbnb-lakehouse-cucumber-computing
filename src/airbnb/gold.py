"""Gold tables for the dashboard and Genie."""

from pyspark.sql import functions as F

from . import config, model


def _save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def write_predictions(df):
    """One row per listing. Model numbers: filter split = 'test' AND in_scope."""
    p, y = F.col("pred"), F.col("price")
    _save(df.select(F.col("id").alias("listing_id"), "city",
                    F.when(F.col("split") == "test", "test").otherwise("train").alias("split"),
                    "in_scope", "exclusion_reason", "price", "baseline_pred", p.alias("model_pred"),
                    (p - y).alias("error"), F.abs(p - y).alias("abs_error"),
                    "room_type", "neighbourhood_cleansed", "accommodates"),
          config.PREDICTIONS_TABLE)


def write_metrics(results, final_name):
    """Baseline and final model ('model') on the test set, per city and 'all'."""
    _save(results.filter(F.col("model").isin("baseline", final_name)).replace(final_name, "model", subset="model"),
          config.METRICS_TABLE)


def write_city_summary(spark):
    """One row per city in silver. Medians use in-scope listings only."""
    s = model.add_scope(spark.table(config.LISTINGS_TABLE))
    price = F.when(F.col("in_scope"), F.col("price"))
    entire = F.when(F.col("in_scope") & (F.col("room_type") == "Entire home/apt"), F.col("price"))
    summary = s.groupBy("city").agg(
        F.max("snapshot_date").alias("snapshot_date"),
        F.count("*").alias("listings"),
        F.sum(F.col("in_scope").cast("int")).alias("in_scope_listings"),
        F.round(F.avg(F.col("exclusion_reason").eqNullSafe("no_price").cast("int")) * 100, 1).alias("pct_no_price"),
        F.percentile_approx(price, 0.5).alias("median_price"),
        F.percentile_approx(entire, 0.5).alias("median_price_entire_home"),
        F.round(F.avg(F.when(F.col("is_valid_price"), F.col("availability_365"))), 1).alias("avg_availability_365"),
        F.round(F.avg("review_scores_rating"), 2).alias("avg_review_score"),
        F.countDistinct("host_id").alias("hosts"),
        F.round(F.avg((F.col("room_type") == "Entire home/apt").cast("int")) * 100, 1).alias("pct_entire_home"),
    )
    _save(summary, config.SUMMARY_TABLE)
    return summary