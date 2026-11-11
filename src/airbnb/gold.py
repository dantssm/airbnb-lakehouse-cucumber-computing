"""Gold tables for the dashboard and Genie."""

from pyspark.sql import functions as F

from . import config, model


def _save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def _replace_city(rows, table, city, extra=""):
    """Replace one city's rows (`extra`: more SQL conditions), so a re-run never duplicates them."""
    spark = rows.sparkSession
    if not spark.catalog.tableExists(table):
        return _save(rows, table)
    spark.sql(f"DELETE FROM {table} WHERE city = '{city}' {extra}")
    rows.select(*spark.table(table).columns).write.mode("append").saveAsTable(table)


def _prediction_rows(df):
    p, y, s = F.col("pred"), F.col("price"), F.col("split")
    return df.select(F.col("id").alias("listing_id"), "city",
                     F.when(s == "test", "test").when(s == "new_city", "new_city").otherwise("train").alias("split"),
                     "in_scope", "exclusion_reason", "price", "baseline_pred", p.alias("model_pred"),
                     (p - y).alias("error"), F.abs(p - y).alias("abs_error"),
                     "room_type", "neighbourhood_cleansed", "accommodates")


def write_predictions(df):
    """One row per listing, cities A and B. Model numbers: split = 'test' AND in_scope.
    Run before write_city_predictions: this overwrites the whole table."""
    _save(_prediction_rows(df), config.PREDICTIONS_TABLE)


def write_city_predictions(df, city):
    """Add (or replace) the rows of the new city C. Model numbers: split = 'new_city' AND in_scope."""
    _replace_city(_prediction_rows(df).filter(F.col("city") == city), config.PREDICTIONS_TABLE, city)


def write_metrics(results, final_name):
    """Baseline and final model ('model') on the test set, per city and 'all'. stage = 'test'.
    Run before write_city_metrics: this overwrites the whole table."""
    _save(results.filter(F.col("model").isin("baseline", final_name)).replace(final_name, "model", subset="model")
                 .withColumn("stage", F.lit("test")),
          config.METRICS_TABLE)


def write_city_metrics(results, city, stage):
    """Add (or replace) baseline and model error for the new city C. stage: 'first_run' or 'after_changes'."""
    spark = results.sparkSession
    if spark.catalog.tableExists(config.METRICS_TABLE) and "stage" not in spark.table(config.METRICS_TABLE).columns:
        spark.sql(f"ALTER TABLE {config.METRICS_TABLE} ADD COLUMN stage STRING")   # table made before `stage` existed
        spark.sql(f"UPDATE {config.METRICS_TABLE} SET stage = 'test'")
    _replace_city(results.filter(F.col("city") == city).withColumn("stage", F.lit(stage)),
                  config.METRICS_TABLE, city, f"AND stage = '{stage}'")


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