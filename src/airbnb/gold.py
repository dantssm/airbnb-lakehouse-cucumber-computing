"""Gold tables for the dashboard and Genie."""

from pyspark.sql import functions as F

from . import config, model


# Table and column comments: what Genie and people read in Unity Catalog. {table: (table comment, {column: comment})}
COMMENTS = {
    config.PREDICTIONS_TABLE: (
        f"One row per listing: actual price, baseline and model prediction, and the error. Prices are {config.CURRENCY} per night. "
        "Model numbers: split = 'test' AND in_scope for Sydney and Melbourne, split = 'new_city' AND in_scope for the new city.",
        {"listing_id": "Inside Airbnb listing id (one row per listing).",
         "city": "City name in lower case.",
         "split": "'train' = used to fit or tune the model; 'test' = 20% hold-out of Sydney and Melbourne; 'new_city' = city C, never seen by the model.",
         "in_scope": "True if the listing has a price, is not a price outlier and is not a long stay (minimum_nights > 30). Use only in-scope listings for model numbers.",
         "exclusion_reason": "Why the listing is out of scope: no_price, price_outlier or long_stay. NULL if in scope.",
         "price": f"Advertised price per night in {config.CURRENCY} on the snapshot date (the actual value). NULL if the listing has no price.",
         "baseline_pred": f"Baseline prediction in {config.CURRENCY}: median price of train listings with the same accommodates and room_type.",
         "model_pred": f"Price model prediction in {config.CURRENCY} per night.",
         "error": f"model_pred - price, in {config.CURRENCY}. Positive = the model over-prices, negative = it under-prices.",
         "abs_error": f"Absolute error |model_pred - price| in {config.CURRENCY}. Its average is the MAE.",
         "room_type": "Entire home/apt, Private room, Shared room or Hotel room.",
         "neighbourhood_cleansed": "Neighbourhood of the listing. Not a model feature (other cities have other names).",
         "accommodates": "Number of guests the listing sleeps."}),
    config.METRICS_TABLE: (
        f"Prediction error of the baseline and of the price model, in {config.CURRENCY}. Lower is better. One row per city, model and stage.",
        {"city": "City name in lower case. 'all' = Sydney and Melbourne together (stage test only).",
         "model": "'baseline' = median price per accommodates and room_type; 'model' = the price model.",
         "mae": f"Mean absolute error in {config.CURRENCY} per night.",
         "medae": f"Median absolute error in {config.CURRENCY} per night (not pulled by a few extreme prices).",
         "n": "Number of in-scope listings the error is computed on.",
         "stage": "'test' = 20% hold-out of Sydney and Melbourne; 'first_run' = new city with the model as at the end of week 1, unchanged; 'after_changes' = new city after model changes."}),
    config.SUMMARY_TABLE: (
        f"One row per city: size and typical prices. Prices are {config.CURRENCY} per night. Medians use in-scope listings only.",
        {"city": "City name in lower case.",
         "snapshot_date": "Date of the Inside Airbnb snapshot.",
         "listings": "All listings in the snapshot.",
         "in_scope_listings": "Listings with a price that are not a price outlier and not a long stay.",
         "pct_no_price": "Percent of listings without a price (mostly inactive listings).",
         "median_price": f"Median price per night in {config.CURRENCY}, in-scope listings.",
         "median_price_entire_home": f"Median price per night in {config.CURRENCY}, in-scope entire homes/apartments.",
         "avg_availability_365": "Average number of days available in the next 365, for listings with a price.",
         "avg_review_score": "Average overall review score (0-5).",
         "hosts": "Number of distinct hosts.",
         "pct_entire_home": "Percent of listings that are entire homes/apartments."}),
}


def _quote(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _comment(spark, table):
    """Set the table and column comments. Never stops the pipeline: a failed comment is only printed."""
    table_comment, columns = COMMENTS[table]
    try:
        spark.sql(f"COMMENT ON TABLE {table} IS {_quote(table_comment)}")
        existing = set(spark.table(table).columns)
        for column, text in columns.items():
            if column in existing:
                spark.sql(f"ALTER TABLE {table} ALTER COLUMN `{column}` COMMENT {_quote(text)}")
    except Exception as e:
        print(f"[gold] could not set comments on {table}: {e}")


def add_comments(spark):
    """Set the comments on the gold tables that already exist. Run it once to add them without rewriting any table."""
    for table in COMMENTS:
        if spark.catalog.tableExists(table):
            _comment(spark, table)


def _save(df, table):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    _comment(df.sparkSession, table)   # a rewritten table can lose its comments


def _replace_city(rows, table, city, extra=""):
    """Replace one city's rows (`extra`: more SQL conditions), so a re-run never duplicates them."""
    spark = rows.sparkSession
    if not spark.catalog.tableExists(table):
        return _save(rows, table)
    spark.sql(f"DELETE FROM {table} WHERE city = '{city}' {extra}")
    rows.select(*spark.table(table).columns).write.mode("append").saveAsTable(table)
    _comment(spark, table)


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