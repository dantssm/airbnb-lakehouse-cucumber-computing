"""Bronze: the raw data, copied as it comes. No cleaning here."""

import os
import urllib.request

from pyspark.sql import functions as F

from . import common, config

BASE_URL = "https://data.insideairbnb.com"


def _city_settings(city):
    """Return (path, snapshot) for a city, with clear errors if the config is incomplete."""
    if city not in config.CITIES:
        raise ValueError(f"City '{city}' is not in config.CITIES. Known cities: {list(config.CITIES)}")
    settings = config.CITIES[city]
    snapshot = settings["snapshot"]
    if not snapshot or snapshot == "TODO":
        raise ValueError(f"Fill in the snapshot date for '{city}' in config.CITIES (format YYYY-MM-DD)")
    return settings["path"], snapshot


def raw_file_path(city):
    """Where the city's listings.csv.gz lives in the Volume, e.g.
    /Volumes/airbnb/bronze/raw_files/sydney/2025-06-10/listings.csv.gz"""
    _, snapshot = _city_settings(city)
    volume_dir = "/Volumes/" + config.VOLUME.replace(".", "/")
    return f"{volume_dir}/{city}/{snapshot}/listings.csv.gz"


def _download(spark, city):
    """Put the raw file into the Volume. Skip if it is already there."""
    path, snapshot = _city_settings(city)
    target = raw_file_path(city)

    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.VOLUME}")
    if os.path.exists(target) and os.path.getsize(target) > 0:
        print(f"[bronze] file already in Volume: {target}")
        return target

    url = f"{BASE_URL}/{path}/{snapshot}/data/listings.csv.gz"
    os.makedirs(os.path.dirname(target), exist_ok=True)
    try:
        urllib.request.urlretrieve(url, target)
    except Exception as e:
        if os.path.exists(target):
            os.remove(target)
        raise RuntimeError(
            f"Could not download {url}: {e}\n"
            f"Download the file on your computer and upload it by hand to: {target}"
        )
    print(f"[bronze] downloaded {url} -> {target} ({os.path.getsize(target) / 1e6:.1f} MB)")
    return target


def load(spark, city):
    """Get the city's raw file and save it to the bronze table.

    Steps:
    1. Download listings.csv.gz from Inside Airbnb into the Volume.
       (If Databricks blocks the download, upload the file to the Volume by hand.)
    2. Read it with every column as text, so nothing gets guessed or lost.
    3. Add three columns: city, snapshot_date, _ingested_at (when we loaded it).
    4. Save with common.write_city into config.RAW_TABLE.
    """
    _, snapshot = _city_settings(city)
    raw_file = _download(spark, city)

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(raw_file)
    )

    df = (
        df
        .withColumn("city", F.lit(city))
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot)))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    n_rows = df.count()
    # A sanity check that the CSV was parsed correctly: id should be a number.
    n_bad_ids = df.filter(~F.col("id").rlike(r"^\d+$")).count()
    print(f"[bronze] {city}: {n_rows} rows, {len(df.columns)} columns, {n_bad_ids} rows with a non-numeric id")

    common.write_city(df, config.RAW_TABLE, city)
    return df