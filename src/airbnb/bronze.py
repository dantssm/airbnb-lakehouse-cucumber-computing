"""Bronze: the raw data, copied as it comes. No cleaning here."""


def load(spark, city):
    """Get the city's raw file and save it to the bronze table.

    Steps:
    1. Download listings.csv.gz from Inside Airbnb into the Volume.
       (If Databricks blocks the download, upload the file to the Volume by hand.)
    2. Read it with every column as text, so nothing gets guessed or lost.
    3. Add three columns: city, snapshot_date, _ingested_at (when we loaded it).
    4. Save with common.write_city into config.RAW_TABLE.
    """
    raise NotImplementedError