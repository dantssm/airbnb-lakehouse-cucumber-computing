"""Silver: the same listings, cleaned and with proper types."""


def build(spark, city):
    """Turn this city's bronze rows into clean silver rows.

    Steps:
    1. Read this city's rows from config.RAW_TABLE.
    2. Make column names consistent (lower case, words joined with _).
    3. Convert text to real types. Example: price "$1,234.00" becomes 1234.0.
    4. Deal with missing values. The rules come from notebooks/01_eda_leakage.
    5. Save with common.write_city into config.LISTINGS_TABLE.
    """
    raise NotImplementedError