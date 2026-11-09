"""Helpers shared by bronze, silver and gold."""


def write_city(df, table, city):
    """Save one city's rows into a table that holds all cities.

    - Table doesn't exist yet: create it.
    - Table exists: replace only this city's rows, leave other cities alone.

    Safe to run twice. The second run replaces the first run's rows,
    so nothing gets duplicated.
    """
    if not df.sparkSession.catalog.tableExists(table):
        df.write.saveAsTable(table)
    else:
        (df.write.mode("overwrite")
           .option("replaceWhere", f"city = '{city}'")
           .saveAsTable(table))