"""Checks that the data is sane. Each check counts bad rows; 0 means it passed."""


def run(spark, city):
    """Run the checks for one city and return {check name: number of bad rows}.

    Ideas: listing_id is unique, price is above 0, no listing lost between
    bronze and silver (or every dropped one is explained).
    """
    raise NotImplementedError