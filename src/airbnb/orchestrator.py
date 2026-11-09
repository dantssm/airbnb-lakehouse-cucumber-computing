"""Runs the whole pipeline for one city. The city is a parameter, not copied code."""
from airbnb import bronze, silver, validation


def run_city(spark, city):
    """Run bronze, then silver, then the checks, for one city.

    To add a new city: add it to config.CITIES. No code changes needed.
    """
    bronze.load(spark, city)
    silver.build(spark, city)
    print(validation.run(spark, city))