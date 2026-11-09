"""Test split, simple baseline, price model and the error metric."""


def split(df):
    """Mark each listing as 'train' or 'test'.

    About 20% go to test, picked separately in each city. The pick is based on
    listing_id, so a listing always lands on the same side, even after a re-run.
    """
    raise NotImplementedError


def baseline(train):
    """The simplest possible guess to compare against.

    For each pair (accommodates, room_type), take the median price in the
    training data. Our model has to beat this.
    """
    raise NotImplementedError


def fit(train):
    """Train the price model (scikit-learn or Spark ML) and return it."""
    raise NotImplementedError


def mae(df, pred_col):
    """Mean absolute error: the average of |real price - predicted price|.

    In AUD, so it reads as "on average we are off by X dollars".
    """
    raise NotImplementedError