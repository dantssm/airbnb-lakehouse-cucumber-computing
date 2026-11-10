"""Exploratory analysis helpers for notebooks/01_eda_leakage (and 03_city_c in week 2).

Reads the bronze and silver tables built by the pipeline; writes nothing.
Spark does the counting on bronze; silver rows for a few cities are small
enough (tens of thousands) to profile in pandas.

Silver keeps listings without a price (is_valid_price = false) and leaves
missing values as NULL. Price analysis uses only is_valid_price = true.
"""
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

from airbnb import config

# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

DATE_COLS = ["host_since", "first_review", "last_review", "last_scraped", "snapshot_date"]


def load_silver(spark, cities):
    """Silver listings of the given cities as pandas, all rows (valid price or not)."""
    sdf = spark.table(config.LISTINGS_TABLE).filter(F.col("city").isin(list(cities)))
    pdf = sdf.drop("_ingested_at").toPandas()
    for c in DATE_COLS:
        if c in pdf:
            pdf[c] = pd.to_datetime(pdf[c], errors="coerce")
    return pdf


def bronze_overview(spark, cities):
    """Per city in bronze: rows, distinct ids, rows without a numeric id, snapshot date."""
    b = spark.table(config.RAW_TABLE).filter(F.col("city").isin(list(cities)))
    num_id = F.expr("try_cast(trim(id) AS BIGINT)")
    return (b.groupBy("city")
             .agg(F.count("*").alias("bronze_rows"),
                  F.countDistinct(num_id).alias("bronze_distinct_ids"),
                  F.sum(F.when(num_id.isNull(), 1).otherwise(0)).alias("rows_without_numeric_id"),
                  F.max("snapshot_date").alias("snapshot_date"),
                  F.max("last_scraped").alias("max_last_scraped"))
             .orderBy("city").toPandas())


def bronze_columns(spark):
    """Column names of the raw file, as stored in bronze."""
    return [c for c in spark.table(config.RAW_TABLE).columns
            if c not in ("city", "snapshot_date", "_ingested_at")]


def price_in_text(spark, cities, cols=("name", "description")):
    """% of listings whose free text in bronze mentions a dollar amount ('$150 per night')."""
    b = spark.table(config.RAW_TABLE).filter(F.col("city").isin(list(cities)))
    cols = [c for c in cols if c in b.columns]
    aggs = [F.round(F.avg(F.when(F.col(c).rlike(r"\$\s?\d"), 1).otherwise(0)) * 100, 2).alias(c)
            for c in cols]
    return b.groupBy("city").agg(*aggs).orderBy("city").toPandas()


# ---------------------------------------------------------------------------
# Dataset description
# ---------------------------------------------------------------------------

def silver_overview(df):
    """Per city: silver rows, valid / missing price, hosts."""
    d = df.assign(_valid=df["is_valid_price"].fillna(False).astype(bool))
    g = d.groupby("city")
    return pd.DataFrame({
        "silver_rows": g.size(),
        "distinct_ids": g["id"].nunique(),
        "valid_price": g["_valid"].sum(),
        "no_valid_price": g["_valid"].apply(lambda s: int((~s).sum())),
        "no_valid_price_%": g["_valid"].apply(lambda s: round((~s).mean() * 100, 1)),
        "distinct_hosts": g["host_id"].nunique(),
    })


# ---------------------------------------------------------------------------
# Missing values
# ---------------------------------------------------------------------------

def missing_table(df, cols):
    """% of NULLs per column and city, worst first."""
    cols = [c for c in cols if c in df and c != "city"]
    t = df.groupby("city")[cols].apply(lambda g: g.isna().mean()).T * 100
    return t.loc[t.max(axis=1).sort_values(ascending=False).index].round(1)


def why_no_price(df):
    """Listings without a valid price vs with one: what else is different?

    If they are mostly blocked calendars, no recent reviews and not bookable,
    a missing price means 'inactive listing', not 'price unknown'.
    """
    d = df.assign(valid=df["is_valid_price"].fillna(False).astype(bool))
    agg = {}
    if "availability_365" in d:
        agg["availability_365 == 0 (%)"] = ("availability_365", lambda s: (s == 0).mean() * 100)
        agg["median availability_365"] = ("availability_365", "median")
    if "has_availability" in d:
        agg["has_availability true (%)"] = ("has_availability", lambda s: (s == True).mean() * 100)  # noqa: E712
    if "number_of_reviews_ltm" in d:
        agg["no reviews in last 12m (%)"] = ("number_of_reviews_ltm", lambda s: (s.fillna(0) == 0).mean() * 100)
    if "last_review" in d and "snapshot_date" in d:
        d["days_since_last_review"] = (d["snapshot_date"] - d["last_review"]).dt.days
        agg["median days since last review"] = ("days_since_last_review", "median")
    if "estimated_occupancy_l365d" in d:
        agg["median estimated occupancy"] = ("estimated_occupancy_l365d", "median")
    if "room_type" in d:
        agg["entire home (%)"] = ("room_type", lambda s: (s == "Entire home/apt").mean() * 100)
    agg["listings"] = ("id", "size")
    return d.groupby(["city", "valid"]).agg(**agg).round(1)


def missing_patterns(df):
    """Is a value missing for a reason? Missing rate when a condition is true vs false."""
    checks = []

    def add(col, label, mask):
        if col not in df:
            return
        for city, g in df.groupby("city"):
            try:
                m = mask(g).fillna(False).astype(bool)
            except KeyError:
                return
            checks.append({
                "city": city, "missing_column": col, "condition": label,
                "rows_condition_true": int(m.sum()),
                "missing_if_true_%": round(g.loc[m, col].isna().mean() * 100, 1) if m.any() else np.nan,
                "missing_if_false_%": round(g.loc[~m, col].isna().mean() * 100, 1) if (~m).any() else np.nan,
            })

    for c in [c for c in df.columns if c.startswith("review_scores_")] + ["reviews_per_month", "first_review"]:
        add(c, "number_of_reviews == 0", lambda g: g["number_of_reviews"] == 0)
    for c in ["bedrooms", "beds"]:
        add(c, "room_type != 'Entire home/apt'", lambda g: g["room_type"] != "Entire home/apt")
    add("bedrooms", "accommodates <= 2", lambda g: g["accommodates"] <= 2)
    add("bathrooms", "bathrooms_text is NULL", lambda g: g["bathrooms_text"].isna())
    for c in ["host_response_rate", "host_response_time"]:
        add(c, "host_acceptance_rate is NULL", lambda g: g["host_acceptance_rate"].isna())
    add("host_is_superhost", "host_since is NULL", lambda g: g["host_since"].isna())
    return pd.DataFrame(checks)


# ---------------------------------------------------------------------------
# Distributions and outliers (valid prices only)
# ---------------------------------------------------------------------------

def price_summary(df, by=("city", "room_type")):
    by = [b for b in by if b in df]
    return df.groupby(by)["price"].describe(percentiles=[.01, .05, .25, .5, .75, .95, .99]).round(1)


def availability_summary(df):
    """% of listings per availability_365 bucket."""
    a = df["availability_365"]
    bucket = pd.cut(a, [-1, 0, 89, 179, 269, 364, 365],
                    labels=["0 (blocked)", "1-89", "90-179", "180-269", "270-364", "365 (always open)"])
    return (pd.crosstab(df["city"], bucket, normalize="index") * 100).round(1)


PRICE_FENCE_K = 3.0      # fences at Q1 - k*IQR and Q3 + k*IQR of log(price)
MIN_GROUP_SIZE = 30      # smaller city x room_type groups use the whole city's fences
LONG_STAY_NIGHTS = 30    # minimum_nights above this: a monthly rental


def _fences(logp):
    q1, q3 = logp.quantile([.25, .75])
    iqr = q3 - q1
    return q1 - PRICE_FENCE_K * iqr, q3 + PRICE_FENCE_K * iqr


def classify_price(df):
    """Add `price_class` (and the fences) to listings with a valid price.

    placeholder  : above the high fence and looks typed in (9999, 10000, 99999...)
    high_outlier : above the high fence of its city x room type, on log scale
    low_outlier  : below the low fence
    long_stay    : minimum_nights > LONG_STAY_NIGHTS, the nightly price means something else
    normal       : everything else
    """
    out = df.copy()
    out["price_class"] = "normal"
    p = out["price"]
    logp = np.log(p.where(p > 0))
    lo = pd.Series(np.nan, index=out.index)
    hi = pd.Series(np.nan, index=out.index)
    for city, gc in out.groupby("city"):
        city_lo, city_hi = _fences(logp[gc.index].dropna())
        for _, g in gc.groupby("room_type", dropna=False):
            vals = logp[g.index].dropna()
            l, h = _fences(vals) if len(vals) >= MIN_GROUP_SIZE else (city_lo, city_hi)
            lo[g.index], hi[g.index] = l, h
    out["price_low_fence"] = np.exp(lo).round(0)
    out["price_high_fence"] = np.exp(hi).round(0)
    looks_typed = p.fillna(0).astype("int64").astype(str).str.fullmatch(r"9{3,}|[1-9]0{3,}")
    out.loc[out["minimum_nights"] > LONG_STAY_NIGHTS, "price_class"] = "long_stay"
    out.loc[logp < lo, "price_class"] = "low_outlier"
    out.loc[logp > hi, "price_class"] = "high_outlier"
    out.loc[(logp > hi) & looks_typed, "price_class"] = "placeholder"
    return out


def other_outliers(df):
    """Values outside what is physically plausible (counts per city)."""
    rules = {
        "accommodates > 16 (Airbnb caps at 16)": lambda g: g["accommodates"] > 16,
        "bedrooms > accommodates": lambda g: g["bedrooms"] > g["accommodates"],
        "beds == 0": lambda g: g["beds"] == 0,
        "bathrooms > 10": lambda g: g["bathrooms"] > 10,
        "minimum_nights > 365": lambda g: g["minimum_nights"] > 365,
        "maximum_nights > 1125 (Airbnb default max)": lambda g: g["maximum_nights"] > 1125,
        "host_listings_count > 100": lambda g: g["host_listings_count"] > 100,
    }
    rows = []
    for city, g in df.groupby("city"):
        for name, rule in rules.items():
            try:
                rows.append({"city": city, "rule": name, "rows": int(rule(g).fillna(False).sum())})
            except KeyError:
                rows.append({"city": city, "rule": name, "rows": None})
    return pd.DataFrame(rows).pivot(index="rule", columns="city", values="rows")


# ---------------------------------------------------------------------------
# Leakage review
# ---------------------------------------------------------------------------

HELPER_COLS = {"price_class", "price_low_fence", "price_high_fence", "is_valid_price", "is_valid_coords"}


def leakage_name_scan(columns):
    """Columns whose name suggests money derived from the price."""
    import re
    pat = re.compile(r"price|revenue|cost|fee|income|earn", re.I)
    return [c for c in columns if pat.search(c) and c != "price" and c not in HELPER_COLS]


def leakage_correlations(df):
    """Spearman correlation of every numeric column with price, per city, strongest first.

    Near-perfect correlation means (almost) a function of price; a high but
    imperfect one is just a strong driver and must be checked by meaning.
    """
    cols = [c for c in df.select_dtypes("number").columns
            if c != "price" and c not in HELPER_COLS and c not in ("id", "host_id")]
    t = pd.DataFrame({city: g[cols].corrwith(g["price"], method="spearman")
                      for city, g in df.groupby("city")})
    return t.reindex(t.abs().max(axis=1).sort_values(ascending=False).index).round(3)


def revenue_identity(df):
    """Is estimated_revenue_l365d == price * estimated_occupancy_l365d?

    Run on ALL silver rows. A share near 100% within 1%, plus revenue missing
    whenever price is missing, means the column is computed from the target.
    """
    need = {"estimated_revenue_l365d", "estimated_occupancy_l365d", "price"}
    if not need.issubset(df.columns):
        return pd.DataFrame({"note": [f"columns missing: {sorted(need - set(df.columns))}"]})
    rows = []
    for city, g in df.groupby("city"):
        m = g["price"].gt(0) & g["estimated_occupancy_l365d"].gt(0) & g["estimated_revenue_l365d"].notna()
        ratio = g.loc[m, "estimated_revenue_l365d"] / (g.loc[m, "price"] * g.loc[m, "estimated_occupancy_l365d"])
        no_price = g["price"].isna()
        rows.append({
            "city": city,
            "rows_checked": int(m.sum()),
            "median_ratio": round(ratio.median(), 4) if m.any() else None,
            "identity_holds_within_1pct_%": round((ratio.sub(1).abs() < 0.01).mean() * 100, 1) if m.any() else None,
            "revenue_null_when_price_null_%": round(g.loc[no_price, "estimated_revenue_l365d"].isna().mean() * 100, 1)
            if no_price.any() else None,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Week 2: compare a new city with the training cities (for 03_city_c)
# ---------------------------------------------------------------------------

def compare_to_training(spark, new_city, train_cities=None):
    """What is different about a new city, before explaining its error.

    Uses bronze for the raw columns and silver for values. Returns a dict of
    small tables; display each one.
    """
    train_cities = list(train_cities or config.TRAIN_CITIES)
    # All cities share one bronze table, so a column missing from C's file shows up
    # as a column that is NULL in every row of C.
    bronze = spark.table(config.RAW_TABLE)
    cols = [c for c in bronze.columns if c not in ("city", "snapshot_date", "_ingested_at")]
    counts = (bronze.filter(F.col("city").isin(train_cities + [new_city]))
                    .groupBy("city").agg(*[F.count(F.col(c)).alias(c) for c in cols])
                    .toPandas().set_index("city"))
    raw_cols = {city: {c for c in cols if counts.loc[city, c] > 0} for city in counts.index}
    train_cols = set.union(*[raw_cols[c] for c in train_cities if c in raw_cols])
    raw_cols.setdefault(new_city, set())

    df = load_silver(spark, train_cities + [new_city])
    ab, c = df[df.city.isin(train_cities)], df[df.city == new_city]
    out = {
        "raw_columns_only_in_training (empty in C)": sorted(train_cols - raw_cols[new_city]),
        "raw_columns_only_in_new_city": sorted(raw_cols[new_city] - train_cols),
        "overview": silver_overview(df),
        "why_no_price": why_no_price(df),
    }
    for col in ["room_type", "property_type", "neighbourhood_cleansed"]:
        if col in df:
            seen = set(ab[col].dropna().unique())
            unseen = c.loc[~c[col].isin(seen) & c[col].notna(), col]
            out[f"unseen_{col} (share of C: {len(unseen) / max(len(c), 1):.0%})"] = unseen.value_counts().head(15)
    valid = df[df.is_valid_price.fillna(False).astype(bool)]
    out["price_by_room_type"] = price_summary(valid)[["count", "25%", "50%", "75%", "99%"]]
    out["room_type_mix_%"] = (pd.crosstab(valid["city"], valid["room_type"], normalize="index") * 100).round(1)
    num = [x for x in ["accommodates", "bedrooms", "beds", "bathrooms", "minimum_nights",
                       "availability_365", "number_of_reviews", "review_scores_rating",
                       "host_listings_count", "amenities_count"] if x in valid]
    out["numeric_medians"] = valid.groupby("city")[num].median()
    out["missing_%"] = missing_table(df, [x for x in df.columns if x not in HELPER_COLS])
    cls = classify_price(valid)
    out["price_classes"] = pd.crosstab(cls["city"], cls["price_class"])
    return out
