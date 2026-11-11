"""Split, baseline, models, metrics."""

import json
import os
from functools import reduce

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config, features

NB = "Random forest + neighbourhood names"  # experiment, never the final model


def add_scope(df):
    """in_scope = has a price, not an outlier, not a long stay. Others get an exclusion_reason."""
    reason = (F.when(~F.coalesce("is_valid_price", F.lit(False)), "no_price")
               .when(F.col("is_price_outlier"), "price_outlier")
               .when(F.col("is_long_stay"), "long_stay"))
    return df.withColumn("exclusion_reason", reason).withColumn("in_scope", reason.isNull())


def add_split(df):
    """20% test, 20% validation, 60% train. Picked by a hash of the id, so it never changes."""
    bucket = F.pmod(F.xxhash64(F.col("id"), F.lit(config.SEED)), F.lit(100))
    pct = int(config.TEST_FRACTION * 100)
    return add_scope(df.withColumn("split", F.when(bucket < pct, "test").when(bucket < 2 * pct, "val").otherwise("train")))


def add_baseline(train_rows, d):
    """Median price per (accommodates, room_type), falling back to room type, then overall."""
    med = F.expr("percentile(price, 0.5)")
    both = train_rows.groupBy("accommodates", "room_type").agg(med.alias("m_both"))
    room = train_rows.groupBy("room_type").agg(med.alias("m_room"))
    overall = train_rows.agg(med).first()[0]
    return (d.join(both, ["accommodates", "room_type"], "left").join(room, "room_type", "left")
             .withColumn("baseline_pred", F.coalesce("m_both", "m_room", F.lit(overall)))
             .drop("m_both", "m_room"))


def build(cat, num, estimator, onehot=False):
    """Index categories, (one-hot), assemble features, then the estimator."""
    idx = [c + "_idx" for c in cat]
    stages = [StringIndexer(inputCols=cat, outputCols=idx, handleInvalid="keep")]
    if onehot:
        ohe = [c + "_ohe" for c in cat]
        stages.append(OneHotEncoder(inputCols=idx, outputCols=ohe, handleInvalid="keep"))
        idx = ohe
    stages.append(VectorAssembler(inputCols=idx + num, outputCol="features", handleInvalid="keep"))
    return Pipeline(stages=stages + [estimator])


def candidates(rows):
    """The models to compare: {name: (categorical, numeric, estimator, one-hot)}."""
    cat, num, names = features.CATEGORICAL, features.NUMERIC, features.NEIGHBOURHOOD
    features.check_no_leakage(cat + names + num + features.CAUTION)
    max_bins = max(32, max(rows.agg(*[F.countDistinct(c) for c in cat + names]).first()) + 2)

    def forest():
        return RandomForestRegressor(labelCol="label", numTrees=100, maxDepth=12, subsamplingRate=0.8,
                                     maxBins=max_bins, seed=config.SEED)

    return {"Linear regression": (cat, num, LinearRegression(labelCol="label", regParam=0.01, maxIter=50), True),
            "Random forest": (cat, num, forest(), False),
            "Random forest + availability": (cat, num + features.CAUTION, forest(), False),
            NB: (cat + names, num, forest(), False)}


def scored(fitted, d):
    """Add `pred`: the price (the model predicts log price)."""
    return fitted.transform(d).withColumn("pred", F.exp("prediction"))


def evaluate(d, pred_col, name):
    """MAE, median absolute error and count, per city and for all cities."""
    err = F.abs(F.col("price") - F.col(pred_col))
    agg = [F.avg(err).alias("mae"), F.percentile_approx(err, 0.5).alias("medae"), F.count("*").alias("n")]
    return (d.groupBy("city").agg(*agg).unionByName(d.agg(*agg).withColumn("city", F.lit("all")))
             .withColumn("model", F.lit(name)).select("city", "model", "mae", "medae", "n"))


def score_new_city(spark, fitted, stats, city):
    """Score a city the model never saw. All its listings count as test (split = 'new_city').
    The baseline medians come from the same train + validation rows of A and B as in 02_model."""
    silver = spark.table(config.LISTINGS_TABLE)
    dev = add_split(silver.filter(F.col("city").isin(*config.TRAIN_CITIES))).filter("in_scope AND split != 'test'")
    new = add_scope(silver.filter(F.col("city") == city)).withColumn("split", F.lit("new_city"))
    return scored(fitted, add_baseline(dev, features.make_features(new, stats)))


def evaluate_new_city(d, city):
    """Baseline and model error on the in-scope listings of the new city."""
    d = d.filter("in_scope")
    return (evaluate(d, "baseline_pred", "baseline").unionByName(evaluate(d, "pred", "model"))
             .filter(F.col("city") == city))


def evaluate_all(models, d):
    return reduce(DataFrame.unionByName, [evaluate(scored(m, d), "pred", n) for n, m in models.items()]).toPandas()


def choose_final(val):
    """Lowest validation MAE. No neighbourhood model; availability must be 2% better."""
    mae = val[val.city == "all"].set_index("model")["mae"]
    score = {m: v * (1.02 if "availability" in m else 1) for m, v in mae.items() if m != NB}
    return min(score, key=score.get)


def model_dir():
    return "/Volumes/" + config.VOLUME.replace(".", "/") + "/models"


def save(fitted, stats):
    os.makedirs(model_dir(), exist_ok=True)
    fitted.write().overwrite().save(f"{model_dir()}/price_model")
    json.dump(stats, open(f"{model_dir()}/feature_stats.json", "w"))
    return model_dir()


def load():
    """Returns (model, stats) for a new city."""
    stats = json.load(open(f"{model_dir()}/feature_stats.json"))
    return PipelineModel.load(f"{model_dir()}/price_model"), stats