# City C: Brisbane

## Why Brisbane


Snapshot: `2026-09-16` (Inside Airbnb, `australia/qld/brisbane`), the same date as Sydney and Melbourne.

## 1. First run, unchanged

Code: tag `week1-final`. The only change before the run: the `brisbane` line in `config.CITIES` (adding a city is a config line, not code).

| Step | Ran? | What happened |
|---|---|---|
| bronze | yes | downloaded 3.9 MB; 6 490 rows, 93 columns (same columns as A and B), 0 rows with a non-numeric id |
| silver | yes | 6 490 rows, 0 duplicates, no missing columns, no values that failed to convert |
| validation | yes, with a warning | 8 entirely NULL columns, the same 8 as Melbourne; 0 unseen room types; 773 listings without a price (~12%) |
| model on C | | MAE on C (filled in by P3) |

The pipeline ran on Brisbane without errors and without any change to the code. No pipeline step failed.

What the first run did show:

- **Small room-type groups.** Brisbane has fewer than 30 priced *Hotel room* and *Shared room* listings, so their outlier fence fell back to the city-wide one (3 719.63 AUD). In Sydney and Melbourne every room type had its own fence. The fallback rule worked as designed, but a shared room is flagged only above 3 719 AUD, so outlier flags for these two types are weak in Brisbane.
- **A much smaller city.** 6 490 listings against 20 833 (Sydney) and 25 715 (Melbourne).
- **Fewer listings without a price:** ~12%, against ~15% and ~26%.

Pipeline output, as printed:

```
[bronze] downloaded https://data.insideairbnb.com/australia/qld/brisbane/2026-09-16/data/listings.csv.gz -> /Volumes/airbnb/bronze/raw_files/brisbane/2026-09-16/listings.csv.gz (3.9 MB)
[bronze] brisbane: 6490 rows, 93 columns, 0 rows with a non-numeric id
[silver] {'city': 'brisbane', 'n_bronze': 6490, 'n_silver': 6490, 'n_duplicates_removed': 0, 'missing_columns': [], 'cast_failures': {}, 'price_outlier_fences': {'Entire home/apt': 2101.31, 'Private room': 929.11, 'Hotel room': 3719.63, 'Shared room': 3719.63, '_city': 3719.63}}
[validation] WARNING brisbane: columns that are entirely NULL: ['host_acceptance_rate', 'host_response_rate', 'host_response_time', 'host_since', 'host_tenure_days', 'host_total_listings_count', 'instant_bookable', 'license']
[validation] brisbane: bronze=6490, silver=6490, dropped: no id=0, duplicate id=0
{'rows_lost_unexplained': 0, 'duplicate_id': 0, 'price_missing': 773, 'price_not_positive': 0, 'room_type_missing': 0, 'accommodates_missing_or_not_positive': 0, 'bad_coordinates': 0, 'availability_365_out_of_range': 0, 'review_score_out_of_range': 0, 'room_type_unseen': 0, 'all_null_columns': 8}
```

## 2. How Brisbane differs from Sydney and Melbourne


| | Sydney | Melbourne | Brisbane |
|---|---|---|---|
| listings | 20 833 | 25 715 | 6 490 |
| without price | 3 112 (~15%) | 6 711 (~26%) | 773 (~12%) |
| price outliers | 91 | 103 | |
| entirely NULL columns | 7 | 8 | 8 |
| outlier fence, entire home/apt (AUD) | 5 031 | 2 395 | 2 101 |
| outlier fence, private room (AUD) | 1 368 | 943 | 929 |
| distinct hosts | 9 769 | 13 999 | 2 782 |
| median price, entire home/apt (AUD) | 394.0 | 301.7 | 326.5 |
| median price, private room (AUD) | 124.7 | 109.0 | 122.5 |
| priced hotel rooms / shared rooms | 53 / 43 | 44 / 203 | 5 / 5 |
| room-type mix: entire / private (%) | 78.4 / 21.0 | 76.3 / 22.4 | 77.0 / 22.8 |
| median accommodates / bedrooms / minimum_nights | 4 / 2 / 2 | 4 / 2 / 2 | 4 / 1 / 1 |
| median availability_365 | 197 | 240 | 241 |
| long-stay listings (EDA price classes) | 1 722 | 109 | 28 |

What is the same:

- **Raw columns.** No column in Brisbane that A and B do not have. The only column filled in training but empty in Brisbane is `license`, and it is empty in Melbourne too.
- **Room types.** No new room type, and the mix is almost identical (~77% entire homes).
- **Price level.** Brisbane's median prices sit between Melbourne and Sydney for both main room types, so the model is not asked to predict a price level it never saw.
- **Why a price is missing.** Same pattern as A and B: listings without a price are inactive (availability_365 = 0 for 87%, no reviews in the last year for 53%).

What is different:

1. **Every neighbourhood is new.** 100% of Brisbane listings are in a `neighbourhood_cleansed` value the model never saw (Brisbane City, South Brisbane, Fortitude Valley, ...). Neighbourhood names are city-specific, so this was certain to happen. Any feature built on the neighbourhood name carries no information in Brisbane.
2. **Hotel rooms and shared rooms are almost absent:** 5 priced listings of each. Their baseline and model errors in Brisbane rest on very few listings.
3. **5 listings with an unseen `property_type`:** Shipping container (3), Private room in island (1), Private room in minsu (1). Under 0.1% of the city.
4. **Fewer long stays** (28 against 1 722 in Sydney) and fewer listings without a price (~12%).

## 3. Fixes

Kind: **pipeline fix** = the pipeline should have handled it from the start (an unexpected value, a schema difference, a failed download). **model change** = the model did not transfer (a feature that means something different in C, a price level it never saw).

**Pipeline fixes: none needed.** The unchanged pipeline loaded, cleaned and checked Brisbane without an error, with the same 93 raw columns, no failed type conversions and no lost rows. The week-1 fixes (Volume creation, typed and flagged silver, validation warnings for empty columns and unseen room types) are what made that possible.

**Model changes**:

| # | What did not transfer | What we changed | Why | Kind |
|---|---|---|---|---|
| 1 | Neighbourhood: 100% of Brisbane listings are in neighbourhoods the model never saw | | neighbourhood names are city-specific | model change |
| 2 | 5 listings with an unseen `property_type` | | the model has no category for them | model change |
| 3 | Hotel rooms and shared rooms: 5 priced listings each | | too few to learn or check a separate price level | model change |

Found in week 1, before city C:

| What broke | What we changed | Kind |
|---|---|---|
| Volume `raw_files` did not exist; `os.makedirs` cannot create a Volume (Errno 95) | `CREATE VOLUME IF NOT EXISTS` in `bronze.py` | pipeline fix |
| New silver columns: Delta refused to change the schema with `replaceWhere` | recreated `silver.listings` | pipeline fix |
| Outlier rule 1.5×IQR per city flagged 777 rows | aligned with EDA: 3×IQR per city and room type (194 rows) | pipeline fix |

## 4. The three numbers

| | MAE, AUD |
|---|---|
| Baseline on Brisbane | |
| Model on Sydney + Melbourne (test set) | |
| Model on Brisbane, first run | |
| Model on Brisbane, after changes | |

Why the gap:
