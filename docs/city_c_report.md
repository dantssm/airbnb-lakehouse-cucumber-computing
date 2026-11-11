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
| median price | | | |

## 3. Fixes

Kind: **pipeline fix** = the pipeline should have handled it from the start (an unexpected value, a schema difference, a failed download). **model change** = the model did not transfer (a feature that means something different in C, a price level it never saw).

| # | What broke | What we changed | Why | Kind |
|---|---|---|---|---|
| 1 | | | | pipeline fix |

Found in week 1, before city C (for the record, not part of the C story):

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
