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
| model on C | yes | MAE 80.3 AUD (baseline 84.5) on 5 644 in-scope listings; details in section 4 |

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
| price outliers | 91 | 103 | 46 |
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
| 1 | Neighbourhood: 100% of Brisbane listings are in neighbourhoods the model never saw | Nothing. The model never used neighbourhood names. The experiment in `02_model` shows what that costs in A and B: MAE 112.2 -> 101.1 (-10%) with names | neighbourhood names are city-specific; the portable location features are distance to the centre and density | model change (none needed) |
| 2 | 5 listings with an unseen `property_type` | Nothing. Property types with fewer than 100 training rows are grouped as `Other`, and the encoder keeps unseen values | the 5 listings are under 0.1% of the city | model change (none needed) |
| 3 | Hotel rooms and shared rooms: 5 priced listings each | Nothing. Errors are reported with n = 5 (hotel room MAE 101, shared room MAE 59) and not interpreted | too few to learn or check a separate price level | model change (none needed) |

Experiments run on city C (notebook `03_city_c`, section 2). Rule: models fitted on the A+B train rows only, compared on A+B validation and on Brisbane; the seed alone moves Brisbane MAE by about ±0.5 AUD, so a difference below ~1 AUD is noise.

| # | Experiment | MAE, A+B validation / Brisbane | Result | Kind |
|---|---|---|---|---|
| 4 | Distance to the centre: as is, as a within-city percentile, removed, removed together with density | as is 120.1 / 81.4; percentile 119.9 / 81.3; no distance 121.5 / 81.6; no distance and no density 125.1 / 80.5 | No variant helps Brisbane by more than ~1 AUD. Dropping both features costs 4% in A and B. Not adopted | model change, tested, not adopted |
| 5 | Leave one feature out (13 features and `property_group`) | see the notebook | Almost every feature helps or is neutral in Brisbane. `host_tenure_months` helps most (+4.0 AUD when removed); `has_reviews` has no effect. Only `calculated_host_listings_count` looked better without (-1.9 AUD in Brisbane, +0.8 in A and B). There was a reason in the data: Brisbane has 2.3 listings per host against 1.8 and 2.1 | model change, tested |
| 6 | `calculated_host_listings_count` removed, 3 seeds | as is 119.8 / 79.8; removed 120.6 / 79.6 | The gain was 0.2 AUD in Brisbane, and the cost 0.8 AUD in A and B on every seed. The -1.9 was seed noise. Not adopted | model change, tested, not adopted |
| 7 | Extra features: beds, maximum_nights, is_shared_bath, is_superhost, is_verified, review sub-scores (location, cleanliness, value) | A+B validation: as is 120.1, with extra features 119.4 (Sydney 151.8, Melbourne 90.3) | The gain is 0.7 AUD (0.6%), inside the noise (~1 AUD) and far below the 2% we require. Not adopted | model change, tested, not adopted |
| 8 | `has_reviews` removed | A+B validation: as is 120.1, removed 119.9 | No effect (0.2 AUD, noise): `number_of_reviews = 0` already carries the same information. Kept as is, because the week-1 model is frozen | model change, tested, not adopted |

**No model change was adopted.** The model on Brisbane after changes is the first-run model (80.3). Choosing by the Brisbane result would have been tuning on city C, so an "after changes" number would be slightly optimistic. We state this and made no such change.

What did not transfer and could not be fixed without Brisbane prices (a price level the model has not seen):

- **`home`** (1 667 listings, 30% of the city, 37% of the total error): over-priced by 21 AUD on average, and worse than the baseline (MAE 100.8 against 95.4).
- **Hotel-like types** (hotel, boutique hotel, serviced apartment, loft, villa; 234 listings): under-priced by 53 to 170 AUD.
- **Moreton Island** (46 holiday homes, 41-55 km from the centre): under-priced by 278 AUD on average. Distance to the centre means something else on an island: far is not cheap. Only ~3% of the total error.

Found in week 1, before city C:

| What broke | What we changed | Kind |
|---|---|---|
| Volume `raw_files` did not exist; `os.makedirs` cannot create a Volume (Errno 95) | `CREATE VOLUME IF NOT EXISTS` in `bronze.py` | pipeline fix |
| New silver columns: Delta refused to change the schema with `replaceWhere` | recreated `silver.listings` | pipeline fix |
| Outlier rule 1.5×IQR per city flagged 777 rows | aligned with EDA: 3×IQR per city and room type (194 rows) | pipeline fix |

## 4. The three numbers

| | MAE, AUD |
|---|---|
| Baseline on Brisbane | 84.5 |
| Model on Sydney + Melbourne (test set) | 112.2 (baseline 144.4) |
| Model on Brisbane, first run | 80.3 |
| Model on Brisbane, after changes | 80.3 (no change adopted) |

Median absolute error: baseline on Brisbane 53.9; model on Sydney + Melbourne 57.9 (baseline 70.5); model on Brisbane 49.3. All numbers use in-scope listings (has a price, not a price outlier, not a long stay). For Brisbane that is 5 644 listings, none of them seen by the model; for Sydney and Melbourne it is the 20% test set (7 079 listings).

Why the gap:

1. **The absolute MAE is not comparable across the cities.** Brisbane prices are less spread out: the 99th percentile of entire-home prices is 1 312 AUD, against 1 705 in Melbourne and 3 881 in Sydney. Because of that, even the baseline is much better in Brisbane (84.5 against 144.4). The model on Brisbane looks better than on A and B (80.3 against 112.2), but that says little about transfer.
2. **Against the baseline, the model transfers badly.** It beats the baseline by 22% in Sydney + Melbourne (MAE), and by 5% in Brisbane (4.9%). On the median error it is 18% and 8.5%.
3. **The advantage in A and B comes from location that does not travel.** With neighbourhood names the model is another 10% better in A and B (test MAE 112.2 -> 101.1). The only location features that we can carry to a new city are distance to the centre and a density percentile, and they describe the geography of each city. Removing both costs 4% in A and B (validation 120.1 -> 125.1) and changes Brisbane by about 1 AUD (81.4 -> 80.5). In Brisbane the model gets its gain from size features only.
4. **The error is not a shift in price level.** The average bias in Brisbane is about +5 AUD. The error is in segments: `home` is over-priced and the hotel-like types are under-priced (section 3).
5. **It is not the pipeline and not missing values.** The pipeline ran without a failure, and the share of missing values is nearly the same in all three cities.

A large first-run error that we can explain is the result we report: in Brisbane the model is only 5% better than a median per room type and number of guests, and the three experiments above did not find a change that improves this in a way we can defend.