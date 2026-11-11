# Genie Agent: Airbnb Gold Market Insights and Price Forecasts

Ask the data a question in plain English and get a number back, with the SQL to prove it.

Our Genie Agent sits on top of the three gold tables and knows three cities: **Sydney** and **Melbourne** (the cities the model was trained on) and **Brisbane** (the new client city the model had never seen). Everything is in AUD per night, from the 2026-09-16 snapshot.

| Table | What is in it |
|---|---|
| `airbnb.gold.city_summary` | One row per city: listings, hosts, median price, availability, review score |
| `airbnb.gold.model_metrics` | How good the price model is, per city, against a simple baseline |
| `airbnb.gold.price_predictions` | One row per listing: actual price, model prediction, error |

## What we told Genie

This is the text we pasted into the Genie instructions. It is short on purpose: a few clear rules work better than a long essay.

```text
You answer questions about Airbnb listings in Australia: Sydney and Melbourne (training cities) and Brisbane (new city). All prices are in AUD per night, from the 2026-09-16 snapshot.

Tables:
- city_summary: one row per city (listings, in_scope_listings, median_price, median_price_entire_home, avg_availability_365, avg_review_score, hosts, pct_entire_home, pct_no_price).
- model_metrics: price model quality per city and for "all". Rows with model = 'baseline' (median price by room type and guests) and model = 'model'. mae and medae are in AUD. If a column stage exists: stage = 'test' for Sydney and Melbourne, stage = 'first_run' for Brisbane.
- price_predictions: one row per listing with price, model_pred, baseline_pred, error, abs_error, room_type, neighbourhood_cleansed, accommodates, split, in_scope.

Rules:
1. For model quality, errors or predictions in Sydney and Melbourne, always filter split = 'test' AND in_scope = true. Other rows were used for training and give optimistic errors. For Brisbane use split = 'new_city' AND in_scope = true (the model never saw Brisbane).
2. For questions about prices, medians or listings in a city, filter in_scope = true. Listings with no price or outliers are excluded.
3. error = model_pred - price. Negative error means the model UNDERPRICES; positive means it OVERPRICES.
4. Use the median, not the average, for prices (prices are skewed).
5. City values are lowercase: 'sydney', 'melbourne', 'brisbane'. Room types: 'Entire home/apt', 'Private room', 'Shared room', 'Hotel room'.
6. Ignore groups with fewer than 30 listings when ranking neighbourhoods, and say so.
7. Always state the currency (AUD) and say whether numbers come from the test set.
```

### Why these rules exist (the tricks)

- **Test rows only.** The predictions table also holds the rows the model trained on, and its error there looks too good. Without rule 1, Genie would happily report a flattering number.
- **Brisbane is different.** The model never saw it, so its honest rows are `split = 'new_city'`, not `'test'`.
- **The sign of `error`.** "Underprices" only means something once you say which way the minus points, so we spell it out.
- **Medians, not averages.** A few very expensive listings drag an average far from what a typical guest pays.
- **At least 30 listings.** Tiny groups give wild averages, so Genie skips them and says so.

## Questions to try

1. What is the median price of entire homes in Sydney?
2. Which neighbourhoods in Melbourne have the most listings?
3. Where does the model underprice listings the most?
4. How much better is the model than the baseline in each city?
5. Which room type has the largest model error in Sydney?
6. Compare median price and availability between Sydney and Melbourne.

## The SQL we gave as examples

**Median price of entire homes in Sydney**

```sql
SELECT ROUND(PERCENTILE(price, 0.5), 0) AS median_price_aud, COUNT(*) AS listings
FROM airbnb.gold.price_predictions
WHERE city = 'sydney' AND room_type = 'Entire home/apt' AND in_scope = true;
```

**Melbourne neighbourhoods with the most listings**

```sql
SELECT neighbourhood_cleansed AS neighbourhood, COUNT(*) AS listings
FROM airbnb.gold.price_predictions
WHERE city = 'melbourne' AND in_scope = true
GROUP BY neighbourhood_cleansed
ORDER BY listings DESC
LIMIT 10;
```

**Where the model underprices the most**

```sql
SELECT city, neighbourhood_cleansed AS neighbourhood,
       COUNT(*) AS listings, ROUND(AVG(error), 1) AS avg_error_aud
FROM airbnb.gold.price_predictions
WHERE split = 'test' AND in_scope = true
GROUP BY city, neighbourhood_cleansed
HAVING COUNT(*) >= 30
ORDER BY avg_error_aud ASC
LIMIT 10;
```

**Model against baseline, per city**

```sql
SELECT city,
       ROUND(MAX(CASE WHEN model = 'baseline' THEN mae END), 1) AS baseline_mae_aud,
       ROUND(MAX(CASE WHEN model = 'model' THEN mae END), 1) AS model_mae_aud,
       ROUND((MAX(CASE WHEN model = 'baseline' THEN mae END)
            - MAX(CASE WHEN model = 'model' THEN mae END))
            / MAX(CASE WHEN model = 'baseline' THEN mae END) * 100, 1) AS improvement_pct
FROM airbnb.gold.model_metrics
WHERE stage IN ('test', 'first_run')
GROUP BY city
ORDER BY city;
```

**Largest model error by room type in Sydney**

```sql
SELECT room_type, COUNT(*) AS listings,
       ROUND(AVG(abs_error), 1) AS mae_aud, ROUND(AVG(error), 1) AS bias_aud
FROM airbnb.gold.price_predictions
WHERE city = 'sydney' AND split = 'test' AND in_scope = true
GROUP BY room_type
HAVING COUNT(*) >= 30
ORDER BY mae_aud DESC;
```

**Sydney against Melbourne: price and availability**

```sql
SELECT city, median_price AS median_price_aud,
       median_price_entire_home AS median_entire_home_aud,
       avg_availability_365 AS avg_availability_days
FROM airbnb.gold.city_summary
WHERE city IN ('sydney', 'melbourne')
ORDER BY city;
```

## Does it give the right answers?

These are the numbers we know are right, taken from the dashboard and the gold tables. Ask Genie each question and write down what it says in the last column.


| Question | Expected | What Genie answered | Match |
|---|---|---|---|
| Median price of entire homes, Sydney | about 396-397 AUD | 397 AUD per night, from 12,762 in-scope listings | yes |
| Melbourne neighbourhoods with the most listings | top of the dashboard list | "Melbourne" 6,488, Port Phillip 1,756, Yarra 1,082, then Yarra Ranges 955, Stonnington 785, Wyndham 675 | yes |
| Model vs baseline, Sydney | MAE 189.8 to 137.7 (-27%) | 189.8 to 137.7 (-27.4%) | yes |
| Model vs baseline, Melbourne | MAE 107.3 to 91.4 (-15%) | 107.3 to 91.4 (-14.8%) | yes |
| Model vs baseline, Brisbane | MAE 84.5 to 80.3 (-5%) | 84.5 to 80.3 (-4.9%) | yes |
| Model vs baseline, Sydney + Melbourne | MAE 144.4 to 112.2 (-22%) | 144.4 to 112.2 (-22.3%) | yes |
| Largest model error by room type, Sydney | Entire home/apt: MAE 161, bias -99 | Entire home/apt MAE 161.2 vs 44.4 for private rooms; bias -99.2 and -23.1 | yes |
| Median price, Sydney / Melbourne | 347 / 263 AUD | 347 vs 263 AUD, a gap of 84 AUD (32%); entire homes 397 vs 301, private rooms 131 vs 109 | yes |
| Average availability, Sydney / Melbourne | 198 / 218 days | Melbourne 217.9 days, Sydney 198.3 days (a difference of 19.6 days) | yes |
| Where does the model underprice the most? | worst neighbourhoods, test set, 30+ listings | Woollahra -375 AUD, Pittwater -272, Mosman -227, Manly -167, Waverley -144; nine of the top 10 are in Sydney (Bayside, Melbourne, -131 is the only exception) | yes |

#