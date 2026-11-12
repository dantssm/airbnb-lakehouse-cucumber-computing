# Airbnb Price Lakehouse — Cucumber Computing

Group Assignment 2. A Databricks lakehouse on Inside Airbnb data: one pipeline for every city, a price model trained on two cities, and an honest test on a third city the model never saw.

**Presentation:** [Canva](https://canva.link/pde2hfwoveoe67o) · team and responsibilities in [`docs/presentation.md`](docs/presentation.md)

## Region and cities

Region: **Australia**, prices in **AUD** per night. Data: `listings.csv.gz` (detailed listings) from [Inside Airbnb](https://insideairbnb.com/get-the-data/).

| City | Role | Inside Airbnb path | Snapshot | Listings |
|---|---|---|---|---|
| Sydney | A, training | `australia/nsw/sydney` | 2026-09-16 | 20 833 |
| Melbourne | B, training | `australia/vic/melbourne` | 2026-09-16 | 25 715 |
| Brisbane | C, new city | `australia/qld/brisbane` | 2026-09-16 | 6 490 |

## Results

MAE in AUD on in-scope listings (has a price, not a price outlier, not a long stay).

| | Baseline | Model |
|---|---|---|
| Sydney + Melbourne, test set (7 079 listings) | 144.4 | **112.2** (−22%) |
| Brisbane, first run (5 644 listings) | 84.5 | **80.3** (−5%) |
| Brisbane, after changes | — | 80.3 (no change adopted) |

Baseline: the median price for the same room type and number of guests, from the Sydney and Melbourne training rows. The pipeline ran on Brisbane without a single fix; the model's edge over the baseline fell from 22% to 5%. Why, and what we tried: [`docs/city_c_report.md`](docs/city_c_report.md).

## Architecture

Medallion layers in Unity Catalog (`airbnb.bronze`, `airbnb.silver`, `airbnb.gold`). Diagram and description: [`docs/architecture.md`](docs/architecture.md).

## Repository

```
src/airbnb/
  config.py         catalog, tables, cities: adding a city is one line here
  bronze.py         download listings.csv.gz to the Volume, load as text
  silver.py         types, dedup, flags (price outliers, long stays)
  validation.py     checks on every run: lost rows, duplicates, empty columns, unseen room types
  orchestrator.py   run_city(city): bronze -> silver -> validation
  eda.py            EDA helpers, compare a new city to the training cities
  features.py       model features and the leakage guard
  model.py          split, baseline, models, metrics
  gold.py           gold tables with Unity Catalog comments
notebooks/
  00_run_pipeline   the job entry point, one city per run (widget `city`)
  01_eda_leakage    EDA and leakage review, Sydney and Melbourne
  02_model          baseline, model choice, test set, gold tables
  03_city_c         Brisbane: first run, comparison, experiments
docs/
  architecture.md   lakehouse diagram and layers
  city_c_report.md  what broke on city C, what we changed, the three numbers
  genie.md          Genie space: instructions and example questions
  presentation.md   presentation link, team and responsibilities
Airbnb Australia: market and price model.lvdash.json   the AI/BI dashboard
```

## How to run

1. Create the catalog, schemas and Volume once: `config.ensure_schemas(spark)`.
2. Run the Databricks Job `airbnb_etl` (or `notebooks/00_run_pipeline`) once per city, with `city` = `sydney`, `melbourne`, `brisbane`.
3. Run `01_eda_leakage`, then `02_model` (trains on Sydney and Melbourne, writes gold).
4. Run `03_city_c` to score Brisbane. The first run uses the code tagged `week1-final`.
5. Import the `.lvdash.json` dashboard; set up Genie as in `docs/genie.md`.