# ecommerce_analytics — dbt Project

Analytics engineering project for a SaaS e-commerce platform. Built with **dbt Core** on **Snowflake** (Postgres-compatible for local dev).

## DAG Overview

```
Raw Layer (Snowflake RAW schema)
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│  STAGING  (views — 1:1 with source tables)              │
│                                                         │
│  stg_orders   stg_customers   stg_products              │
│  stg_order_items   stg_subscriptions                    │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  INTERMEDIATE  (ephemeral — business logic, joins)      │
│                                                         │
│  int_orders_enriched   int_customer_lifetime            │
└────────────────────────┬────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────┐
│  MARTS  (tables/incremental — ready for BI tools)       │
│                                                         │
│  core/                 finance/                         │
│  ├─ fct_orders         └─ fct_revenue_daily             │
│  └─ dim_customers                                       │
└─────────────────────────────────────────────────────────┘
```

## Layer Conventions

| Layer | Materialisation | Purpose |
|---|---|---|
| `staging/` | view | Clean & rename raw source columns, cast types, no joins |
| `intermediate/` | ephemeral | Business logic, multi-table joins, pre-aggregations |
| `marts/core/` | table | Wide facts & dimensions consumed by BI |
| `marts/finance/` | incremental | High-volume aggregations updated daily |

## Local Setup (dbt-core + Postgres)

```bash
# 1. Create a virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dbt
pip install dbt-core dbt-postgres   # or dbt-snowflake for prod

# 3. Copy and edit profiles
cp profiles.yml.example ~/.dbt/profiles.yml
# Edit the dev target with your local Postgres credentials

# 4. Install dbt packages
dbt deps

# 5. Verify connection
dbt debug
```

## Running the Project

```bash
# Full run (all models)
dbt run

# Run only mart layer
dbt run --select marts

# Run a single model and its downstream dependencies
dbt run --select fct_orders+

# Run models modified since last state (CI-friendly)
dbt run --select state:modified+

# Run with full refresh (forces table rebuild)
dbt run --full-refresh --select fct_orders
```

## Testing

```bash
# Run all tests
dbt test

# Run tests for a specific model
dbt test --select stg_orders

# Run only schema tests (generic)
dbt test --select test_type:generic

# Run only data tests (singular)
dbt test --select test_type:singular
```

## Documentation

```bash
# Generate docs site
dbt docs generate

# Serve locally at http://localhost:8080
dbt docs serve
```

## Seeds

```bash
# Load static reference tables (e.g. country_codes)
dbt seed
```

## Design Decisions

### Surrogate Keys
All fact and dimension tables use `dbt_utils.generate_surrogate_key()` to produce MD5-based surrogate keys from natural keys. This decouples the warehouse layer from upstream ID changes and makes joins deterministic across environments.

### Incremental Models
`fct_orders` and `fct_revenue_daily` use the `incremental` materialisation with `unique_key` to support idempotent upserts. The filter `_dbt_max_partition` is applied on `updated_at` so Snowflake can prune micro-partitions efficiently.

### Ephemeral Intermediate Layer
Intermediate models are `ephemeral` — they compile into CTEs within the mart query rather than materialising physical tables. This keeps the warehouse clean while still allowing logical separation of concerns.

### Source Freshness
All sources declare `freshness` thresholds. Running `dbt source freshness` will warn if data is older than 24 h and error if older than 48 h, enabling proactive alerting in CI.

### Schema Isolation per Environment
The custom `generate_schema_name` macro (see `macros/`) ensures dev runs write to a personal schema (`dbt_<user>`) while production writes to shared analytics schemas. This prevents dev pollution of prod tables.

### Accepted Values & Relationship Tests
Every foreign key has a `relationships` test back to the parent model. Categorical columns (status, country_code, category) have `accepted_values` tests to catch upstream enum changes early.

## Project Structure

```
ecommerce_analytics/
├── models/
│   ├── staging/
│   │   ├── _sources.yml
│   │   ├── _staging__models.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   └── stg_order_items.sql
│   ├── intermediate/
│   │   ├── int_orders_enriched.sql
│   │   └── int_customer_lifetime.sql
│   └── marts/
│       ├── core/
│       │   ├── fct_orders.sql
│       │   └── dim_customers.sql
│       └── finance/
│           └── fct_revenue_daily.sql
├── macros/
│   ├── generate_schema_name.sql
│   └── cents_to_euros.sql
├── seeds/
│   └── country_codes.csv
├── tests/
│   └── generic/
│       └── test_positive_amount.sql
├── dbt_project.yml
├── packages.yml
└── profiles.yml.example
```
