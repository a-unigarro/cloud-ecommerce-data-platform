# Data dictionary

Describes the source datasets (Olist e-commerce data and the Frankfurter exchange-rate API) and what the processed layer changes. Column names below are the **processed** names; differences from raw are listed explicitly.

## Layers

| Layer | Location | Content |
|---|---|---|
| Landing | `data/landing/olist/` | Original CSV download, never modified |
| Raw | `data/raw/<source>/<dataset>/ingestion_date=YYYY-MM-DD/` | Unmodified copy (CSV / JSON) plus a manifest |
| Processed | `data/processed/<source>/<dataset>/ingestion_date=YYYY-MM-DD/` | Typed, cleaned Parquet plus a manifest |

Every processed table has one extra column, `ingestion_date` (date): the day the data was ingested, not a business date.

## Olist tables

Row counts are from the initial inspection of the full dataset.

### customers (99,441 rows)

One row per order-specific customer record. `customer_id` is generated per order, so the same person can appear under several `customer_id` values. Use `customer_unique_id` to identify a person across orders.

| Column | Type | Notes |
|---|---|---|
| customer_id | string | Key; joins to `orders.customer_id` |
| customer_unique_id | string | Identifies the real customer across orders |
| customer_zip_code_prefix | string | Kept as text so leading zeros survive |
| customer_city | string | Lowercased |
| customer_state | string | Uppercased, two-letter state code |

### orders (99,441 rows)

| Column | Type | Notes |
|---|---|---|
| order_id | string | Key |
| customer_id | string | Joins to `customers` |
| order_status | string | Lowercased (for example delivered, shipped, canceled) |
| order_purchase_timestamp | timestamp | |
| order_approved_at | timestamp | Null for 160 orders |
| order_delivered_carrier_date | timestamp | Null for 1,783 orders |
| order_delivered_customer_date | timestamp | Null for 2,965 orders |
| order_estimated_delivery_date | timestamp | |

The nulls are real business information (orders that were canceled, unavailable or not yet delivered) and are intentionally kept.

### order_items (112,650 rows)

One row per item in an order.

| Column | Type | Notes |
|---|---|---|
| order_id | string | Joins to `orders` |
| order_item_id | int | Sequence number of the item within the order |
| product_id | string | Joins to `products` |
| seller_id | string | Joins to `sellers` |
| shipping_limit_date | timestamp | |
| price | decimal(12,2) | Exact decimal, not float |
| freight_value | decimal(12,2) | Exact decimal, not float |

### payments (103,886 rows)

An order can have several payment rows (different methods or installments).

| Column | Type | Notes |
|---|---|---|
| order_id | string | Joins to `orders` |
| payment_sequential | int | Sequence of payments within the order |
| payment_type | string | Lowercased |
| payment_installments | int | |
| payment_value | decimal(12,2) | Exact decimal, not float |

### reviews (99,224 rows)

| Column | Type | Notes |
|---|---|---|
| review_id | string | |
| order_id | string | Joins to `orders` |
| review_score | int | |
| review_comment_title | string | Often empty (null after processing) |
| review_comment_message | string | Free text; can contain line breaks and quotes |
| review_creation_date | timestamp | |
| review_answer_timestamp | timestamp | |

Because comments contain line breaks, a physical line count of the CSV does not equal the row count. Ingestion and transformation both parse the CSV properly.

### products (32,951 rows)

| Column | Type | Notes |
|---|---|---|
| product_id | string | Key |
| product_category_name | string | In Portuguese; translate via `category_translation` |
| product_name_length | int | **Renamed** from `product_name_lenght` |
| product_description_length | int | **Renamed** from `product_description_lenght` |
| product_photos_qty | int | |
| product_weight_g | int | |
| product_length_cm | int | |
| product_height_cm | int | |
| product_width_cm | int | |

### sellers (3,095 rows)

| Column | Type | Notes |
|---|---|---|
| seller_id | string | Key |
| seller_zip_code_prefix | string | Kept as text |
| seller_city | string | Lowercased |
| seller_state | string | Uppercased |

### category_translation (71 rows)

| Column | Type | Notes |
|---|---|---|
| product_category_name | string | Portuguese name; joins to `products` |
| product_category_name_english | string | |

### geolocation (1,000,163 rows, optional)

Optional enrichment (zip code prefixes with coordinates). It is ingested into raw but has no processed transformation yet, and nothing in the core pipeline depends on it.

## Relationships

```
customers.customer_id        -> orders.customer_id
orders.order_id              -> order_items.order_id
orders.order_id              -> payments.order_id
orders.order_id              -> reviews.order_id
order_items.product_id       -> products.product_id
order_items.seller_id        -> sellers.seller_id
products.product_category_name -> category_translation.product_category_name
```

Consistency checks on these relationships (every `order_items.order_id` exists in `orders`, and so on) belong to the data-quality milestone.

## Exchange rates (Frankfurter API v2)

Long format: one row per date and currency, base BRL. The Olist period (2016-09-01 to 2018-10-31) has 791 days and 1,582 rows for two currencies (EUR and USD). The API returns every calendar day, including weekends.

| Column | Type | Notes |
|---|---|---|
| rate_date | date | **Renamed** from the API's `date`, a reserved word in SQL |
| base | string | Uppercased; always BRL here |
| quote | string | Uppercased; EUR or USD |
| rate | decimal(18,8) | Units of the quote currency per 1 BRL |

## Processing rules

- Strings are trimmed; empty strings become null.
- Selected text columns are lowercased or uppercased (see tables above).
- Rows are never dropped and nulls are never filled.
- Raw keeps the source's original names, including typos; renaming happens only in processed.
- Reading is strict: headers must match the expected schema, and values that do not fit a type fail the job.
- Row counts are reconciled against the raw manifest before anything is written.

## Known caveats

- **Timezones.** Olist timestamps are Brazilian local time with no timezone attached. Spark's session timezone is UTC, so values are displayed unchanged, but they are not true UTC. Do not treat them as UTC when joining with other sources.
- **Snapshots.** Each `ingestion_date` partition is a full snapshot. Two ingestion dates mean two copies of every table; analytical models must select one (normally the latest completed date).
- **Completion markers.** A date is complete only if its `_manifests/ingestion_date=.../manifest.json` exists. Folders without one must be ignored.
