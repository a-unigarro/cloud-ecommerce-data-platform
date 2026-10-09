from pyspark.sql.types import StringType, StructField, StructType, TimestampType

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
])



from pyspark.sql.types import (DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

S, I, TS, MONEY = StringType(), IntegerType(), TimestampType(), DecimalType(12, 2)


def schema(*fields) -> StructType:
    # Nullable everywhere: Spark ignores nullability when reading CSV anyway.
    # Real "must not be null" rules belong in the data-quality checks.
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# Column names are exactly as in the raw CSV headers (including Olist's typos);
# renaming happens in the cleaning step.
ORDERS_SCHEMA = schema(
    ("order_id", S), ("customer_id", S), ("order_status", S),
    ("order_purchase_timestamp", TS), ("order_approved_at", TS),
    ("order_delivered_carrier_date", TS), ("order_delivered_customer_date", TS),
    ("order_estimated_delivery_date", TS)
    )

CUSTOMERS_SCHEMA = schema(
    ("customer_id", S), ("customer_unique_id", S), ("customer_zip_code_prefix", S),
    ("customer_city", S), ("customer_state", S)
    )

ORDER_ITEMS_SCHEMA = schema(
    ("order_id", S), ("order_item_id", I), ("product_id", S), ("seller_id", S),
    ("shipping_limit_date", TS), ("price", MONEY), ("freight_value", MONEY)
    )

PAYMENTS_SCHEMA = schema(
    ("order_id", S), ("payment_sequential", I), ("payment_type", S),
    ("payment_installments", I), ("payment_value", MONEY)
    )

REVIEWS_SCHEMA = schema(
    ("review_id", S), ("order_id", S), ("review_score", I),
    ("review_comment_title", S), ("review_comment_message", S),
    ("review_creation_date", TS), ("review_answer_timestamp", TS)
    )

PRODUCTS_SCHEMA = schema(
    ("product_id", S), ("product_category_name", S),
    ("product_name_lenght", I), ("product_description_lenght", I),
    ("product_photos_qty", I), ("product_weight_g", I),
    ("product_length_cm", I), ("product_height_cm", I), ("product_width_cm", I)
    )

SELLERS_SCHEMA = schema(
    ("seller_id", S), ("seller_zip_code_prefix", S), ("seller_city", S), ("seller_state", S)
    )

CATEGORY_TRANSLATION_SCHEMA = schema(
    ("product_category_name", S), ("product_category_name_english", S)
    )

