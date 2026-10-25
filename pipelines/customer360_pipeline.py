"""Customer 360 Lakehouse — Databricks Lakeflow Pipeline.

End-to-end declarative pipeline built on the public Olist e-commerce dataset.
It implements Bronze/Silver/Gold processing, Lakeflow data-quality
expectations, a unified Customer 360 model, SCD Type 2 history using AUTO
CDC FROM SNAPSHOT, and incremental file ingestion using Auto Loader.

The code intentionally keeps the portfolio Auto Loader demonstration separate
from the production Gold dependency path so that the validated Gold dataset
remains based on the original Olist order source.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ============================================================
# BRONZE LAYER — BATCH INGESTION
# Raw Olist CSV files are materialized with minimal transformation.
# ============================================================

@dp.materialized_view(
    name="customers_bronze",
    comment="Raw customer data ingested from the Olist CSV source"
)
def customers_bronze():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/Volumes/customer360/default/customer360_raw/olist_customers_dataset.csv")
    )

@dp.materialized_view(name="orders_bronze")
def orders_bronze():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/Volumes/customer360/default/customer360_raw/olist_orders_dataset.csv")
    )


@dp.materialized_view(name="order_items_bronze")
def order_items_bronze():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/Volumes/customer360/default/customer360_raw/olist_order_items_dataset.csv")
    )


@dp.materialized_view(name="order_payments_bronze")
def order_payments_bronze():
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("/Volumes/customer360/default/customer360_raw/olist_order_payments_dataset.csv")
    )


@dp.materialized_view(name="order_reviews_bronze")
def order_reviews_bronze():
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", False)
        .csv("/Volumes/customer360/default/customer360_raw/olist_order_reviews_dataset.csv")
    )



# ============================================================
# SILVER LAYER — CLEANING, STANDARDIZATION & DATA QUALITY
# Curated datasets used by downstream Customer 360 transformations.
# ============================================================

@dp.materialized_view(
    name="customer360.silver.customers_lakeflow",
    comment="One latest profile per unique customer"
)
def customers_silver():

    customers = spark.read.table("customer360.bronze.customers_bronze")
    orders = spark.read.table("customer360.bronze.orders_bronze")

    customer_orders = customers.join(
        orders.select("customer_id", "order_purchase_timestamp"),
        on="customer_id",
        how="left"
    )

    window_spec = (
        Window
        .partitionBy("customer_unique_id")
        .orderBy(F.col("order_purchase_timestamp").desc())
    )

    return (
        customer_orders
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "order_purchase_timestamp")
    )

@dp.materialized_view(
    name="customer360.silver.orders_lakeflow",
    comment="Validated order records"
)
@dp.expect_or_fail(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
@dp.expect(
    "valid_order_status",
    """
    order_status IN (
        'delivered',
        'shipped',
        'canceled',
        'unavailable',
        'invoiced',
        'processing',
        'created',
        'approved'
    )
    """
)
def orders_silver():
    return spark.read.table("customer360.bronze.orders_bronze")


@dp.materialized_view(
    name="customer360.silver.order_items_lakeflow",
    comment="Validated order item records"
)
@dp.expect_or_fail(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
@dp.expect_or_fail(
    "product_id_not_null",
    "product_id IS NOT NULL"
)
@dp.expect(
    "valid_price",
    "price > 0"
)
@dp.expect(
    "valid_freight",
    "freight_value >= 0"
)
def order_items_silver():
    return spark.read.table("customer360.bronze.order_items_bronze")


@dp.materialized_view(
    name="customer360.silver.order_payments_lakeflow",
    comment="Cleaned payment records with standardized payment type and quality checks"
)
@dp.expect_or_fail(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
@dp.expect(
    "valid_payment_value",
    "payment_value >= 0"
)
@dp.expect(
    "valid_installments",
    "payment_installments >= 0"
)
def order_payments_silver():

    payments = spark.read.table(
        "customer360.bronze.order_payments_bronze"
    )

    return (
        payments
        .withColumn(
            "payment_type",
            F.when(
                F.col("payment_type") == "not_defined",
                "unknown"
            ).otherwise(F.col("payment_type"))
        )
        .withColumn(
            "is_zero_payment",
            F.col("payment_value") <= 0
        )
    )


@dp.materialized_view(
    name="customer360.silver.order_reviews_lakeflow",
    comment="Clean review records with corrected datatypes and quality checks"
)
@dp.expect_or_fail(
    "order_id_not_null",
    "order_id IS NOT NULL"
)
@dp.expect(
    "valid_review_score",
    "review_score BETWEEN 1 AND 5"
)
def order_reviews_silver():

    reviews = spark.read.table(
        "customer360.bronze.order_reviews_bronze"
    )

    return (
        reviews
        .withColumn(
            "review_score",
            F.col("review_score").cast("int")
        )
        .withColumn(
            "review_creation_date",
            F.to_timestamp("review_creation_date")
        )
        .withColumn(
            "review_answer_timestamp",
            F.to_timestamp("review_answer_timestamp")
        )
    )

@dp.materialized_view(
    name="customer360.silver.customer_order_map_lakeflow",
    comment="Maps order-level customer IDs to unique customers"
)
def customer_order_map_silver():

    return (
        spark.read.table("customer360.bronze.customers_bronze")
        .select(
            "customer_id",
            "customer_unique_id"
        )
    )

# ============================================================
# GOLD LAYER — UNIFIED CUSTOMER 360
# One analytics-ready profile per unique customer.
# ============================================================

@dp.materialized_view(
    name="customer360.gold.customer_360_lakeflow",
    comment="Unified Customer 360 profile with orders, spend, payment preference, and satisfaction"
)
@dp.expect_or_fail(
    "customer_id_not_null",
    "customer_unique_id IS NOT NULL"
)
@dp.expect(
    "valid_total_orders",
    "total_orders > 0"
)
@dp.expect(
    "spend_data_available",
    "total_spend IS NOT NULL"
)
@dp.expect(
    "non_negative_spend",
    "total_spend IS NULL OR total_spend >= 0"
)
@dp.expect(
    "valid_review_score",
    "average_review_score IS NULL OR average_review_score BETWEEN 1 AND 5"
)
def customer_360_gold():

    customers = spark.read.table("customer360.silver.customers_lakeflow")
    orders = spark.read.table("customer360.silver.orders_lakeflow")
    items = spark.read.table("customer360.silver.order_items_lakeflow")
    payments = spark.read.table("customer360.silver.order_payments_lakeflow")
    reviews = spark.read.table("customer360.silver.order_reviews_lakeflow")
    customer_map = spark.read.table(
        "customer360.silver.customer_order_map_lakeflow"
    )

    # Map every order to the real customer
    customer_orders = (
        orders
        .join(customer_map, on="customer_id", how="left")
    )

    # Order behavior
    order_summary = (
        customer_orders
        .groupBy("customer_unique_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.min("order_purchase_timestamp").alias("first_order_date"),
            F.max("order_purchase_timestamp").alias("latest_order_date")
        )
    )

    # Order value
    order_values = (
        items
        .groupBy("order_id")
        .agg(
            F.sum(F.col("price") + F.col("freight_value")).alias("order_value")
        )
    )

    # Customer spend
    customer_spend = (
        customer_orders
        .join(order_values, on="order_id", how="left")
        .groupBy("customer_unique_id")
        .agg(
            F.round(F.sum("order_value"), 2).alias("total_spend"),
            F.round(F.avg("order_value"), 2).alias("average_order_value")
        )
    )

    # Preferred payment type
    payment_counts = (
        customer_orders
        .select("order_id", "customer_unique_id")
        .join(payments, on="order_id", how="left")
        .groupBy("customer_unique_id", "payment_type")
        .agg(F.count("*").alias("payment_count"))
    )

    payment_window = (
        Window
        .partitionBy("customer_unique_id")
        .orderBy(
            F.col("payment_count").desc(),
            F.col("payment_type").asc()
        )
    )

    preferred_payment = (
        payment_counts
        .withColumn("rank", F.row_number().over(payment_window))
        .filter(F.col("rank") == 1)
        .select(
            "customer_unique_id",
            F.col("payment_type").alias("preferred_payment_type")
        )
    )

    # Review score per order
    order_reviews = (
        reviews
        .groupBy("order_id")
        .agg(
            F.avg("review_score").alias("order_review_score")
        )
    )

    # Customer satisfaction
    customer_reviews = (
        customer_orders
        .select("order_id", "customer_unique_id")
        .join(order_reviews, on="order_id", how="left")
        .groupBy("customer_unique_id")
        .agg(
            F.round(F.avg("order_review_score"), 2)
             .alias("average_review_score"),
            F.count("order_review_score").alias("reviewed_orders")
        )
    )

    # Final Customer 360
    return (
        customers
        .join(order_summary, on="customer_unique_id", how="left")
        .join(customer_spend, on="customer_unique_id", how="left")
        .join(preferred_payment, on="customer_unique_id", how="left")
        .join(customer_reviews, on="customer_unique_id", how="left")
    )

# ============================================================
# CUSTOMER HISTORY — AUTO CDC FROM SNAPSHOT / SCD TYPE 2
# Preserves historical versions of customer profile attributes.
# ============================================================

@dp.materialized_view(
    name="customer360.silver.customer_profile_snapshot",
    comment="Current customer profile snapshot used for SCD Type 2 history"
)
def customer_profile_snapshot():

    return (
        spark.read.table("customer360.silver.customers_lakeflow")
        .select(
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        )
    )
dp.create_streaming_table(
    name="customer360.silver.customer_history_scd2",
    comment="SCD Type 2 history of customer profile changes"
)

dp.create_auto_cdc_from_snapshot_flow(
    target="customer360.silver.customer_history_scd2",
    source="customer360.silver.customer_profile_snapshot",
    keys=["customer_unique_id"],
    stored_as_scd_type=2
)

# ============================================================
# INCREMENTAL INGESTION — AUTO LOADER
# Demonstrates file-aware incremental ingestion for order data.
# ============================================================

@dp.table(
    name="customer360.bronze.orders_bronze_autoloader",
    comment="Incremental order ingestion using Databricks Auto Loader"
)
def orders_bronze_autoloader():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "olist_orders*.csv")
        .load("/Volumes/customer360/default/customer360_raw/")
    )
