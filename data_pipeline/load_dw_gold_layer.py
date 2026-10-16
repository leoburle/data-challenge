import polars as pl
import clickhouse_connect
import logging


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    silver_apfel_files = "output_data/s3/silver/apfel/subscriptions/*.csv"
    silver_fenster_files = "output_data/s3/silver/fenster/subscriptions/*.csv"

    FACT_COLUMNS = [
        "event_id", "customer_uuid", "platform", "event_timestamp", "event_type",
        "subscription_type", "renewal_period", "currency", "revenue", "tax_amount",
        "discount_code", "affiliate_id", "device_type", "app_version", "session_id",
        "internal_ref", "revenue_eur", "tax_eur", "country_code"
    ]

    DIM_COLUMNS = [
        "customer_uuid", "platform", "customer_email", "customer_created_at",
        "country_code", "region", "city", "postal_code", "language", "last_event_type",
        "last_event_timestamp"
    ]

    client = clickhouse_connect.get_client(
        host="localhost", port=8123, username="default", password="changeme"
    )

    apf_curated_df = pl.read_csv(
            silver_apfel_files, infer_schema_length=None
        ).filter(
            pl.col("processing_status") == "completed"
        ).with_columns(pl.lit("apfel").alias("platform")).drop("processing_status")

    fenster_curated_df = pl.read_csv(
        silver_fenster_files, infer_schema_length=None
        ).with_columns(pl.lit("fenster").alias("platform"))

    subscriptions_df = pl.concat([apf_curated_df, fenster_curated_df], how="diagonal").unique(subset="event_id")

    fact_df = subscriptions_df.select(FACT_COLUMNS)

    # Dimension with latest customer information
    dim_customer_df = (
        subscriptions_df
        .sort("event_timestamp")
        .group_by(["customer_uuid", "platform"])
        .agg(
            pl.col("customer_email").drop_nulls().last(),
            pl.col("customer_created_at").drop_nulls().last(),
            pl.col("country_code").drop_nulls().last(),
            pl.col("region").drop_nulls().last(),
            pl.col("city").drop_nulls().last(),
            pl.col("postal_code").drop_nulls().last(),
            pl.col("language").drop_nulls().last(),
            pl.col("event_type").drop_nulls().last().alias("last_event_type"),
            pl.col("event_timestamp").drop_nulls().last().alias("last_event_timestamp"),
        )
        .select(DIM_COLUMNS)
    )

    report = create_monthly_metrics_report(fact_df)

    create_tables_in_clickhouse(client)


    logging.info("Inserting data into fact_subscription_events")
    client.insert(
        "fact_subscription_events",
        fact_df.rows(),
        column_names=fact_df.columns,
    )

    logging.info("Inserting data into dim_customer")
    client.insert(
        "dim_customer",
        dim_customer_df.rows(),
        column_names=dim_customer_df.columns,
    )

    logging.info("Inserting data into monthly_metrics")
    client.insert(
        "monthly_metrics",
        report.rows(),
        column_names=report.columns,
    )


def create_monthly_metrics_report(fact_df):
    """Creates the CFO report based on the fact subscription events."""

    fact_report_df = (
        fact_df
        .with_columns(
            pl.col("event_timestamp").str.to_datetime(time_zone="UTC"),
            pl.when(pl.col("renewal_period") == "yearly")
            .then(pl.col("revenue_eur") / 12)
            .otherwise(pl.col("revenue_eur"))
            .alias("monthly_revenue_eur"),
        )
        .select(
            "customer_uuid",
            "platform",
            "subscription_type",
            "revenue_eur",
            "monthly_revenue_eur",
            "event_type",
            "event_timestamp",
            pl.col("country_code").fill_null("unknown"),
            pl.col("event_timestamp").dt.month_start().cast(pl.Date).alias("month"),
        )
    )

    min_month, max_month = fact_report_df.select(
        pl.col("month").min().alias("min_month"),
        pl.col("month").max().alias("max_month"),
    ).row(0)

    months_df = pl.DataFrame({
        "month": pl.date_range(min_month, max_month, interval="1mo", eager=True)
    })

    # Last event of each customer in each month
    latest_df = (
        fact_report_df
        .sort("event_timestamp")
        .group_by("customer_uuid", "platform", "month", maintain_order=True)
        .last()
        .with_columns(pl.col("month").alias("event_month"))
        .sort("month")
    )

    # Customers' first month of activity
    customers_df = latest_df.group_by("customer_uuid", "platform").agg(
        pl.col("month").min().alias("first_month")
    )

    # Generate the monthly state of each customer based on their activity
    monthly_state_df = (
        months_df
        .join(customers_df, how="cross")
        .filter(pl.col("month") >= pl.col("first_month"))
        .drop("first_month")
        .sort("month")
        # Perform an asof join to get the latest event for each customer up to the current month
        .join_asof(
            latest_df,
            on="month",
            by=["customer_uuid", "platform"],
            strategy="backward",
        )
    )

    mrr_df = (
        monthly_state_df
        .filter(pl.col("event_type") != "SUBSCRIPTION_CANCELLED")
        .group_by("month", "platform", "subscription_type", "country_code")
        .agg(
            pl.col("monthly_revenue_eur").sum().round(2).alias("mrr_eur")
            )
        .sort("month", descending=True)
    )

    metrics_df = (
        fact_report_df
        .group_by("month", "platform", "subscription_type", "country_code")
        .agg(
            (pl.col("event_type") == "SUBSCRIPTION_RENEWED").sum().alias("renewals"),
            (pl.col("event_type") == "SUBSCRIPTION_CANCELLED").sum().alias("cancellations"),
            (pl.col("event_type") == "SUBSCRIPTION_STARTED").sum().alias("acquisitions"),
        )
    )

    report_df = (
        mrr_df
        .join(metrics_df, on=["month", "platform", "subscription_type", "country_code"], how="full", coalesce=True)
        .fill_null(0)
        .sort("month", descending=True)
    )

    return report_df


def create_tables_in_clickhouse(client) -> None:
    # Create the necessary tables in ClickHouse if they do not exist
    logging.info("Creating tables in ClickHouse if they do not exist...")
    client.command("""
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_uuid       String,
            platform            LowCardinality(String),
            customer_email      Nullable(String),
            customer_created_at Nullable(DateTime64(3, 'UTC')),
            country_code        LowCardinality(Nullable(String)),
            region              Nullable(String),
            city                Nullable(String),
            postal_code         Nullable(String),
            language            LowCardinality(Nullable(String)),
            updated_at          DateTime DEFAULT now(),
            last_event_type          LowCardinality(String),
            last_event_timestamp     Nullable(DateTime64(3, 'UTC')),
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (customer_uuid, platform)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS fact_subscription_events (
            event_id           String,
            customer_uuid      String,
            platform           LowCardinality(String),
            event_timestamp    DateTime64(3, 'UTC'),
            event_type         LowCardinality(String),
            subscription_type  LowCardinality(Nullable(String)),
            renewal_period     LowCardinality(Nullable(String)),
            currency           LowCardinality(Nullable(String)),
            revenue            Nullable(Float64),
            revenue_eur        Nullable(Float64),
            tax_amount         Nullable(Float64),
            tax_eur            Nullable(Float64),
            discount_code      Nullable(String),
            affiliate_id       Nullable(String),
            device_type        LowCardinality(Nullable(String)),
            app_version        Nullable(String),
            session_id         Nullable(String),
            internal_ref       Nullable(String),
            country_code       LowCardinality(Nullable(String)),
        )
        ENGINE = ReplacingMergeTree
        PARTITION BY toYYYYMM(event_timestamp)
        ORDER BY (event_timestamp, event_id)
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS monthly_metrics (
            month               Date,
            platform            LowCardinality(String),
            subscription_type   LowCardinality(String),
            country_code         LowCardinality(String),
            mrr_eur             Float64,
            renewals            UInt32,
            cancellations       UInt32,
            acquisitions        UInt32,
        )
        ENGINE = ReplacingMergeTree
        ORDER BY (month, platform, subscription_type, country_code)
    """)


if __name__ == "__main__":
    main()
