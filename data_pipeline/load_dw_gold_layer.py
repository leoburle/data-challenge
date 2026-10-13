import polars as pl
import clickhouse_connect
import logging


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    silver_apfel_files = "output_data/s3/silver/apfel/subscriptions/*.csv"
    silver_fenster_files = "output_data/s3/silver/fenster/subscriptions/*.csv"

    FACT_COLUMNS = [
        "event_id", "customer_uuid", "platform", "event_timestamp", "event_type",
        "subscription_type", "renewal_period", "currency", "amount", "tax_amount",
        "discount_code", "affiliate_id", "device_type", "app_version", "session_id",
        "internal_ref",
    ]

    DIM_COLUMNS = [
        "customer_uuid", "platform", "customer_email", "customer_created_at",
        "country_code", "region", "city", "postal_code", "language",
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
    dim_df = subscriptions_df.select(DIM_COLUMNS)

    create_tables_in_clickhouse(client)
    client.insert(
        "fact_subscription_events",
        fact_df.rows(),
        column_names=fact_df.columns,
    )
    client.insert(
        "dim_customer",
        dim_df.rows(),
        column_names=dim_df.columns,
    )

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
            updated_at          DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY customer_uuid
    """)

    client.command("""
        CREATE TABLE IF NOT EXISTS fact_subscription_events (
            event_id           String,
            customer_uuid      String,
            platform           LowCardinality(String),
            event_timestamp    DateTime64(3, 'UTC'),
            event_date         Date,
            event_type         LowCardinality(String),
            subscription_type  LowCardinality(Nullable(String)),
            renewal_period     LowCardinality(Nullable(String)),
            currency           LowCardinality(Nullable(String)),
            amount             Nullable(Float64),
            tax_amount         Nullable(Float64),
            discount_code      Nullable(String),
            affiliate_id       Nullable(String),
            device_type        LowCardinality(Nullable(String)),
            app_version        Nullable(String),
            session_id         Nullable(String),
            internal_ref       Nullable(String)
        )
        ENGINE = ReplacingMergeTree
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, event_id)
    """)


if __name__ == "__main__":
    main()
