from datetime import datetime, date, timedelta
from pathlib import Path
import logging
import polars as pl


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    processing_date = date.today().strftime("%Y_%m_%d")
    bronze_apf_file = (
        f"output_data/s3/bronze/apfel/subscriptions/apfel_subscriptions_{processing_date}.json"
    )
    bronze_fenster_file = (
        f"output_data/s3/bronze/fenster/subscriptions/fenster_subscriptions_{processing_date}.csv"
    )
    exchange_rates_file = (
        f"output_data/s3/bronze/exchange-rates/exchange_rates_{processing_date}.csv"
    )

    silver_apfel_output_path = "output_data/s3/silver/apfel/subscriptions"
    Path(silver_apfel_output_path).mkdir(parents=True, exist_ok=True)
    silver_fenster_output_path = "output_data/s3/silver/fenster/subscriptions"
    Path(silver_fenster_output_path).mkdir(parents=True, exist_ok=True)

    logging.info(f"Starting normalization process. Processing date {processing_date}.")

    exchange_rates_df = pl.read_csv(exchange_rates_file)

    logging.info("Reading Apfel raw data...")    
    raw_df_apf = pl.read_json(bronze_apf_file, infer_schema_length=None)
    apf_normalized_df = normalize_apfel_events(raw_df_apf)
    apf_normalized_df = add_converted_currency_column(apf_normalized_df, exchange_rates_df)

    apf_normalized_df.write_csv(
        f"{silver_apfel_output_path}/apfel_subscriptions_normalized_{processing_date}.csv"
    )
    logging.info("Apfel data normalization finished, data persisted in the Silver Layer.")

    logging.info("Reading Fenster raw data...")
    raw_df_fenster = pl.read_csv(bronze_fenster_file, null_values=["", "none"])
    normalized_fenster_df = normalize_fenster_events(raw_df_fenster)

    normalized_fenster_df = add_converted_currency_column(normalized_fenster_df, exchange_rates_df)

    normalized_fenster_df.write_csv(
        f"{silver_fenster_output_path}/fenster_subscriptions_normalized_{processing_date}.csv"
    )
    logging.info("Fenster data normalization finished, data persisted in the Silver Layer.")


def normalize_apfel_events(raw_apf_df: pl.DataFrame) -> pl.DataFrame:
    """Normalizes Apfel data to match standard column names and formats."""
    events_df = raw_apf_df.explode("events").unnest("events")

    events_normalized_df = (
        events_df.with_columns(
            # Strip leading and trailing whitespace from all string columns
            # Replace empty strings with None.
            pl.col(pl.String).str.strip_chars().replace("", None)
        )
        .with_columns(
            # String standardization
            pl.col("event_type").str.to_uppercase(),
            pl.col("processing_status").str.to_lowercase(),
            pl.col("currency").str.to_uppercase().replace("EURO", "EUR"),
            pl.col("country_code").str.to_uppercase(),
            pl.col("subscription_type").str.to_lowercase(),
            pl.col("renewal_period").str.to_lowercase(),
            pl.col("device_type").str.to_lowercase(),
            pl.col("customer_email").str.to_lowercase(),
            pl.col("amount").alias("revenue"),
        )
        .drop("count").drop("amount")
        .unique(subset="event_id")
    )
    return events_normalized_df


def normalize_fenster_events(raw_fenster_df: pl.DataFrame) -> pl.DataFrame:
    """Normalizes Fenster data to match standard column names and formats."""
    normalized_fenster_df = (
        raw_fenster_df.with_columns(
            # Stripping whitespace and replacing empty strings with None for all string columns
            pl.col(pl.String).str.strip_chars().replace("", None)
        )
        .select(
            # Renamed / derived columns
            pl.col("id").alias("event_id"),
            pl.col("cid").alias("customer_uuid"),
            pl.col("type").str.to_lowercase().replace(
                {
                    "new": "SUBSCRIPTION_STARTED",
                    "renew": "SUBSCRIPTION_RENEWED",
                    "cancel": "SUBSCRIPTION_CANCELLED",
                }
            ).alias("event_type"),
            pl.col("ctry").str.to_uppercase().replace(
                {
                    "USA": "US",
                    "UNITED STATES": "US",
                    "GBR": "GB",
                    "INVALID": None,
                    "XX": None,
                }
            ).alias("country_code"),
            pl.col("plan").str.extract(r"^([^_]+)", 1).alias("subscription_type"),
            pl.col("plan").str.extract(r"_([^_]+)$", 1).alias("renewal_period"),
            pl.col("mail").str.to_lowercase().alias("customer_email"),
            pl.col("ccy").str.to_uppercase().alias("currency"),
            pl.col("os").str.to_lowercase().alias("device_type"),
            pl.struct(["ts", "tz"]).map_elements(
                lambda row: parse_ts_with_tz(row["ts"], row["tz"]),
                return_dtype=pl.String,
            ).alias("event_timestamp"),
            pl.struct(["signup_ts", "tz"]).map_elements(
                lambda row: parse_ts_with_tz(row["signup_ts"], row["tz"]),
                return_dtype=pl.String,
            ).alias("customer_created_at"),
            pl.col("state").alias("region"),
            pl.col("zip").alias("postal_code"),
            pl.col("price").alias("revenue"),
            pl.col("tax").alias("tax_amount"),
            pl.col("row_hash").alias("internal_ref"),
            pl.col("lang").alias("language"),
            # Kept as-is
            "vat_id",
            "campaign_src",
            "utm_medium",
            "utm_campaign",
            "browser",
            "screen_res",
            "legacy_flag",
            "migrated_from",
            "batch_id",
        ).unique(subset="event_id")
    )
    return normalized_fenster_df


def parse_ts_with_tz(ts: str | None, tz_name: str | None) -> str | None:
    """Convert fenster local timestamp to a UTC string"""
    TZ_OFFSET_HOURS = {
        "GMT": 0,
        "UTC": 0,
        "CET": 1,
        "EET": 2,
        "EST": -5,
        "CST": -6,
        "PST": -8,
        "AEST": 10,
        "JST": 9,
    }

    if ts is None or tz_name is None:
        return None

    offset = TZ_OFFSET_HOURS.get(tz_name.strip().upper())
    if offset is None:
        return None

    try:
        local_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

    return (local_dt - timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
def add_converted_currency_column(df, exchange_rates_df):
    """
    Convert the revenue and tax column in the dataframe to EUR using the provided exchange rates.
    """

    df = df.with_columns(
        pl.col("event_timestamp").str.slice(0, 10).alias("date")
    )

    # Single float used as fallback rate
    avg_rate_to_eur = exchange_rates_df["rate_to_eur"].mean()

    df = df.join(
        exchange_rates_df,
        on=["currency", "date"],
        how="left"
    )

    is_eur = pl.col("currency") == "EUR"

    df = df.with_columns(
        # Convert revenue and tax to EUR based on the exchange rate
        # If the currency is EUR, keep the original value; 
        #  otherwise, convert using the rate or fallback to the average rate
        pl.when(is_eur)
        .then(pl.col("revenue"))
        .when(~is_eur & pl.col("rate_to_eur").is_not_null())
        .then(pl.col("revenue").cast(pl.Decimal(18, 2)) * pl.col("rate_to_eur"))
        .otherwise(pl.col("revenue").cast(pl.Decimal(18, 2)) * float(avg_rate_to_eur))
        .alias("revenue_eur"),

        pl.when(is_eur)
        .then(pl.col("tax_amount"))
        .when(~is_eur & pl.col("rate_to_eur").is_not_null())
        .then(pl.col("tax_amount").cast(pl.Decimal(18, 2)) * pl.col("rate_to_eur"))
        .otherwise(pl.col("tax_amount").cast(pl.Decimal(18, 2)) * float(avg_rate_to_eur))
        .alias("tax_eur"),
    ).drop("rate_to_eur")

    df = df.with_columns(
        pl.col("revenue_eur").cast(pl.Decimal(18, 2)).alias("revenue_eur"),
        pl.col("tax_eur").cast(pl.Decimal(18, 2)).alias("tax_eur"),
    ).drop("date")

    return df


if __name__ == "__main__":
    main()
