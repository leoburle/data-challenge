## Script structure:
The pipeline follows a medallion layout (bronze, silver, gold). Each stage has a script in `data_pipeline/`.

### Bronze layer:
- Script: extract_subscriptions.py
- What it does: 
    - Pulls data from the endpoints
    - Saves each raw file unchanged, suffixed with the extraction date. 
- Where is the data loaded: The files are saved on output_data/s3/bronze created at runtime


### Silver layer:
- Script: data_normalization.py
- What it does: 
    - Reads the daily batch from the bronze layer
    - Defines a common schema for Apfel and Fenster
    - Standardize column values
    - File standardization
    - Currency conversion to EUR
    - Timestamp conversion to UTC.
- Where is the data loaded: The csv files are saved on output_data/s3/silver created at runtime


### Gold layer:
- Script: load_dw_gold_layer.py
- What it does:
  - Reads all existing Silver Layer Apfel and Fenster files.
  - Unions both platforms and deduplicates on `event_id`.
  - Builds the fact table, one row per event.
  - Builds `dim_customer`, one row per customer and platform, with the latest known attributes.
  - Builds `monthly_metrics` report (MRR, renewals, cancellations, acquisitions by month, country, platform and subscription type). MRR is computed from each customer's last event, carried forward until a cancellation.
- Where is the data loaded: Into a clickhouse docker instance


## How to run the pipeline:
- Run `make local-setup`: to create a venv and install all necessary packages
- Run `make setup-infra` to start the API and clickhouse instances
- Run `make run-pipeline` to run all data pipeline scripts in correct order

## How to query the report:
Use the Jupyter Notebook to connect to the Clickhouse instance, the report is the monthly_metrics table. A sample query was provided.

## Notes about design:

- Technical Stack: I tried to use the same technologies (polars for data processing and clickhouse as a data warehouse) mentioned on the interview so my experience doing the challenge would be as close as possible to a real Helu project.

Regarding data modeling:

I did not see the need of creating a very normalized model, so I ended opting for a star schema. 

Apart from the report table there is:

- fact_subscription_events: holds one row per event, with measures such as revenue_eur and tax_eur.
- dim_customer:  a single denormalized dimension, keyed by (customer_uuid, platform), example of fields available: customer_email, language, last_event_type. Although this table was not used to create the report, I believe it makes sense to have a table with latest client information.
