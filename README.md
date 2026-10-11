Table structure:

fct_subscription_events (fact):
----------------------------
event_id (pk),
customer_uuid (fk),
event_timestamp,
event_type,
subscription_type,
renewal_period,
currency,
amount,
tax_amount,
discount_code,
affiliate_id,
device_type,
app_version,
session_id,
internal_ref,
processing_status


customer (dimension)
-------------------
customer_uuid (pk)
customer_email
customer_created_at
country_code
region
city
postal_code
status

