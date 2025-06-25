{{ config (
    materialized = 'view'
) }}

{{ create_udf_get_bigquery_data(
    project='bigquery-public-data',
    dataset='crypto_near_mainnet_us',
    table='receipt_origin_transaction',
    gcp_secret_name='GCP_SERVICE_ACCOUNT_SECRET',
    destination_bucket='fsc-data-snowflake',
    destination_path='{{ target.database }}/receipt_origin_transaction',
    start_date=null,
    end_date=null
) }}
