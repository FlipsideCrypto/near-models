{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['scheduled_non_core']
) }}

SELECT
    asset_id,
    recorded_hour AS HOUR,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    inserted_timestamp,
    modified_timestamp,
    complete_provider_prices_id AS fact_prices_ohlc_hourly_id
FROM
    {{ ref('silver__complete_provider_prices') }}
WHERE platform IN (
    'NEAR Protocol',
    'Near',
    'near-protocol'
)
