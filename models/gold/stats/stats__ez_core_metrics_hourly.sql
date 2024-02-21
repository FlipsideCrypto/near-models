{{ config(
    materialized = 'view',
    tags = ['core']
) }}


WITH prices_oracle_s3 AS (
    SELECT
        DATE_TRUNC('HOUR', block_timestamp) as block_timestamp_hour,
        avg(price_usd) as price_usd
    FROM
        {{ ref('silver__prices_oracle_s3') }}
    WHERE
        token = 'Wrapped NEAR fungible token' 
    GROUP BY 1        
)

SELECT
    s.block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    unique_to_count,
    total_fees AS total_fees_native,
    ROUND(
        total_fees * p.price_usd,
        2
    ) AS total_fees_usd,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    s.inserted_timestamp AS inserted_timestamp,
    s.modified_timestamp AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    LEFT JOIN  prices_oracle_s3
    p
    ON s.block_timestamp_hour = p.block_timestamp_hour