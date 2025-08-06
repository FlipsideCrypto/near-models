{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

WITH prices AS (
    --get closing price for the hour

    SELECT
        HOUR AS block_timestamp_hour,
        price AS price_usd
    FROM
        {{ ref('silver__complete_token_prices') }}
    WHERE
        token_address IN (
            'near',
            'wrap.near'
        ) 
        qualify ROW_NUMBER() over (
            PARTITION BY block_timestamp_hour
            ORDER BY
                HOUR DESC, IS_VERIFIED DESC
        ) = 1
)
SELECT
    b.block_timestamp_hour,
    b.block_number_min,
    b.block_number_max,
    b.block_count,
    t.transaction_count,
    t.transaction_count_success,
    t.transaction_count_failed,
    t.unique_from_count,
    t.unique_to_count,
    t.total_fees AS total_fees_native,
    ROUND(
        t.total_fees * p.price_usd,
        2
    ) AS total_fees_usd,
    t.core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    GREATEST(
        b.inserted_timestamp,
        t.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        b.modified_timestamp,
        t.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_block_hourly') }}
    b
    JOIN {{ ref('silver_stats__core_metrics_hourly') }}
    t
    ON b.block_timestamp_hour = t.block_timestamp_hour
    LEFT JOIN prices p
    ON b.block_timestamp_hour = p.block_timestamp_hour
