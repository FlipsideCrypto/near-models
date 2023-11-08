{{ config(
    materialized = 'incremental',
    unique_key = 'day',
    incremental_strategy = "merge",
    merge_exclude_columns = ["inserted_timestamp"],    
    tags = ['atlas']
) }}

WITH date_range AS (
    SELECT
        date_day as day
    FROM {{ ref('silver__dates') }}
    WHERE
        {% if is_incremental() %}
            date_day >= SYSDATE() - INTERVAL '3 DAY'
        {% else %}
            date_day >= '2021-01-01' -- first day of data
        {% endif %}
    AND date_day <= SYSDATE()::DATE 
)

SELECT
    d.day as day,
    COUNT(t.tx_hash) AS txns,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM date_range d
LEFT JOIN {{ ref('silver__atlas_nft_transactions') }} t
    ON t.day BETWEEN d.day - INTERVAL '29 day' AND d.day
GROUP BY d.day
