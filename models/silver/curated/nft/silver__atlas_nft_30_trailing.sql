{{ config(
    materialized = 'incremental',
    unique_key = 'day',
    incremental_strategy = 'delete+insert',
    tags = ['curated']
) }}

WITH date_range AS (
    SELECT
        day
    FROM {{ ref('silver__atlas_nft_transactions') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days('_inserted_timestamp', 3) }}
        {% endif %}    
    GROUP BY day

)


SELECT
    d.day as day,
    COUNT(t.tx_hash) AS txns
FROM date_range d
LEFT JOIN {{ ref('silver__atlas_nft_transactions') }} t
    ON t.day BETWEEN d.day - INTERVAL '29 day' AND d.day
GROUP BY d.day
