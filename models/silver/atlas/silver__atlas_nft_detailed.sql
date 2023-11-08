

{{ config(
    materialized = 'incremental',
    unique_key = 'action_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],    
    tags = ['atlas']
) }}

WITH nft_data AS (
    SELECT
        *
    FROM {{ ref('silver__atlas_nft_transactions') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_last_x_days('_inserted_timestamp', 3) }}
        {% endif %}
)

select
    concat_ws(
        '-',
        day,
        receiver_id
    ) AS action_id,
    day,
    receiver_id,
    COUNT(DISTINCT token_id) AS tokens,
    COUNT(CASE WHEN method_name = 'nft_transfer' then tx_hash end) AS all_transfers,
    COUNT(DISTINCT owner) AS owners,
    COUNT(*) AS transactions,
    COUNT(CASE WHEN method_name != 'nft_transfer' then tx_hash end) AS mints,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM nft_data
GROUP BY 1, 2, 3
ORDER BY 4 DESC