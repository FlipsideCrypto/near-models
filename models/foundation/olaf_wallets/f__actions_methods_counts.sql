{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['signer_id'],
    unique_key = 'signer_id',
    tags = ['actions', 'olap']
) }}


WITH fact_actions_events AS (

    SELECT
        *
    FROM
        {{ ref('f__actions_methods') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
)

SELECT
    signer_id,
    count(distinct method_name) as total_actions_taken, 
    count(distinct  total_transfers_done) as total_transfers_done,
    count(distinct keys_added) as keys_added,
    count(distinct total_methods_called) as total_methods_called,
    count(distinct approx_nft_txns) as approx_nft_txns,
    count(distinct nft_mints) as nft_mints
FROM fact_actions_events
GROUP BY signer_id //  2 minutes and 6.88 seconds
// Group by block_timestamp::date truncate and signer_account_id
