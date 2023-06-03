{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date'],
    unique_key = 'signer_account_id',
    tags = ['actions', 'olap']
) }}


WITH fact_actions_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__actions_events_s3') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),

actions_methods as (
    SELECT  
        a.signer_id,
        a.receiver_id,
        a.tx_hash,
        nvl(action_data:method_name::string, 'no_method') as method_name,
        _load_timestamp,
        block_timestamp,
        _partition_by_block_number,
        a.action_name,
        CASE WHEN a.action_name = 'FunctionCall' THEN 'FUNCTION_CALL'
            WHEN a.action_name = 'AddKey' THEN 'ADD_KEY'
            WHEN a.action_name = 'Transfer' THEN 'TRANSFER'
            ELSE 'no_action' END as action_kind,             
        CASE WHEN action_kind = 'TRANSFER' THEN tx_hash ELSE NULL END as total_transfers_done,
        CASE WHEN action_kind = 'ADD_KEY' THEN tx_hash ELSE NULL END as keys_added,
        CASE WHEN action_kind = 'FUNCTION_CALL' THEN method_name ELSE NULL END as total_methods_called,

        CASE WHEN lower(method_name) LIKE '%nft%' or lower(method_name) LIKE '%mint%' THEN method_name ELSE NULL END as approx_nft_txns,
        CASE WHEN lower(method_name) LIKE '%nft%' and lower(method_name) LIKE '%mint%' THEN method_name ELSE NULL END as nft_mints
    FROM fact_actions_events a
) // time 5 minutes and 12.29 seconds

select * from actions_methods 
