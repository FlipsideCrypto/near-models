{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    unique_key = 'action_id',
    tags = ['actions', 'olaf']
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
)

select  
        a.action_id,
        a.block_timestamp,
        a.tx_hash                                                                as transaction_hash,
        a.signer_id                                                              as signer_account_id,
        a.receiver_id                                                            as receiver_account_id,
        nvl(action_data:method_name::string, 'no_method') as method_name,
        _load_timestamp,
        _partition_by_block_number,
        CASE WHEN a.action_name = 'FunctionCall' THEN 'FUNCTION_CALL'
            WHEN a.action_name = 'AddKey' THEN 'ADD_KEY'
            WHEN a.action_name = 'Transfer' THEN 'TRANSFER'
            ELSE 'no_action' END as action_kind, 
        CASE WHEN action_kind = 'TRANSFER' THEN transaction_hash else null END as total_transfers_done,
        CASE WHEN action_kind = 'ADD_KEY' THEN transaction_hash else null END as keys_added,
        CASE WHEN action_kind = 'FUNCTION_CALL' THEN method_name else null END as total_methods_called,
        CASE WHEN lower(method_name) like '%nft%' or lower(method_name) like '%mint%' THEN method_name else null END as approx_nft_a,
        CASE WHEN lower(method_name) like '%nft%' and lower(method_name) like '%mint%' THEN method_name else null END as nft_mints

from fact_actions_events a