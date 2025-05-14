{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'fact_lending_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,contract_address,token_address,sender_id,actions,amount_raw,amount_adj);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, LENDING' }} }
) }}

SELECT
    platform,
    tx_hash,
    block_id,
    block_timestamp,
    sender_id,
    actions,
    contract_address,
    token_address,
    amount_raw,
    amount_adj,
    burrow_lending_id AS fact_lending_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__burrow_lending') }}
{% if var('MANUAL_FIX') %}
    WHERE {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
{% else %}
    {% if is_incremental() %}
        WHERE modified_timestamp > (
            SELECT MAX(modified_timestamp) FROM {{ this }}
        )
    {% endif %}
{% endif %}
