{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'fact_dex_swaps_id',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_object_id,receiver_id,signer_id,token_out,token_in);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, SWAPS' }} },
) }}

SELECT
    tx_hash,
    receipt_object_id,
    block_id,
    block_timestamp,
    receiver_id,
    signer_id,
    swap_index,
    amount_out_raw,
    token_out,
    amount_in_raw,
    token_in,
    swap_input_data,
    LOG,
    dex_swaps_v2_id AS fact_dex_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__dex_swaps_v2') }}
        
    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
    {% else %}
        {% if is_incremental() %}
            WHERE modified_timestamp > (
                SELECT MAX(modified_timestamp) FROM {{ this }}
            )
        {% endif %}
    {% endif %}
