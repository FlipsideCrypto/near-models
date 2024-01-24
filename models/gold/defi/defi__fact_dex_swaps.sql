{{ config(
    materialized = 'view',
    secure = false,
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'DEFI, SWAPS' }} },
    tags = ['core']
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
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__dex_swaps_v2') }}
