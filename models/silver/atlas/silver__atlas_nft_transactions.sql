{{ config(
    materialized = "incremental",
    cluster_by = ["day"],
    unique_key = "action_id",
    merge_exclude_columns = ["inserted_timestamp"],    
    incremental_strategy = "merge",
    tags = ['atlas']
) }}

WITH nft_mints AS (

    SELECT
        block_timestamp::date as day,
        tx_hash,
        method_name,
        receiver_id,
        signer_id,
        owner_id as owner,
        token_id,
        _inserted_timestamp
    FROM {{ ref('silver__standard_nft_mint_s3') }}
    WHERE
            {% if var("MANUAL_FIX") %}
                {{ partition_load_manual('no_buffer') }}
            {% else %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% endif %}
),

nft_transfers AS (

    SELECT
        block_timestamp::date as day,
        tx_hash,
        method_name,
        receiver_id,
        signer_id,
        args['receiver_id'] as owner,
        args['token_id'] as token_id,
        _inserted_timestamp
    FROM {{ ref('silver__actions_events_function_call_s3') }}
    WHERE method_name = 'nft_transfer'
    AND 
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),

unioned_nft_data AS (
    SELECT * FROM nft_mints
    UNION ALL
    SELECT * FROM nft_transfers
)

SELECT
    concat_ws(
        '-',
        tx_hash,
        method_name
    ) AS action_id,
    day,
    tx_hash,
    method_name,
    receiver_id,
    signer_id,
    owner,
    token_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM unioned_nft_data