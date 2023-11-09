{{ config(
    materialized = "incremental",
    cluster_by = ["day"],
    unique_key = "id",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_strategy = "merge",
    tags = ['atlas']
) }}

WITH nft_mints AS (

    SELECT
        block_timestamp :: DATE AS DAY,
        receipt_object_id,
        tx_hash,
        method_name,
        receiver_id,
        signer_id,
        owner_id AS owner,
        token_id,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__standard_nft_mint_s3') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
nft_transfers AS (
    SELECT
        block_timestamp :: DATE AS DAY,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id,
        tx_hash,
        method_name,
        receiver_id,
        signer_id,
        args ['receiver_id'] AS owner,
        args ['token_id'] AS token_id,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name = 'nft_transfer'
        AND {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
unioned_nft_data AS (
    SELECT
        *
    FROM
        nft_mints
    UNION ALL
    SELECT
        *
    FROM
        nft_transfers
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'method_name', 'token_id', 'owner']
    ) }} AS id,
    DAY,
    tx_hash,
    method_name,
    receiver_id,
    signer_id,
    owner,
    token_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    unioned_nft_data
WHERE
    -- failed receipts may have unparsable base64 FunctionCall args
    token_id IS NOT NULL
    AND owner IS NOT NULL 
qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC
) = 1
