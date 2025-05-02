{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'nft_transfers_id',
    incremental_strategy = 'merge',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,contract_address,from_address,to_address,token_id);",
    tags = ['scheduled_non_core']
) }}

WITH nft_logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        signer_id,
        receiver_id,
        predecessor_id,
        TRY_PARSE_JSON(clean_log) AS DATA,
        log_index AS logs_rn,
        FLOOR(block_id, -3) AS _partition_by_block_number
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        is_standard
        AND receipt_succeeded
        AND TRY_PARSE_JSON(clean_log) :event :: STRING IN (
            'nft_transfer',
            'nft_mint'
        )
        {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer', 'floor(block_id, -3)') }}
        {% else %}
        {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        {% endif %}
),
nft_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        predecessor_id,
        receiver_id AS contract_address,
        COALESCE(
            A.value :old_owner_id,
            signer_id
        ) :: STRING AS from_address,
        COALESCE(
            A.value :new_owner_id,
            A.value :owner_id
        ) :: STRING AS to_address,
        A.value :token_ids :: ARRAY AS token_ids,
        token_ids [0] :: STRING AS token_id,
        logs_rn + A.index as transfer_rn,
        _partition_by_block_number
    FROM
        nft_logs,
        LATERAL FLATTEN(
            input => DATA :data :: ARRAY
        ) A
    WHERE
        token_id IS NOT NULL
),
nft_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        predecessor_id,
        contract_address,
        from_address,
        to_address,
        B.value :: STRING AS token_id,
        transfer_rn + B.index as rn,
        _partition_by_block_number
    FROM
        nft_transfers,
        LATERAL FLATTEN(
            input => token_ids
        ) B
)
SELECT
    block_id,
    block_timestamp,
    tx_hash,
    receipt_id,
    predecessor_id,
    rn,
    contract_address,
    from_address,
    to_address,
    token_id,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'contract_address', 'from_address', 'to_address', 'token_id', 'rn']
    ) }} AS nft_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    nft_final
