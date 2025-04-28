{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'transfers_liquidity_id',
    tags = ['curated','scheduled_non_core']
) }}

WITH liquidity_logs AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receiver_id,
        predecessor_id,
        signer_id,
        log_index,
        clean_log :: STRING AS log_data,
        receipt_succeeded,
        _partition_by_block_number
    FROM 
        {{ ref('silver__logs_s3') }}
    WHERE 
        receipt_succeeded
        AND log_index = 0 -- Liquidity logs are always first
        AND clean_log :: STRING like 'Liquidity added [%minted % shares'

    {% if var("MANUAL_FIX") %}
        AND {{ partition_load_manual('no_buffer') }}
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
add_liquidity AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"\\d+ ([^"]*)["]',
            1,
            1,
            'e',
            1
        ) :: STRING AS contract_address,
        predecessor_id AS from_address,
        receiver_id AS to_address,
        REGEXP_SUBSTR(
            SPLIT.value,
            '"(\\d+) ',
            1,
            1,
            'e',
            1
        ) :: variant AS amount_unadj,
        'add_liquidity' AS memo,
        log_index + INDEX AS event_index,
        predecessor_id,
        signer_id,
        _partition_by_block_number
    FROM
        liquidity_logs,
        LATERAL FLATTEN (
            input => SPLIT(
                REGEXP_SUBSTR(
                    log_data,
                    '\\["(.*?)"\\]'
                ),
                ','
            )
        ) SPLIT
)
SELECT
    block_timestamp,
    block_id,
    tx_hash,
    receipt_id,
    contract_address,
    from_address,
    to_address,
    amount_unadj,
    memo,
    event_index,
    predecessor_id,
    signer_id,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'contract_address', 'amount_unadj', 'to_address', 'event_index']
    ) }} AS transfers_liquidity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    add_liquidity
