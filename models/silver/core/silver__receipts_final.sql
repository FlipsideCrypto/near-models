{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate","block_timestamp::date"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'receipt_id',
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,receiver_id,predecessor_id);",
    tags = ['scheduled_core', 'core_v2'],
    full_refresh = False
) }}

{% if var('NEAR_MIGRATE_ARCHIVE', False) %}

    SELECT
        chunk_hash,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receipt_json,
        outcome_json,
        _partition_by_block_number,
        streamline_receipts_final_id AS receipts_final_id,
        COALESCE(
            inserted_timestamp, 
            _inserted_timestamp,
            SYSDATE()
        ) AS inserted_timestamp,
        modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('_migrate_receipts') }}

{% else %}

-- TODO if execute block for incremental min blockdate
WITH txs_with_receipts AS (
    SELECT
        chunk_hash,
        origin_block_id,
        origin_block_timestamp,
        tx_hash,
        response_json :receipts :: ARRAY AS receipts_json,
        response_json :receipts_outcome :: ARRAY AS receipts_outcome_json,
        response_json :status :Failure IS NULL AS tx_succeeded,
        partition_key AS _partition_by_block_number
    FROM
        {{ ref('silver__transactions_v2') }}
        -- TODO incremental logic
),
blocks AS (
    SELECT
        block_id,
        block_hash,
        block_timestamp
    FROM
        {{ ref('silver__blocks_v2') }}
        -- TODO incremental logic
),
flatten_receipts AS (
    SELECT
        chunk_hash,
        tx_hash,
        tx_succeeded,
        VALUE :: variant AS receipt_json,
        _partition_by_block_number
    FROM
        txs_with_receipts,
        LATERAL FLATTEN(
            input => receipts_json
        )
),
flatten_receipt_outcomes AS (
    SELECT
        VALUE :block_hash :: STRING AS block_hash,
        tx_hash,
        VALUE :id :: STRING AS receipt_id,
        VALUE :: variant AS outcome_json
    FROM
        txs_with_receipts,
        LATERAL FLATTEN(
            input => receipts_outcome_json
        )
),
receipts_full AS (
    SELECT
        chunk_hash,
        ro.block_hash,
        block_id,
        block_timestamp,
        r.tx_hash,
        r.receipt_id,
        receipt_json,
        outcome_json,
        tx_succeeded,
        _partition_by_block_number
    FROM
        flatten_receipts r
        LEFT JOIN flatten_receipt_outcomes ro
        ON r.receipt_id = ro.receipt_id
        LEFT JOIN blocks b
        ON ro.block_hash = b.block_hash
),
FINAL AS (
    SELECT
        chunk_hash,
        block_hash,
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        receipt_json,
        outcome_json,
        outcome_json :status :Failure IS NULL AS receipt_succeeded,
        _partition_by_block_number
    FROM
        receipts_full
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id']
    ) }} AS receipts_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL

{% endif %}
