{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'receipt_object_id',
    cluster_by = ['_inserted_timestamp::date', '_modified_timestamp::DATE', '_partition_by_block_number'],
    tags = ['receipt_map'],
    full_refresh = False
) }}
{# TODO - check clustering. Add SO? #}

WITH retry_range AS (

    SELECT
        receipt_object_id,
        block_id,
        _partition_by_block_number,
        _inserted_timestamp
        {% if not var('IS_MIGRATION') %}
        , _modified_timestamp
        {% endif %}
    FROM
        {{ this }}

    {% if var('IS_MIGRATION') %}
    WHERE
        _inserted_timestamp >= SYSDATE() - INTERVAL '1 day'
        AND (
            tx_hash IS NULL
            OR block_timestamp IS NULL
        )
    {% else %}
    WHERE
        _modified_timestamp >= SYSDATE() - INTERVAL '1 day'
        AND (
            tx_hash IS NULL
            OR block_timestamp IS NULL
        )
    {% endif %}
),
base_receipts AS (
    SELECT
        receipt_id,
        block_id,
        shard_id,
        receipt_index,
        chunk_hash,
        receipt,
        execution_outcome,
        outcome_receipts,
        receiver_id,
        signer_id,
        receipt_type,
        receipt_succeeded,
        error_type_0,
        error_type_1,
        error_type_2,
        error_message,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_receipts') }}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MIN(_partition_by_block_number)
            FROM
                retry_range
        )
        AND (
            {{ incremental_load_filter('_inserted_timestamp') }}
            OR receipt_id IN (
                SELECT
                    receipt_object_id
                FROM
                    retry_range
            )
        )
),
blocks AS (
    SELECT
        block_id,
        block_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        _partition_by_block_number >= (
            SELECT
                MIN(_partition_by_block_number)
            FROM
                retry_range
        )
),
append_tx_hash AS (
    SELECT
        m.tx_hash,
        r.receipt_id,
        r.block_id,
        r.shard_id,
        r.receipt_index,
        r.chunk_hash,
        r.receipt,
        r.execution_outcome,
        r.outcome_receipts,
        r.receiver_id,
        r.signer_id,
        r.receipt_type,
        r.receipt_succeeded,
        r.error_type_0,
        r.error_type_1,
        r.error_type_2,
        r.error_message,
        r._partition_by_block_number,
        r._inserted_timestamp,
        r._modified_timestamp
    FROM
        base_receipts r
        LEFT JOIN {{ ref('silver__receipt_tx_hash_mapping') }}
        m USING (receipt_id)
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_id AS receipt_object_id,
        r.block_id,
        b.block_timestamp,
        receipt_index,
        chunk_hash,
        receipt AS receipt_actions,
        execution_outcome,
        outcome_receipts AS receipt_outcome_id,
        receiver_id,
        signer_id,
        receipt_type,
        execution_outcome :outcome :gas_burnt :: NUMBER AS gas_burnt,
        execution_outcome :outcome :status :: variant AS status_value,
        execution_outcome :outcome :logs :: ARRAY AS logs,
        execution_outcome :proof :: ARRAY AS proof,
        execution_outcome :outcome :metadata :: variant AS metadata,
        receipt_succeeded,
        error_type_0,
        error_type_1,
        error_type_2,
        error_message,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        append_tx_hash r
        LEFT JOIN blocks b USING (block_id)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id']
    ) }} AS streamline_receipts_final_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
