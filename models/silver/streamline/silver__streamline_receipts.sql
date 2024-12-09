{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST._partition_by_block_number >= (select min(_partition_by_block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'receipt_id',
    cluster_by = ['_inserted_timestamp::date', '_partition_by_block_number'],
    tags = ['load', 'load_shards','scheduled_core']
) }}

WITH shards AS (

    SELECT
        block_id,
        shard_id,
        receipt_execution_outcomes,
        chunk :header :chunk_hash :: STRING AS chunk_hash,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_shards') }}
    WHERE
        ARRAY_SIZE(receipt_execution_outcomes) > 0
    {% if var('MANUAL_FIX') %}
        AND
            {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
            AND _modified_timestamp >= (
                SELECT
                    MAX(_modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
    {% endif %}
),
flatten_receipts AS (

    SELECT
        concat_ws(
            '-',
            shard_id,
            INDEX
        ) AS receipt_execution_outcome_id,
        block_id,
        shard_id,
        chunk_hash,
        INDEX AS receipt_outcome_execution_index,
        VALUE :execution_outcome :: OBJECT AS execution_outcome,
        VALUE :receipt :: OBJECT AS receipt,
        VALUE :receipt :receipt_id :: STRING AS receipt_id,
        VALUE :execution_outcome :id :: STRING AS receipt_outcome_id,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        shards,
        LATERAL FLATTEN(
            input => receipt_execution_outcomes
        )
),
FINAL AS (
    SELECT
        receipt :receipt_id :: STRING AS receipt_id,
        block_id,
        shard_id,
        receipt_outcome_execution_index AS receipt_index,
        chunk_hash,
        receipt,
        execution_outcome,
        execution_outcome :outcome :status :Failure IS NULL AS receipt_succeeded,
        TRY_PARSE_JSON(
            execution_outcome :outcome :status :Failure
        ) AS failure_message,
        object_keys(
            failure_message
        ) [0] :: STRING AS error_type_0,
        COALESCE(
            object_keys(
                TRY_PARSE_JSON(
                    failure_message [error_type_0] :kind
                )
            ) [0] :: STRING,
            failure_message [error_type_0] :kind :: STRING
        ) AS error_type_1,
        COALESCE(
            object_keys(
                TRY_PARSE_JSON(
                    failure_message [error_type_0] :kind [error_type_1]
                )
            ) [0] :: STRING,
            failure_message [error_type_0] :kind [error_type_1] :: STRING
        ) AS error_type_2,
        failure_message [error_type_0] :kind [error_type_1] [error_type_2] :: STRING AS error_message,
        execution_outcome :outcome :receipt_ids :: ARRAY AS outcome_receipts,
        receipt :predecessor_id :: STRING AS predecessor_id,
        receipt :receiver_id :: STRING AS receiver_id,
        receipt :receipt :Action :signer_id :: STRING AS signer_id,
        LOWER(
            object_keys(
                receipt :receipt
            ) [0] :: STRING
        ) AS receipt_type,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        flatten_receipts
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id']
    ) }} AS streamline_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
