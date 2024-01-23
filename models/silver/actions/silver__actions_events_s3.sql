{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    unique_key = 'action_id',
    tags = ['actions', 'curated']
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
flatten_actions AS (
    SELECT
        tx_hash,
        receipt_object_id,
        receiver_id,
        signer_id,
        block_id,
        block_timestamp,
        chunk_hash,
        logs,
        _partition_by_block_number,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp,
        _load_timestamp,
        receipt_actions,
        execution_outcome,
        VALUE AS action_object,
        INDEX AS action_index
    FROM
        receipts,
        LATERAL FLATTEN(
            input => receipt_actions :receipt :Action :actions
        )
),
FINAL AS (
    SELECT
        concat_ws(
            '-',
            receipt_object_id,
            action_index
        ) AS action_id,
        receiver_id,
        signer_id,
        chunk_hash,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        action_index,
        key AS action_name,
        TRY_PARSE_JSON(VALUE) AS action_data,
        logs,
        _partition_by_block_number,
        _inserted_timestamp,
        _load_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['receipt_object_id', 'action_index']
        ) }} AS actions_events_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        flatten_actions,
        LATERAL FLATTEN(
            input => action_object
        )
)
SELECT
    *
FROM
    FINAL
