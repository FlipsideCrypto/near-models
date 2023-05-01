{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    unique_key = 'action_id',
    tags = ['actions', 'curated']
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
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
        _load_timestamp,
        _partition_by_block_number,
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
        tx_hash,
        receipt_object_id,
        receiver_id,
        signer_id,
        block_id,
        block_timestamp,
        chunk_hash,
        _load_timestamp,
        _partition_by_block_number,
        this,
        key AS action_name,
        TRY_PARSE_JSON(VALUE) AS action_data,
        action_index
    FROM
        flatten_actions,
        LATERAL FLATTEN(
            input => action_object
        )
)
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
    action_name,
    action_data,
    _load_timestamp,
    _partition_by_block_number
FROM
    FINAL
