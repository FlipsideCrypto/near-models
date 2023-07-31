{{ config(
    materialized = "incremental",
    cluster_by = ["_load_timestamp::DATE","block_timestamp::DATE"],
    unique_key = "action_id",
    incremental_strategy = "delete+insert",
    tags = ['curated']
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
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp,
        gas_burnt,
        INDEX AS action_index,
        COALESCE(TRY_PARSE_JSON(VALUE), TRY_PARSE_JSON(SPLIT(VALUE, 'EVENT_JSON:') [1]), VALUE :: STRING) AS clean_log,
        VALUE ILIKE 'event_json:%' AS is_standard
    FROM
        receipts,
        LATERAL FLATTEN(
            input => logs
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
    clean_log,
    is_standard,
    tx_hash,
    receipt_object_id,
    block_id,
    gas_burnt,
    block_timestamp,
    _load_timestamp,
    _partition_by_block_number,
    _inserted_timestamp
FROM
    FINAL
