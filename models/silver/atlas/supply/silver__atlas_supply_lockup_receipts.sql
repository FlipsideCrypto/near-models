{{ config(
    materialized = "incremental",
    cluster_by = ["receipt_object_id"],
    unique_key = "atlas_supply_lockup_receipts_id",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_strategy = "merge",
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    tags = ['atlas', 'scheduled_core']
) }}

WITH receipts AS (

    SELECT
        receipt_id AS receipt_object_id,
        tx_hash,
        block_timestamp,
        receipt_json AS receipt_actions,
        receiver_id,
        predecessor_id,
        outcome_json :outcome :status :: VARIANT AS status_value,
        outcome_json :outcome :logs :: ARRAY AS logs,
        _partition_by_block_number
    FROM
        {{ ref('silver__receipts_final') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
),
FINAL AS (
    SELECT
        receipt_object_id,
        tx_hash,
        block_timestamp,
        predecessor_id,
        receiver_id,
        receipt_actions AS actions,
        object_keys(
            status_value
        ) [0] :: STRING AS status,
        logs,
        _partition_by_block_number
    FROM
        receipts
    WHERE
        receiver_id LIKE '%.lockup.near'
        AND status != 'Failure'
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['receipt_object_id']) }} AS atlas_supply_lockup_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
