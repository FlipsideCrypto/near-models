{{ config(
    materialized = "incremental",
    cluster_by = ["receipt_object_id"],
    unique_key = "atlas_supply_lockup_receipts_id",
    merge_exclude_columns = ["inserted_timestamp"],
    incremental_strategy = "merge",
    tags = ['atlas']
) }}

WITH receipts AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_inserted_timestamp') }}
        {% endif %}
),
FINAL AS (
    SELECT
        receipt_object_id,
        tx_hash,
        block_timestamp,
        receipt_actions :predecessor_id :: STRING AS predecessor_id,
        receiver_id,
        receipt_actions AS actions,
        object_keys(
            status_value
        ) [0] :: STRING AS status,
        logs,
        COALESCE(
            _inserted_timestamp,
            _load_timestamp
        ) AS _inserted_timestamp,
        _partition_by_block_number,
        {{ dbt_utils.generate_surrogate_key(['receipt_object_id']) }} AS atlas_supply_lockup_receipts_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id
    FROM
        receipts
    WHERE
        receiver_id LIKE '%.lockup.near'
        AND status != 'Failure'
)
SELECT
    *
FROM
    FINAL
