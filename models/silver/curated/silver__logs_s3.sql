{{ config(
    materialized = "incremental",
    cluster_by = ["block_timestamp::DATE", "_load_timestamp::DATE"],
    unique_key = "tx_hash",
    incremental_strategy = "delete+insert",
    tags = ['curated']
) }}

WITH FINAL AS (

    SELECT
        block_timestamp,
        tx_hash,
        receipt_object_id,
        COALESCE(TRY_PARSE_JSON(VALUE), TRY_PARSE_JSON(SPLIT(VALUE, 'EVENT_JSON:') [1]), VALUE :: STRING) AS clean_log,
        VALUE ILIKE 'event_json:%' AS is_standard,
        _load_timestamp
    FROM
        {{ ref('silver__streamline_receipts_final') }},
        LATERAL FLATTEN(
            input => logs
        ) {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter("_load_timestamp") }}
        {% endif %}
)
SELECT
    *
FROM
    FINAL
