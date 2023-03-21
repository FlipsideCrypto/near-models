{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_load_timestamp::DATE'],
    tags = ['curated']
) }}

WITH txs AS (

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
logs AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        REPLACE(
            VALUE,
            'EVENT_JSON:'
        ) AS json,
        TRY_PARSE_JSON(json) :event :: STRING AS event,
        TRY_PARSE_JSON(json) :standard AS STANDARD,
        TRY_PARSE_JSON(json) :data AS data_log,
        REGEXP_SUBSTR(
            status_value,
            'Success'
        ) AS reg_success,
        _load_timestamp
    FROM
        txs,
        TABLE(FLATTEN(input => logs))
    WHERE
        1 = 1
        AND reg_success IS NOT NULL
        AND event IS NOT NULL
        AND receiver_id = 'usn'
),
FINAL AS (
    SELECT
        block_timestamp,
        block_id,
        tx_hash,
        reg_success AS status,
        VALUE :amount / pow(
            10,
            18
        ) AS amount,
        event,
        COALESCE(
            VALUE :owner_id,
            VALUE :old_owner_id
        ) :: STRING AS from_address,
        VALUE :new_owner_id :: STRING AS to_address,
        _load_timestamp
    FROM
        logs,
        TABLE(FLATTEN(input => data_log))
)
SELECT
    *
FROM
    FINAL
