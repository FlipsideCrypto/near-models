{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    tags = ['curated'],
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date']
) }}

WITH pool_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__pool_events') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        LOG,
        TO_NUMBER(
            REGEXP_SUBSTR(
                LOG,
                'Contract total staked balance is (\\d+)',
                1,
                1,
                'e',
                1
            )
        ) AS amount_raw,
        amount_raw / pow(
            10,
            24
        ) AS amount_adj,
        _load_timestamp,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        pool_events
    WHERE
        LOG LIKE 'Contract total staked balance is%'
)
SELECT
    *
FROM
    FINAL
