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
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
staking_actions AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        status_value,
        logs,
        LOG,
        SPLIT(
            LOG,
            ' '
        ) AS log_parts,
        SPLIT(
            log_parts [0] :: STRING,
            '@'
        ) [1] :: STRING AS log_signer_id,
        log_parts [1] :: STRING AS action,
        SPLIT(
            log_parts [2] :: STRING,
            '.'
        ) [0] :: NUMBER AS amount_raw,
        amount_raw :: FLOAT / pow(
            10,
            24
        ) AS amount_adj,
        LENGTH(
            amount_raw :: STRING
        ) AS decimals,
        signer_id = log_signer_id AS _log_signer_id_match,
        _load_timestamp,
        _partition_by_block_number
    FROM
        pool_events
    WHERE
        TRUE
        AND receiver_id != signer_id
        AND LOG LIKE '@%'
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        status_value,
        logs,
        LOG,
        log_signer_id,
        action,
        amount_raw,
        amount_adj,
        decimals,
        _log_signer_id_match,
        _load_timestamp,
        _partition_by_block_number
    FROM
        staking_actions
)
SELECT
    *
FROM
    FINAL
