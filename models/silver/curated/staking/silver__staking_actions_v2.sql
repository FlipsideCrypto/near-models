{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['curated', 'scheduled_non_core'],
    cluster_by = ['_partition_by_block_number', 'block_timestamp::date']
) }}

WITH pool_events AS (

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
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__pool_events') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
            {% endif %}
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
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
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
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        staking_actions
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS staking_actions_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
