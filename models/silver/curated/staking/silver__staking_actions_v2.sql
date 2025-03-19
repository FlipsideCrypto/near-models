{{ config(
    materialized = 'table',
    unique_key = 'staking_actions_v2_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['curated', 'scheduled_non_core'],
    cluster_by = [ 'block_timestamp::date','_partition_by_block_number'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receiver_id,signer_id);",
) }}

WITH pool_events AS (

    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id,
        receiver_id,
        signer_id,
        predecessor_id,
        status_value,
        logs,
        LOG,
        _partition_by_block_number,
        inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__pool_events') }}
        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        {% if is_incremental() %}
        WHERE modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
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
        predecessor_id,
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
        _inserted_timestamp
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
        predecessor_id,
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
        _inserted_timestamp
    FROM
        staking_actions
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_object_id', 'log_signer_id', 'action', 'amount_raw']
    ) }} AS staking_actions_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
