{{ config(
    materialized = 'table',
    unique_key = '_epoch_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core'],
    cluster_by = ['block_id']
) }}

WITH pool_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__pool_events') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE 
            modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp)
                FROM
                    {{ this }}
            )
        {% endif %}
        AND LOG LIKE 'Epoch%'
),
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receiver_id AS pool_id,
        SUBSTR(REGEXP_SUBSTR(LOG, 'Epoch [0-9]+'), 7) :: NUMBER AS epoch_number,
        REGEXP_SUBSTR(
            REGEXP_SUBSTR(
                LOG,
                'Contract received total rewards of [0-9]+'
            ),
            '[0-9]+'
        ) :: NUMBER AS reward_tokens,
        REGEXP_SUBSTR(
            REGEXP_SUBSTR(
                LOG,
                'New total staked balance is [0-9]+'
            ),
            '[0-9]+'
        ) :: NUMBER AS total_staked_balance,
        REGEXP_SUBSTR(
            REGEXP_SUBSTR(
                LOG,
                'Total number of shares [0-9]+'
            ),
            '[0-9]+'
        ) :: NUMBER AS total_staking_shares,
        LOG,
        _partition_by_block_number,
        {{ dbt_utils.generate_surrogate_key(['pool_id', 'epoch_number']) }} AS _epoch_id,
        inserted_timestamp AS _inserted_timestamp
    FROM
        pool_events
)
SELECT
    *,
    _epoch_id AS staking_epochs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
