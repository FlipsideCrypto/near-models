{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    tags = ['curated','scheduled_non_core'],
    cluster_by = ['block_timestamp::date','_partition_by_block_number'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receiver_id,signer_id);",
) }}

WITH pool_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__pool_events') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            True
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
FINAL AS (
    SELECT
        tx_hash,
        block_id,
        block_timestamp,
        receipt_object_id, -- slated for rename to receipt_id
        receiver_id,
        signer_id,
        predecessor_id,
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
        _partition_by_block_number,
        inserted_timestamp AS _inserted_timestamp
    FROM
        pool_events
    WHERE
        LOG LIKE 'Contract total staked balance is%'
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS pool_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
