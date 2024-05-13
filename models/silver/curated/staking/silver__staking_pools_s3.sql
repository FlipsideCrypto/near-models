{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp'],
    unique_key = 'tx_hash',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['curated','scheduled_non_core']
) }}
{# Note - multisource model #}
WITH txs AS (

    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        tx_signer,
        tx_receiver,
        tx,
        tx_status,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}

    {% if var("MANUAL_FIX") %}
      WHERE {{ partition_load_manual('no_buffer') }}
    {% else %}
        WHERE _modified_timestamp >= (
            SELECT
                MAX(_modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),
function_calls AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        SPLIT(
            action_id,
            '-'
        ) [0] :: STRING AS receipt_object_id,
        receiver_id,
        signer_id,
        method_name,
        args,
        _partition_by_block_number,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'create_staking_pool',
            'update_reward_fee_fraction',
            'new'
        ) 

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
            {% if is_incremental() %}
        AND _modified_timestamp >= (
            SELECT
                MAX(_modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    {% endif %}
),
add_addresses_from_tx AS (
    SELECT
        fc.tx_hash,
        fc.block_timestamp,
        fc.block_id,
        receipt_object_id,
        tx_receiver,
        tx_signer,
        receiver_id,
        signer_id,
        method_name,
        args,
        tx_status,
        txs._partition_by_block_number,
        txs._inserted_timestamp,
        txs._modified_timestamp
    FROM
        function_calls fc
        LEFT JOIN txs USING (tx_hash)
),
new_pools AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        args :owner_id :: STRING AS owner,
        receiver_id AS address,
        TRY_PARSE_JSON(
            args :reward_fee_fraction
        ) AS reward_fee_fraction,
        'Create' AS tx_type,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        add_addresses_from_tx
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                add_addresses_from_tx
            WHERE
                method_name = 'create_staking_pool'
        )
        AND method_name = 'new'
),
updated_pools AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        tx_signer AS owner,
        tx_receiver AS address,
        TRY_PARSE_JSON(
            args :reward_fee_fraction
        ) AS reward_fee_fraction,
        'Update' AS tx_type,
        _partition_by_block_number,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        add_addresses_from_tx
    WHERE
        method_name = 'update_reward_fee_fraction'
        AND reward_fee_fraction IS NOT NULL
),
FINAL AS (
    SELECT
        *
    FROM
        new_pools
    UNION ALL
    SELECT
        *
    FROM
        updated_pools
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS staking_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    address LIKE '%pool%'
