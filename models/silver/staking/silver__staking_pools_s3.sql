{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'staking_pools_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash,receipt_id,owner,address);",
    tags = ['curated','scheduled_non_core']
) }}

WITH actions AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        receipt_id,
        receipt_receiver_id,
        receipt_signer_id,
        tx_signer,
        tx_receiver,
        action_data :method_name :: STRING AS method_name,
        action_data :args AS args,
        receipt_succeeded,
        tx_succeeded,
        _partition_by_block_number
    FROM 
        {{ ref('core__ez_actions') }}
    WHERE 
        action_name = 'FunctionCall'
        AND receipt_succeeded
        AND tx_succeeded
        AND method_name IN (
            'create_staking_pool',
            'update_reward_fee_fraction',
            'new'
        )

    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),

new_pools AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        receipt_id,
        args :owner_id :: STRING AS owner,
        receipt_receiver_id AS address,
        TRY_PARSE_JSON(
            args :reward_fee_fraction
        ) AS reward_fee_fraction,
        'Create' AS tx_type,
        _partition_by_block_number
    FROM
        actions
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                actions
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
        receipt_id,
        tx_signer AS owner,
        tx_receiver AS address,
        TRY_PARSE_JSON(
            args :reward_fee_fraction
        ) AS reward_fee_fraction,
        'Update' AS tx_type,
        _partition_by_block_number
    FROM
        actions
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
    tx_hash,
    block_timestamp,
    block_id,
    receipt_id,
    owner,
    address,
    reward_fee_fraction,
    tx_type,
    _partition_by_block_number,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id']
    ) }} AS staking_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    address LIKE '%pool%'
