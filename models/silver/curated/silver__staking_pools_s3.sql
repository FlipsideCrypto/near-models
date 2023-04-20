{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp'],
    unique_key = 'tx_hash',
    incremental_strategy = 'merge',
    tags = ['curated']
) }}

WITH txs AS (

    SELECT
        tx_hash,
        block_timestamp,
        block_id,
        tx_signer,
        tx_receiver,
        tx,
        tx_status,
        _load_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
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
        _load_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'create_staking_pool',
            'update_reward_fee_fraction',
            'new'
        ) {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_load_timestamp') }}
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
        _load_timestamp
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
        _load_timestamp
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
        _load_timestamp
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
    *
FROM
    FINAL
WHERE
    address LIKE '%pool%'
