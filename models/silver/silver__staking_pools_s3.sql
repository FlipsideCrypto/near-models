{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp'],
    unique_key = 'tx_hash',
    incremental_strategy = 'merge',
    tags = ['curated', 'curated_s3']
) }}

WITH txs AS (

    SELECT
        tx_hash,
        block_timestamp,
        tx_signer,
        tx_receiver,
        tx,
        _load_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        {{ incremental_load_filter('_load_timestamp') }}
),
function_calls AS (
    SELECT
        tx_hash,
        args,
        method_name,
        _load_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'create_staking_pool',
            'update_reward_fee_fraction'
        )
        AND {{ incremental_load_filter('_load_timestamp') }}
),
pool_txs AS (
    SELECT
        txs.tx_hash AS tx_hash,
        block_timestamp,
        tx_signer,
        tx_receiver,
        args,
        method_name,
        tx,
        txs._load_timestamp AS _load_timestamp
    FROM
        txs
        INNER JOIN function_calls
        ON txs.tx_hash = function_calls.tx_hash
    WHERE
        tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
        OR (
            method_name = 'create_staking_pool'
            AND tx :receipt [0] :outcome :status :SuccessReceiptId IS NOT NULL
            AND tx :receipt [1] :outcome :status :SuccessValue IS NOT NULL
        )
),
FINAL AS (
    SELECT
        pool_txs.tx_hash AS tx_hash,
        block_timestamp,
        IFF(
            method_name = 'create_staking_pool',
            args :: variant :: OBJECT :owner_id,
            tx_signer
        ) AS owner,
        IFF(
            method_name = 'create_staking_pool',
            tx :receipt [1] :outcome :executor_id :: text,
            tx_receiver
        ) AS address,
        args :: variant :: OBJECT :reward_fee_fraction AS reward_fee_fraction,
        IFF(
            method_name = 'create_staking_pool',
            'Create',
            'Update'
        ) AS tx_type,
        _load_timestamp
    FROM
        pool_txs
)
SELECT
    *
FROM
    FINAL
