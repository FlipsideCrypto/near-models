{{ config(
    materialized = 'incremental',
    incremental = 'merge',
    cluster_by = ['block_timestamp'],
    unique_key = 'tx_hash',
    tags = ['curated']
) }}

WITH actions_events_function_call AS (

    SELECT
        tx_hash,
        method_name,
        _load_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        method_name IN (
            'deposit_and_stake',
            'stake',
            'unstake',
            'unstake_all'
        ) 
        {% if var("MANUAL_FIX") %}
            AND {{ partition_load_manual('no_buffer') }}
        {% else %}
            AND {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
base_txs AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}

        {% if var("MANUAL_FIX") %}
        WHERE
            {{ partition_load_manual('no_buffer') }}
        {% else %}
        WHERE
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
txs AS (
    SELECT
        *
    FROM
        base_txs
    WHERE
        (
            tx_receiver LIKE '%.pool.near'
            OR tx_receiver LIKE '%.poolv1.near'
        )
),
pool_txs AS (
    SELECT
        txs.tx_hash AS tx_hash,
        block_timestamp,
        tx_receiver,
        tx_signer,
        tx,
        method_name,
        txs._load_timestamp AS _load_timestamp
    FROM
        txs
        INNER JOIN actions_events_function_call
        ON txs.tx_hash = actions_events_function_call.tx_hash
),
deposit_and_stake_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        tx_receiver AS pool_address,
        tx_signer,
        REGEXP_SUBSTR(
            ARRAY_TO_STRING(
                tx :receipt [0] :outcome :logs,
                ','
            ),
            'staking (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS stake_amount,
        'Stake' AS action,
        _load_timestamp
    FROM
        pool_txs
    WHERE
        method_name = 'deposit_and_stake'
        AND tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
),
stake_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        tx_receiver AS pool_address,
        tx_signer,
        REGEXP_SUBSTR(
            ARRAY_TO_STRING(
                tx :receipt [0] :outcome :logs,
                ','
            ),
            'staking (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS stake_amount,
        'Stake' AS action,
        _load_timestamp
    FROM
        pool_txs
    WHERE
        method_name = 'stake'
        AND tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
),
stake_all_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        tx_receiver AS pool_address,
        tx_signer,
        REGEXP_SUBSTR(
            ARRAY_TO_STRING(
                tx :receipt [0] :outcome :logs,
                ','
            ),
            'staking (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS stake_amount,
        'Stake' AS action,
        _load_timestamp
    FROM
        pool_txs
    WHERE
        method_name = 'stake_all'
        AND tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
),
unstake_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        tx_receiver AS pool_address,
        tx_signer,
        REGEXP_SUBSTR(
            ARRAY_TO_STRING(
                tx :receipt [0] :outcome :logs,
                ','
            ),
            'unstaking (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS stake_amount,
        'Unstake' AS action,
        _load_timestamp
    FROM
        pool_txs
    WHERE
        method_name = 'unstake'
        AND tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
),
unstake_all_txs AS (
    SELECT
        tx_hash,
        block_timestamp,
        tx_receiver AS pool_address,
        tx_signer,
        REGEXP_SUBSTR(
            ARRAY_TO_STRING(
                tx :receipt [0] :outcome :logs,
                ','
            ),
            'unstaking (\\d+)',
            1,
            1,
            'e'
        ) :: NUMBER AS stake_amount,
        'Unstake' AS action,
        _load_timestamp
    FROM
        pool_txs
    WHERE
        method_name = 'unstake_all'
        AND tx :receipt [0] :outcome :status :SuccessValue IS NOT NULL
),
FINAL AS (
    SELECT
        *
    FROM
        deposit_and_stake_txs
    UNION
    SELECT
        *
    FROM
        stake_all_txs
    UNION
    SELECT
        *
    FROM
        unstake_txs
    UNION
    SELECT
        *
    FROM
        unstake_all_txs
)
SELECT
    *
FROM
    FINAL
