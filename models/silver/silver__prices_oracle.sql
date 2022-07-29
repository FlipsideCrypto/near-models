{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
token_labels AS (
    SELECT
        *
    FROM
        {{ ref('seeds__token_labels') }}
),
oracle_msgs AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        ARRAY_SIZE(
            tx :actions
        ) AS actions_len,
        tx :actions [0] :FunctionCall :method_name :: STRING AS method_name,
        TRY_PARSE_JSON(
            TRY_BASE64_DECODE_STRING(
                tx :actions [0] :FunctionCall :args
            )
        ) AS response,
        tx,
        _inserted_timestamp
    FROM
        txs
    WHERE
        tx_receiver = 'priceoracle.near'
        AND method_name = 'report_prices'
),
prices AS (
    SELECT
        block_id,
        tx_hash,
        block_timestamp,
        tx_receiver,
        actions_len,
        INDEX,
        VALUE :asset_id :: STRING AS token_contract,
        CASE
            WHEN asset_id = 'aurora' THEN VALUE :price :multiplier :: DOUBLE / pow (
                10,
                7
            )
            ELSE VALUE :price :multiplier :: DOUBLE / pow (
                10,
                4
            )
        END AS price_usd,
        _inserted_timestamp
    FROM
        oracle_msgs,
        LATERAL FLATTEN(
            input => response :prices
        )
),
add_labels AS (
    SELECT
        p.block_id,
        p.tx_hash,
        p.block_timestamp,
        p.actions_len,
        p.index,
        l.token,
        l.symbol,
        p.token_contract,
        p.price_usd,
        p.tx_receiver AS source,
        p._inserted_timestamp
    FROM
        prices p
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
    *
FROM
    add_labels
