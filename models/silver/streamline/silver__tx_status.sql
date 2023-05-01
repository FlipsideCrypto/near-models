{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ['_load_timestamp::date', 'tx_hash']
) }}

WITH epoch_window AS (
    -- transactions greater than 5 epochs old must use an archival node
    -- see UNKNOWN_TRANSACTION error message https://docs.near.org/api/rpc/transactions#what-could-go-wrong-2

    SELECT
        MAX(block_id) AS curr_max_block,
        curr_max_block - (
            43200 * 5
        ) AS min_epoch_window
    FROM
        {{ ref('silver__streamline_blocks') }}
),
txs AS (
    SELECT
        tx_hash,
        tx_signer,
        block_id,
        _load_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        block_id >= (
            SELECT
                min_epoch_window
            FROM
                epoch_window
        )

{% if is_incremental() %}
AND (
    _load_timestamp >= (
        SELECT
            MAX(_load_timestamp)
        FROM
            {{ this }}
    )
    OR block_id >= (
        SELECT
            MAX(block_id)
        FROM
            {{ this }}
    )
)
{% endif %}
ORDER BY 
    block_id
LIMIT 10000
),
stored_key AS (
    SELECT
        api_key
    FROM
        near._internal.api_key
    WHERE
        platform = 'pagoda'
),
udf_call AS (
    SELECT
        tx_hash,
        tx_signer,
        block_id,
        ethereum.streamline.udf_api(
            'POST',
            'https://near-mainnet.api.pagoda.co/rpc/v1/',
            {
                'x-api-key': api_key,
                'Content-Type': 'application/json' 
            },
            { 
                'jsonrpc': '2.0',
                'id': 'dontcare',
                'method' :'tx',
                'params' :[tx_hash, tx_signer] 
            }
        ) AS res,
        _load_timestamp
    FROM
        txs full
        OUTER JOIN stored_key
),
FINAL AS (
    SELECT
        tx_hash,
        tx_signer,
        block_id,
        object_keys(
            res :data
        ) [0] != 'error' AS api_call_succeeded,
        TRY_PARSE_JSON(
            res :data :result :status
        ) AS status,
        object_keys(status) [0] :: STRING != 'Failure' AS tx_succeeded,
        res,
        _load_timestamp
    FROM
        udf_call
)
SELECT
    *
FROM
    FINAL
WHERE
    api_call_succeeded



-- Erroring out, not sure if this is a rate limit issue, or what.
{# 
19:19:04  Completed with 1 error and 0 warnings:
19:19:04  
19:19:04  Database Error in model silver__tx_status (models/silver/streamline/silver__tx_status.sql)
19:19:04    100287 (22P02): Error parsing JSON response for external function UDF_API with request batch id: 01ac003c-0403-52da-3d4f-83011ae60c97:2:9:0:6881:0. Error: top-level JSON object must contain "data" JSON array element
19:19:04    compiled Code at target/run/near/models/silver/streamline/silver__tx_status.sql #}
