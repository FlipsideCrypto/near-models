{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ['_load_timestamp::date', 'tx_hash']
) }}

{# TODO - if I have to partition the calls, then a standard incr load will not work #}


WITH epoch_window AS (
    -- transactions greater than 5 epochs old must use an archival node. Using 4 for buffer
    -- see UNKNOWN_TRANSACTION error message https://docs.near.org/api/rpc/transactions#what-could-go-wrong-2

    SELECT
        MAX(block_id) AS curr_max_block,
        curr_max_block - (
            43200 * 1 -- TODO change back to 4 for prod
        ) AS est_epoch_start
    FROM
        {{ ref('silver__streamline_blocks') }}
),
failed_receipts AS (
    SELECT
        tx_hash
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        NOT receipt_succeeded
        AND block_id >= (
            SELECT
                est_epoch_start
            FROM
                epoch_window
        )

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
                est_epoch_start
            FROM
                epoch_window
        )
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                failed_receipts
        )


ORDER BY
    block_id
LIMIT
    1000
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
        res, -- do we care about storing the rest of the response? prob no, we have it
        _load_timestamp
    FROM
        udf_call
)
SELECT
    *
FROM
    FINAL

    -- Erroring out, not sure if this is a rate limit issue, or what.
    {#
    19 :19 :04 completed WITH 1 error
    AND 0 warnings: 19 :19 :04 19 :19 :04 database error IN model silver__tx_status (
        models / silver / streamline / silver__tx_status.sql
    ) 19 :19 :04 100287 (22P02): error parsing json response for EXTERNAL FUNCTION udf_api WITH request batch id: 01ac003c -0403 - 52da - 3d4f - 83011ae60c97 :2 :9 :0 :6881 :0. error: top - LEVEL json OBJECT must contain "data" json ARRAY ELEMENT 19 :19 :04 COMPILED code AT target / run / near / models / silver / streamline / silver__tx_status.sql #}
