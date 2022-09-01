{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__receipts') }}
    WHERE
        {{ incremental_load_filter('_inserted_timestamp') }}
),
tx AS (
    SELECT
        REPLACE(
            VALUE,
            'EVENT_JSON:'
        ) AS json,
        TRY_PARSE_JSON(json) :event :: STRING AS event,
        TRY_PARSE_JSON(json) :data AS data_log,
        REGEXP_SUBSTR(
            status_value,
            'Success'
        ) AS reg_success,
        _ingested_at,
        _inserted_timestamp
    FROM
        txs,
        TABLE(FLATTEN(input => logs))
    WHERE
        1 = 1
        AND reg_success IS NOT NULL
        AND event IS NOT NULL
),
nft_event AS (
    SELECT
        VALUE :authorized_id :: STRING AS nft_project,
        VALUE :new_owner_id :: STRING AS buyer,
        VALUE :old_owner_id :: STRING AS seller,
        VALUE :token_ids [0] :: STRING AS nft_id,
        reg_success AS tx_status,
        _ingested_at,
        _inserted_timestamp
    FROM
        tx,
        LATERAL FLATTEN(
            input => data_log
        )
    WHERE
        event = 'nft_transfer'
),
FINAL AS (
    SELECT
        nft_project,
        buyer,
        seller,
        nft_id,
        tx_status,
        _ingested_at,
        _inserted_timestamp
    FROM
        nft_event
)
SELECT
    *
FROM
    FINAL
