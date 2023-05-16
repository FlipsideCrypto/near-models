{{ config(
    materialized = "incremental",
    cluster_by = ["_load_timestamp::DATE","block_timestamp::DATE"],
    unique_key = "action_id",
    incremental_strategy = "delete+insert",
    tags = ['curated']
) }}

WITH logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs_s3') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
receipts AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_receipts_final') }}
    WHERE
        {% if target.name == 'manual_fix' or target.name == 'manual_fix_dev' %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {{ incremental_load_filter('_load_timestamp') }}
        {% endif %}
),
standard_logs AS (
    SELECT
        action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        TRY_PARSE_JSON(clean_log) AS clean_log
    FROM
        logs
    WHERE
        is_standard = TRUE
),
nft_events AS (
    SELECT
        *,
        clean_log :data AS DATA,
        clean_log :event AS event,
        clean_log :standard AS STANDARD,
        clean_log :version AS version
    FROM
        standard_logs
    WHERE
        STANDARD = 'nep171' -- nep171 nft STANDARD, version  nep245 IS multitoken STANDARD,  nep141 IS fungible token STANDARD
        AND event = 'nft_mint'
    ORDER BY
        block_timestamp DESC
),
raw_mint_events AS (
    SELECT
        action_id,
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        INDEX AS batch_index,
        ARRAY_SIZE(
            DATA :: ARRAY
        ) AS owner_per_txns,
        VALUE :owner_id :: STRING AS owner_id,
        VALUE :token_ids :: ARRAY AS tokens,
        TRY_PARSE_JSON(
            DATA :memo
        ) AS memo
    FROM
        nft_events,
        LATERAL FLATTEN(
            input => DATA
        )
    ORDER BY
        block_timestamp DESC
),
mint_events AS (
    SELECT
        tx_hash,
        receipt_object_id,
        block_id,
        block_timestamp,
        receiver_id,
        signer_id,
        _LOAD_TIMESTAMP,
        _PARTITION_BY_BLOCK_NUMBER,
        owner_per_txns,
        batch_index,
        owner_id,
        tokens,
        memo,
        INDEX AS token_index,
        ARRAY_SIZE(
            tokens :: ARRAY
        ) AS mint_per_txns,
        VALUE AS token_id,
        concat_ws(
            '-',
            receipt_object_id,
            batch_index,
            token_index
        ) AS action_id
    FROM
        raw_mint_events,
        LATERAL FLATTEN(
            input => tokens
        )
),
mint_receipts AS (
    SELECT
        tx_hash,
        receipt_index,
        receipt_object_id AS receipt_id,
        receipt_outcome_id :: STRING AS receipt_outcome_id,
        receiver_id,
        gas_burnt / pow(
            10,
            16
        ) AS mint_fee -- // Tgas
    FROM
        receipts
    WHERE
        receipt_id IN (
            SELECT
                DISTINCT receipt_object_id
            FROM
                mint_events
        )
)
SELECT
    mint_events.action_id,
    mint_events.tx_hash,
    mint_events.receipt_object_id,
    mint_events.block_id,
    mint_events.block_timestamp,
    mint_events.receiver_id,
    mint_events.signer_id,
    mint_events._LOAD_TIMESTAMP,
    mint_events._PARTITION_BY_BLOCK_NUMBER,
    mint_events.owner_per_txns,
    mint_events.owner_id,
    mint_events.memo,
    mint_events.mint_per_txns,
    mint_events.token_id,
    (
        mint_receipts.mint_fee / (
            owner_per_txns * mint_per_txns
        )
    ) :: FLOAT AS mint_fee
FROM
    mint_events
    LEFT JOIN mint_receipts
    ON mint_events.receipt_object_id = mint_receipts.receipt_id
ORDER BY
    block_timestamp DESC
