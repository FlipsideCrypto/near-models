{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_other_sales_id',
    incremental_strategy = 'merge',
    tags = ['curated','scheduled_non_core']
) }}

WITH actions_events AS (

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        action_id,
        signer_id,
        receiver_id,
        method_name,
        deposit,
        args,
        logs,
        attached_gas,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        AND  receiver_id IN (
            'apollo42.near',
            'market.tradeport.near',
            'market.nft.uniqart.near',
            'market.l2e.near',
            'market.fewandfar.near',
            'a.mitte-orderbook.near'
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
raw_logs AS (
    SELECT
        *,
        l.index AS logs_index,
        TRY_PARSE_JSON(REPLACE(l.value :: STRING, 'EVENT_JSON:', '')) AS event_json
    FROM
        actions_events A,
        LATERAL FLATTEN(
            input => A.logs
        ) l
),
------------------------ OTHER MARKETPLACES  -------------------------------
other_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        COALESCE(
            args :market_data :owner_id,
            args :sale :owner_id,
            args :seller_id
        ) :: STRING AS seller_address,
        COALESCE(
            args :buyer_id,
            args :offer_data :buyer_id
        ) :: STRING AS buyer_address,
        receiver_id AS platform_address,
        CASE
            WHEN receiver_id = 'market.l2e.near' THEN 'L2E'
            WHEN receiver_id = 'market.nft.uniqart.near' THEN 'UniqArt'
            WHEN receiver_id = 'market.tradeport.near' THEN 'TradePort'
            WHEN receiver_id = 'market.fewandfar.near' THEN 'FewAndFar'
            WHEN receiver_id = 'apollo42.near' THEN 'Apollo42'
        END :: STRING AS platform_name,
        COALESCE(
            args :market_data :nft_contract_id,
            args :sale :nft_contract_id,
            args :offer_data :nft_contract_id
        ) :: STRING AS nft_address,
        COALESCE(
            args :market_data :token_id,
            args :sale :token_id,
            args :token_id
        ) :: STRING AS token_id,
        COALESCE(
            args :price,
            args :offer_data :price,
            args: market_data :price
        ) / 1e24 AS price,
        method_name,
        args AS LOG,
        logs_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        raw_logs
    WHERE
        receiver_id IN (
            'apollo42.near',
            'market.tradeport.near',
            'market.nft.uniqart.near',
            'market.l2e.near',
            'market.fewandfar.near'
        )
        AND method_name IN (
            'resolve_purchase',
            'resolve_offer'
        )
),
------------------------------- MITTE  -------------------------------
mitte_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        CASE
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near' THEN event_json :data :order [6]
            ELSE event_json :data :order [1]
        END :: STRING AS seller_address,
        CASE
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near' THEN event_json :data :order [1]
            ELSE event_json :data :order [6]
        END :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Mitte' AS platform_name,
        CASE
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near' THEN SPLIT(
                event_json :data :order [7],
                ':'
            ) [1]
            ELSE SPLIT(
                event_json :data :order [2],
                ':'
            ) [1]
        END :: STRING AS nft_address,
        CASE
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near'
            AND SPLIT(
                event_json :data :order [7],
                ':'
            ) [4] IS NULL THEN SPLIT(
                event_json :data :order [7],
                ':'
            ) [2]
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near'
            AND SPLIT(
                event_json :data :order [7],
                ':'
            ) [4] IS NOT NULL THEN SPLIT(
                event_json :data :order [7],
                ':'
            ) [2] || ':' || SPLIT(
                event_json :data :order [7],
                ':'
            ) [3]
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [4] IS NULL THEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [2]
            ELSE SPLIT(
                event_json :data :order [2],
                ':'
            ) [2] || ':' || SPLIT(
                event_json :data :order [2],
                ':'
            ) [3]
        END :: STRING AS token_id,
        CASE
            WHEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [1] = 'near' THEN SPLIT(
                event_json :data :order [2],
                ':'
            ) [3]
            ELSE SPLIT(
                event_json :data :order [7],
                ':'
            ) [3]
        END / 1e24 AS price,
        event_json :event :: STRING AS method_name,
        event_json AS LOG,
        logs_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        raw_logs
    WHERE
        receiver_id = 'a.mitte-orderbook.near'
        AND event_json :event :: STRING != 'nft_mint'
        AND event_json :data :order [6] :: STRING != ''
),
------------------------------- FINAL   -------------------------------
sales_union AS (
    SELECT
        *
    FROM
        other_nft_sales
    UNION ALL
    SELECT
        *
    FROM
        mitte_nft_sales
),
FINAL AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        seller_address,
        buyer_address,
        platform_address,
        platform_name,
        nft_address,
        token_id,
        method_name,
        LOG,
        price,
        NULL :: STRING AS affiliate_id,
        NULL :: STRING AS affiliate_amount,
        '{}' :: VARIANT AS royalties,
        NULL :: FLOAT AS platform_fee,
        logs_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        sales_union s
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'logs_index']
    ) }} AS nft_other_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
