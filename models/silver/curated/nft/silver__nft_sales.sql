{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_sales_id',
    incremental_strategy = 'merge',
    tags = ['curated']
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
        _inserted_timestamp,
        modified_timestamp as _modified_timestamp
    FROM
         {{ ref('silver__actions_events_function_call_s3') }}
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
),

tx AS (
    SELECT
        tx_hash,
        transaction_fee
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        {% if var("MANUAL_FIX") %}
            {{ partition_load_manual('no_buffer') }}
        {% else %}
            {% if var('IS_MIGRATION') %}
                {{ incremental_load_filter('_inserted_timestamp') }}
            {% else %}
                {{ incremental_load_filter('_modified_timestamp') }}
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
minbase_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        CASE
            WHEN method_name = 'buy' THEN args :nft_contract_id
            WHEN method_name = 'resolve_nft_payout' THEN args :token :owner_id
        END :: STRING AS seller_address,
        CASE
            WHEN method_name = 'buy' THEN signer_id
            WHEN method_name = 'resolve_nft_payout' THEN args :token :current_offer :from
        END :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Mintbase' AS platform_name,
        CASE
            WHEN method_name = 'buy' THEN args :nft_contract_id
            WHEN method_name = 'resolve_nft_payout' THEN args :token :store_id
        END :: STRING AS nft_address,
        CASE
            WHEN method_name = 'buy' THEN args :token_id :: STRING
            WHEN method_name = 'resolve_nft_payout' THEN args :token :id :: STRING -- typos
        END :: STRING AS nft_id,
        CASE
            WHEN method_name = 'buy' THEN deposit
            WHEN method_name = 'resolve_nft_payout' THEN args :token :current_offer :price
        END / 1e24 AS price,
        CASE
            WHEN method_name = 'buy' THEN 'nft_sale' -- buy
            WHEN method_name = 'resolve_nft_payout' THEN 'nft_sold' -- offer
        END :: STRING AS method_name,
        args AS LOG,
        logs_index,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        raw_logs
    WHERE
        (
            receiver_id = 'simple.market.mintbase1.near'
            AND method_name = 'buy'
        )
        OR (
            receiver_id = 'market.mintbase1.near'
            AND method_name = 'resolve_nft_payout'
        )
),
other_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
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
            WHEN receiver_id = 'marketplace.paras.near' THEN 'Paras'
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
        ) :: STRING AS nft_id,
        COALESCE(
            args :price,
            args :offer_data :price,
            args : market_data :price
        ) / 1e24 AS price,
        method_name,
        args AS LOG,
        logs_index,
        _inserted_timestamp,
        _modified_timestamp
    FROM
        raw_logs
    WHERE
        receiver_id IN (
            'apollo42.near',
            'market.tradeport.near',
            'market.nft.uniqart.near',
            'market.l2e.near',
            'marketplace.paras.near',
            'market.fewandfar.near'
        )
        AND method_name IN (
            'resolve_purchase',
            'resolve_offer'
        )
),
mitte_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
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
        END :: STRING AS nft_id,
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
        _modified_timestamp
    FROM
        raw_logs
    WHERE
        receiver_id = 'a.mitte-orderbook.near'
        AND event_json :event :: STRING != 'nft_mint'
        AND event_json :data :order [6] :: STRING != ''
),
sales_union AS (
    SELECT
        *
    FROM
        minbase_nft_sales
    UNION ALL
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
FINAL AS 
    (
    SELECT
        s.*,
        t.transaction_fee as transaction_fee
    FROM
        sales_union s
    INNER JOIN tx t ON s.tx_hash = t.tx_hash
    )
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'logs_index']
    ) }} AS nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL 