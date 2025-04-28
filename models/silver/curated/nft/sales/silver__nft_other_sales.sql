{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'nft_other_sales_id',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom","block_timestamp::date"],
    tags = ['curated','scheduled_non_core']
) }}

WITH actions AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        receipt_id,
        action_index,
        receipt_signer_id AS signer_id,
        receipt_receiver_id AS receiver_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        action_data :deposit :: INT AS deposit,
        action_data :gas :: NUMBER AS attached_gas,
        _partition_by_block_number,
        _inserted_timestamp
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_succeeded
        AND action_name = 'FunctionCall'
        AND receipt_receiver_id IN (
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
logs AS (
    SELECT
        l.tx_hash,
        l.receipt_id,
        l.block_id,
        l.block_timestamp,
        l.log_index,
        l.receiver_id,
        l.predecessor_id,
        TRY_PARSE_JSON(clean_log) AS event_json,
        event_json :event :: STRING AS event_type
    FROM
        {{ ref('silver__logs_s3') }} l
    INNER JOIN actions a 
    ON a.tx_hash = l.tx_hash
    WHERE
        l.is_standard
    {% if var("MANUAL_FIX") %}
      AND {{ partition_load_manual('no_buffer') }}
    {% else %}
        {% if is_incremental() %}
        AND l.block_timestamp >= (
            SELECT
                MAX(block_timestamp) - INTERVAL '48 HOURS'
            FROM
                {{ this }}
        )
        {% endif %}
    {% endif %}
),
other_nft_sales AS (
    SELECT
        receipt_id,
        action_index,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        args AS LOG,
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
            args :market_data :price
        ) / 1e24 AS price,
        method_name,
        _partition_by_block_number
    FROM
        actions
    WHERE
        receiver_id IN (
            'apollo42.near',
            'market.tradeport.near',
            'market.nft.uniqart.near',
            'market.l2e.near',
            'market.fewandfar.near'
        )
        AND method_name IN ('resolve_purchase', 'resolve_offer')
),

mitte_nft_sales AS (
    SELECT
        receipt_id,
        log_index AS action_index,
        block_id,
        block_timestamp,
        tx_hash,
        NULL AS attached_gas,
        event_json AS LOG,
        CASE
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near' THEN LOG :data :order [6]
            ELSE LOG :data :order [1]
        END :: STRING AS seller_address,
        CASE
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near' THEN LOG :data :order [1]
            ELSE LOG :data :order [6]
        END :: STRING AS buyer_address,
        predecessor_id AS platform_address,
        'Mitte' AS platform_name,
        CASE
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near' THEN SPLIT(
                LOG :data :order [7],
                ':'
            ) [1]
            ELSE SPLIT(
                LOG :data :order [2],
                ':'
            ) [1]
        END :: STRING AS nft_address,
        CASE
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near'
            AND SPLIT(
                LOG :data :order [7],
                ':'
            ) [4] IS NULL THEN SPLIT(
                LOG :data :order [7],
                ':'
            ) [2]
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near'
            AND SPLIT(
                LOG :data :order [7],
                ':'
            ) [4] IS NOT NULL THEN SPLIT(
                LOG :data :order [7],
                ':'
            ) [2] || ':' || SPLIT(
                LOG :data :order [7],
                ':'
            ) [3]
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [4] IS NULL THEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [2]
            ELSE SPLIT(
                LOG :data :order [2],
                ':'
            ) [2] || ':' || SPLIT(
                LOG :data :order [2],
                ':'
            ) [3]
        END :: STRING AS token_id,
        CASE
            WHEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [1] = 'near' THEN SPLIT(
                LOG :data :order [2],
                ':'
            ) [3]
            ELSE SPLIT(
                LOG :data :order [7],
                ':'
            ) [3]
        END / 1e24 AS price,
        LOG :event :: STRING AS method_name,
        _partition_by_block_number
    FROM
        logs
    WHERE
        receiver_id = 'a.mitte-orderbook.near'
        AND LOG :event :: STRING != 'nft_mint'
        AND LOG :data :order [6] :: STRING != ''
),
sales_union AS (
    SELECT * FROM other_nft_sales
    UNION ALL
    SELECT * FROM mitte_nft_sales
),
FINAL AS (
    SELECT
        receipt_id,
        action_index,
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
        _partition_by_block_number
    FROM sales_union
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['receipt_id', 'action_index']
    ) }} AS nft_other_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM FINAL
