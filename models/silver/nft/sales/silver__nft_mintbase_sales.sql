{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    unique_key = 'nft_mintbase_sales_id',
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate_custom", "block_timestamp::date"],
    tags = ['scheduled_non_core']
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
        receipt_predecessor_id AS predecessor_id,
        action_data :method_name :: STRING AS method_name,
        action_data :args :: VARIANT AS args,
        action_data :deposit :: INT AS deposit,
        action_data :gas :: NUMBER AS attached_gas,
        _partition_by_block_number
    FROM
        {{ ref('core__ez_actions') }}
    WHERE
        receipt_succeeded
        AND action_name = 'FunctionCall'
        AND receipt_receiver_id in ('simple.market.mintbase1.near' , 'market.mintbase1.near')
        AND method_name in ('buy', 'resolve_nft_payout')

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
        l.log_index,
        TRY_PARSE_JSON(clean_log) AS event_json,
        event_json :event :: STRING AS event_type,
        event_json :standard :: STRING AS standard,
        TRY_PARSE_JSON(event_json:data) as mb_logs
    FROM
        {{ ref('silver__logs_s3') }} l
    INNER JOIN actions a 
    ON a.tx_hash = l.tx_hash
    WHERE
        is_standard
        AND event_json :standard :: STRING = 'mb_market' 
        AND (event_type = 'nft_sale' OR event_type = 'nft_sold')

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
mintbase_nft_sales AS (
    SELECT
        receipt_id,
        action_index,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        method_name = 'buy' AS is_buy,  --- else resolve_nft_payout
        IFF(is_buy, args :nft_contract_id, args :token :owner_id) :: STRING AS seller_address,        
        IFF(is_buy, signer_id, args :token :current_offer :from) :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Mintbase' AS platform_name,
        IFF(is_buy, args :nft_contract_id, args :token :store_id) :: STRING AS nft_address,
        IFF(is_buy, args :token_id, args :token :id) :: STRING AS token_id,
        IFF(is_buy, deposit, args :token :current_offer :price) / 1e24 AS price,
        IFF(is_buy, 'nft_sale', 'nft_sold') AS method_name,
        args AS LOG,
        _partition_by_block_number
    FROM
        actions
),
FINAL AS (
    SELECT
        m.receipt_id,
        m.action_index,
        m.block_id,
        m.block_timestamp,
        m.tx_hash,
        m.attached_gas,
        m.method_name,
        m.is_buy,
        m.seller_address,
        m.buyer_address,
        m.platform_address,
        m.platform_name,
        m.nft_address,
        m.token_id,
        m.price,
        m.LOG,
        m._partition_by_block_number,
        mb_logs :affiliate_id :: STRING AS affiliate_id,
        mb_logs :affiliate_amount / 1e24 :: STRING  AS affiliate_amount,
        mb_logs :payout :: object AS royalties,
        COALESCE(mb_logs :mintbase_amount / 1e24, 
                 IFF(affiliate_amount IS NULL AND royalties IS NULL, 
                    price * 0.05, 
                    price * 0.025)
        ) :: FLOAT AS platform_fee,
        r.log_index
    FROM
        mintbase_nft_sales m
    LEFT JOIN
        logs r 
    ON
        m.tx_hash = r.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'receipt_id', 'action_index', 'log_index']
    ) }} AS nft_mintbase_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
