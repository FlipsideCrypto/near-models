{{ config(
    materialized = 'incremental',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'nft_mintbase_sales_id',
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
        AND receiver_id in ('simple.market.mintbase1.near' , 'market.mintbase1.near')
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
        TRY_PARSE_JSON(REPLACE(l.value :: STRING, 'EVENT_JSON:', '')) AS event_json,
        event_json:event as event_type,
        event_json:standard as standard
    FROM
        actions_events A,
        LATERAL FLATTEN(
            input => A.logs
        ) l
),
mintbase_nft_sales AS (
    SELECT
        action_id,
        block_id,
        block_timestamp,
        tx_hash,
        attached_gas,
        IFF(method_name = 'buy', TRUE, FALSE) AS is_buy,  --- else resolve_nft_payout
        IFF(is_buy, args :nft_contract_id, args :token :owner_id) :: STRING AS seller_address,
        IFF( is_buy, signer_id, args :token :current_offer :from) :: STRING AS buyer_address,
        receiver_id AS platform_address,
        'Mintbase' AS platform_name,
        IFF(is_buy, args :nft_contract_id, args :token :store_id) :: STRING AS nft_address,
        IFF(is_buy, args :token_id, args :token :id) :: STRING AS token_id,
        IFF(is_buy, deposit, args :token :current_offer :price) / 1e24 AS price,
        IFF(is_buy, 'nft_sale', 'nft_sold') AS method_name,
        args AS LOG,
        logs_index,
        _inserted_timestamp,
        _partition_by_block_number
    FROM
        raw_logs
    WHERE
        (
           method_name = 'buy'
        )
        OR (
            method_name = 'resolve_nft_payout'
        )
),
mintbase_royalties AS (
    SELECT
        tx_hash,
        method_name,
        PARSE_JSON(event_json:data) as mb_logs
    FROM
        raw_logs
    WHERE
        standard = 'mb_market' and (event_type = 'nft_sale' OR event_type = 'nft_sold')
),
FINAL AS (
    SELECT
        m.*,
        mb_logs,
        mb_logs:affiliate_id:: STRING AS affiliate_id,
        mb_logs:affiliate_amount / 1e24 :: STRING  AS affiliate_amount,
        mb_logs:payout :: object AS royalties,
        COALESCE(mb_logs:mintbase_amount / 1e24, 
                 IFF(affiliate_amount IS NULL AND royalties IS NULL, 
                    price * 0.05, 
                    price * 0.025)
        ) :: FLOAT AS platform_fee
    FROM
        mintbase_nft_sales m
    LEFT JOIN
        mintbase_royalties r 
    ON
        m.tx_hash = r.tx_hash
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['action_id', 'logs_index']
    ) }} AS nft_mintbase_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL



-- TODO clean this up and create test cases