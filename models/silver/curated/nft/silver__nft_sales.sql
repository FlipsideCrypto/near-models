-- {{ config(
--     materialized = 'incremental',
--     merge_exclude_columns = ["inserted_timestamp"],
--     cluster_by = ['block_timestamp::DATE'],
--     unique_key = 'nft_sales_id',
--     incremental_strategy = 'merge',
--     tags = ['curated']
-- ) }}


WITH actions_events AS (

    SELECT
        *
    FROM
        near_dev.silver.actions_events_function_call_s3
    WHERE
        receipt_succeeded = TRUE
        AND logs [0] IS NOT NULL
        AND BLOCK_ID > 116338780
	   AND tx_hash = 'H2RhfiMNpHoxjMC7zc5uCUddxZKvMMCKMD3DnjhqaHqU'
),
raw_transfer as (
    SELECT
        *,
        TRY_PARSE_JSON(REPLACE(l.value::STRING, 'EVENT_JSON:', '')) AS event_json
    FROM actions_events a,
    LATERAL 
        FLATTEN(INPUT => a.logs) l
),
minbase_nft_sales as (
    --buy
    select 
        BLOCK_ID,
        BLOCK_TIMESTAMP,
        tx_hash,
    CASE 
        WHEN method_name = 'buy' THEN args:nft_contract_id
        WHEN method_name = 'resolve_nft_payout' THEN args:token:owner_id
    END :: STRING AS seller_address,
    CASE 
        WHEN method_name = 'buy' THEN signer_id
        WHEN method_name = 'resolve_nft_payout' THEN args:token:current_offer:from
    END :: STRING AS buyer_address,
    RECEIVER_ID AS platform_address,
    'Mintbase' AS platform_name,
    CASE 
        WHEN method_name = 'buy' THEN args:nft_contract_id
        WHEN method_name = 'resolve_nft_payout' THEN args:token:store_id
    END :: STRING AS nft_address,
    CASE 
        WHEN method_name = 'buy' THEN args:token_id::string
        WHEN method_name = 'resolve_nft_payout' THEN args:token:id::string
    END :: STRING AS nft_id,
    CASE 
        WHEN method_name = 'buy' THEN deposit
        WHEN method_name = 'resolve_nft_payout' THEN args:token:current_offer:price
    END / 1e24 AS price,
    CASE 
        WHEN method_name = 'buy' THEN 'nft_sale' -- buy
        WHEN method_name = 'resolve_nft_payout' THEN 'nft_sold' -- offer
    END :: STRING AS method_name,
    args AS log
    from raw_transfer
    WHERE 
    (RECEIVER_ID='simple.market.mintbase1.near' and method_name='buy') 
        OR
    (RECEIVER_ID='market.mintbase1.near' and method_name='resolve_nft_payout')
),
other_nft_sales as (
    select 
        BLOCK_ID,
        BLOCK_TIMESTAMP,
        tx_hash,
        COALESCE(args:market_data:owner_id,args:sale:owner_id, args:seller_id) as seller_address,
        COALESCE(args:buyer_id, args:offer_data:buyer_id) as buyer_address,
        RECEIVER_ID as platform_address,
        case 
            when RECEIVER_ID='marketplace.paras.near' then 'Paras'
            when RECEIVER_ID='market.l2e.near' then 'L2E'
            when RECEIVER_ID='market.nft.uniqart.near' then 'UniqArt'
            when RECEIVER_ID='market.tradeport.near' then 'TradePort' 
            when RECEIVER_ID='market.fewandfar.near' then 'FewAndFar'
            when RECEIVER_ID='apollo42.near' then 'Apollo42'
        end as platform_name,
        COALESCE(args:market_data:nft_contract_id, args:sale:nft_contract_id, args:offer_data:nft_contract_id) as nft_address,
        COALESCE(args:market_data:token_id, args:sale:token_id, args:token_id)::string as nft_id,
        COALESCE(args:price,args:offer_data:price)/1e24 as price,
        method_name,
        args as log
    FROM
        raw_transfer
    WHERE RECEIVER_ID IN ('apollo42.near','market.tradeport.near','market.nft.uniqart.near','market.l2e.near','marketplace.paras.near','market.fewandfar.near')
    AND method_name IN ('resolve_purchase','resolve_offer')
),
mitte_nft_sales as (
    SELECT
        *,
        parse_json(event_json):data:order as data,
        case 
            when split(data[2],':')[1]='near' then data[6]
            else data[1] 
        end as seller_address,
        case 
            when split(data[2],':')[1]='near' then data[1]
            else data[6] end 
        as buyer_address,
        case 
            when split(data[2],':')[1]='near' then split(data[7],':')[1]
            else split(data[2],':')[1] 
            end as nft_address,
        case 
            when split(data[2],':')[1]='near' and split(data[7],':')[4] is null then split(data[7],':')[2]::string
            when split(data[2],':')[1]='near' and split(data[7],':')[4] is not null then split(data[7],':')[2]||':'||split(data[7],':')[3]
            when split(data[2],':')[4] is null then split(data[2],':')[2]::string
            else split(data[2],':')[2]||':'||split(data[2],':')[3] 
        end as nft_id,
        case 
            when split(data[2],':')[1]='near' then split(data[2],':')[3]/1e24
            else split(data[7],':')[3]/1e24 
        end as price, 
        block_timestamp, block_id, RECEIVER_ID,
        tx_hash, 
        try_parse_json(event_json):event as method_name, 
        try_parse_json(event_json) as log
    FROM
        raw_transfer
    WHERE 
        RECEIVER_ID = 'a.mitte-orderbook.near'
        and try_parse_json( event_json ):event != 'nft_mint'
        and data[6]!='')
SELECT * FROM mitte_nft_sales