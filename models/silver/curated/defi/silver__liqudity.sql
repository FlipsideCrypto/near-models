{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH actions as (
    SELECT
        *
    FROM
        {{ ref('silver__actions_events_function_call_s3') }}
    {% if is_incremental() %}
        WHERE
            {{ incremental_load_filter('_modified_timestamp') }}
    {% endif %}
),
pricet as (
    select
    timestamp::date as date,
    token_contract,
    avg(price_usd) as usd_price
    from near.price.fact_prices
    group by 1, 2
),-- Here we get the price of the tokens


--Note that some pools, their information is not in the transaction. Therefore, we have to hard code the pools in the code. Therefore, the reason why we have added this section is because of the pool not being clear in the transaction information. We have obtained this information in this way:

veax_pools as (
    select
    'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' as token0,
    '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' as token1,
    3658233 as pool_id

    union all

    select
    'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 1995

    union all

    select
    'usdt.tether-token.near', 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 5248

    union all

    select
    'usdt.tether-token.near', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 15998

    union all

    select
    'wrap.near', 'linear-protocol.near', 4948

    union all

    select
    'aurora', 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 15894

    union all

    select
    'meta-token.near', 'wrap.near', 4511531

    union all

    select
    'token.sweat', 'wrap.near', 5144

    union all

    select
    'aurora', 'usdt.tether-token.near', 15893

    union all

    select
    '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near', 'usdt.tether-token.near', 15844

    union all

    select
    'wrap.near', 'usdt.tether-token.near', 15763

    union all

    select
    'aurora', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 15945 

    union all

    select
    'token.stlb.near', 'wrap.near', 5123

    union all

    select
    'aurora', 'wrap.near', 5212

    union all

    select
    '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near', 'wrap.near', 5179

    union all

    select
    'wrap.near', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 3802

    union all

    select
    'aurora', '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near', 5174 

    union all

    select
    'meta-pool.near', 'wrap.near', 4941

    union all

    select
    'token.paras.near', 'wrap.near', 254242

    union all

    select
    'f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near', 'wrap.near', 254931

    union all

    select
    'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near', 'wrap.near', 5146

    union all

    select
    '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near', 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near', 15846

    union all

    select
    '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near', 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 15845

    union all

    select
    'wrap.near', 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near', 5318
),

ref_finance_deposit AS (
    -- First we will review Ref Finance. In the transactions of this marketplace, there is a method called add_liquidity. First, we calculate this part and form the deposit table. Note that part of the data is in the fact_receipts table and part of the data is in the same fact_actions_events_function_call table. So, we will make a relationship between these two tables and through logs and with the help of regular expressions, we will get the first and second token along with their value. A properly written rule separates the different sections.
    -- It is also important to note that in all sections, we have tried to count successful transactions and therefore unsuccessful transactions will not have a place in the results.
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        'Deposit' AS method_name,
        'Ref Finance' AS platform,
        args:pool_id AS pool_id,
        trim(regexp_substr(logs[0], '["]\\d+([^"]*)', 1, 1, 'e', 1)) AS token0_contract,
        nvl(regexp_substr(logs[0], '[ \\"](\\d+)[ ]', 1, 1, 'e', 1), args:amounts[0])::variant AS token0_amount_raw,
        trim(regexp_substr(logs[0], '["]\\d+([^"]*)', 1, 2, 'e', 1)) AS token1_contract,
        nvl(regexp_substr(logs[0], '[ \\"](\\d+)[ ]', 1, 2, 'e', 1), args:amounts[1])::variant AS token1_amount_raw,
        regexp_substr(logs[0], '[ \\"](\\d+)[ ]', 1, 3, 'e', 1)::variant AS lp_token_amount_raw,
        null AS lp_token
        FROM
            actions
        WHERE
            action_name = 'FunctionCall'
            AND method_name = 'add_liquidity'
            AND receiver_id = 'v2.ref-finance.near'
            AND logs[0] IS NOT NULL
            AND receipt_succeeded = True

    UNION ALL

    -- In this section, we will examine the transactions that have the add_stable_liquidity method. The interesting thing is that in this section, the pool number is known, but the token contract cannot be found. To solve this problem, we have obtained tokens number zero and one based on the pool ID. Note that the check has been done and the contracts are working correctly based on the pool ID.
    -- A very important point to think about is the IDs 1910 and 4179. These two identifiers have 4 tokens. According to the flip side tables, only two tokens are checked and the third or fourth token is not checked.
    -- We have done the same here. We have considered only and only token number zero and one (respectively). This means that the third and fourth tokens are ignored and their data is not in the output because the desired columns for the third and fourth tokens are not considered, but in lp_token, their names are given in full and in the same correct order.

    SELECT 
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        'Deposit' as method_name,
        'Ref Finance' as platform,
        args:pool_id as pool_id,
        case pool_id
            when 1910 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' -- This pool has three tokens, ignored last one
            when 3020 then 'usn'
            when 3433 then 'usn'
            when 3514 then 'meta-pool.near'
            when 3515 then 'linear-protocol.near'
            when 3364 then '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            when 3612 then 'nearx.stader-labs.near'
            when 3688 then 'v2-nearx.stader-labs.near'
            when 3689 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            when 4179 then 'usdt.tether-token.near' -- This pool has four tokens, ignored last 2 tokens
        end as token0_contract,
        args:amounts[0]::variant as token0_amount_raw,
        case pool_id
            when 1910 then 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            when 3020 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            when 3433 then 'cusd.token.a11bd.near'
            when 3514 then 'wrap.near'
            when 3515 then 'wrap.near'
            when 3364 then '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            when 3612 then 'wrap.near'
            when 3688 then 'wrap.near'
            when 3689 then 'usdt.tether-token.near'
            when 4179 then '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
        end as token1_contract,
        args:amounts[1]::variant as token1_amount_raw,
        args:min_shares::variant as lp_token_amount_raw,
        case pool_id
            when 1910 then 'USDT.e-USDC.e-DAI' -- 3 Pairs (third one one is ignored)
            when 4179 then 'USDT-USDC-USDT.e-USDC.e' -- 4 Pairs (third and forth ones are ignored)
        end as lp_token
    FROM 
        actions
    WHERE action_name = 'FunctionCall'
    AND method_name = 'add_stable_liquidity'
    AND receiver_id = 'v2.ref-finance.near'
    AND logs[0] IS NOT NULL
    AND receipt_succeeded = True
),
--Now let's look at remove_liquidity in Ref Finance. To get withdraw in Ref Finance, we have divided it into three parts because each part needs its own special and different method to get the required information.
ref_finance_withdraw as (
    -- In the first type, there is a log in which this text is written:
    -- shares of liquidity removed: receive back
    -- If we find this text, we just need to use regular expressions to get token value and token contract.

    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        'Withdraw' as method_name,
        'Ref Finance' as platform,
        args:pool_id as pool_id,
        trim(regexp_substr(logs[0], '["]\\d+ (([^"]*))["]', 1, 1, 'e', 1)) as token0_contract,
        regexp_substr(logs[0], '["](\\d+) ([^"]*)["]', 1, 1, 'e', 1)::variant as token0_amount_raw,
        trim(regexp_substr(logs[0], '["]\\d+ (([^"]*))["]', 1, 2, 'e', 1)) as token1_contract,
        regexp_substr(logs[0], '["](\\d+)[ ]', 1, 2, 'e', 1)::variant as token1_amount_raw,
        args:shares as lp_token_amount_raw,
        null as lp_token
    FROM
            actions
    WHERE 
        action_name = 'FunctionCall'
        AND method_name = 'remove_liquidity'
        AND receiver_id = 'v2.ref-finance.near'
        AND logs[0]::string like '%shares of liquidity removed: receive back%'
        AND receipt_succeeded = True

    UNION ALL
    -- In the second type, there is also a log in which this text is written:
    -- shares to gain tokens
    -- If we find this text, it is again true that contract tokens are not specific. Since we know the order of the tokens, we have to find the values in that order. In order for the sequence to be observed correctly, it is enough to look at the order of its occurrence with the help of action_id to correctly match the value with the token contract that we know.
    -- Exactly this code:
    -- and b.method_name = 'withdraw'
    -- and split(b.action_id, '-')[1] = '1'
    -- To find the arrangement. We do the same for the second token (if any).

    SELECT
        a.block_id,
        a.block_timestamp,
        a.tx_hash,
        a.signer_id,
        'Withdraw' as method_name,
        'Ref Finance' as platform,
        a.args:pool_id as pool_id,
        CASE
            when b.args:token_id is not null then b.args:token_id
            when pool_id = 1910 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' -- This pool has three tokens, ignored last one
            when pool_id = 3020 then 'usn'
            when pool_id = 3433 then 'usn'
            when pool_id = 3514 then 'meta-pool.near'
            when pool_id = 3515 then 'linear-protocol.near'
            when pool_id = 3364 then '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            when pool_id = 3612 then 'nearx.stader-labs.near'
            when pool_id = 3688 then 'v2-nearx.stader-labs.near'
            when pool_id = 3689 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            when pool_id = 4179 then 'usdt.tether-token.near'
            end as token0_contract,
        nvl(regexp_substr(a.logs[0], '[\\[, ](\\d+)[,\\]]', 1, 1, 'e', 1), regexp_substr(a.logs[0], '[\\[ ](\\d+)[,\\]]', 1, 1, 'e', 1))::variant as token0_amount_raw,
        CASE
            when c.args:token_id is not null then c.args:token_id
            when pool_id = 1910 then 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            when pool_id = 3020 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            when pool_id = 3433 then 'cusd.token.a11bd.near'
            when pool_id = 3514 then 'wrap.near'
            when pool_id = 3515 then 'wrap.near'
            when pool_id = 3364 then '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            when pool_id = 3612 then 'wrap.near'
            when pool_id = 3688 then 'wrap.near'
            when pool_id = 3689 then 'usdt.tether-token.near'
            when pool_id = 4179 then '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
            end as token1_contract,
        nvl(regexp_substr(a.logs[0], '[\\[, ](\\d+)[,\\]]', 1, 2, 'e', 1),regexp_substr(a.logs[0], '[\\[ ](\\d+)[,\\]]', 1, 2, 'e', 1))::variant as token1_amount_raw,
        a.args:shares::variant as lp_token_amount_raw,
        case pool_id
            when 1910 then 'USDT.e-USDC.e-DAI'
            when 4179 then 'USDT-USDC-USDT.e-USDC.e'
        end as lp_token
    FROM actions a
    LEFT JOIN actions b
        ON a.tx_hash = b.tx_hash AND b.method_name = 'withdraw' AND split(b.action_id, '-')[1] = '1'
    LEFT JOIN actions c
        ON a.args:min_amounts[1] is not null AND a.tx_hash = c.tx_hash AND c.method_name = 'withdraw' AND split(c.action_id, '-')[1] = '2'
    WHERE 
        a.receiver_id = 'v2.ref-finance.near'
        AND a.method_name = 'remove_liquidity'
        AND a.logs[0] like '% remove % shares to gain tokens %'
        AND a.receipt_succeeded = True

    
    
    
    UNION ALL

    -- In the third and last type, there is also a log in which this text is written:
    -- removed % shares by given tokens
    -- If we find this text, it is again true that contract tokens may not be specific. If they were specified, we will use them, but if the same pools that we have already checked as hardcoded, we must specify the tokens in our own order.
    -- In these transactions, we can find the transferred value in the function call table, whose method is all ft_transfer.
    -- It is important to mention that transactions with address aab2644330798a6b066cd6495156e81e00aecc3967104e61f67533f66eccf9b2 should not be included in this list. Because these transactions do not have the main call method remove_liquidity
    SELECT
        a.block_id,
        a.block_timestamp,
        a.tx_hash,
        a.signer_id,
        'Withdraw' as method_name,
        'Ref Finance' as platform,
        a.args:pool_id as pool_id,
        b.receiver_id as token0_contract,
        nvl(c.args:amount, a.args:amounts[0])::variant as token0_amount_raw,
        c.receiver_id as token1_contract,
        nvl(c.args:amount, a.args:amounts[1])::variant as token1_amount_raw,
        regexp_substr(a.logs[0], 'removed (\\d+) shares by given tokens', 1, 1, 'e', 1)::variant as lp_token_amount_raw,
        CASE pool_id
            when 1910 then 'USDT.e-USDC.e-DAI'
            when 4179 then 'USDT-USDC-USDT.e-USDC.e'
        END as lp_token
    FROM actions a
    LEFT JOIN actions b
        ON a.tx_hash = b.tx_hash 
    AND 
        CASE a.args:pool_id
            WHEN 1910 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN 3020 THEN 'usn'
            WHEN 3433 THEN 'usn'
            WHEN 3514 THEN 'meta-pool.near'
            WHEN 3515 THEN 'linear-protocol.near'
            WHEN 3364 THEN '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            WHEN 3612 THEN 'nearx.stader-labs.near'
            WHEN 3688 THEN 'v2-nearx.stader-labs.near'
            WHEN 3689 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN 4179 THEN 'usdt.tether-token.near'
        END = a.receiver_id and b.method_name = 'ft_transfer'
    LEFT JOIN actions c
    ON a.tx_hash = c.tx_hash 
    AND 
        CASE a.args:pool_id
            when 1910 then 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            when 3020 then 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            when 3433 then 'cusd.token.a11bd.near'
            when 3514 then 'wrap.near'
            when 3515 then 'wrap.near'
            when 3364 then '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            when 3612 then 'wrap.near'
            when 3688 then 'wrap.near'
            when 3689 then 'usdt.tether-token.near'
            when 4179 then '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
        end = c.receiver_id and c.method_name = 'ft_transfer'
    AND c.receiver_id != b.receiver_id
    WHERE 
        a.receiver_id = 'v2.ref-finance.near'
        AND a.method_name = 'remove_liquidity_by_tokens'
        AND a.logs[0] like '%removed % shares by given tokens%'
        AND a.signer_id != 'aab2644330798a6b066cd6495156e81e00aecc3967104e61f67533f66eccf9b2'
        AND a.receipt_succeeded = True
),

-- In Ref Finance, there is also a separate method called batch_update_liquidity. According to the updated amount, we can distinguish whether it is a deposit or withdrawal. If the sum of deposits is more than withdrawals, then there is a deposit and vice versa.
-- Now how to calculate? There is a log in these transactions with which we can see all the updates and find out what changes have been made. For example, if liquidity addition has happened, there will be paid_token_x in the log arrays, but if it is the opposite, there will be refund_token_x. x is token_0 and y is token_1. With the help of lp_token_amount_raw, we can get the total amount of changes.
-- Finally, it is enough to get the total changes and see what the transaction type is and how much the final changes are.

ref_finance_update as (
    select
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        iff(lp_token_amount_raw > 0, 'Deposit', 'Withdraw') as method_name,
        'Ref Finance' as platform,
        pool_id,
        token0_contract,
        abs(token0_amount_raw)::variant as token0_amount_raw,
        token1_contract,
        abs(token1_amount_raw)::variant as token1_amount_raw,
        abs(lp_token_amount_raw)::variant as lp_token_amount_raw,
        null as lp_token
    FROM (
        SELECT 
            block_id,
            tx_hash,
            block_timestamp,
            try_parse_json(replace(LOGS[0], 'EVENT_JSON:')):data[0] as data,
            data:owner_id as signer_id,
            split(data:pool_id, '|')[2] as pool_id,
            split(data:pool_id, '|')[0] as token0_contract,
            abs(nvl(data:paid_token_x, -1 * data:refund_token_x))::variant as token0_amount_raw,
            split(data:pool_id, '|')[1] as token1_contract,
            abs(nvl(data:paid_token_y, -1 * data:refund_token_y))::variant as token1_amount_raw,
            nvl(data:added_amount, -1 * data:removed_amount) as lp_token_amount_raw,
            iff(lp_token_amount_raw > 0, 'Deposit', 'Withdraw') as method_name,
            'Ref Finance' as platform,
        FROM actions 
        WHERE method_name = 'batch_update_liquidity' 
        AND  receiver_id = 'dclv2.ref-labs.near'
        AND logs[0] is not null
        AND data:claim_fee_token_x > 0
        AND data:claim_fee_token_y > 0
    )
),

uinon_all as (
    select * from ref_finance_deposit

    union all

    select * from ref_finance_withdraw

    union all

    select * from ref_finance_update

    -- union all

    -- select * from veax_deposit

    -- union all

    -- select * from veax_withdraw

    -- union all

    -- select * from jumbo_exchange
),

--We have reached the final stage of the work. Here we calculate the USD value of the tokens and also the adjusted value of the tokens.
final as (
    select
    block_id,
    block_timestamp,
    tx_hash,
    signer_id,
    method_name,
    a.symbol as token0,
    token0_amount_raw::float as token0_amount_unadj,
    token0_amount_unadj / pow(10, a.decimals) as token0_amount,
    token0_amount * c.usd_price as token0_amount_usd,
    b.symbol as token1,
    token1_amount_raw::float as token1_amount_unadj,
    token1_amount_unadj / pow(10, b.decimals) as token1_amount,
    token1_amount * d.usd_price as token1_amount_usd,
    nvl(lp_token, token0 || '-' || token1) as lp_token,
    lp_token_amount_raw::float as lp_token_amount_unadj,
    platform,
    nvl(pool_id, lp_token) as pool_id
    from uinon_all
    left join near.core.dim_ft_contract_metadata a on a.contract_address = token0_contract
    left join near.core.dim_ft_contract_metadata b on b.contract_address = token1_contract
    left join pricet c on c.date = block_timestamp::date and token0_contract = c.token_contract
    left join pricet d on d.date = block_timestamp::date and token1_contract = d.token_contract
)

select * from final
order by block_timestamp 

