{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH pricet AS (

    SELECT
        TIMESTAMP :: DATE AS date_pr,
        token_contract,
        AVG(price_usd) AS usd_price
    FROM
        near.price.fact_prices
    GROUP BY
        1,
        2
),
actions as (
    select
        *
    FROM 
        near.core.fact_actions_events_function_call
),
receipts as (
    select
        *
    FROM 
        near.core.fact_receipts
),
--Note that some pools, their information is not in the transaction. Therefore, we have to hard code the pools in the code. Therefore, the reason why we have added this section is because of the pool not being clear in the transaction information. We have obtained this information in this way:
veax_pools AS (
    SELECT
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' AS token0,
        '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' AS token1,
        3658233 AS pool_id
    UNION ALL
    SELECT
        'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near',
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
        1995
    UNION ALL
    SELECT
        'usdt.tether-token.near',
        'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near',
        5248
    UNION ALL
    SELECT
        'usdt.tether-token.near',
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
        15998
    UNION ALL
    SELECT
        'wrap.near',
        'linear-protocol.near',
        4948
    UNION ALL
    SELECT
        'aurora',
        'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near',
        15894
    UNION ALL
    SELECT
        'meta-token.near',
        'wrap.near',
        4511531
    UNION ALL
    SELECT
        'token.sweat',
        'wrap.near',
        5144
    UNION ALL
    SELECT
        'aurora',
        'usdt.tether-token.near',
        15893
    UNION ALL
    SELECT
        '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
        'usdt.tether-token.near',
        15844
    UNION ALL
    SELECT
        'wrap.near',
        'usdt.tether-token.near',
        15763
    UNION ALL
    SELECT
        'aurora',
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
        15945
    UNION ALL
    SELECT
        'token.stlb.near',
        'wrap.near',
        5123
    UNION ALL
    SELECT
        'aurora',
        'wrap.near',
        5212
    UNION ALL
    SELECT
        '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
        'wrap.near',
        5179
    UNION ALL
    SELECT
        'wrap.near',
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
        3802
    UNION ALL
    SELECT
        'aurora',
        '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
        5174
    UNION ALL
    SELECT
        'meta-pool.near',
        'wrap.near',
        4941
    UNION ALL
    SELECT
        'token.paras.near',
        'wrap.near',
        254242
    UNION ALL
    SELECT
        'f5cfbc74057c610c8ef151a439252680ac68c6dc.factory.bridge.near',
        'wrap.near',
        254931
    UNION ALL
    SELECT
        'aaaaaa20d9e0e2461697782ef11675f668207961.factory.bridge.near',
        'wrap.near',
        5146
    UNION ALL
    SELECT
        '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
        'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near',
        15846
    UNION ALL
    SELECT
        '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near',
        'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near',
        15845
    UNION ALL
    SELECT
        'wrap.near',
        'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near',
        5318
),
ref_finance_deposit AS (
    -- First we will review Ref Finance. In the transactions of this marketplace, there is a method called add_liquidity. First, we calculate this part and form the deposit table. Note that part of the data is in the fact_receipts table and part of the data is in the same fact_actions_events_function_call table. So, we will make a relationship between these two tables and through logs and with the help of regular expressions, we will get the first and second token along with their value. A properly written rule separates the different sections.
    -- It is also important to note that in all sections, we have tried to count successful transactions and therefore unsuccessful transactions will not have a place in the results.
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        receipt_object_id,
        'Deposit' AS method_name,
        'Ref Finance' AS platform,
        args :pool_id AS pool_id,
        TRIM(
            REGEXP_SUBSTR(
                logs [0],
                '["]\\d+([^"]*)',
                1,
                1,
                'e',
                1
            )
        ) AS token0_contract,
        NVL(
            REGEXP_SUBSTR(
                logs [0],
                '[ \\"](\\d+)[ ]',
                1,
                1,
                'e',
                1
            ),
            args :amounts [0]
        ) :: variant AS token0_amount_raw,
        TRIM(
            REGEXP_SUBSTR(
                logs [0],
                '["]\\d+([^"]*)',
                1,
                2,
                'e',
                1
            )
        ) AS token1_contract,
        NVL(
            REGEXP_SUBSTR(
                logs [0],
                '[ \\"](\\d+)[ ]',
                1,
                2,
                'e',
                1
            ),
            args :amounts [1]
        ) :: variant AS token1_amount_raw,
        REGEXP_SUBSTR(
            logs [0],
            '[ \\"](\\d+)[ ]',
            1,
            3,
            'e',
            1
        ) :: variant AS lp_token_amount_raw,
        NULL AS lp_token
    FROM
        actions A
        JOIN receipts b USING (tx_hash)
    WHERE
        action_name = 'FunctionCall'
        AND method_name = 'add_liquidity'
        AND A.receiver_id = 'v2.ref-finance.near'
        AND A.receiver_id = b.receiver_id
        AND logs [0] IS NOT NULL
        AND status_value :: STRING LIKE '%Success%'
    UNION ALL
        -- In this section, we will examine the transactions that have the add_stable_liquidity method. The interesting thing is that in this section, the pool number is known, but the token contract cannot be found. To solve this problem, we have obtained tokens number zero and one based on the pool ID. Note that the check has been done and the contracts are working correctly based on the pool ID.
        -- A very important point to think about is the IDs 1910 and 4179. These two identifiers have 4 tokens. According to the flip side tables, only two tokens are checked and the third or fourth token is not checked.
        -- We have done the same here. We have considered only and only token number zero and one (respectively). This means that the third and fourth tokens are ignored and their data is not in the output because the desired columns for the third and fourth tokens are not considered, but in lp_token, their names are given in full and in the same correct order.
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        receipt_object_id,
        'Deposit' AS method_name,
        'Ref Finance' AS platform,
        args :pool_id AS pool_id,
        CASE
            pool_id
            WHEN 1910 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' -- This pool has three tokens, ignored last one
            WHEN 3020 THEN 'usn'
            WHEN 3433 THEN 'usn'
            WHEN 3514 THEN 'meta-pool.near'
            WHEN 3515 THEN 'linear-protocol.near'
            WHEN 3364 THEN '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            WHEN 3612 THEN 'nearx.stader-labs.near'
            WHEN 3688 THEN 'v2-nearx.stader-labs.near'
            WHEN 3689 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN 4179 THEN 'usdt.tether-token.near' -- This pool has four tokens, ignored last 2 tokens
        END AS token0_contract,
        args :amounts [0] :: variant AS token0_amount_raw,
        CASE
            pool_id
            WHEN 1910 THEN 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            WHEN 3020 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN 3433 THEN 'cusd.token.a11bd.near'
            WHEN 3514 THEN 'wrap.near'
            WHEN 3515 THEN 'wrap.near'
            WHEN 3364 THEN '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            WHEN 3612 THEN 'wrap.near'
            WHEN 3688 THEN 'wrap.near'
            WHEN 3689 THEN 'usdt.tether-token.near'
            WHEN 4179 THEN '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
        END AS token1_contract,
        args :amounts [1] :: variant AS token1_amount_raw,
        args :min_shares :: variant AS lp_token_amount_raw,
        CASE
            pool_id
            WHEN 1910 THEN 'USDT.e-USDC.e-DAI' -- 3 Pairs (third one one is ignored)
            WHEN 4179 THEN 'USDT-USDC-USDT.e-USDC.e' -- 4 Pairs (third and forth ones are ignored)
        END AS lp_token
    FROM
        actions A
        JOIN receipts b USING (tx_hash)
    WHERE
        action_name = 'FunctionCall'
        AND method_name = 'add_stable_liquidity'
        AND A.receiver_id = 'v2.ref-finance.near'
        AND A.receiver_id = b.receiver_id
        AND logs [0] IS NOT NULL
        AND status_value :: STRING LIKE '%Success%'
),
--Now let's look at remove_liquidity in Ref Finance. To get withdraw in Ref Finance, we have divided it into three parts because each part needs its own special and different method to get the required information.
ref_finance_withdraw AS (
    -- In the first type, there is a log in which this text is written:
    -- shares of liquidity removed: receive back
    -- If we find this text, we just need to use regular expressions to get token value and token contract.
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        receipt_object_id,
        'Withdraw' AS method_name,
        'Ref Finance' AS platform,
        args :pool_id AS pool_id,
        TRIM(
            REGEXP_SUBSTR(
                logs [0],
                '["]\\d+ (([^"]*))["]',
                1,
                1,
                'e',
                1
            )
        ) AS token0_contract,
        REGEXP_SUBSTR(
            logs [0],
            '["](\\d+) ([^"]*)["]',
            1,
            1,
            'e',
            1
        ) :: variant AS token0_amount_raw,
        TRIM(
            REGEXP_SUBSTR(
                logs [0],
                '["]\\d+ (([^"]*))["]',
                1,
                2,
                'e',
                1
            )
        ) AS token1_contract,
        REGEXP_SUBSTR(
            logs [0],
            '["](\\d+)[ ]',
            1,
            2,
            'e',
            1
        ) :: variant AS token1_amount_raw,
        args :shares AS lp_token_amount_raw,
        NULL AS lp_token
    FROM
        receipts e
        JOIN actions A USING (
            tx_hash,
            receiver_id
        )
    WHERE
        receiver_id = 'v2.ref-finance.near'
        AND method_name = 'remove_liquidity'
        AND action_name = 'FunctionCall'
        AND logs [0] :: STRING LIKE '%shares of liquidity removed: receive back%'
        AND status_value :: STRING ILIKE '%Success%'
    UNION ALL
        -- In the second type, there is also a log in which this text is written:
        -- shares to gain tokens
        -- If we find this text, it is again true that contract tokens are not specific. Since we know the order of the tokens, we have to find the values in that order. In order for the sequence to be observed correctly, it is enough to look at the order of its occurrence with the help of action_id to correctly match the value with the token contract that we know.
        -- Exactly this code:
        -- and b.method_name = 'withdraw'
        -- and split(b.action_id, '-')[1] = '1'
        -- To find the arrangement. We do the same for the second token (if any).
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_hash,
        A.signer_id,
        receipt_object_id,
        'Withdraw' AS method_name,
        'Ref Finance' AS platform,
        A.args :pool_id AS pool_id,
        CASE
            WHEN b.args :token_id IS NOT NULL THEN b.args :token_id
            WHEN pool_id = 1910 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near' -- This pool has three tokens, ignored last one
            WHEN pool_id = 3020 THEN 'usn'
            WHEN pool_id = 3433 THEN 'usn'
            WHEN pool_id = 3514 THEN 'meta-pool.near'
            WHEN pool_id = 3515 THEN 'linear-protocol.near'
            WHEN pool_id = 3364 THEN '2260fac5e5542a773aa44fbcfedf7c193bc2c599.factory.bridge.near'
            WHEN pool_id = 3612 THEN 'nearx.stader-labs.near'
            WHEN pool_id = 3688 THEN 'v2-nearx.stader-labs.near'
            WHEN pool_id = 3689 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN pool_id = 4179 THEN 'usdt.tether-token.near'
        END AS token0_contract,
        NVL(
            REGEXP_SUBSTR(
                logs [0],
                '[\\[, ](\\d+)[,\\]]',
                1,
                1,
                'e',
                1
            ),
            REGEXP_SUBSTR(
                logs [0],
                '[\\[ ](\\d+)[,\\]]',
                1,
                1,
                'e',
                1
            )
        ) :: variant AS token0_amount_raw,
        CASE
            WHEN C.args :token_id IS NOT NULL THEN C.args :token_id
            WHEN pool_id = 1910 THEN 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            WHEN pool_id = 3020 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN pool_id = 3433 THEN 'cusd.token.a11bd.near'
            WHEN pool_id = 3514 THEN 'wrap.near'
            WHEN pool_id = 3515 THEN 'wrap.near'
            WHEN pool_id = 3364 THEN '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            WHEN pool_id = 3612 THEN 'wrap.near'
            WHEN pool_id = 3688 THEN 'wrap.near'
            WHEN pool_id = 3689 THEN 'usdt.tether-token.near'
            WHEN pool_id = 4179 THEN '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
        END AS token1_contract,
        NVL(
            REGEXP_SUBSTR(
                logs [0],
                '[\\[, ](\\d+)[,\\]]',
                1,
                2,
                'e',
                1
            ),
            REGEXP_SUBSTR(
                logs [0],
                '[\\[ ](\\d+)[,\\]]',
                1,
                2,
                'e',
                1
            )
        ) :: variant AS token1_amount_raw,
        A.args :shares :: variant AS lp_token_amount_raw,
        CASE
            pool_id
            WHEN 1910 THEN 'USDT.e-USDC.e-DAI'
            WHEN 4179 THEN 'USDT-USDC-USDT.e-USDC.e'
        END AS lp_token
    FROM
        actions A
        LEFT JOIN actions b
        ON A.tx_hash = b.tx_hash
        AND b.method_name = 'withdraw'
        AND SPLIT(
            b.action_id,
            '-'
        ) [1] = '1'
        LEFT JOIN actions C
        ON A.args :min_amounts [1] IS NOT NULL
        AND A.tx_hash = C.tx_hash
        AND C.method_name = 'withdraw'
        AND SPLIT(
            C.action_id,
            '-'
        ) [1] = '2'
        JOIN receipts e
        ON A.tx_hash = e.tx_hash
    WHERE
        A.method_name = 'remove_liquidity'
        AND A.receiver_id = 'v2.ref-finance.near'
        AND A.receiver_id = e.receiver_id
        AND status_value :: STRING LIKE '%Success%'
        AND logs [0] LIKE '% remove % shares to gain tokens %'
    UNION ALL
        -- In the third and last type, there is also a log in which this text is written:
        -- removed % shares by given tokens
        -- If we find this text, it is again true that contract tokens may not be specific. If they were specified, we will use them, but if the same pools that we have already checked as hardcoded, we must specify the tokens in our own order.
        -- In these transactions, we can find the transferred value in the function call table, whose method is all ft_transfer.
        -- It is important to mention that transactions with address aab2644330798a6b066cd6495156e81e00aecc3967104e61f67533f66eccf9b2 should not be included in this list. Because these transactions do not have the main call method remove_liquidity
    SELECT
        A.block_id,
        A.block_timestamp,
        A.tx_hash,
        A.signer_id,
        receipt_object_id,
        'Withdraw' AS method_name,
        'Ref Finance' AS platform,
        A.args :pool_id AS pool_id,
        b.receiver_id AS token0_contract,
        NVL(
            C.args :amount,
            A.args :amounts [0]
        ) :: variant AS token0_amount_raw,
        C.receiver_id AS token1_contract,
        NVL(
            C.args :amount,
            A.args :amounts [1]
        ) :: variant AS token1_amount_raw,
        REGEXP_SUBSTR(
            logs [0],
            'removed (\\d+) shares by given tokens',
            1,
            1,
            'e',
            1
        ) :: variant AS lp_token_amount_raw,
        CASE
            pool_id
            WHEN 1910 THEN 'USDT.e-USDC.e-DAI'
            WHEN 4179 THEN 'USDT-USDC-USDT.e-USDC.e'
        END AS lp_token
    FROM
        actions A
        LEFT JOIN actions b
        ON A.tx_hash = b.tx_hash
        AND CASE
            A.args :pool_id
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
        END = b.receiver_id
        AND b.method_name = 'ft_transfer'
        LEFT JOIN actions C
        ON A.tx_hash = C.tx_hash
        AND CASE
            A.args :pool_id
            WHEN 1910 THEN 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near' -- This pool has three tokens, ignored last one
            WHEN 3020 THEN 'dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near'
            WHEN 3433 THEN 'cusd.token.a11bd.near'
            WHEN 3514 THEN 'wrap.near'
            WHEN 3515 THEN 'wrap.near'
            WHEN 3364 THEN '0316eb71485b0ab14103307bf65a021042c6d380.factory.bridge.near'
            WHEN 3612 THEN 'wrap.near'
            WHEN 3688 THEN 'wrap.near'
            WHEN 3689 THEN 'usdt.tether-token.near'
            WHEN 4179 THEN '17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1' -- This pool has four tokens, ignored last 2 tokens
        END = C.receiver_id
        AND C.method_name = 'ft_transfer'
        AND C.receiver_id != b.receiver_id
        JOIN receipts e
        ON A.tx_hash = e.tx_hash
    WHERE
        A.method_name = 'remove_liquidity_by_tokens'
        AND A.receiver_id = 'v2.ref-finance.near'
        AND e.receiver_id = A.receiver_id
        AND logs [0] LIKE '%removed % shares by given tokens%'
        AND status_value :: STRING LIKE '%Success%'
        AND A.signer_id != 'aab2644330798a6b066cd6495156e81e00aecc3967104e61f67533f66eccf9b2'
),
-- In Ref Finance, there is also a separate method called batch_update_liquidity. According to the updated amount, we can distinguish whether it is a deposit or withdrawal. If the sum of deposits is more than withdrawals, then there is a deposit and vice versa.
-- Now how to calculate? There is a log in these transactions with which we can see all the updates and find out what changes have been made. For example, if liquidity addition has happened, there will be paid_token_x in the log arrays, but if it is the opposite, there will be refund_token_x. x is token_0 and y is token_1. With the help of lp_token_amount_raw, we can get the total amount of changes.
-- Finally, it is enough to get the total changes and see what the transaction type is and how much the final changes are.
ref_finance_update AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        receipt_object_id,
        IFF(
            lp_token_amount_raw > 0,
            'Deposit',
            'Withdraw'
        ) AS method_name,
        'Ref Finance' AS platform,
        pool_id,
        token0_contract,
        ABS(token0_amount_raw) :: variant AS token0_amount_raw,
        token1_contract,
        ABS(token1_amount_raw) :: variant AS token1_amount_raw,
        ABS(lp_token_amount_raw) :: variant AS lp_token_amount_raw,
        NULL AS lp_token
    FROM
        (
            SELECT
                block_id,
                tx_hash,
                block_timestamp,
                signer_id,
                receipt_object_id,
                pool_id,
                token0_contract,
                SUM(token0_amount_raw) AS token0_amount_raw,
                token1_contract,
                SUM(token1_amount_raw) AS token1_amount_raw,
                SUM(lp_token_amount_raw) AS lp_token_amount_raw
            FROM
                (
                    SELECT
                        block_id,
                        tx_hash,
                        block_timestamp,
                        TRY_PARSE_JSON(REPLACE(f.value, 'EVENT_JSON:')) :data [0] AS DATA,
                        DATA :owner_id AS signer_id,
                        receipt_object_id,
                        SPLIT(
                            DATA :pool_id,
                            '|'
                        ) [2] AS pool_id,
                        SPLIT(
                            DATA :pool_id,
                            '|'
                        ) [0] AS token0_contract,
                        NVL(
                            DATA :paid_token_x,
                            -1 * DATA :refund_token_x
                        ) AS token0_amount_raw,
                        SPLIT(
                            DATA :pool_id,
                            '|'
                        ) [1] AS token1_contract,
                        NVL(
                            DATA :paid_token_y,
                            -1 * DATA :refund_token_y
                        ) AS token1_amount_raw,
                        NVL(
                            DATA :added_amount,
                            -1 * DATA :removed_amount
                        ) AS lp_token_amount_raw
                    FROM
                        receipts A
                        JOIN LATERAL FLATTEN (
                            input => A.logs
                        ) f
                    WHERE
                        receiver_id = 'dclv2.ref-labs.near'
                        AND status_value LIKE '%Success%'
                        AND logs [0] IS NOT NULL
                        AND DATA :claim_fee_token_x > 0
                        AND DATA :claim_fee_token_y > 0
                        AND tx_hash IN (
                            SELECT
                                DISTINCT tx_hash
                            FROM
                                actions b
                            WHERE
                                method_name = 'batch_update_liquidity'
                                AND A.receiver_id = b.receiver_id
                        )
                )
            GROUP BY
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                9
        )
),
-- Transactions with condition receiver_id = 'veax.near' have three methods open_position, ft_transfer_call and ft_on_transfer, which contain information about transfers. By checking them, we can access the deposit transactions. In the arguments of the records of these transactions, the OpenPosition object may be found, or the token values may be in the form of token_a and token_b. In any case, we check two possibilities and in each of which we see data, we extract tokens. Note that the maximum amount is actually the amount paid. Importantly, we checked the transactions but found no trace of the lp_token_amount_raw value in the transaction information. Therefore, we have set this value as a null exception. Another point is that if there are duplicate records for a transaction, we will help them
-- qualify row_number() over (partition by tx_hash order by block_timestamp) = 1
-- We show only one of them. To identify the pools as well, we get the data from the veax_pools table that I extracted myself. Please note that whenever we have hard-coded the data of the pools, there is no data in the transaction itself and we have obtained this information through the desired platform.
--Finally, we collect all the collections in the form of one piece.
--Note that the query may not be executed because it is heavy, so to test it, you can comment a part and execute a part and see the result.
uinon_all AS (
    SELECT
        *
    FROM
        ref_finance_deposit
    UNION ALL
    SELECT
        *
    FROM
        ref_finance_withdraw
    UNION ALL
    SELECT
        *
    FROM
        ref_finance_update
),
--We have reached the final stage of the work. Here we calculate the USD value of the tokens and also the adjusted value of the tokens.
FINAL AS (
    SELECT
        block_id,
        block_timestamp,
        tx_hash,
        signer_id,
        receipt_object_id,
        method_name,
        A.symbol AS token0,
        token0_amount_raw :: FLOAT AS token0_amount_unadj,
        token0_amount_unadj / pow(
            10,
            A.decimals
        ) AS token0_amount,
        token0_amount * C.usd_price AS token0_amount_usd,
        b.symbol AS token1,
        token1_amount_raw :: FLOAT AS token1_amount_unadj,
        token1_amount_unadj / pow(
            10,
            b.decimals
        ) AS token1_amount,
        token1_amount * d.usd_price AS token1_amount_usd,
        NVL(
            lp_token,
            token0 || '-' || token1
        ) AS lp_token,
        lp_token_amount_raw :: FLOAT AS lp_token_amount_unadj,
        platform,
        NVL(
            pool_id,
            lp_token
        ) AS pool_id
    FROM
        uinon_all
        LEFT JOIN near.core.dim_ft_contract_metadata A
        ON A.contract_address = token0_contract
        LEFT JOIN near.core.dim_ft_contract_metadata b
        ON b.contract_address = token1_contract
        LEFT JOIN pricet C
        ON C.date_pr = block_timestamp :: DATE
        AND token0_contract = C.token_contract
        LEFT JOIN pricet d
        ON d.date_pr = block_timestamp :: DATE
        AND token1_contract = d.token_contract
)
SELECT
    *
FROM
    FINAL
ORDER BY
    block_timestamp
