# NEAR Daily Supply
The set of models in this folder calculate the total supply of NEAR, derived from a model originally built by the Foundation. The composite parts of the below query (locked staked, locked unstaked, staked) have been segmented into separate sql models and are combined in `silver__atlas_supply.sql`. The model `silver__atlas_supply_daily_staked_supply.sql` has been replaced with an existing model in the Flipside project `silver__pool_balance_daily`, which follows a similar mechanic of extracting staked balance per pool, per day, from the logs emitted by staking actions on-chain.  


### Original SQL
[Link to Data Studio Query by dweinstein13](https://flipsidecrypto.xyz/dweinstein33/q/u29te7T8c36t/ad---total-supply-atlas)  

```sql
 WITH

lockup_receipts AS (
    SELECT fr.tx_hash
         , fr.block_timestamp
         , fr.actions:predecessor_id::string AS predecessor_id
         , fr.receiver_id
         , fr.actions
         , OBJECT_KEYS(fr.status_value)[0]::string AS status
         , fr.logs
    
    FROM near.core.fact_receipts AS fr
    WHERE fr.receiver_id LIKE '%.lockup.near'
      AND status != 'Failure'
),


daily_lockup_locked_balances AS (
    WITH

    -- new lockup contract created
    new_lockup_txs AS (
        SELECT lr.tx_hash
             , lr.block_timestamp
             , lr.receiver_id AS lockup_account_id
        FROM lockup_receipts AS lr,
            LATERAL FLATTEN( input => lr.actions:receipt:Action:actions )
        WHERE value:FunctionCall:method_name::string = 'new'
    ),
    
    
    -- vesting is stopped by the Foundation
    terminate_vesting_txs AS (
        SELECT lr.tx_hash
             , lr.block_timestamp
             , lr.receiver_id AS lockup_account_id
             , split(lr.logs[0], 'unvested balance is ')[1]::bigint / 1e24 AS unvested_balance
        FROM lockup_receipts AS lr,
            LATERAL FLATTEN( input => lr.actions:receipt:Action:actions )
        WHERE value:FunctionCall:method_name::string = 'terminate_vesting'
    ),
    
    
    terminate_vesting_txs_with_vesting_schedule AS (
        SELECT tv.*
             , fc.args:vesting_schedule_with_salt:vesting_schedule AS vesting_schedule
    
        FROM terminate_vesting_txs AS tv
            LEFT JOIN near.core.fact_actions_events_function_call AS fc
                ON fc.tx_hash = tv.tx_hash
                AND fc.method_name = 'terminate_vesting'
    
        QUALIFY row_number() OVER (partition by tv.tx_hash order by tv.block_timestamp) = 1  -- dedupe
    ),
    
    
    -- unvested tokens are withdrawn (effectively unlocked into circulating supply)
    termination_withdraw_txs AS (
        SELECT lr.tx_hash
             , lr.block_timestamp
             , lr.receiver_id AS lockup_account_id
             , split(split(lr.logs[0], ' of terminated unvested balance')[0], 'Withdrawing ')[1]::bigint / 1e24 AS withdrawn_amount
        FROM lockup_receipts AS lr,
            LATERAL FLATTEN( input => lr.actions:receipt:Action:actions )
        WHERE value:FunctionCall:method_name::string = 'termination_withdraw'
     -- Simplify logic -> get only first termination withdrawal
        -- QUALIFY row_number() OVER (partition by lockup_account_id order by block_timestamp) = 1
    ),
    
    
    daily_termination_withdrawn_amount AS (
        SELECT lockup_account_id
             , block_timestamp::date AS utc_date
             , sum(withdrawn_amount) AS withdrawn_amount
        FROM termination_withdraw_txs
        GROUP BY 1,2
    ),
    
    
    -- lockup amounts
    deposits AS (
        SELECT lr.tx_hash
             , lr.block_timestamp
             , value:Transfer:deposit::bigint / 1e24 AS deposit_amount
        FROM lockup_receipts AS lr,
            LATERAL FLATTEN( input => lr.actions:receipt:Action:actions )
        WHERE OBJECT_KEYS(value:Transfer)[0]::string = 'deposit'
    ),
    
    
    lockup_contracts AS (
        SELECT lcr.tx_hash
             , lcr.block_timestamp
             , lcr.lockup_account_id
             , fc1.args:lockup_duration::bigint AS input_lockup_duration_ns
             , fc1.args:lockup_timestamp::bigint AS input_lockup_timestamp_epoch
             , fc1.args:owner_account_id::string AS owner_account_id
             , fc1.args:release_duration::bigint AS input_release_duration_ns
             , coalesce(tv.vesting_schedule, fc1.args:vesting_schedule:VestingSchedule) AS vesting_schedule_
             , vesting_schedule_:cliff_timestamp::bigint AS vesting_cliff_timestamp_epoch
             , vesting_schedule_:start_timestamp::bigint AS vesting_start_timestamp_epoch
             , vesting_schedule_:end_timestamp::bigint AS vesting_end_timestamp_epoch
             , coalesce( fc1.args:transfers_information:TransfersEnabled:transfers_timestamp::bigint, 
                         1602614338293769340 ) AS transfers_enabled_timestamp_epoch
             , d.deposit_amount
    
             , tv.block_timestamp AS terminate_vesting_timestamp
             , tv.unvested_balance AS termination_unvested_amount
    
             , tw.block_timestamp AS termination_withdraw_timestamp
             , tw.withdrawn_amount AS termination_withdrawn_amount
    
             , (CASE WHEN OBJECT_KEYS(fc1.args:vesting_schedule)[0]::string = 'VestingHash'
                     THEN True ELSE False END) AS is_private_vesting
    
        FROM new_lockup_txs AS lcr
            LEFT JOIN near.core.fact_actions_events_function_call AS fc1
                ON fc1.tx_hash = lcr.tx_hash
                AND fc1.method_name = 'new'
    
            LEFT JOIN deposits AS d
                ON d.tx_hash = lcr.tx_hash
    
            LEFT JOIN terminate_vesting_txs_with_vesting_schedule AS tv
                ON tv.lockup_account_id = lcr.lockup_account_id
    
            LEFT JOIN termination_withdraw_txs AS tw
                ON tw.lockup_account_id = lcr.lockup_account_id
    
        WHERE lcr.tx_hash IN (SELECT tx_hash FROM new_lockup_txs)
          AND d.deposit_amount > 0
    ),
    
    
    lockup_contracts__parsed AS (
        SELECT lockup_account_id
            -- the number of times the same lockup account ID has been used (used as part of lockup unique identifier)
             , row_number() OVER (partition by lockup_account_id order by block_timestamp) AS lockup_index
             , owner_account_id
             , deposit_amount AS lockup_amount
    
            -- timestamp when tokens were locked (lock start)
             , block_timestamp AS deposit_timestamp
    
            -- timestamp when transfers were enabled in the blockchain (default reference when lockup_timestamp is null)
             , to_timestamp(transfers_enabled_timestamp_epoch, 9) AS transfers_enabled_timestamp
    
            -- timestamp when tokens start unlocking (explicit parameter)
             , to_timestamp(input_lockup_timestamp_epoch, 9) AS input_lockup_timestamp
    
            -- if lockup_timestamp is null, calculate unlock start from lockup duration
             , timestampadd(nanoseconds, input_lockup_duration_ns, transfers_enabled_timestamp) AS calculated_lockup_timestamp
    
            -- lockup mechanism
             , input_lockup_duration_ns
             , input_release_duration_ns
    
            -- Max between input and calculated lockup timestamp
             , (CASE WHEN input_lockup_timestamp IS NOT NULL
                     THEN greatest(input_lockup_timestamp, calculated_lockup_timestamp)
                     ELSE calculated_lockup_timestamp END) AS lockup_timestamp
    
            -- If release_duration is not provided, tokens are immediately unlocked
             , (CASE WHEN input_release_duration_ns IS NOT NULL
                     THEN timestampadd(nanosecond, input_release_duration_ns, lockup_timestamp)  -- linear release if release_duration is provided, else full unlock
                     ELSE lockup_timestamp END) AS lockup_end_timestamp
    
            -- vesting mechanism
             , is_private_vesting
             , to_timestamp(vesting_start_timestamp_epoch, 9) AS vesting_start_timestamp
             , to_timestamp(vesting_end_timestamp_epoch, 9) AS vesting_end_timestamp
             , to_timestamp(vesting_cliff_timestamp_epoch, 9) AS vesting_cliff_timestamp
    
            -- vesting termination
             , terminate_vesting_timestamp
             , termination_unvested_amount
             , termination_withdraw_timestamp
             , termination_withdrawn_amount
    
             , tx_hash AS _tx_hash
    
             , (CASE WHEN lockup_timestamp IS NOT NULL AND vesting_start_timestamp IS NULL
                     THEN least(deposit_timestamp, lockup_timestamp)
                     WHEN lockup_timestamp IS NULL AND vesting_start_timestamp IS NOT NULL
                     THEN least(deposit_timestamp, vesting_start_timestamp)
                     ELSE least(deposit_timestamp, lockup_timestamp, vesting_start_timestamp) END)::date AS _lockup_start_date
    
             , (CASE WHEN lockup_end_timestamp IS NOT NULL AND vesting_end_timestamp IS NULL
                     THEN lockup_end_timestamp
                     WHEN lockup_end_timestamp IS NULL AND vesting_end_timestamp IS NOT NULL
                     THEN vesting_end_timestamp
                     ELSE greatest(lockup_end_timestamp, vesting_end_timestamp) END)::date AS _lockup_end_date
    
        FROM lockup_contracts
    ),
    
    
    lockup_contracts_daily_balance__prep_1 AS (
        WITH dates AS ( SELECT DISTINCT dateadd(day, -seq4(), CURRENT_DATE) AS utc_date
                        FROM TABLE(GENERATOR(rowcount => 10000))
                        WHERE utc_date BETWEEN '2020-01-01' AND CURRENT_DATE )
    
        SELECT lc.lockup_account_id
             , lc.lockup_index
             , lc.owner_account_id
    
             , d.utc_date
             , d.utc_date + interval '1 day' - interval '1 nanosecond' AS block_timestamp  -- End of day block timestamp
    
             , lc.lockup_amount
             , lc.deposit_timestamp
    
            -- Lockup logic
             , lc.lockup_timestamp
             , lc.lockup_end_timestamp
             , greatest(0, timestampdiff(nanosecond, block_timestamp, lockup_end_timestamp)) AS lockup_time_left_ns
             , (CASE WHEN block_timestamp >= lockup_timestamp
                     THEN (CASE WHEN input_release_duration_ns > 0
                                THEN (CASE WHEN block_timestamp >= lockup_end_timestamp
                                           THEN 0  -- everything is released
                                           ELSE lockup_amount * lockup_time_left_ns / input_release_duration_ns
                                           END)
                                ELSE 0 END)
                     ELSE lockup_amount  -- The entire balance is still locked before the lockup timestamp
                     END) AS unreleased_amount
    
            -- Vesting logic
             , lc.vesting_start_timestamp
             , lc.vesting_cliff_timestamp
             , lc.vesting_end_timestamp
             , lc.terminate_vesting_timestamp
             , lc.termination_unvested_amount
             , greatest(0, timestampdiff(nanosecond, block_timestamp, vesting_end_timestamp)) AS vesting_time_left_ns
             , timestampdiff(nanosecond, vesting_start_timestamp, vesting_end_timestamp) AS vesting_total_time_ns
    
        FROM lockup_contracts__parsed AS lc, dates AS d
        WHERE d.utc_date BETWEEN lc._lockup_start_date
                             AND lc._lockup_end_date
    ),
    
    
    lockup_contracts_daily_balance__prep_2 AS (
        SELECT lc.*
             , sum(coalesce(dtw.withdrawn_amount, 0))
                    OVER (partition by lc.lockup_account_id, lc.lockup_index
                              order by lc.utc_date
                              rows between unbounded preceding
                                       and current row) AS termination_withdrawn_amount
        
        FROM lockup_contracts_daily_balance__prep_1 AS lc
            LEFT JOIN daily_termination_withdrawn_amount AS dtw
                ON dtw.lockup_account_id = lc.lockup_account_id
                AND dtw.utc_date = lc.utc_date
    
    ),
    
    
    lockup_contracts_daily_balance AS (
        SELECT lc.*
    
            -- Vesting logic
    
            -- Not 100% accurate due to private vesting lockups (unknown/hidden vesting parameters)
             , (CASE WHEN block_timestamp >= terminate_vesting_timestamp
                     THEN termination_unvested_amount - termination_withdrawn_amount
                     ELSE (CASE WHEN block_timestamp < vesting_cliff_timestamp
                                THEN lockup_amount  -- Before the cliff, nothing is vested
                                WHEN block_timestamp >= vesting_end_timestamp
                                THEN 0  -- After the end, everything is vested
                                ELSE lockup_amount * vesting_time_left_ns / vesting_total_time_ns
                                END)
                     END) AS unvested_amount
    
            -- Combined logic
             , greatest(unreleased_amount - termination_withdrawn_amount, coalesce(unvested_amount, 0)) AS locked_amount
    
             , locked_amount - coalesce(lag(locked_amount) OVER (partition by lc.lockup_account_id, lc.lockup_index order by lc.utc_date), 0) AS unlocked_amount_today
    
        FROM lockup_contracts_daily_balance__prep_2 AS lc
    )

    SELECT * FROM lockup_contracts_daily_balance
),


daily_lockup_staking_balances AS (
    WITH

    lockup_staking_logs AS (
        SELECT lr.tx_hash
             , lr.block_timestamp
             , value:FunctionCall:method_name::string AS method_name
             , lr.receiver_id AS lockup_account_id
             , (CASE method_name
                    WHEN 'stake'
                    THEN split(split(lr.logs[0], ' at the staking pool')[0], 'Staking ')[1]::bigint / 1e24
                    
                    WHEN 'deposit_and_stake'
                    THEN split(split(lr.logs[0], ' to the staking pool')[0], 'Depositing and staking ')[1]::bigint / 1e24
    
                    WHEN 'unstake'
                    THEN split(split(lr.logs[0], ' from the staking pool')[0], 'Unstaking ')[1]::bigint / 1e24
    
                    END) AS amount
    
             , lr.logs
    
        FROM lockup_receipts AS lr,
            LATERAL FLATTEN( input => lr.actions:receipt:Action:actions )
        WHERE method_name IN ('stake','deposit_and_stake',
                              'unstake','unstake_all')
    ),
    
    
    daily_staking_stats AS (
        SELECT lockup_account_id
             , block_timestamp::date AS utc_date
             , sum(CASE WHEN method_name IN ('stake','deposit_and_stake') THEN amount ELSE 0 END) AS staked_amount_
             , sum(CASE WHEN method_name IN ('unstake') THEN amount ELSE 0 END) AS unstaked_amount_
             , (CASE WHEN count(CASE WHEN method_name = 'unstake_all' THEN tx_hash ELSE NULL END) > 0
                     THEN True ELSE False END) AS unstaked_all
        FROM lockup_staking_logs
        GROUP BY 1,2
    ),
    
    
    lockup_stakers AS (
        SELECT lockup_account_id
             , min(block_timestamp)::date AS start_date
        FROM lockup_staking_logs
        GROUP BY 1
    ),
    
    
    lockup_stakers_daily_balances__prep_1 AS (
        WITH dates AS ( SELECT DISTINCT dateadd(day, -seq4(), CURRENT_DATE) AS utc_date
                        FROM TABLE(GENERATOR(rowcount => 10000))
                        WHERE utc_date BETWEEN '2020-09-01' AND CURRENT_DATE )
    
        SELECT ls.lockup_account_id
             , d.utc_date
    
        FROM lockup_stakers AS ls, dates AS d
        WHERE d.utc_date >= ls.start_date
    ),
    
    
    lockup_stakers_daily_balances__prep_2 AS (
        SELECT d.lockup_account_id
             , d.utc_date
             , coalesce(dss.staked_amount_, 0) AS staked_amount
             , coalesce(dss.unstaked_amount_, 0) AS unstaked_amount
             , dss.unstaked_all
             , sum(CASE WHEN dss.unstaked_all = True THEN 1 ELSE 0 END)
                  OVER (partition by d.lockup_account_id
                        order by d.utc_date
                        rows between unbounded preceding
                                 and current row) AS _unstake_counter
    
        FROM lockup_stakers_daily_balances__prep_1 AS d
            LEFT JOIN daily_staking_stats AS dss
                ON dss.lockup_account_id = d.lockup_account_id
                AND dss.utc_date = d.utc_date
    ),
    
    
    lockup_stakers_daily_balances__prep_3 AS (
        SELECT *
             , coalesce(lag(_unstake_counter) 
                  OVER (partition by lockup_account_id
                            order by utc_date)
               , 0) AS staking_period_index
        FROM lockup_stakers_daily_balances__prep_2
    ),
    
    
    lockup_stakers_daily_balances__prep_4 AS (
        SELECT *
             , sum(staked_amount - unstaked_amount) 
                  OVER (partition by lockup_account_id, staking_period_index
                            order by utc_date
                            rows between unbounded preceding
                                     and current row) AS _cumulative_staked_amount
    
             , (CASE WHEN unstaked_all = True THEN 0 ELSE _cumulative_staked_amount END) AS staked_balance
        FROM lockup_stakers_daily_balances__prep_3
    ),
    
    
    lockup_stakers_daily_balances AS (
        SELECT lockup_account_id
             , utc_date
             , staked_balance
        FROM lockup_stakers_daily_balances__prep_4
    )
    
    
    SELECT * FROM lockup_stakers_daily_balances
),


daily_lockup_locked_and_staking_balances AS (
    SELECT l.lockup_account_id
         , l.utc_date
         , l.locked_amount
         , coalesce(s.staked_balance, 0) AS staked_amount
         , least(staked_amount, locked_amount) AS locked_and_staked_amount

    FROM daily_lockup_locked_balances AS l
        LEFT JOIN daily_lockup_staking_balances AS s
            ON s.lockup_account_id = l.lockup_account_id
            AND s.utc_date = l.utc_date
),


daily_locked_and_staked_supply AS (
    SELECT utc_date
         , sum(locked_amount) AS total_locked_supply
         , sum(locked_and_staked_amount) AS locked_and_staked_supply

    FROM daily_lockup_locked_and_staking_balances
    GROUP BY 1
),


daily_staked_supply AS (
    WITH
    
    dim_epochs AS (
     	SELECT epoch_id
      		 , min(block_id) AS min_block_id
      		 , max(block_id) AS max_block_id
      		 , count(*) AS blocks
      		 , count(distinct block_author) AS block_producers
      		 , min(block_timestamp) AS start_time
      		 , max(block_timestamp) AS end_time
      		 , max(total_supply) / 1e24 AS total_near_supply
      		 , row_number() OVER (order by min_block_id asc) - 1 + 900 AS epoch_num
      
      	FROM near.core.fact_blocks AS b
      	GROUP BY 1
    ),
    
    
    staking_actions AS (
      	SELECT r.tx_hash
          	 , r.block_timestamp
          	 , r.receiver_id AS validator_address
          	 , replace(split(l.value::string, ': Contract received total')[0], 'Epoch ', '')::integer AS epoch_num
          	 , split(split(l.value::string, 'New total staked balance is ')[1], '. Total number of shares')[0]::bigint / 1e24 AS staked_balance
          
        FROM near.core.fact_receipts AS r
      	   , lateral flatten( input => r.logs ) AS l
      
        WHERE ( right(receiver_id, 12) = '.poolv1.near' OR right(receiver_id, 10) = '.pool.near' )
          AND r.tx_hash IN ( SELECT tx_hash 
          				     FROM near.core.fact_actions_events_function_call
          				     WHERE method_name IN ('ping','stake','unstake','stake_all','unstake_all','deposit_and_stake') )
          AND left(l.value::string, 6) = 'Epoch '
    
      	QUALIFY row_number() OVER (partition by epoch_num, validator_address order by block_timestamp desc) = 1
    ),
    
    
    proposals AS (
      	SELECT b.block_id
          	 , b.block_timestamp
          	 , b.epoch_id
          	 , vp.value['account_id'] AS validator_address
          	 , vp.value['stake']::bigint / 1e24 AS staked_balance
        FROM near.core.fact_blocks AS b
           , lateral flatten( input => b.chunks ) AS c
           , lateral flatten( input => c.value['validator_proposals']) AS vp
      	-- WHERE b.block_timestamp >= '2021-09-01'
        QUALIFY row_number() OVER (partition by validator_address, epoch_id order by block_timestamp desc) = 1
    ),
    
    
    proposals_per_epoch AS (
      	SELECT p.block_timestamp
      		 , p.epoch_id
      		 , p.validator_address
      		 , p.staked_balance
      		 , e.epoch_num
      
      	FROM proposals AS p
      		INNER JOIN dim_epochs AS e
      			ON e.epoch_id = p.epoch_id
    
      	QUALIFY row_number() OVER (partition by epoch_num, validator_address order by block_timestamp desc) = 1
    ),
    
    
    block_producers_per_epoch AS (
      	SELECT b.epoch_id
      		 , e.epoch_num
      		 , b.block_author AS validator_address
      		 , sa.staked_balance
      		 , count(distinct b.block_id) OVER (partition by b.epoch_id, b.block_author) AS blocks_produced
      
      	FROM near.core.fact_blocks AS b
      		INNER JOIN dim_epochs AS e
      			ON e.epoch_id = b.epoch_id
    
      		LEFT JOIN staking_actions AS sa
      			ON sa.epoch_num = e.epoch_num
      			AND sa.validator_address = b.block_author
    
      	QUALIFY row_number() OVER (partition by b.epoch_id, b.block_author order by b.block_timestamp desc) = 1
    ),
    
    
    dim_validators AS (
      	SELECT validator_address
      		 , min(start_epoch) AS start_epoch
      		 , min(start_time) AS start_time
      
      	FROM (
    		SELECT validator_address
        		 , min(epoch_num) AS start_epoch
        		 , min(block_timestamp) AS start_time
        	FROM staking_actions AS sa
        	GROUP BY 1
      
        	UNION ALL
      
        	SELECT block_author AS validator_address
        		 , min(e.epoch_num) AS start_epoch
        		 , min(b.block_timestamp) AS start_time
        	FROM near.core.fact_blocks AS b
        		LEFT JOIN dim_epochs AS e
        			ON b.block_id BETWEEN e.min_block_id AND e.max_block_id
        	GROUP BY 1
        ) AS x
      
      	GROUP BY 1
    ),
    
    
    dim_table AS (
      	SELECT v.validator_address
      		 , e.epoch_num
      		 , e.start_time
      		 , e.total_near_supply
      
      	FROM dim_validators AS v, dim_epochs AS e
      	WHERE v.start_epoch <= e.epoch_num
    ),
    
    
    validator_status_per_epoch AS (
      	SELECT dt.epoch_num
      		 , dt.start_time
      		 , dt.validator_address
      		 , coalesce(
      				last_value(coalesce(bp.staked_balance, p.staked_balance)) IGNORE NULLS
      					OVER (partition by dt.validator_address 
      					  	  order by dt.epoch_num
      					  	  rows between unbounded preceding
      								   and current row),
      				0) AS staked_balance
      		 , bp.blocks_produced
      		 , (CASE WHEN p.validator_address IS NOT NULL THEN True ELSE False END) AS is_proposer
      
      	FROM dim_table AS dt
      		LEFT JOIN block_producers_per_epoch AS bp
      			ON bp.epoch_num = dt.epoch_num
      			AND bp.validator_address= dt.validator_address
    
      		LEFT JOIN proposals_per_epoch AS p
      			ON p.epoch_num = dt.epoch_num
      			AND p.validator_address= dt.validator_address
    
    ),
    
    
    epoch_stats AS (
      	SELECT epoch_num
          	 , start_time
      		 , sum(staked_balance) AS total_near_staked
          
        FROM validator_status_per_epoch
        WHERE staked_balance > 0
        GROUP BY 1,2
    ),
    
    
    epoch_stats_2 AS (
      	SELECT es.*
      		 , de.total_near_supply
      		 , de.total_near_supply - es.total_near_staked AS other_near_supply
      	 	 , 100.00 * total_near_staked / total_near_supply AS perc_staked_supply
      	FROM epoch_stats AS es
      		LEFT JOIN dim_epochs AS de
      			ON de.epoch_num = es.epoch_num
    )
    
    
    SELECT start_time::date AS utc_date
         , total_near_staked AS total_staked_supply
         , total_near_supply AS total_supply
    FROM epoch_stats_2
    QUALIFY row_number() OVER (partition by utc_date order by start_time desc) = 1
),


daily_supply_stats AS (
    SELECT s.utc_date
         , s.total_supply
         , s.total_staked_supply
         , s.total_supply - s.total_staked_supply AS total_nonstaked_supply
         , ls.total_locked_supply
         , ls.locked_and_staked_supply
         , greatest(0, total_staked_supply - locked_and_staked_supply) AS nonlocked_and_staked_supply
         , greatest(0, total_locked_supply - locked_and_staked_supply) AS locked_and_nonstaked_supply
         , total_supply
            - locked_and_staked_supply
            - locked_and_nonstaked_supply
            - nonlocked_and_staked_supply AS nonlocked_and_nonstaked_supply

         , total_supply - total_locked_supply AS circulating_supply
         , total_locked_supply AS locked_supply

    FROM daily_staked_supply AS s
        LEFT JOIN daily_locked_and_staked_supply AS ls
            ON ls.utc_date = s.utc_date
),


output AS (
    SELECT utc_date
         , utc_date AS "Date"
         , total_supply AS "Total Supply - Actual"
         , total_staked_supply AS "Staked Supply"
         , total_nonstaked_supply AS "Non-staked Supply"
         , circulating_supply AS "Circulating Supply"
         , total_supply - circulating_supply as "Total Supply"
         , total_locked_supply AS "Locked Supply"
         , nonlocked_and_nonstaked_supply AS "Liquid Supply"
         , total_supply - nonlocked_and_nonstaked_supply  AS "Non-liquid Supply"

         , locked_and_staked_supply AS "Staked (Locked Supply)"
         , locked_and_nonstaked_supply AS "Non-staked (Locked Supply)"

         , nonlocked_and_staked_supply AS "Staked (Circulating Supply)"
         , nonlocked_and_nonstaked_supply AS "Non-staked (Circulating Supply)"

         , total_locked_supply / total_supply AS perc_locked_supply
         , circulating_supply / total_supply AS perc_circulating_supply

         , locked_and_staked_supply / total_locked_supply AS perc_staked__locked
         , nonlocked_and_staked_supply / circulating_supply AS perc_staked__circulating

         , 1 AS dummy

    FROM daily_supply_stats
)


SELECT *
FROM output
WHERE utc_date >= '2023-01-01'
ORDER BY utc_date DESC 
```
